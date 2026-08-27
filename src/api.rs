use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, MatchedPath, Path, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use futures_util::TryStreamExt;
use serde::{
    Deserialize, Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq, SerializeStruct},
};
use serde_json::Value;
use sqlx::{
    Column, Either, Executor, Row, Sqlite, TypeInfo, ValueRef,
    sqlite::{SqliteArguments, SqliteRow},
};
use subtle::ConstantTimeEq;
use tower_http::{
    catch_panic::CatchPanicLayer,
    compression::CompressionLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

use crate::{config::Settings, db::DatabaseManager, error::AppError};

#[derive(Clone)]
pub struct AppState {
    settings: Arc<Settings>,
    manager: Arc<DatabaseManager>,
}

impl AppState {
    pub fn new(settings: Arc<Settings>, manager: Arc<DatabaseManager>) -> Self {
        Self { settings, manager }
    }
}

pub fn router(state: AppState) -> Router {
    let request_id_header = header::HeaderName::from_static("x-request-id");
    let api = Router::new()
        .route("/db/{database}/query", post(query))
        .route("/db/{database}/execute", post(execute))
        .route("/db/{database}/batch", post(batch))
        .route("/admin/stats", get(stats))
        .route("/admin/metrics", get(metrics))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize))
        .route_layer(middleware::from_fn_with_state(state.clone(), observe));

    Router::new()
        .route("/health", get(health))
        .nest("/v1", api)
        .with_state(state.clone())
        .layer(DefaultBodyLimit::max(
            state.settings.server.body_limit_bytes,
        ))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(state.settings.server.request_timeout_seconds),
        ))
        // Result sets are highly repetitive JSON; over a network this is
        // the cheapest bandwidth win available. It is a no-op for clients
        // that do not advertise an encoding.
        .layer(CompressionLayer::new())
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(SetRequestIdLayer::new(request_id_header, MakeRequestUuid))
}

async fn health() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(serde_json::json!({ "status": "ok" })))
}

async fn authorize(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, AppError> {
    let Some(expected) = state.settings.server.auth_token.as_deref() else {
        return Ok(next.run(request).await);
    };
    let provided = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if provided.len() != expected.len()
        || !bool::from(provided.as_bytes().ct_eq(expected.as_bytes()))
    {
        return Err(AppError::Unauthorized);
    }
    Ok(next.run(request).await)
}

#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<Value>,
    #[serde(default)]
    pub format: RowFormat,
}

/// `arrays` drops the repeated column names from every row. On a wide result
/// set that is most of the payload.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RowFormat {
    #[default]
    Objects,
    Arrays,
}

#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    pub statements: Vec<SqlRequest>,
}

/// Rows are always held as positional values. The object shape is produced
/// during serialization by borrowing the column names, so a result set costs
/// one allocation per cell instead of one per cell plus one key per cell.
#[derive(Debug)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub format: RowFormat,
    pub truncated: bool,
}

impl QueryResponse {
    fn empty(format: RowFormat) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            format,
            truncated: false,
        }
    }

    /// Emits the row fields into an in-progress struct, so a batch entry can
    /// carry them alongside its effect without a second serializer.
    fn write_rows<S: SerializeStruct>(&self, target: &mut S) -> Result<(), S::Error> {
        target.serialize_field("columns", &self.columns)?;
        match self.format {
            RowFormat::Arrays => target.serialize_field("rows", &self.rows)?,
            RowFormat::Objects => target.serialize_field(
                "rows",
                &RowsAsObjects {
                    columns: &self.columns,
                    rows: &self.rows,
                },
            )?,
        }
        target.serialize_field("truncated", &self.truncated)
    }
}

impl Serialize for QueryResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut response = serializer.serialize_struct("QueryResponse", 3)?;
        self.write_rows(&mut response)?;
        response.end()
    }
}

struct RowsAsObjects<'a> {
    columns: &'a [String],
    rows: &'a [Vec<Value>],
}

impl Serialize for RowsAsObjects<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.rows.len()))?;
        for row in self.rows {
            sequence.serialize_element(&RowAsObject {
                columns: self.columns,
                values: row,
            })?;
        }
        sequence.end()
    }
}

struct RowAsObject<'a> {
    columns: &'a [String],
    values: &'a [Value],
}

impl Serialize for RowAsObject<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.values.len()))?;
        for (column, value) in self.columns.iter().zip(self.values) {
            map.serialize_entry(column.as_str(), value)?;
        }
        map.end()
    }
}

#[derive(Debug, Serialize)]
pub struct ExecuteResult {
    pub rows_affected: u64,
    pub last_insert_rowid: i64,
}

#[derive(Debug, Serialize)]
pub struct BatchResponse {
    pub results: Vec<BatchStatementResult>,
}

/// A statement's effect and whatever it returned. Batching only saves a round
/// trip if reads can travel in it too, so every statement reports rows.
#[derive(Debug)]
pub struct BatchStatementResult {
    pub rows_affected: u64,
    pub last_insert_rowid: i64,
    pub rows: QueryResponse,
}

impl Serialize for BatchStatementResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut result = serializer.serialize_struct("BatchStatementResult", 5)?;
        result.serialize_field("rows_affected", &self.rows_affected)?;
        result.serialize_field("last_insert_rowid", &self.last_insert_rowid)?;
        self.rows.write_rows(&mut result)?;
        result.end()
    }
}

/// Reads run on the reader pool, which is opened `query_only`: a write sent
/// here is rejected by SQLite and belongs on `/execute` or `/batch`.
async fn query(
    State(state): State<AppState>,
    Path(database): Path<String>,
    Json(request): Json<SqlRequest>,
) -> Result<Json<QueryResponse>, AppError> {
    validate_sql(&request.sql)?;
    let format = request.format;
    let lease = state.manager.acquire(&database).await?;
    let mut stream = bind_query(&request.sql, request.params)?.fetch(lease.readers());
    let limit = state.manager.max_result_rows();
    let mut rows = Vec::with_capacity(limit.min(256));
    let mut columns = Vec::new();
    let mut truncated = false;
    while let Some(row) = stream.try_next().await? {
        // Column names are read once per result set, not once per row.
        if columns.is_empty() {
            columns = row
                .columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect();
        }
        if rows.len() == limit {
            truncated = true;
            break;
        }
        rows.push(row_values(&row)?);
    }
    Ok(Json(QueryResponse {
        columns,
        rows,
        format,
        truncated,
    }))
}

async fn execute(
    State(state): State<AppState>,
    Path(database): Path<String>,
    Json(request): Json<SqlRequest>,
) -> Result<Json<ExecuteResult>, AppError> {
    validate_sql(&request.sql)?;
    let lease = state.manager.acquire(&database).await?;
    let result = bind_query(&request.sql, request.params)?
        .execute(lease.writer())
        .await?;
    Ok(Json(ExecuteResult {
        rows_affected: result.rows_affected(),
        last_insert_rowid: result.last_insert_rowid(),
    }))
}

/// Runs every statement in one transaction and returns what each produced,
/// so a client can spend one round trip on a read-modify-read sequence.
async fn batch(
    State(state): State<AppState>,
    Path(database): Path<String>,
    Json(request): Json<BatchRequest>,
) -> Result<Json<BatchResponse>, AppError> {
    if request.statements.is_empty() {
        return Err(AppError::InvalidRequest(
            "statements must not be empty".into(),
        ));
    }
    let limit = state.manager.max_result_rows();
    let lease = state.manager.acquire(&database).await?;
    let mut transaction = lease.writer().begin().await?;
    let mut results = Vec::with_capacity(request.statements.len());
    for statement in request.statements {
        validate_sql(&statement.sql)?;
        let mut result = BatchStatementResult {
            rows_affected: 0,
            last_insert_rowid: 0,
            rows: QueryResponse::empty(statement.format),
        };
        // Executor::fetch_many is the only path that reports both the rows a
        // statement returned and the effect it had.
        let mut stream =
            (&mut *transaction).fetch_many(bind_query(&statement.sql, statement.params)?);
        while let Some(item) = stream.try_next().await? {
            match item {
                Either::Left(effect) => {
                    result.rows_affected = effect.rows_affected();
                    result.last_insert_rowid = effect.last_insert_rowid();
                }
                Either::Right(row) => {
                    if result.rows.columns.is_empty() {
                        result.rows.columns = row
                            .columns()
                            .iter()
                            .map(|column| column.name().to_owned())
                            .collect();
                    }
                    if result.rows.rows.len() == limit {
                        result.rows.truncated = true;
                        break;
                    }
                    result.rows.rows.push(row_values(&row)?);
                }
            }
        }
        drop(stream);
        results.push(result);
    }
    transaction.commit().await?;
    Ok(Json(BatchResponse { results }))
}

async fn stats(State(state): State<AppState>) -> Json<crate::db::ManagerStats> {
    Json(state.manager.stats().await)
}

async fn metrics(
    State(state): State<AppState>,
) -> ([(header::HeaderName, &'static str); 1], String) {
    let stats = state.manager.stats().await;
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.manager.metrics().render(&stats),
    )
}

/// Times requests by their matched path, so the label set is the route table
/// rather than the tenant list.
async fn observe(State(state): State<AppState>, request: Request<Body>, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_owned());
    let started = Instant::now();
    let response = next.run(request).await;
    if let Some(route) = route {
        state.manager.metrics().record_request(
            &route,
            started.elapsed(),
            response.status().is_success(),
        );
    }
    response
}

fn validate_sql(sql: &str) -> Result<(), AppError> {
    if sql.trim().is_empty() {
        Err(AppError::InvalidRequest("sql must not be empty".into()))
    } else {
        Ok(())
    }
}

fn bind_query<'q>(
    sql: &'q str,
    params: Vec<Value>,
) -> Result<sqlx::query::Query<'q, Sqlite, SqliteArguments<'q>>, AppError> {
    let mut query = sqlx::query(sql);
    for value in params {
        query = match value {
            Value::Null => query.bind(Option::<String>::None),
            Value::Bool(value) => query.bind(value),
            Value::Number(value) if value.is_i64() => {
                query.bind(value.as_i64().expect("checked i64"))
            }
            Value::Number(value) if value.is_u64() => {
                let number = i64::try_from(value.as_u64().expect("checked u64")).map_err(|_| {
                    AppError::InvalidRequest(
                        "unsigned parameter is larger than SQLite INTEGER".into(),
                    )
                })?;
                query.bind(number)
            }
            Value::Number(value) => query.bind(value.as_f64().expect("JSON number")),
            Value::String(value) => query.bind(value),
            value @ (Value::Array(_) | Value::Object(_)) => query.bind(value.to_string()),
        };
    }
    Ok(query)
}

fn row_values(row: &SqliteRow) -> Result<Vec<Value>, sqlx::Error> {
    let mut values = Vec::with_capacity(row.columns().len());
    for index in 0..row.columns().len() {
        let raw = row.try_get_raw(index)?;
        let value = if raw.is_null() {
            Value::Null
        } else {
            match raw.type_info().name() {
                "INTEGER" => Value::from(row.try_get::<i64, _>(index)?),
                "REAL" => Value::from(row.try_get::<f64, _>(index)?),
                "BLOB" => {
                    serde_json::json!({ "$blob": BASE64.encode(row.try_get::<Vec<u8>, _>(index)?) })
                }
                _ => Value::from(row.try_get::<String, _>(index)?),
            }
        };
        values.push(value);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, http::Request};
    use tower::ServiceExt;

    async fn test_app(token: Option<&str>) -> (Router, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.database.directory = dir.path().into();
        settings.server.auth_token = token.map(str::to_owned);
        let settings = Arc::new(settings);
        let manager = Arc::new(
            DatabaseManager::new(settings.database.clone())
                .await
                .unwrap(),
        );
        (router(AppState::new(settings, manager)), dir)
    }

    #[tokio::test]
    async fn executes_and_queries_tenant_database() {
        let (app, _dir) = test_app(None).await;
        let create = Request::post("/v1/db/acme/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create).await.unwrap().status(),
            StatusCode::OK
        );
        let insert = Request::post("/v1/db/acme/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"INSERT INTO items(name) VALUES (?)","params":["book"]}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(insert).await.unwrap().status(),
            StatusCode::OK
        );
        let select = Request::post("/v1/db/acme/query")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"sql":"SELECT id, name FROM items"}"#))
            .unwrap();
        let response = app.oneshot(select).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["rows"][0]["name"], "book");
    }

    #[tokio::test]
    async fn reports_metrics_labelled_by_route_not_tenant() {
        let (app, _dir) = test_app(None).await;
        for tenant in ["alpha", "beta"] {
            let request = Request::post(format!("/v1/db/{tenant}/execute"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"sql":"CREATE TABLE t (id INTEGER)"}"#))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(request).await.unwrap().status(),
                StatusCode::OK
            );
        }
        let scrape = Request::get("/v1/admin/metrics")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(scrape).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains(
            "tinkiva_requests_total{route=\"/v1/db/{database}/execute\",outcome=\"ok\"} 2"
        ));
        assert!(body.contains("tinkiva_databases_opened_total 2"));
        assert!(body.contains("tinkiva_open_databases 2"));
        assert!(
            !body.contains("alpha"),
            "tenant names must not become labels"
        );
    }

    #[tokio::test]
    async fn protects_api_but_not_health() {
        let (app, _dir) = test_app(Some("secret")).await;
        let health = Request::get("/health").body(Body::empty()).unwrap();
        assert_eq!(
            app.clone().oneshot(health).await.unwrap().status(),
            StatusCode::OK
        );
        let denied = Request::get("/v1/admin/stats").body(Body::empty()).unwrap();
        assert_eq!(
            app.clone().oneshot(denied).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let allowed = Request::get("/v1/admin/stats")
            .header("authorization", "Bearer secret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(app.oneshot(allowed).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn batch_returns_rows_for_each_statement() {
        let (app, _dir) = test_app(None).await;
        let batch = Request::post("/v1/db/acme/batch")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"statements":[
                    {"sql":"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"},
                    {"sql":"INSERT INTO items(name) VALUES (?)","params":["book"]},
                    {"sql":"SELECT id, name FROM items"}
                ]}"#,
            ))
            .unwrap();
        let response = app.oneshot(batch).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[1]["rows_affected"], 1);
        assert_eq!(results[1]["last_insert_rowid"], 1);
        assert!(results[1]["rows"].as_array().unwrap().is_empty());
        assert_eq!(results[2]["columns"], serde_json::json!(["id", "name"]));
        assert_eq!(results[2]["rows"][0]["name"], "book");
    }

    #[tokio::test]
    async fn rolls_back_a_failed_batch() {
        let (app, _dir) = test_app(None).await;
        let create = Request::post("/v1/db/acme/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER CHECK(amount > 0))"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create).await.unwrap().status(),
            StatusCode::OK
        );

        let batch = Request::post("/v1/db/acme/batch")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"statements":[{"sql":"INSERT INTO ledger(amount) VALUES (?)","params":[10]},{"sql":"INSERT INTO ledger(amount) VALUES (?)","params":[-1]}]}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(batch).await.unwrap().status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );

        let count = Request::post("/v1/db/acme/query")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"SELECT count(*) AS total FROM ledger"}"#,
            ))
            .unwrap();
        let response = app.oneshot(count).await.unwrap();
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["rows"][0]["total"], 0);
    }

    #[tokio::test]
    async fn serves_rows_as_objects_or_arrays() {
        let (app, _dir) = test_app(None).await;
        for sql in [
            r#"{"sql":"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}"#,
            r#"{"sql":"INSERT INTO items(name) VALUES (?)","params":["book"]}"#,
        ] {
            let request = Request::post("/v1/db/acme/execute")
                .header("content-type", "application/json")
                .body(Body::from(sql))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(request).await.unwrap().status(),
                StatusCode::OK
            );
        }

        let objects = Request::post("/v1/db/acme/query")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"sql":"SELECT id, name FROM items"}"#))
            .unwrap();
        let response = app.clone().oneshot(objects).await.unwrap();
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["columns"], serde_json::json!(["id", "name"]));
        assert_eq!(body["rows"][0]["name"], "book");

        let arrays = Request::post("/v1/db/acme/query")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"SELECT id, name FROM items","format":"arrays"}"#,
            ))
            .unwrap();
        let response = app.oneshot(arrays).await.unwrap();
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["columns"], serde_json::json!(["id", "name"]));
        assert_eq!(body["rows"], serde_json::json!([[1, "book"]]));
    }

    #[tokio::test]
    async fn query_endpoint_refuses_writes() {
        let (app, _dir) = test_app(None).await;
        let create = Request::post("/v1/db/acme/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create).await.unwrap().status(),
            StatusCode::OK
        );
        let write_through_query = Request::post("/v1/db/acme/query")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"INSERT INTO items(name) VALUES ('smuggled')"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(write_through_query)
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let count = Request::post("/v1/db/acme/query")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"SELECT count(*) AS total FROM items"}"#,
            ))
            .unwrap();
        let response = app.oneshot(count).await.unwrap();
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["rows"][0]["total"], 0);
    }

    #[tokio::test]
    async fn keeps_tenant_files_isolated() {
        let (app, dir) = test_app(None).await;
        for tenant in ["alpha", "beta"] {
            let request = Request::post(format!("/v1/db/{tenant}/execute"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sql":"CREATE TABLE identity (tenant TEXT)"}"#,
                ))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(request).await.unwrap().status(),
                StatusCode::OK
            );
        }
        assert!(dir.path().join("alpha.db").exists());
        assert!(dir.path().join("beta.db").exists());
    }
}
