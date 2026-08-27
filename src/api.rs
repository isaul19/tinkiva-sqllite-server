use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    Column, Row, Sqlite, TypeInfo, ValueRef,
    sqlite::{SqliteArguments, SqliteRow},
};
use subtle::ConstantTimeEq;
use tower_http::{
    catch_panic::CatchPanicLayer,
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
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize));

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
}

#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    pub statements: Vec<SqlRequest>,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<BTreeMap<String, Value>>,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct ExecuteResult {
    pub rows_affected: u64,
    pub last_insert_rowid: i64,
}

#[derive(Debug, Serialize)]
pub struct BatchResponse {
    pub results: Vec<ExecuteResult>,
}

async fn query(
    State(state): State<AppState>,
    Path(database): Path<String>,
    Json(request): Json<SqlRequest>,
) -> Result<Json<QueryResponse>, AppError> {
    validate_sql(&request.sql)?;
    let lease = state.manager.acquire(&database).await?;
    let mut stream = bind_query(&request.sql, request.params)?.fetch(&*lease);
    let limit = state.manager.max_result_rows();
    let mut rows = Vec::with_capacity(limit.min(256));
    let mut columns = Vec::new();
    let mut truncated = false;
    while let Some(row) = stream.try_next().await? {
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
        rows.push(row_to_json(&row)?);
    }
    Ok(Json(QueryResponse {
        columns,
        rows,
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
        .execute(&*lease)
        .await?;
    Ok(Json(ExecuteResult {
        rows_affected: result.rows_affected(),
        last_insert_rowid: result.last_insert_rowid(),
    }))
}

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
    let lease = state.manager.acquire(&database).await?;
    let mut transaction = lease.begin().await?;
    let mut results = Vec::with_capacity(request.statements.len());
    for statement in request.statements {
        validate_sql(&statement.sql)?;
        let result = bind_query(&statement.sql, statement.params)?
            .execute(&mut *transaction)
            .await?;
        results.push(ExecuteResult {
            rows_affected: result.rows_affected(),
            last_insert_rowid: result.last_insert_rowid(),
        });
    }
    transaction.commit().await?;
    Ok(Json(BatchResponse { results }))
}

async fn stats(State(state): State<AppState>) -> Json<crate::db::ManagerStats> {
    Json(state.manager.stats().await)
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

fn row_to_json(row: &SqliteRow) -> Result<BTreeMap<String, Value>, sqlx::Error> {
    let mut result = BTreeMap::new();
    for (index, column) in row.columns().iter().enumerate() {
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
        result.insert(column.name().to_owned(), value);
    }
    Ok(result)
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
}
