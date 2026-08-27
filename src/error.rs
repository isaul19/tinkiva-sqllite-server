use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("invalid database name: use 1-64 ASCII letters, numbers, '-' or '_'")]
    InvalidDatabaseName,
    #[error("all open databases are currently busy")]
    CapacityBusy,
    #[error("unauthorized")]
    Unauthorized,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("internal server error")]
    Internal(#[source] anyhow::Error),
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}
#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            Self::InvalidDatabaseName | Self::InvalidRequest(_) => {
                (StatusCode::BAD_REQUEST, "invalid_request")
            }
            Self::CapacityBusy => (StatusCode::SERVICE_UNAVAILABLE, "capacity_busy"),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::Database(sqlx::Error::RowNotFound) => (StatusCode::NOT_FOUND, "not_found"),
            Self::Database(_) => (StatusCode::UNPROCESSABLE_ENTITY, "database_error"),
            Self::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        };
        (
            status,
            Json(ErrorBody {
                error: ErrorDetail {
                    code,
                    message: self.to_string(),
                },
            }),
        )
            .into_response()
    }
}
