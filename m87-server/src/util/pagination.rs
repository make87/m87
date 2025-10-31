use axum::extract::{FromRequestParts, Query};
use axum::http::{request::Parts, StatusCode};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
struct PaginationParams {
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct RequestPagination {
    pub offset: u64,
    pub limit: u32,
}

impl<S> FromRequestParts<S> for RequestPagination
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(params) = Query::<PaginationParams>::from_request_parts(parts, state)
            .await
            .unwrap_or_else(|_| Query(PaginationParams::default()));

        Ok(Self {
            offset: params.offset.unwrap_or(0),
            limit: params.limit.unwrap_or(50),
        })
    }
}

impl RequestPagination {
    pub fn max_limit() -> Self {
        Self {
            offset: 0,
            limit: 100,
        }
    }
}
