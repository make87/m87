use axum::{
    http::header::CONTENT_TYPE,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, IntoResponseParts, Response, ResponseParts},
};
use hex::FromHexError;
use hmac::digest::MacError;
use quinn::{ConnectionError, ReadError};
use serde::Serialize;
use std::{fmt::Display, num::ParseIntError, string::FromUtf8Error};
use tracing::error;

#[derive(Debug)]
pub struct ServerResponse<T: Serialize> {
    pub body: Option<T>,
    pub headers: HeaderMap,
    pub status_code: StatusCode,
    pub pagination: Option<ResponsePagination>,
}

#[derive(Debug)]
pub struct ResponsePagination {
    pub count: u64,
    pub offset: u64,
    pub limit: u32,
}

impl IntoResponseParts for ResponsePagination {
    type Error = (StatusCode, String);

    fn into_response_parts(self, mut res: ResponseParts) -> Result<ResponseParts, Self::Error> {
        res.headers_mut()
            .insert("x-pagination-count", self.count.into());

        res.headers_mut()
            .insert("x-pagination-offset", self.offset.into());

        res.headers_mut()
            .insert("x-pagination-limit", self.limit.into());

        Ok(res)
    }
}

#[derive(Debug)]
pub struct ServerResponseBuilder<T: Serialize> {
    pub body: Option<T>,
    pub headers: Option<HeaderMap>,
    pub status_code: Option<StatusCode>,
    pub pagination: Option<ResponsePagination>,
}

impl<T> ServerResponseBuilder<T>
where
    T: Serialize,
{
    pub fn body(mut self, body: T) -> Self {
        self.body = Some(body);
        self
    }

    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn status_code(mut self, status_code: StatusCode) -> Self {
        self.status_code = Some(status_code);
        self
    }

    pub fn ok(mut self) -> Self {
        self.status_code = Some(StatusCode::OK);
        self
    }

    pub fn switching_protocols(mut self) -> Self {
        self.status_code = Some(StatusCode::SWITCHING_PROTOCOLS);
        self
    }

    pub fn created(mut self) -> Self {
        self.status_code = Some(StatusCode::CREATED);
        self
    }

    pub fn accepted(mut self) -> Self {
        self.status_code = Some(StatusCode::ACCEPTED);
        self
    }

    pub fn no_content(mut self) -> Self {
        self.status_code = Some(StatusCode::NO_CONTENT);
        self
    }

    pub fn bad_request(mut self) -> Self {
        self.status_code = Some(StatusCode::BAD_REQUEST);
        self
    }

    pub fn unauthorized(mut self) -> Self {
        self.status_code = Some(StatusCode::UNAUTHORIZED);
        self
    }

    pub fn forbidden(mut self) -> Self {
        self.status_code = Some(StatusCode::FORBIDDEN);
        self
    }

    pub fn not_found(mut self) -> Self {
        self.status_code = Some(StatusCode::NOT_FOUND);
        self
    }

    pub fn internal_server_error(mut self) -> Self {
        self.status_code = Some(StatusCode::INTERNAL_SERVER_ERROR);
        self
    }

    pub fn pagination(mut self, pagination: ResponsePagination) -> Self {
        self.pagination = Some(pagination);
        self
    }

    pub fn build(self) -> ServerResponse<T> {
        ServerResponse {
            body: self.body,
            headers: self.headers.unwrap_or_default(),
            status_code: self.status_code.unwrap_or(StatusCode::OK),
            pagination: self.pagination,
        }
    }

    pub fn new() -> Self {
        Self {
            body: None,
            headers: None,
            status_code: None,
            pagination: None,
        }
    }
}

impl<T: Serialize> ServerResponse<T> {
    pub fn builder() -> ServerResponseBuilder<T> {
        ServerResponseBuilder::new()
    }
}

impl<T: Serialize> IntoResponse for ServerResponse<T>
where
    axum::Json<T>: IntoResponse,
{
    fn into_response(self) -> Response {
        let body = match self.body {
            Some(body) => body,
            None => return self.status_code.into_response(),
        };

        let bytes = match serde_json::to_vec(&body) {
            Ok(b) => b,
            Err(err) => {
                tracing::error!("Error serializing response body: {:?}", err);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };

        let mut headers = self.headers.clone();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        match self.pagination {
            Some(p) => (self.status_code, p, headers, bytes).into_response(),
            None => (self.status_code, headers, bytes).into_response(),
        }
    }
}

#[derive(Debug)]
pub enum AuthError {
    MissingToken(String),
    InvalidToken(String),
    ExpiredToken(String),
    Unauthorized(String),
    Forbidden(String),
}

impl Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::MissingToken(token) => write!(f, "Missing token: {}", token),
            AuthError::InvalidToken(token) => write!(f, "Invalid token: {}", token),
            AuthError::ExpiredToken(token) => write!(f, "Expired token: {}", token),
            AuthError::Unauthorized(token) => write!(f, "Unauthorized token: {}", token),
            AuthError::Forbidden(token) => write!(f, "Forbidden token: {}", token),
        }
    }
}

#[derive(Debug)]
pub enum ServerError {
    // The request body contained invalid JSON
    InternalError(String),
    AuthError(AuthError),
    BadRequest(String),
    NotFound(String),
}

impl Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::InternalError(message) => write!(f, "Internal Error: {}", message),
            ServerError::AuthError(error) => write!(f, "Authentication Error: {}", error),
            ServerError::BadRequest(message) => write!(f, "Bad Request: {}", message),
            ServerError::NotFound(message) => write!(f, "Not Found: {}", message),
        }
    }
}

impl From<ParseIntError> for ServerError {
    fn from(error: ParseIntError) -> Self {
        ServerError::InternalError(error.to_string())
    }
}

impl From<mongodb::error::Error> for ServerError {
    fn from(error: mongodb::error::Error) -> Self {
        ServerError::InternalError(error.to_string())
    }
}

impl From<std::io::Error> for ServerError {
    fn from(err: std::io::Error) -> Self {
        ServerError::InternalError(err.to_string())
    }
}

impl From<FromHexError> for ServerError {
    fn from(err: FromHexError) -> Self {
        ServerError::InternalError(err.to_string())
    }
}

impl From<MacError> for ServerError {
    fn from(err: MacError) -> Self {
        ServerError::InternalError(err.to_string())
    }
}

impl From<base64::DecodeError> for ServerError {
    fn from(err: base64::DecodeError) -> Self {
        ServerError::InternalError(err.to_string())
    }
}

impl From<FromUtf8Error> for ServerError {
    fn from(err: FromUtf8Error) -> Self {
        ServerError::InternalError(err.to_string())
    }
}

impl From<mongodb::bson::oid::Error> for ServerError {
    fn from(err: mongodb::bson::oid::Error) -> Self {
        ServerError::InternalError(err.to_string())
    }
}

impl From<ConnectionError> for ServerError {
    fn from(err: ConnectionError) -> Self {
        ServerError::InternalError(err.to_string())
    }
}

impl From<ReadError> for ServerError {
    fn from(err: ReadError) -> Self {
        ServerError::InternalError(err.to_string())
    }
}

impl ServerError {
    pub fn internal_error(message: &str) -> Self {
        ServerError::InternalError(message.to_string())
    }

    pub fn invalid_token(message: &str) -> Self {
        ServerError::AuthError(AuthError::InvalidToken(message.to_string()))
    }

    pub fn missing_token(message: &str) -> Self {
        ServerError::AuthError(AuthError::MissingToken(message.to_string()))
    }

    pub fn expired_token(message: &str) -> Self {
        ServerError::AuthError(AuthError::ExpiredToken(message.to_string()))
    }

    pub fn bad_request(message: &str) -> Self {
        ServerError::BadRequest(message.to_string())
    }

    pub fn not_found(message: &str) -> Self {
        ServerError::NotFound(message.to_string())
    }

    pub fn unauthorized(message: &str) -> Self {
        ServerError::AuthError(AuthError::Unauthorized(message.to_string()))
    }

    pub fn forbidden(message: &str) -> Self {
        ServerError::AuthError(AuthError::Forbidden(message.to_string()))
    }
}

// Tell axum how `AppError` should be converted into a response.
impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        // How we want errors responses to be serialized
        #[derive(Serialize)]
        struct ErrorResponse {
            message: String,
        }

        let (status, message) = match &self {
            ServerError::InternalError(rejection) => {
                // This error is caused by bad user input so don't log it
                (StatusCode::INTERNAL_SERVER_ERROR, rejection)
            }
            ServerError::AuthError(AuthError::InvalidToken(message)) => {
                (StatusCode::UNAUTHORIZED, message)
            }
            ServerError::AuthError(AuthError::MissingToken(message)) => {
                (StatusCode::UNAUTHORIZED, message)
            }
            ServerError::AuthError(AuthError::ExpiredToken(message)) => {
                (StatusCode::UNAUTHORIZED, message)
            }
            ServerError::AuthError(AuthError::Unauthorized(message)) => {
                (StatusCode::UNAUTHORIZED, message)
            }
            ServerError::AuthError(AuthError::Forbidden(message)) => {
                (StatusCode::FORBIDDEN, message)
            }
            ServerError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            ServerError::NotFound(message) => (StatusCode::NOT_FOUND, message),
        };

        error!("Returning error response {} {}", status, message);

        let response = (
            status,
            ServerResponse::<ErrorResponse>::builder()
                .body(ErrorResponse {
                    message: message.to_owned(),
                })
                .status_code(status)
                .build(),
        )
            .into_response();
        // if let Some(err) = err {
        //     // Insert our error into the response, our logging middleware will use this.
        //     // By wrapping the error in an Arc we can use it as an Extension regardless of any inner types not deriving Clone.
        //     response.extensions_mut().insert(Arc::new(err));
        // }
        response
    }
}

pub type ServerResult<T> = Result<T, ServerError>;
pub type ServerAppResult<T> = Result<ServerResponse<T>, ServerError>;
