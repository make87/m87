use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PaginationMetadata {
    pub count: u64,
    pub offset: u64,
    pub limit: u32,
}
