use crate::models::common::PaginationResults;
use crate::models::explorer::Share;
use serde::{Deserialize, Deserializer, Serialize};

fn null_to_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// Create or update a share link.
// Mirrors Cloudreve's Go `ShareCreateService` request DTO; "Service" means request payload, not a stateful service.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShareCreateService {
    /// Cloudreve URI of the file or folder to share.
    pub uri: String,
    #[serde(default)]
    pub is_private: bool,
    #[serde(default)]
    pub password: String,
    /// Remaining downloads; 0 means unlimited.
    #[serde(default, rename = "downloads")]
    pub remain_downloads: i32,
    /// Expiry in seconds from now; 0 means never.
    #[serde(default)]
    pub expire: i32,
    #[serde(default)]
    pub share_view: bool,
    #[serde(default)]
    pub show_readme: bool,
}

impl ShareCreateService {
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            ..Default::default()
        }
    }
}

/// Query parameters for listing the current user's share links.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListShareService {
    pub page_size: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

impl Default for ListShareService {
    fn default() -> Self {
        Self {
            page_size: 50,
            order_by: None,
            order_direction: None,
            next_page_token: None,
        }
    }
}

/// List share response.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ListShareResponse {
    #[serde(default, deserialize_with = "null_to_default")]
    pub shares: Vec<Share>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub pagination: PaginationResults,
}

/// Batch-delete request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchDeleteShareService {
    pub ids: Vec<String>,
}
