//! JSON types shared by the daemon and first-party clients.

use dendrite_core::{FilePriority, Sha1Hash, Sha256Hash, TorrentId, TorrentState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const API_VERSION: &str = "2.0";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub api_version: String,
    pub daemon_version: String,
    pub uptime_seconds: u64,
    pub loaded_torrents: usize,
    pub active_torrents: usize,
    pub connected_peers: usize,
    pub quarantined_records: usize,
    pub storage_backend: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserSessionResponse {
    pub csrf_token: String,
    pub expires_in_seconds: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenRotationResponse {
    pub token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TorrentSummary {
    pub id: TorrentId,
    pub name: String,
    pub state: TorrentState,
    pub v1_info_hash: Option<Sha1Hash>,
    pub v2_info_hash: Option<Sha256Hash>,
    pub total_length: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub download_rate: u64,
    pub upload_rate: u64,
    pub peers: u32,
    #[serde(default)]
    pub inbound_peers: u32,
    #[serde(default)]
    pub outbound_peers: u32,
    #[serde(default)]
    pub seed_peers: u32,
    #[serde(default)]
    pub active_downloaders: u32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AddTorrentOptions {
    #[serde(default)]
    pub start: bool,
    pub destination: Option<String>,
    #[serde(default)]
    pub sequential: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum AddTorrentRequest {
    Magnet {
        uri: String,
        #[serde(default)]
        options: AddTorrentOptions,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentAction {
    Pause,
    Resume,
    Recheck,
    Announce,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TorrentActionRequest {
    pub action: TorrentAction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilePriorityUpdate {
    pub file_index: u32,
    pub priority: FilePriority,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub id: Uuid,
    pub kind: String,
    pub state: OperationState,
    pub progress: Option<f32>,
    pub error: Option<Problem>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Pending,
    Running,
    Complete,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_version: u16,
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
    pub resource_id: Option<String>,
    pub kind: String,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Problem {
    #[serde(rename = "type")]
    pub problem_type: String,
    pub title: String,
    pub status: u16,
    pub code: String,
    pub detail: String,
    pub instance: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListResponse<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}
