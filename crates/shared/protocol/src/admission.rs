use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportImpact {
    Direct,
    Relay,
    Gateway,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdmissionBatch {
    pub impact: TransportImpact,
    pub session_id: String,
    pub timestamp: u64,
}
