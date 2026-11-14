//! Bounded safe evidence shares the RemoteHost lifetime, never execution authority.
use super::*;
use microcrab_host_protocol::BridgeDiagnostic;
use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Reason {
    UnresolvedCall,
    UnknownResponse,
    RejectedNotReady,
    LateResponse,
    HeartbeatBackpressure,
}

impl RemoteHost {
    /// Newest evidence first, bounded independently from operation receipts.
    pub fn diagnostics(&self, limit: usize) -> Vec<serde_json::Value> {
        self.diagnostics
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .rev()
            .take(limit.min(64))
            .cloned()
            .collect()
    }

    fn remember_diagnostic(&self, event: serde_json::Value) {
        let mut diagnostics = self.diagnostics.lock().unwrap_or_else(|e| e.into_inner());
        diagnostics.push_back(event);
        while diagnostics.len() > 256 {
            diagnostics.pop_front();
        }
    }

    pub(super) fn readiness_diagnostic(
        &self,
        generation: u64,
        request_id: Option<&str>,
        operation_id: Option<&str>,
        reason: Reason,
    ) {
        // Never publish an untrusted response identifier as arbitrary text.
        let request_id = request_id.and_then(|id| uuid::Uuid::parse_str(id).ok());
        let operation_id = operation_id.and_then(|id| uuid::Uuid::parse_str(id).ok());
        let event_id = uuid::Uuid::new_v4();
        tracing::warn!(%event_id, generation,
            bridge_request_id = ?request_id, bridge_operation_id = ?operation_id,
            reason = ?reason, "Remote Host readiness diagnostic");
        self.remember_diagnostic(serde_json::json!({
            "source":"remote_host", "event_id":event_id.to_string(),
            "timestamp_ms":timestamp_ms(), "connection_generation":generation.to_string(),
            "request_id":request_id.map(|id| id.to_string()),
            "operation_id":operation_id.map(|id| id.to_string()), "reason":reason,
        }));
    }

    pub(super) fn receive_diagnostic(
        &self,
        generation: u64,
        event: BridgeDiagnostic,
    ) -> Result<(), ToolError> {
        // Keep route validation and publication atomic with route replacement.
        let mut route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        let valid_route = route.as_ref().is_some_and(|route| {
            route.generation == generation
                && route.lease == event.lease_generation
                && route.bridge_instance_id == event.bridge_instance_id
        });
        if !valid_route
            || !safe_identity(&event.bridge_instance_id)
            || !safe_identity(&event.lease_generation)
            || !valid_uuid(&event.event_id)
            || event.request_id.as_ref().is_some_and(|id| !valid_uuid(id))
            || event
                .operation_id
                .as_ref()
                .is_some_and(|id| !valid_uuid(id))
            || event
                .method
                .as_deref()
                .is_some_and(|name| !known_method(name))
        {
            tracing::warn!(
                generation,
                reason = "invalid_diagnostic",
                "Rejected Host diagnostic"
            );
            return Err(unavailable("invalid Host diagnostic"));
        }
        if let Some(route) = route.as_mut() {
            route.last_received = Instant::now();
        }
        tracing::warn!(event_id = %event.event_id, bridge_instance_id = %event.bridge_instance_id,
            bridge_request_id = ?event.request_id, bridge_operation_id = ?event.operation_id,
            lease_generation = %event.lease_generation, generation, method = ?event.method,
            phase = ?event.phase, error_code = ?event.error_code, elapsed_ms = ?event.elapsed_ms,
            reason = "bridge_diagnostic", "Host bridge diagnostic");
        self.remember_diagnostic(serde_json::json!({
            "source":"local_bridge", "connection_generation":generation.to_string(), "event":event,
        }));
        Ok(())
    }
}

fn safe_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}
fn valid_uuid(value: &str) -> bool {
    value.len() == 36 && uuid::Uuid::parse_str(value).is_ok()
}
fn known_method(value: &str) -> bool {
    matches!(
        value,
        "cache_point"
            | "read"
            | "write"
            | "glob"
            | "grep"
            | "normalize"
            | "read_file"
            | "stat"
            | "process_spawn"
            | "shell_launch"
            | "process_inventory"
            | "process_status"
            | "process_pid"
            | "process_wait"
            | "process_signal"
            | "process_kill"
            | "process_release"
            | "process_write"
            | "process_close_stdin"
            | "process_resize"
            | "process_stream_open"
            | "process_stream_read"
            | "shell_capture_status"
            | "shell_read"
            | "shell_drain"
            | "shell_write"
            | "recovery"
            | "settle_unknown"
            | "unsupported"
    )
}
fn timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}
