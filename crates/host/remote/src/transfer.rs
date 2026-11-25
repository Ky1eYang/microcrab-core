//! Bounded JSON transfers; digest, length and sequence are verified before use.
use super::*;
use microcrab_host_protocol::{MAX_CHUNK_BYTES, MAX_TRANSFER_BYTES, TransferRef};
use serde_json::Value;
use sha2::{Digest, Sha256};
pub(crate) struct Incoming {
    pub request_id: String,
    operation_id: String,
    generation: u64,
    reference: TransferRef,
    next: u32,
    bytes: Vec<u8>,
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
impl RemoteHost {
    // Caller holds the web lane lock when attempting to reserve a shared ID.
    pub(super) fn legacy_owns_transfer(&self, id: &str, generation: u64) -> bool {
        self.transfers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(id)
            || self
                .retired_transfers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .any(|(retired, old_generation)| retired == id && *old_generation == generation)
    }

    pub(crate) fn retire_transfer(&self, id: String, generation: u64) {
        if id.is_empty() || id.len() > 256 {
            return;
        }
        let mut retired = self
            .retired_transfers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        retired.push_back((id, generation));
        while retired.len() > 256 {
            retired.pop_front();
        }
    }
    pub(crate) fn begin_transfer(
        &self,
        request_id: String,
        operation_id: String,
        generation: u64,
        reference: TransferRef,
    ) -> Result<(), ToolError> {
        if reference.transfer_id.is_empty()
            || reference.byte_length > MAX_TRANSFER_BYTES as u64
            || reference.sha256.len() != 64
        {
            return Err(unavailable("invalid Host transfer declaration"));
        }
        let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
        if !route
            .as_ref()
            .is_some_and(|route| route.generation == generation)
        {
            return Err(unavailable("stale Host transfer route"));
        }
        let web = self.web.lock().unwrap_or_else(|e| e.into_inner());
        if web.owns_transfer_id(&reference.transfer_id) {
            return Err(unavailable("duplicate Host transfer id"));
        }
        // Cancellation also locks pending first; hold it through transfer admission.
        let pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if !pending.get(&request_id).is_some_and(|pending| {
            pending.generation == generation && pending.operation_id == operation_id
        }) {
            let retired = self
                .retired
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .any(|(request, operation, old_generation)| {
                    request == &request_id
                        && operation == &operation_id
                        && *old_generation == generation
                });
            if retired {
                self.retire_transfer(reference.transfer_id, generation);
                return Ok(());
            }
            return Err(unavailable("uncorrelated Host transfer declaration"));
        }
        let mut transfers = self.transfers.lock().unwrap_or_else(|e| e.into_inner());
        if reference.transfer_id.len() > 256
            || !matches!(reference.kind.as_str(), "json" | "bytes")
            || transfers.len() >= microcrab_host_protocol::MAX_PENDING_REQUESTS
            || transfers.values().any(|transfer| {
                transfer.request_id == request_id && transfer.generation == generation
            })
            || transfers.contains_key(&reference.transfer_id)
            || transfers
                .values()
                .map(|transfer| transfer.reference.byte_length)
                .sum::<u64>()
                + reference.byte_length
                > MAX_TRANSFER_BYTES as u64
        {
            return Err(unavailable(
                "Host transfer budget exhausted or duplicate transfer id",
            ));
        }
        transfers.insert(
            reference.transfer_id.clone(),
            Incoming {
                request_id,
                operation_id,
                generation,
                reference,
                next: 0,
                bytes: Vec::new(),
            },
        );
        drop(transfers);
        drop(pending);
        Ok(())
    }
    pub(crate) fn accept_chunk(
        &self,
        generation: u64,
        transfer_id: String,
        sequence: u32,
        data: Vec<u8>,
        eof: bool,
    ) -> Result<(), ToolError> {
        let mut transfers = self.transfers.lock().unwrap_or_else(|e| e.into_inner());
        if !transfers.contains_key(&transfer_id)
            && self
                .retired_transfers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .any(|(id, old_generation)| id == &transfer_id && *old_generation == generation)
        {
            return Ok(());
        }
        let transfer = transfers
            .get_mut(&transfer_id)
            .filter(|transfer| transfer.generation == generation)
            .ok_or_else(|| unavailable("unknown Host transfer"))?;
        if sequence != transfer.next
            || data.len() > MAX_CHUNK_BYTES
            || transfer.bytes.len() + data.len() > transfer.reference.byte_length as usize
        {
            return Err(unavailable("invalid Host transfer sequence or size"));
        }
        transfer.next = transfer
            .next
            .checked_add(1)
            .ok_or_else(|| unavailable("Host transfer sequence overflow"))?;
        transfer.bytes.extend_from_slice(&data);
        if !eof {
            return Ok(());
        }
        let transfer = transfers
            .remove(&transfer_id)
            .ok_or_else(|| unavailable("missing Host transfer"))?;
        drop(transfers);
        let result = if transfer.bytes.len() != transfer.reference.byte_length as usize
            || digest(&transfer.bytes) != transfer.reference.sha256
        {
            Err(unavailable("Host transfer integrity check failed"))
        } else if transfer.reference.kind == "bytes" {
            Ok(Reply::Value(serde_json::json!(transfer.bytes)))
        } else if transfer.reference.kind == "json" {
            serde_json::from_slice::<Value>(&transfer.bytes)
                .map(Reply::Value)
                .map_err(|_| unavailable("invalid Host transfer JSON"))
        } else {
            Err(unavailable("unsupported Host transfer kind"))
        };
        if result.is_ok() {
            self.forget_consumption(&transfer.operation_id);
            self.unresolved
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&transfer.operation_id);
        }
        self.complete(&transfer.request_id, generation, result);
        Ok(())
    }
}
