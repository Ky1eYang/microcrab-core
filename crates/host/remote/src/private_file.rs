//! Route-scoped binary reads. Only a trusted embedder can install a negotiated
//! reader; ordinary Host frames have no representation for this operation.
use super::*;
use microcrab_host_api::{OpContext, PrivateFileBuffer};
use microcrab_host_protocol::{
    BindingScope, MAX_CHUNK_BYTES, MAX_PRIVATE_FILE_BUDGET_MS, MAX_PRIVATE_FILE_BYTES,
    MAX_PRIVATE_FILE_PATH_BYTES, pb,
};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, time::Duration};

/// Installed only by an authenticated carrier that negotiated the typed lane.
#[async_trait::async_trait]
pub trait PrivateFileReader: Send + Sync {
    async fn read(&self, transfer: PrivateFileTransfer) -> Result<PrivateFileBuffer, ToolError>;
}

/// Bounded, exact-operation reassembly into the Session's precharged buffer.
/// The carrier must feed all frames and call `finish` only after clean RPC EOF.
pub struct PrivateFileTransfer {
    request: pb::PrivateFileRequest,
    buffer: PrivateFileBuffer,
    declared: Option<pb::PrivateFileTransfer>,
    offset: usize,
    next: u32,
    eof: bool,
}

impl PrivateFileTransfer {
    pub fn request(&self) -> &pb::PrivateFileRequest {
        &self.request
    }

    pub fn receive(&mut self, frame: pb::PrivateFileFrame) -> Result<(), ToolError> {
        use pb::private_file_frame::Body;
        match frame.body {
            Some(Body::Transfer(transfer)) if self.declared.is_none() => {
                if transfer.operation_id != self.request.operation_id || transfer.sha256.len() != 32
                {
                    return Err(unavailable("invalid private file transfer declaration"));
                }
                self.buffer.allocate(transfer.byte_length as usize)?;
                self.declared = Some(transfer);
                Ok(())
            }
            Some(Body::Chunk(chunk)) if !self.eof && self.declared.is_some() => {
                let end = self.offset + chunk.data.len();
                let length = self.buffer.as_bytes().len();
                if chunk.transfer_id != self.request.operation_id
                    || chunk.sequence != self.next
                    || chunk.data.len() > MAX_CHUNK_BYTES
                    || (chunk.data.is_empty() && length != 0)
                    || end > length
                    || chunk.eof != (end == length)
                {
                    return Err(unavailable(
                        "invalid private file transfer identity, sequence or size",
                    ));
                }
                self.buffer.as_bytes_mut()[self.offset..end].copy_from_slice(&chunk.data);
                self.offset = end;
                self.next += 1; // Nonempty chunks except for the single, empty file EOF.
                self.eof = chunk.eof;
                Ok(())
            }
            _ => Err(unavailable("unexpected private file transfer frame")),
        }
    }

    pub fn finish(self) -> Result<PrivateFileBuffer, ToolError> {
        if !self.eof
            || self.declared.as_ref().is_none_or(|transfer| {
                Sha256::digest(self.buffer.as_bytes())[..] != transfer.sha256
            })
        {
            return Err(unavailable(
                "private file transfer incomplete or failed its integrity check",
            ));
        }
        Ok(self.buffer)
    }
}

#[cfg(test)]
mod tests;

impl RemoteHost {
    pub(super) fn private_file_supported(&self) -> bool {
        self.route
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|route| route.private_files.is_some())
    }

    pub(super) async fn read_private_file_on_route(
        &self,
        path: PathBuf,
        buffer: PrivateFileBuffer,
        context: &OpContext,
    ) -> Result<PrivateFileBuffer, ToolError> {
        context.check().map_err(ToolError::Host)?;
        let path = path
            .into_os_string()
            .into_string()
            .map_err(|_| ToolError::invalid_request("private file path must be Unicode"))?;
        if path.is_empty() || path.len() > MAX_PRIVATE_FILE_PATH_BYTES || path.contains('\0') {
            return Err(ToolError::invalid_request("unsupported private file path"));
        }
        if !(1..=MAX_PRIVATE_FILE_BYTES).contains(&buffer.max_bytes()) {
            return Err(ToolError::FileLimit);
        }
        let mut context = context.clone();
        let maximum = Instant::now() + Duration::from_millis(MAX_PRIVATE_FILE_BUDGET_MS.into());
        let deadline = context.deadline.map_or(maximum, |value| value.min(maximum));
        context.deadline = Some(deadline);
        let timeout = || {
            ToolError::Host(microcrab_host_api::HostError::known(
                microcrab_host_api::ErrorCategory::Timeout,
                "private file read deadline exceeded",
            ))
        };
        let generation = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline.into()) => return Err(timeout()),
            result = self.await_ready(&context.cancel) => result?,
        };
        let (reader, stop, lease_generation) = {
            let route = self.route.lock().unwrap_or_else(|e| e.into_inner());
            let route = route
                .as_ref()
                .filter(|route| route.generation == generation)
                .ok_or_else(|| unavailable("Host route changed before private file admission"))?;
            self.check_ready_admission(generation)?;
            let reader = route
                .private_files
                .clone()
                .ok_or_else(|| ToolError::unsupported("read_private_file"))?;
            (reader, route.stop.clone(), route.lease.clone())
        };
        let scope = {
            let scope = self.scope.lock().unwrap_or_else(|e| e.into_inner());
            let Some(BindingScope::Project(scope)) = scope.as_ref() else {
                return Err(unavailable(
                    "private file read requires an exact Project binding",
                ));
            };
            pb::PrivateFileScope {
                project_id: scope.project_id.clone(),
                backend_id: scope.backend_id.clone(),
                binding_id: scope.binding_id.clone(),
            }
        };
        context.check().map_err(ToolError::Host)?;
        let remaining_budget_ms = deadline
            .saturating_duration_since(Instant::now())
            .as_millis() as u32;
        if remaining_budget_ms == 0 {
            return Err(timeout());
        }
        let disconnected = || unavailable("Host route retired during private file read");
        let buffer = tokio::select! {
            biased;
            _ = context.cancel.cancelled() => return Err(ToolError::Cancelled),
            _ = stop.cancelled() => return Err(disconnected()),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(timeout()),
            result = reader.read(PrivateFileTransfer {
                request: pb::PrivateFileRequest {
                    operation_id: uuid::Uuid::new_v4().to_string(),
                    lease_generation,
                    scope: Some(scope),
                    remaining_budget_ms,
                    path,
                    max_bytes: buffer.max_bytes() as u32,
                },
                buffer,
                declared: None,
                offset: 0,
                next: 0,
                eof: false,
            }) => result?,
        };
        context.check().map_err(ToolError::Host)?;
        if stop.is_cancelled() {
            return Err(disconnected());
        }
        Ok(buffer)
    }
}
