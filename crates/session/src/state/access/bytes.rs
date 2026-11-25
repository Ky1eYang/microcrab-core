//! Native-only private Bytes variables. No snapshot, serde, guest or generic read port
//! owns this backing. The mutable allocation moves to immutable shared ownership
//! at commit, and the charge follows the allocation through the last read lease.
use super::{LiveState, State, VariableCommit, VariableWitness, Writer};
use crate::admission::image::{MAX_IMAGE_BYTES, image_type, validate_image};
use crate::{Error, LimitKind};
use microcrab_protocol::VariableRole;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

const MAX_PRIVATE_BYTES: usize = 64 * 1024 * 1024;
const MAX_PRIVATE_ENTRIES: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BytesVariableKind {
    Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BytesVariableLifecycle {
    Ephemeral,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BytesVariableAvailability {
    Available,
}

/// Owner inspection data. A selector identifies an exact live version, but does
/// not authorize anyone to read it. No row exists once the value is gone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BytesVariableMetadata {
    pub name: String,
    pub kind: BytesVariableKind,
    pub media_type: String,
    pub byte_length: u64,
    pub lifecycle: BytesVariableLifecycle,
    pub availability: BytesVariableAvailability,
    pub revision: String,
    pub selector: String,
    /// Exact successful Job operation within the inspected runtime instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_execution_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BytesWriteReceipt {
    pub commit: VariableCommit,
    pub metadata: BytesVariableMetadata,
}

#[derive(Default)]
struct ByteBudget(Mutex<Usage>);

#[derive(Default)]
struct Usage {
    bytes: usize,
    entries: usize,
}

pub(crate) struct ByteCharge {
    budget: Arc<ByteBudget>,
    bytes: usize,
    entries: usize,
}

fn increment(used: usize, added: usize) -> Result<usize, Error> {
    used.checked_add(added).ok_or_else(|| {
        crate::budget::reject(LimitKind::Arithmetic, "private byte counter overflow")
    })
}

impl ByteBudget {
    fn reserve(self: &Arc<Self>, bytes: usize, entries: usize) -> Result<ByteCharge, Error> {
        let mut used = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let next_bytes = increment(used.bytes, bytes)?;
        let next_entries = increment(used.entries, entries)?;
        crate::budget::check(
            LimitKind::PrivateVariableBytes,
            next_bytes,
            MAX_PRIVATE_BYTES,
        )?;
        crate::budget::check(
            LimitKind::PrivateVariableEntries,
            next_entries,
            MAX_PRIVATE_ENTRIES,
        )?;
        *used = Usage {
            bytes: next_bytes,
            entries: next_entries,
        };
        Ok(ByteCharge {
            budget: self.clone(),
            bytes,
            entries,
        })
    }
}

impl Drop for ByteCharge {
    fn drop(&mut self) {
        let mut used = self
            .budget
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Only a successful reservation creates a charge; it is never cloned.
        used.bytes -= self.bytes;
        used.entries -= self.entries;
    }
}

/// Unpublished upload ticket bound to one State incarnation and exact length.
/// No queue fence is held while the embedder streams. Dropping it refunds quota.
#[must_use = "drop discards the unpublished Bytes upload"]
pub struct PreparedBytesWrite {
    name: String,
    media_type: &'static str,
    byte_length: u64,
    length: usize,
    expected: VariableWitness,
    references_revision: u64,
    overflowed: bool,
    data: Vec<u8>,
    charge: ByteCharge,
}

impl fmt::Debug for PreparedBytesWrite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedBytesWrite")
            .finish_non_exhaustive()
    }
}

impl PreparedBytesWrite {
    pub fn append(&mut self, chunk: &[u8]) -> Result<(), Error> {
        if self.overflowed || chunk.len() > self.length - self.data.len() {
            self.overflowed = true;
            return Err(Error::config("Bytes upload exceeds its declared length"));
        }
        self.data.extend_from_slice(chunk);
        Ok(())
    }
}

struct Backing {
    data: BackingData,
    // Uploaded allocations or logical per-variable quota for a shared redirect.
    _charge: ByteCharge,
}

enum BackingData {
    Uploaded(Vec<u8>),
    Redirected(crate::runtime::execution::PrivateOutputLease),
}

impl BackingData {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Uploaded(bytes) => bytes,
            Self::Redirected(lease) => lease.as_bytes(),
        }
    }
}

/// Literal, create-only destination witnessed before permission is requested.
#[derive(Clone)]
pub(crate) struct RedirectDestination {
    name: String,
    expected: VariableWitness,
    references_revision: u64,
}

impl RedirectDestination {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone)]
struct ByteValue {
    metadata: BytesVariableMetadata,
    backing: Arc<Backing>,
}

/// Session-owned typed variables coexist with but never enter the generic JSON
/// Variables store. Keys are exclusive across both stores under the State lock.
#[derive(Clone, Default)]
pub(super) struct ByteVariables {
    values: BTreeMap<String, ByteValue>,
    budget: Arc<ByteBudget>,
}

impl ByteVariables {
    pub(super) fn clear(&mut self) {
        self.values.clear();
    }

    pub(super) fn owns(&self, name: &str) -> bool {
        self.values.contains_key(name)
    }

    pub(super) fn metadata(&self) -> BTreeMap<String, BytesVariableMetadata> {
        self.values
            .iter()
            .map(|(name, value)| (name.clone(), value.metadata.clone()))
            .collect()
    }
}

fn next_revision(live: &LiveState) -> Result<u64, Error> {
    live.variables_revision
        .checked_add(1)
        .ok_or_else(|| crate::budget::reject(LimitKind::Arithmetic, "variable revision overflow"))
}

fn validate_name(name: &str) -> Result<(), Error> {
    super::validate_custom_key(name)?;
    if !microcrab_protocol::valid_secret_key(name) {
        return Err(Error::config("invalid Bytes variable name"));
    }
    Ok(())
}

fn validate_destination(live: &LiveState, name: &str) -> Result<(), Error> {
    if live.snapshot.references.owns(name) || live.snapshot.variables.get_all(name).is_some() {
        return Err(Error::VariableForbidden(
            "Bytes destination is owned by a JSON variable or reference".into(),
        ));
    }
    Ok(())
}

fn validate_redirect_destination(live: &LiveState, name: &str) -> Result<(), Error> {
    validate_destination(live, name)?;
    if live.bytes.owns(name) {
        return Err(Error::VariableForbidden(
            "private Bytes creation requires a new variable name".into(),
        ));
    }
    Ok(())
}

impl RedirectDestination {
    fn check(&self, live: &LiveState) -> Result<(), Error> {
        if live.witness() != self.expected || live.references_revision != self.references_revision {
            return Err(Error::VariableConflict);
        }
        validate_redirect_destination(live, &self.name)
    }
}

fn publish_bytes(
    live: &mut LiveState,
    name: String,
    media_type: &str,
    byte_length: u64,
    before: VariableWitness,
    backing: Backing,
    redirect_execution_id: Option<String>,
) -> Result<BytesWriteReceipt, Error> {
    let revision = next_revision(live)?;
    let metadata = BytesVariableMetadata {
        name: name.clone(),
        kind: BytesVariableKind::Bytes,
        media_type: media_type.into(),
        byte_length,
        lifecycle: BytesVariableLifecycle::Ephemeral,
        availability: BytesVariableAvailability::Available,
        revision: revision.to_string(),
        selector: format!(
            "bytes_{}_{revision:016x}",
            live.runtime_instance_id.replace('-', "")
        ),
        redirect_execution_id,
    };
    live.bytes.values.insert(
        name,
        ByteValue {
            metadata: metadata.clone(),
            backing: Arc::new(backing),
        },
    );
    live.variables_revision = revision;
    Ok(BytesWriteReceipt {
        commit: VariableCommit {
            before,
            after: live.witness(),
            changed: true,
        },
        metadata,
    })
}

impl State {
    fn bytes_endpoint(&self) -> Result<Arc<super::VariableEndpoint>, Error> {
        if self.actor != VariableRole::Client {
            return Err(Error::denied(
                "Bytes ingress is available only to the trusted Client",
            ));
        }
        self.root_bytes_endpoint()
    }

    fn root_bytes_endpoint(&self) -> Result<Arc<super::VariableEndpoint>, Error> {
        self.ensure_root_bytes()?;
        let Writer::Session(owner) = &self.writer else {
            return Err(Error::Closed);
        };
        owner.upgrade().ok_or(Error::Closed)
    }

    /// Reserve a bounded owned staging allocation before reading the request body.
    /// The authenticated embedder supplies only canonical image MIME and raw bytes.
    pub fn prepare_bytes_write(
        &self,
        name: &str,
        media_type: &str,
        byte_length: u64,
        expected: VariableWitness,
    ) -> Result<PreparedBytesWrite, Error> {
        let endpoint = self.bytes_endpoint()?;
        endpoint.ensure_open()?;
        validate_name(name)?;
        let media_type =
            image_type(media_type).ok_or_else(|| Error::config("unsupported image media type"))?;
        let length = usize::try_from(byte_length).map_err(|_| {
            crate::budget::reject(
                LimitKind::AttachmentBytes,
                "Bytes upload length exceeds the image bound",
            )
        })?;
        if length == 0 {
            return Err(Error::config("image data must be nonempty"));
        }
        let (charge, references_revision) = {
            let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            endpoint.ensure_open()?;
            if live.witness() != expected {
                return Err(Error::VariableConflict);
            }
            validate_destination(&live, name)?;
            crate::budget::check(
                LimitKind::AttachmentBytes,
                length,
                live.snapshot
                    .limits
                    .max_tool_image_bytes
                    .min(MAX_IMAGE_BYTES),
            )?;
            (
                live.bytes.budget.reserve(length, 1)?,
                live.references_revision,
            )
        };
        let mut data = Vec::new();
        data.try_reserve_exact(length).map_err(|_| {
            crate::budget::reject(
                LimitKind::PrivateVariableBytes,
                "could not allocate Bytes upload",
            )
        })?;
        Ok(PreparedBytesWrite {
            name: name.into(),
            media_type,
            byte_length,
            length,
            expected,
            references_revision,
            overflowed: false,
            data,
            charge,
        })
    }

    /// Validate and publish a complete upload once, under the existing variable
    /// queue fence. No mutation is queued; errors are pre-commit and there is no
    /// internal ACK await after publication. An external reply can still be lost.
    pub async fn commit_bytes(
        &self,
        prepared: PreparedBytesWrite,
    ) -> Result<BytesWriteReceipt, Error> {
        let endpoint = self.bytes_endpoint()?;
        endpoint.ensure_open()?;
        if prepared.overflowed {
            return Err(Error::config("Bytes upload exceeds its declared length"));
        }
        if prepared.data.len() != prepared.length {
            return Err(Error::config("Bytes upload is incomplete"));
        }
        validate_image(prepared.media_type, &prepared.data)?;
        let _fence = endpoint.fence().await?;
        let mut live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        endpoint.ensure_open()?;
        if !Arc::ptr_eq(&live.bytes.budget, &prepared.charge.budget)
            || live.witness() != prepared.expected
            || live.references_revision != prepared.references_revision
        {
            return Err(Error::VariableConflict);
        }
        validate_destination(&live, &prepared.name)?;
        crate::budget::check(
            LimitKind::AttachmentBytes,
            prepared.length,
            live.snapshot
                .limits
                .max_tool_image_bytes
                .min(MAX_IMAGE_BYTES),
        )?;
        publish_bytes(
            &mut live,
            prepared.name,
            prepared.media_type,
            prepared.byte_length,
            prepared.expected,
            Backing {
                data: BackingData::Uploaded(prepared.data),
                _charge: prepared.charge,
            },
            None,
        )
    }

    pub(crate) fn prepare_redirect_destination(
        &self,
        name: &str,
    ) -> Result<RedirectDestination, Error> {
        let endpoint = self.root_bytes_endpoint()?;
        validate_name(name)?;
        let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        endpoint.ensure_open()?;
        validate_redirect_destination(&live, name)?;
        Ok(RedirectDestination {
            name: name.into(),
            expected: live.witness(),
            references_revision: live.references_revision,
        })
    }

    /// Commit one bounded image staged by a certified invocation. The image
    /// shares the canonical private variable budget and never enters JSON State.
    pub(crate) async fn commit_private_image(
        &self,
        destination: RedirectDestination,
        media_type: &str,
        data: Vec<u8>,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<BytesWriteReceipt, Error> {
        let endpoint = self.root_bytes_endpoint()?;
        validate_image(media_type, &data)?;
        let length = data.len();
        let check = |live: &LiveState| {
            destination.check(live)?;
            crate::budget::check(
                LimitKind::AttachmentBytes,
                length,
                live.snapshot.limits.max_tool_image_bytes.min(MAX_IMAGE_BYTES),
            )
        };
        let charge = {
            let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            endpoint.ensure_open()?;
            check(&live)?;
            live.bytes.budget.reserve(length, 1)?
        };
        let _fence = tokio::select! { biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            fence = endpoint.fence() => fence?,
        };
        let mut live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        endpoint.ensure_open()?;
        check(&live)?;
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        publish_bytes(
            &mut live, destination.name, media_type, length as u64, destination.expected,
            Backing { data: BackingData::Uploaded(data), _charge: charge }, None,
        )
    }

    /// Publish an owned, certified registry lease without inspecting its bytes.
    /// The original allocation permit follows the lease; our independent charge
    /// bounds every staged/committed name even if names share one allocation.
    pub(crate) async fn commit_redirect(
        &self,
        destination: RedirectDestination,
        lease: crate::runtime::execution::PrivateOutputLease,
        operation: crate::runtime::execution::ExecutionId,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<BytesWriteReceipt, Error> {
        let endpoint = self.root_bytes_endpoint()?;
        // Capture admission already certified canonical MIME; this value is
        // a declaration, not a consumer-specific proof of the byte format.
        let mime = lease.media_type().to_owned();
        let length = lease.as_bytes().len();
        let charge = {
            let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            endpoint.ensure_open()?;
            destination.check(&live)?;
            live.bytes.budget.reserve(length, 1)?
        };
        let _fence = tokio::select! { biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            fence = endpoint.fence() => fence?,
        };
        let mut live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        endpoint.ensure_open()?;
        destination.check(&live)?;
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        // Fence ACK precedes the mutation; no await or fallible IO after publish.
        // Dropped observation can still be reconciled by exact operation metadata.
        publish_bytes(
            &mut live,
            destination.name,
            &mime,
            length as u64, // bounded by the private byte budget
            destination.expected,
            Backing {
                data: BackingData::Redirected(lease),
                _charge: charge,
            },
            Some(operation.0.to_string()),
        )
    }

    /// Delete only a typed Bytes value. Does not fall through to a JSON delete.
    pub async fn remove_bytes_checked(
        &self,
        name: &str,
        expected: VariableWitness,
    ) -> Result<VariableCommit, Error> {
        let endpoint = self.bytes_endpoint()?;
        validate_name(name)?;
        let _fence = endpoint.fence().await?;
        let mut live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        endpoint.ensure_open()?;
        if live.witness() != expected {
            return Err(Error::VariableConflict);
        }
        let changed = live.bytes.owns(name);
        if changed {
            let revision = next_revision(&live)?;
            live.bytes.values.remove(name);
            live.variables_revision = revision;
        }
        Ok(VariableCommit {
            before: expected,
            after: live.witness(),
            changed,
        })
    }

    fn check_live_root_bytes(&self, live: &LiveState) -> Result<(), Error> {
        self.ensure_variable_open()?;
        if !live.is_main
            || live.snapshot.session_kind != Some(crate::SessionKind::Interactive)
            || live
                .snapshot
                .communication
                .as_ref()
                .is_some_and(|value| value.identity.depth != 0)
        {
            return Err(Error::denied(
                "view_image and private Bytes require an interactive root Session",
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_root_bytes(&self) -> Result<(), Error> {
        let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        self.check_live_root_bytes(&live)
    }

    /// Bind a literal Bytes name to its current exact live revision. The returned
    /// selector carries no read authority and must not be resolved afresh after Ask.
    pub(crate) fn image_bytes_selector_for_name(&self, name: &str) -> Result<String, Error> {
        let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        self.check_live_root_bytes(&live)?;
        validate_name(name)?;
        live.bytes
            .values
            .get(name)
            .map(|value| value.metadata.selector.clone())
            .ok_or_else(|| Error::tool("named Bytes variable is unavailable"))
    }

    /// Resolve only an already-admitted exact selector. This is not a guest or
    /// embedder read port. No State guard is held across validation or allocation.
    pub(crate) fn with_image_bytes<T>(
        &self,
        selector: &str,
        read: impl FnOnce(&str, &[u8]) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let value = {
            let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            self.check_live_root_bytes(&live)?;
            live.bytes
                .values
                .values()
                .find(|value| value.metadata.selector == selector)
                .cloned()
                .ok_or_else(|| Error::tool("exact Bytes selector is stale or unavailable"))?
        };
        read(&value.metadata.media_type, value.backing.data.as_bytes())
    }

    pub(crate) fn bound_image_host(&self) -> Result<Arc<dyn microcrab_host_api::Host>, Error> {
        self.ensure_root_bytes()?;
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .image_host
            .clone()
            .ok_or_else(|| Error::tool("view_image path requires a bound Host"))
    }

    /// The Context/registry reservation is made before dispatch. This second
    /// charge covers our transient allocation until it becomes admitted image Data.
    pub(crate) fn reserve_image_transfer(&self, length: usize) -> Result<ByteCharge, Error> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .bytes
            .budget
            .reserve(length, 0)
    }
}

#[cfg(test)]
mod tests;
