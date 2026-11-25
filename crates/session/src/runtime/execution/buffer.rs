//! Runtime-owned output. Guest/native handles never borrow producer memory.
use super::{
    ExecutionId, ExecutionRegistry, OutputChunk, OutputMetadata, OutputOptions, OutputPage,
    PrivateOutputKind, PrivateOutputMetadata, ToolFailure,
};
use crate::Error;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::{self, Write},
    sync::{Arc, Weak},
};
use tokio_util::sync::CancellationToken;

pub(crate) const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;

// Paging needs JSON's exact escaping cost, but never the serialized scratch bytes.
#[derive(Default)]
struct JsonSize(usize);
impl Write for JsonSize {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn serialized_size(value: &impl Serialize) -> Result<usize, serde_json::Error> {
    let mut size = JsonSize::default();
    serde_json::to_writer(&mut size, value)?;
    Ok(size.0)
}
fn cursor_digits(cursor: u64) -> usize {
    cursor
        .checked_ilog10()
        .map_or(1, |digits| digits as usize + 1)
}
fn set_prefix(target: &mut String, prefix: &str) {
    if prefix.len() < target.len() {
        target.truncate(prefix.len());
    } else {
        target.push_str(&prefix[target.len()..]);
    }
}

pub struct BufferedOutput {
    pub(super) registry: Arc<ExecutionRegistry>,
    pub(super) id: ExecutionId,
    pub(super) nonce: u64,
    pub(super) adopted: bool,
    pub(super) include_output: bool,
    pub(super) on_adopt: Option<Box<dyn FnOnce() + Send + Sync>>,
}
impl std::fmt::Debug for BufferedOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferedOutput")
            .field("execution_id", &self.id)
            .finish_non_exhaustive()
    }
}
impl BufferedOutput {
    pub fn execution_id(&self) -> ExecutionId {
        self.id
    }
    pub fn with_initial_output(mut self, include: bool) -> Self {
        self.include_output = include;
        self
    }
    pub(crate) fn start_on_adoption(&mut self, start: impl FnOnce() + Send + Sync + 'static) {
        debug_assert!(self.on_adopt.is_none());
        self.on_adopt = Some(Box::new(start));
    }
    pub fn snapshot(&self, max_bytes: usize) -> Result<OutputPage, Error> {
        self.registry.read(self.id, 0, max_bytes)
    }
    pub async fn wait_for_change(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<u64, Error> {
        self.registry
            .wait_for_execution_change(self.id, Some(revision), cancellation)
            .await
            .map(|(revision, _)| revision)
    }
}
impl Drop for BufferedOutput {
    fn drop(&mut self) {
        if !self.adopted {
            self.registry.abandon(self.id, self.nonce);
        }
    }
}

struct WriterLease {
    registry: Weak<ExecutionRegistry>,
    id: ExecutionId,
    nonce: u64,
}
impl Drop for WriterLease {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let _ = registry.interrupt_producer(
                self.id,
                self.nonce,
                "Output producer ended without reporting its outcome".into(),
                None,
            );
        }
    }
}
#[derive(Clone)]
pub struct OutputWriter {
    lease: Arc<WriterLease>,
}
impl OutputWriter {
    pub(super) fn new(registry: &Arc<ExecutionRegistry>, id: ExecutionId, nonce: u64) -> Self {
        Self {
            lease: Arc::new(WriterLease {
                registry: Arc::downgrade(registry),
                id,
                nonce,
            }),
        }
    }
    pub(crate) fn belongs_to(&self, registry: &Arc<ExecutionRegistry>, id: ExecutionId) -> bool {
        self.lease.id == id
            && self
                .lease
                .registry
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, registry))
    }
    pub fn execution_id(&self) -> ExecutionId {
        self.lease.id
    }
    pub fn append(&self, channel: Option<&str>, content: &str) -> Result<(), Error> {
        self.lease.registry.upgrade().ok_or(Error::Closed)?.append(
            self.lease.id,
            self.lease.nonce,
            channel,
            content,
        )
    }
    pub(super) fn append_private(&self, bytes: &[u8]) -> Result<(), Error> {
        self.lease
            .registry
            .upgrade()
            .ok_or(Error::Closed)?
            .append_private(self.lease.id, self.lease.nonce, bytes)
    }
    pub fn mark_truncated(&self) -> Result<(), Error> {
        self.lease
            .registry
            .upgrade()
            .ok_or(Error::Closed)?
            .mark_truncated(self.lease.id, self.lease.nonce)
    }
    pub fn confirm_cancelled(&self) -> Result<(), Error> {
        self.lease
            .registry
            .upgrade()
            .ok_or(Error::Closed)?
            .confirm_cancelled(self.lease.id, self.lease.nonce)
    }
    pub fn interrupt(&self, message: impl Into<String>) -> Result<(), Error> {
        self.lease
            .registry
            .upgrade()
            .ok_or(Error::Closed)?
            .interrupt_producer(self.lease.id, self.lease.nonce, message.into(), None)
    }
    #[cfg(feature = "wasm")]
    pub(crate) fn interrupt_wasm(&self, error: &Error) -> Result<(), Error> {
        self.lease.registry.upgrade().ok_or(Error::Closed)?.interrupt_producer(
            self.lease.id, self.lease.nonce, String::new(), Some(error.tool_diagnostic()),
        )
    }
    pub fn finish(&self, outcome: Result<(), ToolFailure>) -> Result<(), Error> {
        self.lease
            .registry
            .upgrade()
            .ok_or(Error::Closed)?
            .finish_producer(self.lease.id, self.lease.nonce, outcome)
    }
}

/// An exact, completed private source. This is trusted Core memory, not a wire
/// value. Clones share the original raster and retain its allocation permit.
#[derive(Clone)]
pub struct PrivateOutputLease {
    media_type: String,
    backing: Arc<PrivateOutputBacking>,
}
impl std::fmt::Debug for PrivateOutputLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateOutputLease").finish_non_exhaustive()
    }
}
impl PrivateOutputLease {
    pub fn media_type(&self) -> &str {
        &self.media_type
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.backing.bytes
    }
}
struct PrivateOutputBacking {
    bytes: Vec<u8>,
    // One slot reserves the full per-source byte budget even after release while
    // an admitted reader/variable still holds the shared backing.
    _allocation: tokio::sync::OwnedSemaphorePermit,
}
struct PrivateOutput {
    media_type: String,
    end: u64,
    truncated: bool,
    backing: Option<Arc<PrivateOutputBacking>>,
}

// Parallel byte/tag rings bound storage even for one-byte alternating channels.
// Tags belong to UTF-8 scalars; public chunks are projected only for a bounded page.
pub(super) struct Buffer {
    redaction: super::InvocationRedaction,
    stream: super::InvocationRedactionStream,
    pub channels: Vec<String>,
    pub declared: bool,
    pub end: u64,
    pub start: u64,
    pub closed: bool,
    pub limit: usize,
    content: VecDeque<u8>,
    tags: VecDeque<u8>,
    loss_marked_at_tail: bool,
    private: Option<PrivateOutput>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedRing {
    encoding: String,
    content_base64: String,
    tags_base64: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    withheld_redaction_tail: Option<bool>,
}
const MARKER_TAG: u8 = 17;
const CHUNK_BYTES: usize = 64 * 1024;
impl Buffer {
    pub fn new(options: OutputOptions, limit: usize) -> Result<Self, Error> {
        if options.channels.len() > 16
            || options
                .channels
                .iter()
                .any(|s| s.is_empty() || s.len() > 64)
        {
            return Err(Error::tool("invalid output channels"));
        }
        let mut channels = options.channels;
        channels.sort();
        channels.dedup();
        Ok(Self {
            redaction: super::InvocationRedaction::default(),
            stream: super::InvocationRedaction::default().stream(),
            declared: !channels.is_empty(),
            channels,
            end: 0,
            start: 0,
            closed: false,
            limit,
            content: VecDeque::new(),
            tags: VecDeque::new(),
            loss_marked_at_tail: false,
            private: None,
        })
    }
    pub(super) fn install_private(
        &mut self,
        media_type: String,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), Error> {
        if !microcrab_agent_types::is_capture_mime(&media_type)
            || self.redaction.redact_text(&media_type) != media_type
        {
            return Err(Error::tool("invalid private capture MIME"));
        }
        self.private = Some(PrivateOutput {
            media_type,
            end: 0,
            truncated: false,
            backing: Some(Arc::new(PrivateOutputBacking {
                bytes: Vec::new(),
                _allocation: permit,
            })),
        });
        Ok(())
    }
    pub(super) fn is_private(&self) -> bool {
        self.private.is_some()
    }
    pub(super) fn clear_private(&mut self) {
        if let Some(private) = &mut self.private {
            private.backing = None;
        }
    }
    pub(super) fn append_private(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.closed {
            return Err(Error::tool("output producer already completed"));
        }
        let private = self
            .private
            .as_mut()
            .ok_or_else(|| Error::tool("output is not private"))?;
        private.end = private.end.checked_add(bytes.len() as u64).ok_or_else(|| {
            crate::budget::reject(
                crate::LimitKind::Arithmetic,
                "private output cursor overflow",
            )
        })?;
        if private.end > self.limit as u64 {
            // The incomplete raster cannot be redirected. Keep its retained
            // allocation until cleanup ACK, while continuing to drain and count.
            private.truncated = true;
        } else if !private.truncated {
            let backing = private
                .backing
                .as_mut()
                .and_then(Arc::get_mut)
                .ok_or_else(|| Error::tool("private output is unavailable"))?;
            let wanted = private.end as usize;
            if wanted > backing.bytes.capacity() {
                let target = wanted
                    .max(backing.bytes.capacity().saturating_mul(2))
                    .min(self.limit);
                backing
                    .bytes
                    .try_reserve_exact(target - backing.bytes.len())
                    .map_err(|_| Error::tool("private output allocation refused"))?;
            }
            backing.bytes.extend_from_slice(bytes);
        }
        Ok(())
    }
    pub(super) fn private_output(
        &self,
        channel: &str,
        expected_mime: &str,
    ) -> Result<PrivateOutputLease, Error> {
        let private = self
            .private
            .as_ref()
            .ok_or_else(|| Error::tool("execution has no private byte channel"))?;
        if channel != "stdout" {
            return Err(Error::tool("private output channel does not match"));
        }
        if private.media_type != expected_mime {
            return Err(Error::tool("private output MIME does not match"));
        }
        if !self.closed || private.truncated {
            return Err(Error::tool(
                "private output capture is incomplete or truncated",
            ));
        }
        let backing = private
            .backing
            .as_ref()
            .ok_or_else(|| Error::tool("private output is unavailable"))?;
        Ok(PrivateOutputLease {
            media_type: private.media_type.clone(),
            backing: backing.clone(),
        })
    }
    pub fn metadata(&self) -> OutputMetadata {
        OutputMetadata {
            channels: self.channels.clone(),
            bytes: self.end,
            start_cursor: self.start,
            end_cursor: self.end,
            closed: self.closed,
            private_stdout: self.private_metadata(),
        }
    }
    fn private_metadata(&self) -> Option<PrivateOutputMetadata> {
        self.private.as_ref().map(|private| {
            let available = private.backing.is_some() && !private.truncated;
            PrivateOutputMetadata {
                kind: PrivateOutputKind::Bytes,
                channel: "stdout".into(),
                media_type: private.media_type.clone(),
                start_cursor: if available { 0 } else { private.end },
                end_cursor: private.end,
                closed: self.closed,
                truncated: private.truncated,
                available,
            }
        })
    }
    pub(super) fn recovery_metadata(&self) -> OutputMetadata {
        let mut metadata = self.metadata();
        if let Some(private) = &mut metadata.private_stdout {
            private.available = false;
            private.start_cursor = private.end_cursor;
        }
        metadata
    }
    pub fn clear(&mut self) {
        self.clear_private();
        self.content = VecDeque::new();
        self.tags = VecDeque::new();
        self.start = self.end;
        self.loss_marked_at_tail = false;
    }
    pub fn append(&mut self, channel: Option<&str>, content: &str) -> Result<(), Error> {
        if self.is_private() {
            return Err(Error::tool("private capture does not accept public output"));
        }
        if self.closed {
            return Err(Error::tool("output producer already completed"));
        }
        self.end.checked_add(content.len() as u64).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "output cursor overflow")
        })?;
        let _tag = if let Some(channel) = channel {
            if channel.is_empty() || channel.len() > 64 {
                return Err(Error::tool("invalid output channel"));
            }
            if self.redaction.redact_text(channel) != channel {
                return Err(Error::tool("output channel contains a secret"));
            }
            let index = match self.channels.iter().position(|s| s == channel) {
                Some(index) => index,
                None => {
                    if self.declared || self.channels.len() >= 16 {
                        return Err(Error::tool("undeclared output channel"));
                    }
                    self.channels.push(channel.into());
                    self.channels.len() - 1
                }
            };
            (index + 1) as u8
        } else {
            0
        };
        if content.is_empty() {
            return Ok(());
        }
        let mut remaining = content;
        while !remaining.is_empty() {
            let mut width = remaining.len().min(16 * 1024);
            while !remaining.is_char_boundary(width) {
                width -= 1;
            }
            let safe = self.stream.push_ordered(channel, &remaining[..width])?;
            self.append_projected(safe)?;
            remaining = &remaining[width..];
        }
        Ok(())
    }
    pub(super) fn install_redaction(&mut self, redaction: super::InvocationRedaction) {
        self.stream = redaction.stream();
        self.redaction = redaction;
    }
    pub(super) fn flush_redaction(&mut self) -> Result<(), Error> {
        let safe = self.stream.finish_all()?;
        self.append_projected(safe)
    }
    fn append_projected(&mut self, chunks: Vec<(Option<String>, String)>) -> Result<(), Error> {
        for (channel, text) in chunks {
            let tag = match channel {
                Some(channel) => self
                    .channels
                    .iter()
                    .position(|known| known == &channel)
                    .map(|index| (index + 1) as u8)
                    .ok_or_else(|| Error::tool("redacted output references an unknown channel"))?,
                None => 0,
            };
            self.append_tag(tag, &text)?;
            self.loss_marked_at_tail = false;
        }
        Ok(())
    }
    fn append_tag(&mut self, tag: u8, content: &str) -> Result<(), Error> {
        if self.closed {
            return Err(Error::tool("output producer already completed"));
        }
        let end = self.end.checked_add(content.len() as u64).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "output cursor overflow")
        })?;
        let mut begin = content.len().saturating_sub(self.limit);
        while !content.is_char_boundary(begin) {
            begin += 1;
        }
        if begin > 0 {
            self.clear();
        }
        let retained = &content.as_bytes()[begin..];
        // Evict before growing either ring, including a partial UTF-8 front scalar.
        let mut remove = self
            .content
            .len()
            .saturating_add(retained.len())
            .saturating_sub(self.limit);
        while remove < self.content.len() && self.content[remove] & 0xc0 == 0x80 {
            remove += 1;
        }
        self.content.drain(..remove);
        self.tags.drain(..remove);
        self.content.extend(retained.iter().copied());
        self.tags.extend(std::iter::repeat_n(tag, retained.len()));
        self.end = end;
        self.start = end - self.content.len() as u64;
        Ok(())
    }
    pub fn mark_truncated(&mut self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::tool("output producer already completed"));
        }
        if let Some(private) = &mut self.private {
            private.truncated = true;
            return Ok(());
        }
        if self.loss_marked_at_tail {
            return Ok(());
        }
        let marker = if self.content.back().is_some_and(|byte| *byte != b'\n') {
            "\n[upstream lost]\n"
        } else {
            "[upstream lost]\n"
        };
        self.append_tag(MARKER_TAG, marker)?;
        self.loss_marked_at_tail = true;
        Ok(())
    }
    pub fn persisted(&self) -> String {
        debug_assert!(!self.is_private());
        // Checkpoints may make bounded temporary copies; pages never expand all runs.
        let content: Vec<u8> = self.content.iter().copied().collect();
        let tags: Vec<u8> = self.tags.iter().copied().collect();
        serde_json::to_string(&PersistedRing {
            encoding: "ring-v1".into(),
            content_base64: STANDARD.encode(content),
            tags_base64: STANDARD.encode(tags),
            withheld_redaction_tail: self.stream.has_withheld_tail().then_some(true),
        })
        .expect("ring persistence contains only serializable strings")
    }
    pub fn restore(
        output: &OutputMetadata,
        result: Option<&str>,
        limit: usize,
    ) -> Result<Self, Error> {
        let retained = output
            .end_cursor
            .checked_sub(output.start_cursor)
            .filter(|n| *n <= limit as u64)
            .ok_or_else(|| Error::tool("invalid persisted output retention"))?;
        if output.bytes != output.end_cursor {
            return Err(Error::tool("invalid persisted output byte count"));
        }
        let mut ring = Self::new(
            OutputOptions {
                channels: output.channels.clone(),
            },
            limit,
        )?;
        if ring.channels.len() != output.channels.len() {
            return Err(Error::tool("duplicate persisted output channel"));
        }
        // Tags index the serialized catalog, including dynamically discovered order.
        ring.channels = output.channels.clone();
        ring.start = output.start_cursor;
        ring.end = output.end_cursor;
        ring.closed = output.closed;
        if let Some(private) = &output.private_stdout {
            if private.channel != "stdout"
                || !microcrab_agent_types::is_capture_mime(&private.media_type)
                || private.start_cursor != private.end_cursor
                || private.available
                || private.closed != output.closed
                || !output.channels.is_empty()
                || output.end_cursor != 0
                || result.is_some()
            {
                return Err(Error::tool("invalid persisted private output metadata"));
            }
            // Private captures are always ephemerally lost at a history boundary.
            ring.private = Some(PrivateOutput {
                media_type: private.media_type.clone(),
                end: private.end_cursor,
                truncated: private.truncated,
                backing: None,
            });
        }
        let Some(result) = result else {
            if retained != 0 {
                return Err(Error::tool("persisted output is missing retained data"));
            }
            return Ok(ring);
        };
        let persisted: PersistedRing = serde_json::from_str(result).map_err(|error| {
            Error::Tool(
                format!(
                    "persisted ring-v1 output: {}",
                    microcrab_protocol::diagnostic::json_reason(&error)
                )
                .into(),
            )
        })?;
        let encoded_limit = (retained as usize)
            .div_ceil(3)
            .checked_mul(4)
            .ok_or_else(|| Error::tool("persisted output size overflow"))?;
        if persisted.encoding != "ring-v1"
            || persisted.content_base64.len() != encoded_limit
            || persisted.tags_base64.len() != encoded_limit
        {
            return Err(Error::tool("invalid persisted ring encoding or length"));
        }
        let content = STANDARD.decode(persisted.content_base64).map_err(|error| {
            Error::Tool(format!("persisted output content base64: {error}").into())
        })?;
        let tags = STANDARD.decode(persisted.tags_base64).map_err(|error| {
            Error::Tool(format!("persisted output tags base64: {error}").into())
        })?;
        if content.len() as u64 != retained || tags.len() != content.len() {
            return Err(Error::tool("invalid persisted output decoded length"));
        }
        let text = std::str::from_utf8(&content)
            .map_err(|_| Error::tool("persisted output is not UTF-8"))?;
        for (offset, character) in text.char_indices() {
            let tag = tags[offset];
            if (tag != 0 && tag != MARKER_TAG && usize::from(tag) > ring.channels.len())
                || tags[offset..offset + character.len_utf8()]
                    .iter()
                    .any(|other| *other != tag)
            {
                return Err(Error::tool(
                    "invalid persisted output channel tag or UTF-8 scalar boundary",
                ));
            }
        }
        ring.loss_marked_at_tail = tags.last() == Some(&MARKER_TAG);
        ring.content = content.into();
        ring.tags = tags.into();
        if let Some(withheld) = persisted.withheld_redaction_tail {
            if !withheld {
                return Err(Error::tool("invalid withheld redaction tail witness"));
            }
            ring.closed = false;
            ring.append_tag(MARKER_TAG, "[upstream lost: withheld redaction tail]")?;
            ring.loss_marked_at_tail = true;
            ring.closed = output.closed;
        }
        Ok(ring)
    }
    pub fn page(
        &self,
        id: ExecutionId,
        cursor: u64,
        maximum: usize,
        revision: u64,
    ) -> Result<OutputPage, Error> {
        self.page_capped(id, cursor, maximum, revision, None)
    }
    pub fn page_capped(
        &self,
        id: ExecutionId,
        cursor: u64,
        maximum: usize,
        revision: u64,
        content_limit: Option<usize>,
    ) -> Result<OutputPage, Error> {
        if maximum == 0 || maximum > MAX_PAGE_BYTES {
            return Err(Error::tool("invalid output response budget"));
        }
        if cursor > self.end {
            return Err(Error::tool("output cursor exceeds produced bytes"));
        }
        let actual = cursor.max(self.start);
        let mut offset = (actual - self.start) as usize;
        if offset < self.content.len() && self.content[offset] & 0xc0 == 0x80 {
            return Err(Error::tool("output cursor must be a UTF-8 boundary"));
        }
        let mut page = OutputPage {
            execution_id: id,
            chunks: Vec::new(),
            cursor: actual,
            next_cursor: actual,
            start_cursor: self.start,
            end_cursor: self.end,
            closed: self.closed,
            revision,
            private_stdout: self.private_metadata(),
        };
        let envelope_size = serialized_size(&page)?;
        crate::budget::check(crate::LimitKind::ToolOutput, envelope_size, maximum)?;
        let mut chunks_size = 0usize;
        let mut remaining = content_limit.unwrap_or(usize::MAX);
        while offset < self.content.len() && remaining > 0 {
            let tag = self.tags[offset];
            let mut end = offset;
            let ceiling = self
                .content
                .len()
                .min(offset.saturating_add(CHUNK_BYTES.min(maximum).min(remaining)));
            while end < ceiling && self.tags[end] == tag {
                end += 1;
            }
            while end > offset && end < self.content.len() && self.content[end] & 0xc0 == 0x80 {
                end -= 1;
            }
            if end == offset {
                break;
            }
            let bytes: Vec<u8> = self.content.range(offset..end).copied().collect();
            let text = std::str::from_utf8(&bytes).expect("ring retains complete UTF-8 scalars");
            let channel = if tag == 0 || tag == MARKER_TAG {
                None
            } else {
                Some(self.channels[usize::from(tag) - 1].clone())
            };
            let chunk_cursor = self.start + offset as u64;
            let mut copied = OutputChunk {
                channel,
                content: String::new(),
                cursor: chunk_cursor,
                next_cursor: chunk_cursor,
            };
            let mut low = 0;
            let mut high = text.len();
            while low < high {
                let probe = (low + high).div_ceil(2);
                let mut length = probe;
                while !text.is_char_boundary(length) {
                    length -= 1;
                }
                set_prefix(&mut copied.content, &text[..length]);
                copied.next_cursor = chunk_cursor + length as u64;
                let size = envelope_size
                    .saturating_add(chunks_size)
                    .saturating_add(usize::from(!page.chunks.is_empty()))
                    .saturating_add(serialized_size(&copied)?)
                    .saturating_add(cursor_digits(copied.next_cursor) - cursor_digits(actual));
                if size <= maximum {
                    low = probe;
                } else {
                    high = probe - 1;
                }
            }
            while !text.is_char_boundary(low) {
                low -= 1;
            }
            if low == 0 {
                break;
            }
            set_prefix(&mut copied.content, &text[..low]);
            copied.next_cursor = chunk_cursor + low as u64;
            chunks_size = chunks_size
                .saturating_add(usize::from(!page.chunks.is_empty()))
                .saturating_add(serialized_size(&copied)?);
            page.next_cursor = copied.next_cursor;
            page.chunks.push(copied);
            offset += low;
            remaining -= low;
            if low < text.len() {
                break;
            }
        }
        Ok(page)
    }
}
#[cfg(test)]
#[path = "tests/ring.rs"]
mod ring_tests;
