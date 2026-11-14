//! Runtime-owned output. Guest/native handles never borrow producer memory.
use super::{
    ExecutionId, ExecutionRegistry, OutputChunk, OutputMetadata, OutputOptions, OutputPage,
    ToolFailure,
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
            .interrupt_producer(self.lease.id, self.lease.nonce, message.into())
    }
    pub fn finish(&self, outcome: Result<(), ToolFailure>) -> Result<(), Error> {
        self.lease
            .registry
            .upgrade()
            .ok_or(Error::Closed)?
            .finish_producer(self.lease.id, self.lease.nonce, outcome)
    }
}

// Parallel byte/tag rings bound storage even for one-byte alternating channels.
// Tags belong to UTF-8 scalars; public chunks are projected only for a bounded page.
pub(super) struct Buffer {
    pub channels: Vec<String>,
    pub declared: bool,
    pub end: u64,
    pub start: u64,
    pub closed: bool,
    pub limit: usize,
    content: VecDeque<u8>,
    tags: VecDeque<u8>,
    loss_marked_at_tail: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedRing {
    encoding: String,
    content_base64: String,
    tags_base64: String,
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
            declared: !channels.is_empty(),
            channels,
            end: 0,
            start: 0,
            closed: false,
            limit,
            content: VecDeque::new(),
            tags: VecDeque::new(),
            loss_marked_at_tail: false,
        })
    }
    pub fn metadata(&self) -> OutputMetadata {
        OutputMetadata {
            channels: self.channels.clone(),
            bytes: self.end,
            start_cursor: self.start,
            end_cursor: self.end,
            closed: self.closed,
        }
    }
    pub fn clear(&mut self) {
        self.content = VecDeque::new();
        self.tags = VecDeque::new();
        self.start = self.end;
        self.loss_marked_at_tail = false;
    }
    pub fn append(&mut self, channel: Option<&str>, content: &str) -> Result<(), Error> {
        if self.closed {
            return Err(Error::tool("output producer already completed"));
        }
        self.end.checked_add(content.len() as u64).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "output cursor overflow")
        })?;
        let tag = if let Some(channel) = channel {
            if channel.is_empty() || channel.len() > 64 {
                return Err(Error::tool("invalid output channel"));
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
        self.append_tag(tag, content)?;
        self.loss_marked_at_tail = false;
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
        // Checkpoints may make bounded temporary copies; pages never expand all runs.
        let content: Vec<u8> = self.content.iter().copied().collect();
        let tags: Vec<u8> = self.tags.iter().copied().collect();
        serde_json::to_string(&PersistedRing {
            encoding: "ring-v1".into(),
            content_base64: STANDARD.encode(content),
            tags_base64: STANDARD.encode(tags),
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
