//! Reusable incremental Server-Sent Events (SSE) frame reader.
use crate::error::Error;

#[derive(Default)]
pub(crate) struct SseFrameReader {
    pending: Vec<u8>,
    event_type: Option<String>,
    data_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SseFrame {
    pub event: Option<String>,
    pub data: String,
}

impl SseFrameReader {
    pub fn push(&mut self, bytes: &[u8], eof: bool) -> Result<Vec<SseFrame>, Error> {
        self.pending.extend_from_slice(bytes);
        let mut frames = Vec::new();
        let mut lines = Vec::new();
        let mut consumed = 0;
        loop {
            let rest = &self.pending[consumed..];
            let Some(end) = rest.iter().position(|byte| matches!(byte, b'\r' | b'\n')) else {
                break;
            };
            if rest[end] == b'\r' && end + 1 == rest.len() && !eof {
                break;
            }
            let width = if rest[end] == b'\r' && rest.get(end + 1) == Some(&b'\n') {
                2
            } else {
                1
            };
            let line = std::str::from_utf8(&rest[..end]).map_err(|error| {
                Error::decode_reason(format!("SSE UTF-8 decoding failed: {error}"))
            })?;
            lines.push(line.to_string());
            consumed += end + width;
        }
        self.pending.drain(..consumed);
        for line in lines {
            if let Some(frame) = self.process_line(&line)? {
                frames.push(frame);
            }
        }
        if eof && (!self.pending.is_empty() || !self.data_lines.is_empty()) {
            return Err(Error::decode_reason(
                "SSE ended with an incomplete line or event",
            ));
        }
        Ok(frames)
    }

    fn process_line(&mut self, line: &str) -> Result<Option<SseFrame>, Error> {
        if line.is_empty() {
            // SSE dispatches only events with a data field; event-only heartbeats
            // reset framing state without manufacturing a model event.
            if self.data_lines.is_empty() {
                self.event_type = None;
                return Ok(None);
            }
            let event = self.event_type.take();
            let data = std::mem::take(&mut self.data_lines).join("\n");
            return Ok(Some(SseFrame { event, data }));
        }
        if line.starts_with(':') {
            return Ok(None);
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => {
                self.event_type = Some(value.to_owned());
            }
            "data" => {
                self.data_lines.push(value.to_owned());
            }
            _ => {}
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sse_frames() {
        let mut reader = SseFrameReader::default();
        let chunk1 = b"event: message_start\r\ndata: {\"type\":\"start\"}\r\n\r\n";
        let frames = reader.push(chunk1, false).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event.as_deref(), Some("message_start"));
        assert_eq!(frames[0].data, "{\"type\":\"start\"}");

        let chunk2 = b"data: part 1\ndata: part 2\n\n";
        let frames2 = reader.push(chunk2, false).unwrap();
        assert_eq!(frames2.len(), 1);
        assert_eq!(frames2[0].event, None);
        assert_eq!(frames2[0].data, "part 1\npart 2");

        let frames_eof = reader.push(&[], true).unwrap();
        assert!(frames_eof.is_empty());
    }
}
