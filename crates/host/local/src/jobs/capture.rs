//! Single Host-observed stream order. Logical lines split at newlines, stream
//! switches and the byte cap; no unbounded unfinished physical line is retained.
#[cfg(test)]
mod tests;
use microcrab_host_api::{ErrorCategory, ExpiredRange, HostError, ShellOutput, ShellPosition};
use std::collections::VecDeque;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Stream {
    Stdout,
    Stderr,
}
struct Line {
    number: u64,
    stream: Stream,
    text: String,
    complete: bool,
}
pub(super) struct Capture {
    lines: VecDeque<Line>,
    limit: usize,
    bytes: usize,
    next_line: u64,
    cursor: ShellPosition,
    expired_through: u64,
    truncated: bool,
}
impl Capture {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            limit,
            bytes: 0,
            next_line: 1,
            cursor: ShellPosition::default(),
            expired_through: 0,
            truncated: false,
        }
    }
    pub(super) fn append(&mut self, stream: Stream, text: &str) {
        for character in text.chars() {
            let width = character.len_utf8();
            let append = self.lines.back().is_some_and(|line| {
                !line.complete && line.stream == stream && line.text.len() + width <= self.limit
            });
            if !append {
                self.seal();
                self.lines.push_back(Line {
                    number: self.next_line,
                    stream,
                    text: String::new(),
                    complete: false,
                });
                self.next_line += 1;
            }
            let line = self
                .lines
                .back_mut()
                .expect("append installs a logical line");
            line.text.push(character);
            line.complete = character == '\n' || line.text.len() >= self.limit;
            self.bytes += width;
            while self.bytes > self.limit {
                // A single scalar may exceed a very small configured cap. Its
                // entire fragment expires, preserving UTF-8 and the memory bound.
                let line = self.lines.pop_front().expect("excess bytes have a line");
                self.bytes -= line.text.len();
                self.expired_through = line.number;
                self.truncated = true;
            }
        }
    }
    fn seal(&mut self) {
        if let Some(line) = self.lines.back_mut() {
            line.complete = true;
        }
    }
    pub(super) fn finish(&mut self) {
        self.seal();
    }
    pub(super) fn cursor(&self) -> ShellPosition {
        self.cursor
    }
    pub(super) fn truncated(&self) -> bool {
        self.truncated
    }
    pub(super) fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }
    pub(super) fn counts(&self) -> (usize, usize) {
        self.lines
            .iter()
            .fold((0, 0), |(out, err), line| match line.stream {
                Stream::Stdout => (out + line.text.len(), err),
                Stream::Stderr => (out, err + line.text.len()),
            })
    }
    fn end(&self) -> ShellPosition {
        match self.lines.back() {
            Some(line) if !line.complete => ShellPosition {
                line: line.number,
                column: line.text.len(),
            },
            _ => ShellPosition {
                line: self.next_line,
                column: 0,
            },
        }
    }
    pub(super) fn read(&self, from: Option<ShellPosition>) -> Result<ShellOutput, HostError> {
        let mut from = from.unwrap_or(self.cursor);
        if from.line == 0 {
            return Err(HostError::known(
                ErrorCategory::Conflict,
                "line must be at least 1",
            ));
        }
        let expired = if from.line <= self.expired_through {
            from = ShellPosition {
                line: self
                    .lines
                    .front()
                    .map_or(self.next_line, |line| line.number),
                column: 0,
            };
            Some(ExpiredRange {
                through_line: self.expired_through,
            })
        } else {
            None
        };
        let end = self.end();
        if from.line > end.line || (from.line == end.line && from.column > end.column) {
            return Err(HostError::known(
                ErrorCategory::Conflict,
                "position is beyond captured output",
            ));
        }
        let mut stdout = String::new();
        let mut stderr = String::new();
        for line in &self.lines {
            if line.number < from.line {
                continue;
            }
            let column = if line.number == from.line {
                from.column
            } else {
                0
            };
            if column > line.text.len() || !line.text.is_char_boundary(column) {
                return Err(HostError::known(
                    ErrorCategory::Conflict,
                    "column must be a UTF-8 byte boundary within the line",
                ));
            }
            match line.stream {
                Stream::Stdout => stdout.push_str(&line.text[column..]),
                Stream::Stderr => stderr.push_str(&line.text[column..]),
            }
        }
        // A consumed line is no longer retained even though the absolute cursor
        // still names its end. Any earlier position has expired on drain.
        if from.line == end.line && from.column != end.column && self.lines.is_empty() {
            return Err(HostError::known(
                ErrorCategory::Conflict,
                "column is beyond captured output",
            ));
        }
        Ok(ShellOutput {
            stdout,
            stderr,
            next: end,
            cursor: self.cursor,
            expired,
            truncated: self.truncated,
        })
    }
    pub(super) fn drain(&mut self) -> Result<ShellOutput, HostError> {
        self.seal();
        let mut output = self.read(None)?;
        if let Some(last) = self.lines.back() {
            self.expired_through = self.expired_through.max(last.number);
        }
        self.lines.clear();
        self.bytes = 0;
        self.cursor = output.next;
        output.cursor = self.cursor;
        Ok(output)
    }
}

/// Keep an incomplete UTF-8 scalar across pipe reads. Invalid sequences use the
/// same replacement decoding as the existing blocking shell.
#[derive(Default)]
pub(super) struct Decoder {
    pending: Vec<u8>,
}
impl Decoder {
    pub(super) fn push(&mut self, bytes: &[u8], eof: bool) -> String {
        self.pending.extend_from_slice(bytes);
        let mut output = String::new();
        let mut offset = 0;
        while offset < self.pending.len() {
            match std::str::from_utf8(&self.pending[offset..]) {
                Ok(text) => {
                    output.push_str(text);
                    offset = self.pending.len();
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    // The decoder's validated prefix is always UTF-8.
                    output.push_str(
                        std::str::from_utf8(&self.pending[offset..offset + valid])
                            .expect("UTF-8 decoder prefix is valid"),
                    );
                    offset += valid;
                    match error.error_len() {
                        Some(length) => {
                            output.push('\u{fffd}');
                            offset += length;
                        }
                        None if eof => {
                            output.push('\u{fffd}');
                            offset = self.pending.len();
                        }
                        None => break,
                    }
                }
            }
        }
        self.pending.drain(..offset);
        output
    }
}
