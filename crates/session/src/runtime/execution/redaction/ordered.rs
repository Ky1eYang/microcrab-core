//! Source-order runs awaiting a complete per-channel literal decision.
use super::{Error, InvocationRedaction};
struct PendingRun {
    channel: Option<String>,
    raw: String,
    consumed: usize,
    safe: String,
}
const MAX_PENDING_RAW: usize = 256 * 1024;
const MAX_PENDING_RUNS: usize = 4096;
const MAX_PENDING_TOTAL: usize = 4 * 1024 * 1024;

pub struct InvocationRedactionStream {
    owner: InvocationRedaction,
    channels: std::collections::BTreeSet<Option<String>>,
    runs: std::collections::VecDeque<PendingRun>,
    capture_lost: bool,
}
impl std::fmt::Debug for InvocationRedactionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvocationRedactionStream")
            .finish_non_exhaustive()
    }
}
impl InvocationRedactionStream {
    pub(super) fn new(owner: InvocationRedaction) -> Self {
        Self {
            owner,
            channels: std::collections::BTreeSet::new(),
            runs: std::collections::VecDeque::new(),
            capture_lost: false,
        }
    }
    /// Single-channel convenience. Mixed streams must preserve returned tags via
    /// push_ordered; reject before accepting a second channel here.
    pub fn push(&mut self, channel: Option<&str>, text: &str) -> Result<String, Error> {
        if self
            .channels
            .iter()
            .any(|known| known.as_deref() != channel)
        {
            return Err(Error::tool(
                "mixed output channels require ordered redaction",
            ));
        }
        Ok(self
            .push_ordered(channel, text)?
            .into_iter()
            .map(|(_, text)| text)
            .collect())
    }
    pub fn push_ordered(
        &mut self,
        channel: Option<&str>,
        text: &str,
    ) -> Result<Vec<(Option<String>, String)>, Error> {
        if channel.is_some_and(|value| value.is_empty() || value.len() > 64) {
            return Err(Error::tool("invalid output channel"));
        }
        if channel.is_some_and(|value| self.owner.redact_text(value) != value) {
            return Err(Error::tool("output channel contains a secret"));
        }
        let key = channel.map(str::to_owned);
        if !self.channels.contains(&key) && self.channels.len() >= 17 {
            return Err(Error::tool("too many redacted output channels"));
        }
        if self.runs.is_empty()
            && self
                .owner
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .matcher
                .is_none()
        {
            self.channels.insert(key.clone());
            return Ok(if text.is_empty() {
                Vec::new()
            } else {
                vec![(key, text.to_owned())]
            });
        }
        if self
            .runs
            .iter()
            .map(|run| run.raw.len())
            .sum::<usize>()
            .saturating_add(text.len())
            > MAX_PENDING_RAW
        {
            self.capture_lost = true;
            return Err(Error::tool(
                "redacted output pending source capacity exceeded; capture may have been lost",
            ));
        }
        if self.runs.len() >= MAX_PENDING_RUNS {
            self.capture_lost = true;
            return Err(Error::tool(
                "redacted output pending run capacity exceeded; capture may have been lost",
            ));
        }
        self.channels.insert(key.clone());
        if !text.is_empty() {
            if let Some(last) = self.runs.back_mut().filter(|run| run.channel == key) {
                last.raw.push_str(text);
            } else {
                self.runs.push_back(PendingRun {
                    channel: key.clone(),
                    raw: text.to_owned(),
                    consumed: 0,
                    safe: String::new(),
                });
            }
        }
        self.project_channel(&key, false)?;
        Ok(self.publish_ready())
    }
    fn project_channel(&mut self, channel: &Option<String>, finish: bool) -> Result<(), Error> {
        let text = self
            .runs
            .iter()
            .filter(|run| &run.channel == channel)
            .map(|run| run.raw.as_str())
            .collect::<String>();
        if text.is_empty() {
            return Ok(());
        }
        let owner = self.owner.clone();
        let witnesses = owner.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut cutoff = if finish || witnesses.matcher.is_none() {
            text.len()
        } else {
            text.len()
                .saturating_sub(witnesses.max_value_bytes.saturating_sub(1))
        };
        while !text.is_char_boundary(cutoff) {
            cutoff -= 1;
        }
        let mut offset = 0;
        let mut run_cursor = 0;
        if let Some(matcher) = &witnesses.matcher {
            for matched in matcher.find_iter(&text) {
                if matched.start() >= cutoff {
                    break;
                }
                self.apply_fragment(channel, matched.start() - offset, None, &mut run_cursor);
                let pattern = matched.pattern().as_usize();
                self.apply_fragment(
                    channel,
                    matched.len(),
                    if pattern < witnesses.values.len() {
                        Some(&witnesses.values[pattern].1)
                    } else {
                        None
                    },
                    &mut run_cursor,
                );
                offset = matched.end();
            }
        }
        self.apply_fragment(channel, cutoff.max(offset) - offset, None, &mut run_cursor);
        for run in self.runs.iter_mut().filter(|run| &run.channel == channel) {
            if run.consumed != 0 {
                run.raw.drain(..run.consumed);
                run.consumed = 0;
            }
        }
        if self.pending_bytes() > MAX_PENDING_TOTAL {
            self.runs.clear();
            self.capture_lost = true;
            return Err(Error::tool(
                "redacted output pending projection capacity exceeded; capture may have been lost",
            ));
        }
        Ok(())
    }
    fn apply_fragment(
        &mut self,
        channel: &Option<String>,
        mut bytes: usize,
        replacement: Option<&str>,
        cursor: &mut usize,
    ) {
        let mut inserted = false;
        while bytes != 0 && *cursor < self.runs.len() {
            let run = &mut self.runs[*cursor];
            if &run.channel != channel || run.consumed == run.raw.len() {
                *cursor += 1;
                continue;
            }
            let take = bytes.min(run.raw.len() - run.consumed);
            if let Some(replacement) = replacement {
                if !inserted {
                    run.safe.push_str(replacement);
                    inserted = true;
                }
            } else {
                run.safe
                    .push_str(&run.raw[run.consumed..run.consumed + take]);
            }
            run.consumed += take;
            bytes -= take;
        }
    }
    fn publish_ready(&mut self) -> Vec<(Option<String>, String)> {
        let mut output = Vec::new();
        while let Some(run) = self.runs.front_mut() {
            if !run.safe.is_empty() {
                output.push((run.channel.clone(), std::mem::take(&mut run.safe)));
            }
            if !run.raw.is_empty() {
                break;
            }
            self.runs.pop_front();
        }
        output
    }
    pub(crate) fn has_withheld_tail(&self) -> bool {
        self.capture_lost || self.pending_bytes() != 0
    }
    pub(crate) fn pending_bytes(&self) -> usize {
        self.runs
            .iter()
            .map(|run| run.raw.len() + run.safe.len())
            .sum()
    }
    pub fn finish_all(&mut self) -> Result<Vec<(Option<String>, String)>, Error> {
        for channel in self.channels.clone() {
            self.project_channel(&channel, true)?;
        }
        Ok(self.publish_ready())
    }
    pub fn finish(&mut self, channel: Option<&str>) -> Result<String, Error> {
        if self
            .channels
            .iter()
            .any(|known| known.as_deref() != channel)
        {
            return Err(Error::tool(
                "mixed output channels require ordered redaction",
            ));
        }
        Ok(self
            .finish_all()?
            .into_iter()
            .map(|(_, text)| text)
            .collect())
    }
}
