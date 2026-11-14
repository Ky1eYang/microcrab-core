//! Creator-owned complete display recording. This is never model Context or audit injection.
use microcrab_session::Error;
use microcrab_storage::display::{
    ArchiveRecord, DisplayGraph, DisplayHeadTransition, DisplaySource, DisplayStore,
};
use microcrab_transport::{ModelObservation, ModelRecording, ObservationState};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot};
mod graph;

pub type DisplayMetadata = Arc<dyn Fn(&ModelObservation) -> Value + Send + Sync>;
pub type DisplayCommitted = Arc<dyn Fn(ArchiveRecord) + Send + Sync>;
pub type DisplayFailed = Arc<dyn Fn(String) + Send + Sync>;

struct Attempt {
    metadata: Value,
    terminal_accepted: bool,
    dropped: Option<ModelObservation>,
}
#[derive(Default)]
struct State {
    attempts: HashMap<String, Attempt>,
    failure: Option<String>,
    closed: bool,
    close_result: Option<tokio::sync::watch::Receiver<Option<Result<(), String>>>>,
}
enum Command {
    Record {
        attempt: String,
        event: Value,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    Wake,
    Flush(oneshot::Sender<Result<(), String>>),
    Close(oneshot::Sender<Result<(), String>>),
}
/// The blocking writer owns accepted commands even if their caller is cancelled.
/// A Drop terminal lives in the registered attempt slot, not a lossy body queue.
pub struct DisplayRecorder {
    sender: mpsc::Sender<Command>,
    state: Arc<Mutex<State>>,
    metadata: DisplayMetadata,
}
impl DisplayRecorder {
    pub async fn open(
        source: DisplaySource,
        metadata: DisplayMetadata,
        committed: DisplayCommitted,
        failed: DisplayFailed,
    ) -> Result<Arc<Self>, Error> {
        let (sender, receiver) = mpsc::channel(16);
        let state = Arc::new(Mutex::new(State::default()));
        let writer_state = state.clone();
        let (opened, ready) = oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let opened_store = source.open().and_then(|mut store| {
                let sequence = store.revision()?;
                let graph = source
                    .graph_at(Some(sequence))?
                    .ok_or_else(|| Error::audit("display archive disappeared during reopen"))?;
                let offsets = graph::offsets(&source, sequence)?;
                Ok((store, sequence, graph, offsets))
            });
            match opened_store {
                Ok((store, sequence, graph, offsets)) => {
                    let _ = opened.send(Ok(()));
                    Writer {
                        store,
                        sequence,
                        graph,
                        offsets,
                        state: writer_state,
                        committed,
                        failed,
                    }
                    .run(receiver);
                }
                Err(error) => {
                    let _ = opened.send(Err(error.to_string()));
                }
            }
        });
        ready
            .await
            .map_err(|_| Error::audit("display writer startup abandoned"))?
            .map_err(|error| Error::audit_with_private("display archive open failed", error))?;
        Ok(Arc::new(Self {
            sender,
            state,
            metadata,
        }))
    }
    /// Append owner-supplied admission/history certificates without Context mutation.
    pub async fn append(&self, attempt_id: String, event: Value) -> Result<u64, String> {
        let permit = self
            .sender
            .reserve()
            .await
            .map_err(|_| "display writer closed".to_owned())?;
        let (reply, result) = oneshot::channel();
        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(error) = &state.failure {
                return Err(error.clone());
            }
            if state.closed {
                return Err("display writer closed".into());
            }
            // Acceptance and sealing share one lock. A reserved permit alone is
            // not acceptance; no Record may be sent after Close is scheduled.
            permit.send(Command::Record {
                attempt: attempt_id,
                event,
                reply,
            });
        }
        result
            .await
            .map_err(|_| "display acknowledgement abandoned".to_owned())?
    }
    /// Select an existing display ancestor without editing any immutable node.
    /// The writer certifies the previous head atomically with this transition.
    pub async fn select_parent(&self, parent_block_id: Option<String>) -> Result<u64, String> {
        self.append(
            "display-selection".into(),
            serde_json::json!({
                "phase": "display_head_selected", "parent_block_id": parent_block_id,
            }),
        )
        .await
    }
    pub async fn checkpoint(&self, checkpoint_id: String) -> Result<u64, String> {
        self.append(
            "display-checkpoint".into(),
            serde_json::json!({
                "phase": "display_checkpoint", "checkpoint_id": checkpoint_id,
            }),
        )
        .await
    }
    pub async fn select_attempt_parent(&self, attempt_id: String) -> Result<u64, String> {
        self.append(
            attempt_id,
            serde_json::json!({
                "phase": "display_head_selected", "select_attempt_parent": true,
            }),
        )
        .await
    }
    /// Canonical DTO/reference bytes belong to display only, never model Context.
    pub async fn append_block(
        &self,
        attempt_id: String,
        block_type: String,
        content: Value,
    ) -> Result<u64, String> {
        self.append(
            attempt_id,
            serde_json::json!({
                "phase": "display_canonical", "block_type": block_type, "content": content,
            }),
        )
        .await
    }
    pub async fn flush(&self) -> Result<(), Error> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::Flush(reply))
            .await
            .map_err(|_| Error::audit("display writer closed"))?;
        result
            .await
            .map_err(|_| Error::audit("display flush abandoned"))?
            .map_err(|error| Error::audit_with_private("display archive flush failed", error))
    }
    pub async fn close(&self) -> Result<(), Error> {
        let mut completion = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(completion) = &state.close_result {
                completion.clone()
            } else {
                state.closed = true;
                let (finished, completion) = tokio::sync::watch::channel(None);
                state.close_result = Some(completion.clone());
                let sender = self.sender.clone();
                // The close owner survives a cancelled caller, including a full
                // command queue. Every caller awaits this same acknowledged result.
                tokio::spawn(async move {
                    let (reply, result) = oneshot::channel();
                    let outcome = match sender.send(Command::Close(reply)).await {
                        Ok(()) => result
                            .await
                            .unwrap_or_else(|_| Err("display close abandoned".into())),
                        Err(_) => Err("display writer closed".into()),
                    };
                    finished.send_replace(Some(outcome));
                });
                completion
            }
        };
        loop {
            let result = completion.borrow_and_update().clone();
            if let Some(result) = result {
                return result.map_err(|error| {
                    Error::audit_with_private("display archive close failed", error)
                });
            }
            completion
                .changed()
                .await
                .map_err(|_| Error::audit("display close owner ended"))?;
        }
    }
}
fn decorate(observation: &ModelObservation, metadata: &Value) -> Result<Value, String> {
    let mut value = serde_json::to_value(observation).map_err(|e| e.to_string())?;
    if let Some(fields) = metadata.as_object() {
        // Producer identity/phase/body are authoritative, not decorator overrides.
        for (key, field) in fields {
            if value.get(key).is_none() {
                value[key] = field.clone();
            }
        }
    }
    value["attempt_id"] = Value::String(observation.call_id.clone());
    Ok(value)
}
#[async_trait::async_trait]
impl ModelRecording for DisplayRecorder {
    async fn record(&self, observation: ModelObservation) -> Result<(), String> {
        // Helpers have no ordinary conversation placement authority.
        if observation.subagent.is_some() {
            return Ok(());
        }
        let terminal = matches!(
            observation.state,
            ObservationState::Validated { .. } | ObservationState::Invalidated { .. }
        );
        let metadata = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(error) = &state.failure {
                return Err(error.clone());
            }
            if state.closed {
                return Err("display writer closed".into());
            }
            let attempt = state
                .attempts
                .entry(observation.call_id.clone())
                .or_insert_with(|| Attempt {
                    metadata: (self.metadata)(&observation),
                    terminal_accepted: false,
                    dropped: None,
                });
            attempt.metadata.clone()
        };
        let event = decorate(&observation, &metadata)?;
        let permit = self
            .sender
            .reserve()
            .await
            .map_err(|_| "display writer closed".to_owned())?;
        let (reply, result) = oneshot::channel();
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(error) = &state.failure {
                return Err(error.clone());
            }
            if state.closed {
                return Err("display writer closed".into());
            }
            let attempt = state
                .attempts
                .get_mut(&observation.call_id)
                .ok_or("display attempt already finalized")?;
            if terminal {
                attempt.terminal_accepted = true;
                attempt.dropped = None;
            }
            permit.send(Command::Record {
                attempt: observation.call_id,
                event,
                reply,
            });
        }
        result
            .await
            .map_err(|_| "display acknowledgement abandoned".to_owned())??;
        Ok(())
    }
    fn dropped(&self, observation: ModelObservation) {
        if observation.subagent.is_some() {
            return;
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(attempt) = state.attempts.get_mut(&observation.call_id)
                && !attempt.terminal_accepted
            {
                attempt.dropped = Some(observation);
            }
        }
        // If full, the worker already has accepted work and drains terminal slots
        // after it. If empty, wake it; no content is placed in this wake command.
        let _ = self.sender.try_send(Command::Wake);
    }
}
struct Writer {
    store: Box<dyn DisplayStore>,
    sequence: u64,
    graph: DisplayGraph,
    offsets: HashMap<(String, String), u64>,
    state: Arc<Mutex<State>>,
    committed: DisplayCommitted,
    failed: DisplayFailed,
}
impl Writer {
    fn failure(&self, error: String) -> String {
        let first = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let first = state.failure.is_none();
            if first {
                state.failure = Some(error.clone());
            }
            first
        };
        if first {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (self.failed)(error.clone())
            }));
        }
        error
    }
    fn append(&mut self, attempt: String, mut event: Value) -> Result<u64, String> {
        if !event.is_object() {
            return Err("display event must be an object".into());
        }
        event["display_graph_version"] = Value::from(1);
        if let Some(error) = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .failure
            .clone()
        {
            return Err(error);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| self.failure("display sequence exhausted".into()))?;
        let terminal = matches!(event["phase"].as_str(), Some("validated" | "invalidated"));
        let stream = match event["phase"].as_str() {
            Some("reasoning_delta") => Some((
                "reasoning".to_owned(),
                event["text"].as_str().unwrap_or_default().len(),
            )),
            Some("text_delta") => Some((
                "text".to_owned(),
                event["text"].as_str().unwrap_or_default().len(),
            )),
            Some("tool_call_delta") => Some((
                format!("tool:{}", event["index"]),
                event["arguments"].as_str().unwrap_or_default().len(),
            )),
            _ => None,
        };
        let next_offset = if let Some((kind, bytes)) = stream {
            let key = (attempt.clone(), kind);
            let offset = self.offsets.get(&key).copied().unwrap_or(0);
            let end = offset
                .checked_add(bytes as u64)
                .ok_or_else(|| self.failure("display stream offset exhausted".into()))?;
            event["stream_offset"] = Value::from(offset);
            Some((key, end))
        } else {
            None
        };
        if terminal {
            event["reasoning_bytes"] = Value::from(
                self.offsets
                    .get(&(attempt.clone(), "reasoning".into()))
                    .copied()
                    .unwrap_or(0),
            );
            event["text_bytes"] = Value::from(
                self.offsets
                    .get(&(attempt.clone(), "text".into()))
                    .copied()
                    .unwrap_or(0),
            );
            event["archive_complete"] = Value::Bool(true);
        }

        let mut record = ArchiveRecord {
            sequence,
            attempt_id: attempt.clone(),
            event,
        };
        if matches!(
            record.event["phase"].as_str(),
            Some("display_checkpoint" | "selection_intent")
        ) {
            record.event["selected_head"] =
                serde_json::to_value(&self.graph.selected_head).map_err(|e| e.to_string())?;
            record.event["selected_head_known"] = Value::Bool(self.graph.selection_known);
        }
        if record.event["phase"] == "display_head_selected" {
            if record.event.get("display_head").is_some()
                || record.event.get("display_block").is_some()
            {
                return Err("display graph fields are recorder-owned".into());
            }
            let selected_block_id: Option<String> = if record.event["select_attempt_parent"] == true
            {
                self.graph
                    .attempt_parents
                    .get(&attempt)
                    .cloned()
                    .ok_or_else(|| "display retry attempt has no recorded parent".to_owned())?
            } else {
                serde_json::from_value(record.event["parent_block_id"].clone())
                    .map_err(|e| e.to_string())?
            };
            record.event["display_head"] = serde_json::to_value(DisplayHeadTransition {
                previous_block_id: self.graph.selected_head.clone(),
                selected_block_id,
            })
            .map_err(|e| e.to_string())?;
        } else {
            let timestamp_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis()
                .min(u64::MAX as u128) as u64;
            self.graph
                .decorate(&mut record, timestamp_ms)
                .map_err(|e| e.to_string())?;
        }
        self.graph.validate(&record).map_err(|e| e.to_string())?;
        self.store
            .append(self.sequence, &record)
            .map_err(|e| self.failure(e.to_string()))?;
        self.sequence = sequence;
        self.graph
            .apply(&record)
            .map_err(|e| self.failure(e.to_string()))?;
        if let Some((key, end)) = next_offset {
            self.offsets.insert(key, end);
        }
        // Publish committed identity before acknowledging the provider's display delivery.
        let published =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.committed)(record)));
        if published.is_err() {
            return Err(self.failure("display publication callback failed".into()));
        }
        if terminal {
            self.offsets.retain(|(id, _), _| id != &attempt);
            self.state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .attempts
                .remove(&attempt);
        }
        Ok(sequence)
    }
    fn drain_dropped(&mut self) {
        let terminals: Vec<_> = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state
                .attempts
                .iter_mut()
                .filter_map(|(_, attempt)| {
                    attempt
                        .dropped
                        .take()
                        .map(|observation| (observation, attempt.metadata.clone()))
                })
                .collect()
        };
        for (observation, metadata) in terminals {
            match decorate(&observation, &metadata) {
                Ok(event) => {
                    let _ = self.append(observation.call_id, event);
                }
                Err(error) => {
                    self.failure(error);
                }
            }
        }
    }
    fn flush(&mut self) -> Result<(), String> {
        self.drain_dropped();
        if let Some(error) = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .failure
            .clone()
        {
            return Err(error);
        }
        self.store.flush().map_err(|e| self.failure(e.to_string()))
    }
    fn run(mut self, mut receiver: mpsc::Receiver<Command>) {
        while let Some(command) = receiver.blocking_recv() {
            match command {
                Command::Record {
                    attempt,
                    event,
                    reply,
                } => {
                    let result = self.append(attempt, event);
                    let _ = reply.send(result);
                }
                Command::Wake => {}
                Command::Flush(reply) => {
                    let result = self.flush();
                    let _ = reply.send(result);
                }
                Command::Close(reply) => {
                    let result = self.flush();
                    let _ = reply.send(result);
                    break;
                }
            }
            if receiver.is_empty() {
                self.drain_dropped();
            }
        }
        let _ = self.flush();
    }
}
#[cfg(test)]
mod tests;
