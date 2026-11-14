//! One bounded lane owns recording. publish never waits; loss is a sticky error,
//! never a silently complete-looking audit. flush is an acknowledged barrier.
use super::recorder::{EffectiveRequest, Recorder, RecorderOptions, error};
use microcrab_auditor::{
    AuditBody, AuditRecord, Auditor, AuditorError, BoxFuture, HookContext, Injection, Moment,
};
use microcrab_storage::AuditAnchor;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot, watch};

struct Shared {
    failure: Mutex<Option<AuditorError>>,
    heads: Mutex<HashMap<String, String>>,
    closing: Mutex<bool>,
    settled: watch::Sender<Option<Result<(), AuditorError>>>,
}
impl Shared {
    fn fail(&self, failure: AuditorError) {
        let mut fault = self.failure.lock().unwrap_or_else(|e| e.into_inner());
        if fault.is_none() {
            *fault = Some(failure);
        }
    }
    fn healthy(&self) -> Result<(), AuditorError> {
        self.failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .map_or(Ok(()), Err)
    }
    fn is_closing(&self) -> bool {
        *self.closing.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn settle(&self, result: Result<(), AuditorError>) {
        if let Err(error) = &result {
            self.fail(error.clone());
        }
        *self.closing.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.settled.send_if_modified(|settled| {
            if settled.is_some() {
                false
            } else {
                *settled = Some(result);
                true
            }
        });
    }
    async fn closed(&self) -> Result<(), AuditorError> {
        let mut receiver = self.settled.subscribe();
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            receiver
                .changed()
                .await
                .map_err(|_| error("audit close owner disappeared"))?;
        }
    }
}
enum Command {
    Effective(
        EffectiveRequest,
        oneshot::Sender<Result<String, AuditorError>>,
    ),
    Published(serde_json::Value),
    Flush(oneshot::Sender<Result<(), AuditorError>>),
    Close,
}

#[cfg(all(test, feature = "storage-file"))]
mod tests;

#[derive(Clone)]
struct RequestScope {
    epoch: u64,
    subagent: Option<String>,
}
struct PreparedScope {
    scope: RequestScope,
    generation: u64,
}
#[derive(Default)]
struct ScopeHeads {
    history_head: Option<String>,
    main_head: Option<String>,
    generation: u64,
    helpers: HashMap<(u64, String), String>,
    known: HashMap<String, RequestScope>,
    prepared: HashMap<String, PreparedScope>,
    // In-flight foreground tools retain their admission branch across checkpoint
    // movement. Their producer start clock disambiguates reused conversation IDs.
    tool_history: HashMap<String, Option<String>>,
}
impl ScopeHeads {
    // External selection is authoritative. Even ordinary checkpoint movement
    // invalidates ephemeral helper continuity rather than guessing its ancestry.
    fn synchronize(&mut self, anchor: &AuditAnchor) {
        if self.history_head != anchor.history_head || self.main_head != anchor.request_head {
            self.reset_helpers();
            self.history_head = anchor.history_head.clone();
            self.main_head = anchor.request_head.clone();
        }
    }
    fn reset_helpers(&mut self) {
        self.helpers.clear();
        // Clear admissions as well: a late old-branch send cannot advance a head.
        self.prepared.clear();
        self.generation = self.generation.wrapping_add(1);
    }
    fn parent(&self, epoch: u64, subagent: Option<&str>) -> Option<String> {
        match subagent {
            Some(scope) => self.helpers.get(&(epoch, scope.to_owned())).cloned(),
            None => self
                .main_head
                .as_ref()
                .filter(|head| {
                    self.known
                        .get(*head)
                        .is_some_and(|scope| scope.epoch == epoch && scope.subagent.is_none())
                })
                .cloned(),
        }
    }
}

pub struct NativeAuditor {
    sender: mpsc::Sender<Command>,
    shared: Arc<Shared>,
    anchor: Arc<Mutex<AuditAnchor>>,
    scopes: Mutex<ScopeHeads>,
    sequence: Mutex<u64>,
    admission: tokio::sync::Mutex<()>,
    max_record_bytes: usize,
}
impl NativeAuditor {
    pub async fn open(
        directory: impl AsRef<Path>,
        anchor: Arc<Mutex<AuditAnchor>>,
        options: RecorderOptions,
    ) -> Result<Arc<Self>, AuditorError> {
        Self::open_with_observer(directory, anchor, options, None).await
    }
    pub async fn open_with_observer(
        directory: impl AsRef<Path>,
        anchor: Arc<Mutex<AuditAnchor>>,
        options: RecorderOptions,
        observer: Option<super::CommitObserver>,
    ) -> Result<Arc<Self>, AuditorError> {
        Self::open_source_with_observer(
            microcrab_storage::audit::AuditSource::File(directory.as_ref().to_owned()),
            anchor,
            options,
            observer,
        )
        .await
    }
    pub async fn open_source_with_observer(
        source: microcrab_storage::audit::AuditSource,
        anchor: Arc<Mutex<AuditAnchor>>,
        options: RecorderOptions,
        observer: Option<super::CommitObserver>,
    ) -> Result<Arc<Self>, AuditorError> {
        options.validate()?;
        let max_record_bytes = options.max_record_bytes;
        let pending = options.pending_records;
        let recorder = tokio::task::spawn_blocking(move || {
            let mut recorder = Recorder::open_source(source, options)?;
            recorder.observer = observer;
            // Opening already verified the chain and indexed retained effectives,
            // so the branch map is copied instead of re-reading the file.
            let known = recorder
                .effective_scopes()
                .iter()
                .map(|(digest, (epoch, subagent))| {
                    (
                        digest.clone(),
                        RequestScope {
                            epoch: *epoch,
                            subagent: subagent.clone(),
                        },
                    )
                })
                .collect();
            Ok::<_, AuditorError>((recorder, known))
        })
        .await
        .map_err(|_| error("audit open worker failed"))??;
        let (recorder, known) = recorder;
        let mut scopes = ScopeHeads {
            known,
            ..Default::default()
        };
        scopes.synchronize(&anchor.lock().unwrap_or_else(|e| e.into_inner()));
        let (settled, _) = watch::channel(None);
        let shared = Arc::new(Shared {
            failure: Mutex::new(None),
            heads: Mutex::new(HashMap::new()),
            closing: Mutex::new(false),
            settled,
        });
        let (sender, receiver) = mpsc::channel(pending);
        tokio::spawn(run(recorder, receiver, shared.clone()));
        Ok(Arc::new(Self {
            sender,
            shared,
            anchor,
            scopes: Mutex::new(scopes),
            sequence: Mutex::new(0),
            admission: tokio::sync::Mutex::new(()),
            max_record_bytes,
        }))
    }
    /// Retained identity only, useful for checkpoints without exposing request text.
    pub fn request_head(&self, call: &str) -> Option<String> {
        self.shared
            .heads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(call)
            .cloned()
    }
    pub fn failure(&self) -> Option<AuditorError> {
        self.shared.healthy().err()
    }
    /// The actor owns shutdown once admitted; cancellation of a waiter cannot
    /// abandon the lock or lose the settled result needed by another cleanup path.
    pub async fn close(&self) -> Result<(), AuditorError> {
        {
            let _admission = self.admission.lock().await;
            if !self.shared.is_closing() {
                let permit = match self.sender.reserve().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        *self
                            .shared
                            .closing
                            .lock()
                            .unwrap_or_else(|e| e.into_inner()) = true;
                        self.shared.settle(Err(self
                            .shared
                            .healthy()
                            .err()
                            .unwrap_or_else(|| error("audit writer closed"))));
                        return self.shared.closed().await;
                    }
                };
                // Synchronous publishers share this ordering guard. Reserve first:
                // cancellation before capacity becomes available leaves Open intact.
                let _sequence = self.sequence.lock().unwrap_or_else(|e| e.into_inner());
                *self
                    .shared
                    .closing
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = true;
                permit.send(Command::Close);
            }
        }
        self.shared.closed().await
    }
}

impl Auditor for NativeAuditor {
    fn is_enabled(&self) -> bool {
        !self.shared.is_closing()
    }
    fn publish(&self, event: Moment, body: AuditBody) {
        if self.shared.healthy().is_err() {
            return;
        }
        let mut sequence = self.sequence.lock().unwrap_or_else(|e| e.into_inner());
        if self.shared.is_closing() {
            return;
        }
        let Some(next) = sequence.checked_add(1) else {
            self.shared.fail(error("audit producer sequence overflow"));
            return;
        };
        let sent = event == Moment::RequestPrepared && body.sent_request.is_some();
        let complete = body.outcome.is_some();
        let call = body.call_id.clone();
        let scope = RequestScope {
            epoch: body.epoch,
            subagent: body.subagent.clone(),
        };
        let restored = event == Moment::SessionRestored;
        let record = AuditRecord {
            sequence: next,
            event,
            epoch: body.epoch,
            subagent: body.subagent,
            turn_id: body.turn_id,
            round: body.round,
            call_id: body.call_id,
            context_fingerprint: body.context_fingerprint,
            context_target: body.context_target,
            tool_call_id: body.tool_call_id,
            payload: body.payload,
            sent_request: body.sent_request,
            request_diff: None,
            response: body.response,
            response_bytes: body.response_bytes,
            response_complete: body.response_complete,
            response_status: body.response_status,
            response_content_type: body.response_content_type,
            started_at_unix_ms: body.started_at_unix_ms,
            elapsed_ms: body.elapsed_ms,
            first_token_ms: body.first_token_ms,
            generation_ms: body.generation_ms,
            timing_source: body.timing_source,
            outcome: body.outcome,
            usage: body.usage,
        };
        let mut value = match serde_json::to_value(record) {
            Ok(value) => value,
            Err(_) => {
                self.shared
                    .fail(error("audit publication serialization failed"));
                return;
            }
        };
        if matches!(event, Moment::ToolCall | Moment::ToolResult) {
            let anchor = self
                .anchor
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let mut scopes = self
                .scopes
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let key = serde_json::json!([
                value["epoch"],
                value["turn_id"],
                value["round"],
                value["subagent"],
                value["tool_call_id"],
                value["started_at_unix_ms"]
            ])
            .to_string();
            let history = if event == Moment::ToolCall && value["tool_call_id"].is_string() {
                if scopes.tool_history.len() >= 4096 && !scopes.tool_history.contains_key(&key) {
                    self.shared
                        .fail(error("audit in-flight tool metadata budget exceeded"));
                    return;
                }
                scopes
                    .tool_history
                    .entry(key)
                    .or_insert_with(|| anchor.history_head.clone())
                    .clone()
            } else {
                // Never attach a late/unpaired result to the newly selected branch.
                scopes.tool_history.remove(&key).flatten()
            };
            value["history_head"] = serde_json::json!(history);
        }
        // The queue has both a record count cap and a per-message byte cap.
        if serde_json::to_vec(&value).map_or(true, |bytes| bytes.len() > self.max_record_bytes) {
            self.shared
                .fail(error("audit publication byte budget exceeded"));
            return;
        }
        if self.sender.try_send(Command::Published(value)).is_err() {
            self.shared
                .fail(error("audit writer queue exhausted or closed"));
            return;
        }
        *sequence = next;
        let mut anchor = self.anchor.lock().unwrap_or_else(|e| e.into_inner());
        let mut scopes = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
        scopes.synchronize(&anchor);
        if restored {
            scopes.reset_helpers();
        }
        if let Some(call) = call {
            let mut heads = self.shared.heads.lock().unwrap_or_else(|e| e.into_inner());
            if sent
                && let (Some(head), Some(prepared)) = (heads.get(&call), scopes.prepared.get(&call))
                && prepared.generation == scopes.generation
                && prepared.scope.epoch == scope.epoch
                && prepared.scope.subagent == scope.subagent
            {
                if let Some(helper) = &scope.subagent {
                    scopes
                        .helpers
                        .insert((scope.epoch, helper.clone()), head.clone());
                } else {
                    anchor.request_head = Some(head.clone());
                    // This is our own advancement, not external branch movement.
                    scopes.main_head = anchor.request_head.clone();
                }
            }
            if complete {
                heads.remove(&call);
                scopes.prepared.remove(&call);
            }
        }
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            self.shared.healthy()?;
            if moment != Moment::RequestPrepared || context.request_body.is_none() {
                return Ok(Injection::default());
            }
            let call_id = context
                .call_id
                .ok_or_else(|| error("RequestPrepared recording requires call_id"))?;
            let body = context.request_body.expect("checked request body");
            if body.len() > self.max_record_bytes {
                self.shared
                    .fail(error("audit effective request byte budget exceeded"));
                return Err(error("audit effective request byte budget exceeded"));
            }
            let (history_head, parent_request, generation) = {
                let anchor = self.anchor.lock().unwrap_or_else(|e| e.into_inner());
                let mut scopes = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
                scopes.synchronize(&anchor);
                (
                    anchor.history_head.clone(),
                    scopes.parent(context.epoch, context.subagent.as_deref()),
                    scopes.generation,
                )
            };
            let scope = RequestScope {
                epoch: context.epoch,
                subagent: context.subagent.clone(),
            };
            let request = EffectiveRequest {
                call_id: call_id.clone(),
                epoch: context.epoch,
                turn: context.turn,
                round: context.round,
                parent_request,
                history_head,
                subagent: context.subagent,
                request_body: body.into(),
            };
            let (reply, result) = oneshot::channel();
            {
                let _admission = self.admission.lock().await;
                if self.shared.is_closing() {
                    return Err(error("audit writer is closed"));
                }
                self.sender
                    .send(Command::Effective(request, reply))
                    .await
                    .map_err(|_| error("audit writer closed"))?;
            }
            let identity = result
                .await
                .map_err(|_| error("audit effective acknowledgement abandoned"))??;
            {
                let anchor = self.anchor.lock().unwrap_or_else(|e| e.into_inner());
                let mut scopes = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
                scopes.synchronize(&anchor);
                scopes.known.insert(identity.clone(), scope.clone());
                if scopes.generation == generation {
                    scopes
                        .prepared
                        .insert(call_id.clone(), PreparedScope { scope, generation });
                }
                self.shared
                    .heads
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(call_id, identity);
            }
            Ok(Injection::default())
        })
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), AuditorError>> {
        Box::pin(async move {
            let result = {
                let admission = self.admission.lock().await;
                if self.shared.is_closing() {
                    drop(admission);
                    return self.shared.closed().await;
                }
                let (reply, result) = oneshot::channel();
                self.sender
                    .send(Command::Flush(reply))
                    .await
                    .map_err(|_| error("audit writer closed"))?;
                result
            };
            result
                .await
                .map_err(|_| error("audit flush acknowledgement abandoned"))??;
            self.shared.healthy()
        })
    }
}

async fn run(mut recorder: Recorder, mut receiver: mpsc::Receiver<Command>, shared: Arc<Shared>) {
    while let Some(command) = receiver.recv().await {
        if let Command::Close = command {
            let state = shared.clone();
            let shutdown = tokio::task::spawn_blocking(move || {
                // Durability already happened per record; close only seals the writer.
                let result = state.healthy().and_then(|_| recorder.flush());
                drop(recorder);
                state.settle(result);
            })
            .await;
            if shutdown.is_err() {
                shared.settle(Err(error("audit close worker failed")));
            }
            return;
        }
        let state = shared.clone();
        // The spawned blocking operation retains ownership even if its caller's
        // intervention/flush future is cancelled. It finishes before the next item.
        let operation = tokio::task::spawn_blocking(move || {
            let result = state.healthy();
            match command {
                Command::Close => {
                    unreachable!("close was handled before starting a publication")
                }
                Command::Effective(request, reply) => {
                    let result = result.and_then(|_| recorder.effective(request));
                    if let Err(error) = &result {
                        state.fail(error.clone());
                    }
                    let _ = reply.send(result);
                }
                Command::Published(record) => {
                    if let Err(error) = result.and_then(|_| recorder.publish(record)) {
                        state.fail(error);
                    }
                }
                Command::Flush(reply) => {
                    let result = result.and_then(|_| recorder.flush());
                    if let Err(error) = &result {
                        state.fail(error.clone());
                    }
                    let _ = reply.send(result);
                }
            }
            recorder
        })
        .await;
        match operation {
            Ok(writer) => recorder = writer,
            Err(_) => {
                shared.settle(Err(error("audit writer task failed")));
                return;
            }
        }
    }
}
