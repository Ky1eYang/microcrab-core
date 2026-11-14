//! Process identity, bounded admission, descendant routing and owned Host cleanup.
use super::{ProcessControl, SessionProcessState, SessionProcessStatus};
use crate::{Error, Limits, observation::diagnostics::ToolDiagnostic};
use async_trait::async_trait;
use futures_util::{FutureExt, StreamExt};
use microcrab_host_api::{
    BoxStream, Bytes, ErrorCategory, Host, HostError, OpContext, ProcessApi, ShellApi,
    ShellCaptureStatus, ShellPosition, ShellRequest, ToolError,
    process::{ProcessHandle, ProcessSpec, ProcessState, Signal},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Notify, oneshot};

// All tree attachments share this short synchronous transaction. Traversal and
// parent claims cannot race another attachment into a reciprocal ownership cycle.
static HIERARCHY: Mutex<()> = Mutex::new(());
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProcessId(pub u64);

pub struct OwnedProcess {
    pub process_id: ProcessId,
    pub tool_name: String,
    pub session_id: String,
    pub agent: String,
    pub call_id: String,
    pub registered_at_unix_ms: u64,
    pub handle: ProcessHandle,
    pub pid: Option<u32>,
    pub capture: bool,
    releasing: AtomicBool,
    release_completion: Mutex<Option<ProcessRelease>>,
    bridged: AtomicBool,
    stdout: tokio::sync::Mutex<RawPipe>,
    stderr: tokio::sync::Mutex<RawPipe>,
    status: Mutex<SessionProcessStatus>,
}

// Waiters share the Host acknowledgment. A closed pending channel means runtime
// shutdown dropped the worker, so another runtime may retry the owned process.
struct ProcessRelease {
    completion: tokio::sync::watch::Receiver<Option<Result<(), ToolDiagnostic>>>,
}
struct ProcessReleaseAttempt {
    process: Arc<OwnedProcess>,
    completion: tokio::sync::watch::Sender<Option<Result<(), ToolDiagnostic>>>,
}
impl Drop for ProcessReleaseAttempt {
    fn drop(&mut self) {
        // Reset before the sender closes: a retry must never inherit a dead worker's
        // in-flight flag. The process remains in Contents until Host acknowledgment.
        self.process.releasing.store(false, Ordering::Release);
    }
}

struct Contents {
    entries: BTreeMap<ProcessId, Arc<OwnedProcess>>,
    children: BTreeMap<String, Arc<ProcessRegistry>>,
    sessions: BTreeMap<String, SessionBudget>,
}
// One slot follows an admission through publication and cleanup. It is refunded
// only after Host acknowledgement, never while an owned handle is releasing.
struct SessionBudget {
    max_processes: usize,
    sealed: bool,
    occupied: usize,
    workers: usize,
    stopping: CancellationToken,
    close_gate: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Clone)]
struct PendingProcess {
    handle: ProcessHandle,
    session: String,
}
// Completed outcomes remain stable; closed pending attempts can retry on a new runtime.
struct ProcessClose {
    completion: tokio::sync::watch::Receiver<Option<Result<(), ToolDiagnostic>>>,
}

pub struct ProcessRegistry {
    owner: String,
    host: Option<Arc<dyn Host>>,
    limits: Limits,
    sealed: AtomicBool,
    reserved: AtomicUsize,
    contents: Mutex<Contents>,
    changed: Arc<Notify>,
    generation: AtomicU64,
    self_ref: Weak<ProcessRegistry>,
    parent: Mutex<Option<Weak<ProcessRegistry>>>,
    stopping: CancellationToken,
    pending: Mutex<BTreeMap<u64, PendingProcess>>,
    close_gate: tokio::sync::Mutex<()>,
    close_result: Mutex<Option<ProcessClose>>,
    cleanup_failed: AtomicBool,
}
include!("source_registry.rs");

impl ProcessRegistry {
    pub fn new(owner: String, host: Option<Arc<dyn Host>>, limits: Limits) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            owner: owner.clone(),
            host,
            limits: limits.clone(),
            sealed: AtomicBool::new(false),
            reserved: AtomicUsize::new(0),
            contents: Mutex::new(Contents {
                entries: BTreeMap::new(),
                children: BTreeMap::new(),
                sessions: BTreeMap::from([(
                    owner.clone(),
                    SessionBudget {
                        max_processes: limits.max_processes,
                        sealed: false,
                        occupied: 0,
                        workers: 0,
                        stopping: CancellationToken::new(),
                        close_gate: Arc::new(tokio::sync::Mutex::new(())),
                    },
                )]),
            }),
            changed: Arc::new(Notify::new()),
            generation: AtomicU64::new(0),
            self_ref: weak.clone(),
            parent: Mutex::new(None),
            stopping: CancellationToken::new(),
            pending: Mutex::new(BTreeMap::new()),
            close_gate: tokio::sync::Mutex::new(()),
            close_result: Mutex::new(None),
            cleanup_failed: AtomicBool::new(false),
        })
    }
    /// Trusted Session assembly registers the effective immutable quota. Merely
    /// constructing a scoped capability never creates capacity for an identity.
    pub(crate) fn register_session(&self, session_id: &str, limits: &Limits) -> Result<(), Error> {
        if session_id.is_empty() || session_id.contains('/') || limits.max_processes == 0 {
            return Err(Error::config("invalid process Session budget"));
        }
        let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
        if self.sealed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if let Some(previous) = contents.sessions.get(session_id) {
            if previous.sealed {
                return Err(Error::Closed);
            }
            if previous.max_processes != limits.max_processes {
                return Err(Error::config("process Session budget is immutable"));
            }
            return Ok(());
        }
        contents.sessions.insert(
            session_id.into(),
            SessionBudget {
                max_processes: limits.max_processes,
                sealed: false,
                occupied: 0,
                workers: 0,
                stopping: CancellationToken::new(),
                close_gate: Arc::new(tokio::sync::Mutex::new(())),
            },
        );
        Ok(())
    }
    pub fn seal_session(&self, session_id: &str) {
        let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(budget) = contents.sessions.get_mut(session_id) {
            budget.sealed = true;
            budget.stopping.cancel();
        }
        drop(contents);
        self.signal();
    }
    fn refund_session(&self, session_id: &str) {
        let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(budget) = contents.sessions.get_mut(session_id) {
            debug_assert!(budget.occupied > 0, "process slot refunded once");
            budget.occupied = budget.occupied.saturating_sub(1);
        }
        drop(contents);
        self.signal();
    }
    /// Scoped shutdown preserves sibling admissions and ownership. The registry
    /// root still owns all Sessions and remains responsible for final tree close.
    pub async fn close_session(&self, session_id: &str) -> Result<(), Error> {
        self.seal_session(session_id);
        let gate = self
            .contents
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sessions
            .get(session_id)
            .map(|budget| budget.close_gate.clone());
        let Some(gate) = gate else {
            return Ok(());
        };
        let _close = gate.lock().await;
        let mut first_error = None;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let workers = self
                .contents
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .sessions
                .get(session_id)
                .map_or(0, |budget| budget.workers);
            if workers != 0 {
                notified.await;
                continue;
            }
            let processes = self
                .local()
                .into_iter()
                .filter(|process| process.session_id == session_id)
                .collect::<Vec<_>>();
            for process in processes {
                if let Some(host) = &self.host {
                    let _ = AssertUnwindSafe(ProcessApi::kill_tree(
                        host.as_ref(),
                        &self.context(CancellationToken::new()),
                        process.handle,
                        true,
                    ))
                    .catch_unwind()
                    .await;
                }
                if let Err(error) = self
                    .source_release(session_id, process.handle, CancellationToken::new())
                    .await
                {
                    // Retain the failed owner for retry, but still attempt every
                    // independent resource before reporting this close failure.
                    first_error.get_or_insert(error);
                }
            }
            let pending = {
                let contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
                if contents
                    .sessions
                    .get(session_id)
                    .is_some_and(|budget| budget.workers != 0)
                {
                    None
                } else {
                    Some(
                        self.pending
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .clone(),
                    )
                }
            };
            let Some(pending) = pending else {
                notified.await;
                continue;
            };
            for (id, process) in pending
                .into_iter()
                .filter(|(_, process)| process.session == session_id)
            {
                if let Some(host) = &self.host {
                    if !release_handle(host, process.handle).await {
                        first_error.get_or_insert_with(|| {
                            Error::tool("process containment cleanup failed")
                        });
                        continue;
                    }
                    let removed = self
                        .pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&id)
                        .is_some();
                    if removed {
                        self.refund_session(session_id);
                    }
                }
            }
            if let Some(error) = first_error {
                return Err(error);
            }
            let occupied = self
                .contents
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .sessions
                .get(session_id)
                .map_or(0, |budget| budget.occupied);
            if occupied == 0 {
                return Ok(());
            }
            notified.await;
        }
    }
    /// Quiesce this Session's exact resources without permanently sealing future
    /// admissions. Session composition holds its replacement fence until return.
    pub async fn quiesce_session_for_restore(&self, session_id: &str) -> Result<(), Error> {
        let owner = self.self_ref.upgrade().ok_or(Error::Closed)?;
        let session = session_id.to_owned();
        let (sender, receiver) = oneshot::channel();
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            Error::lifecycle("scoped process replacement cleanup needs the embedding async runtime")
        })?;
        runtime.spawn(async move {
            let result = owner.close_session(&session).await;
            {
                let mut contents = owner.contents.lock().unwrap_or_else(|e| e.into_inner());
                if !owner.sealed.load(Ordering::Acquire) {
                    if let Some(budget) = contents.sessions.get_mut(&session) {
                        budget.sealed = false;
                        budget.stopping = CancellationToken::new();
                    }
                }
            }
            owner.signal();
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| Error::lifecycle("scoped process replacement cleanup worker stopped"))?
    }
    pub fn configured_limits(&self) -> &Limits {
        &self.limits
    }
    pub fn detached(limits: Limits) -> Arc<Self> {
        Self::new("detached".into(), None, limits)
    }
    fn signal(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_waiters();
    }
    pub async fn wait_until_closed(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.has_unclosed() {
                return;
            }
            notified.await;
        }
    }
    pub fn seal(&self) {
        // Admission checks and increments under this same gate. Close must never
        // observe zero between an accepted check and its reservation increment.
        let admission = self.contents.lock().unwrap_or_else(|e| e.into_inner());
        self.sealed.store(true, Ordering::Release);
        self.stopping.cancel();
        drop(admission);
        self.signal();
    }
    /// Seals admission and requests best-effort cleanup on the current runtime.
    /// Without a runtime, owners remain available for a later asynchronous `close`.
    /// Embedders must await `close` before shutting down their final runtime to
    /// guarantee Host cleanup; synchronous destruction cannot perform async I/O.
    pub fn abort(&self) {
        self.seal();
        if tokio::runtime::Handle::try_current().is_err() {
            let children = self
                .contents
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .children
                .values()
                .cloned()
                .collect::<Vec<_>>();
            for child in children {
                child.abort();
            }
            tracing::warn!("process abort needs an async runtime; await close for Host cleanup");
            return;
        }
        let (processes, children) = {
            let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
            let ids = contents
                .entries
                .iter()
                .filter(|(_, process)| !process.releasing.load(Ordering::Acquire))
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            let processes = ids
                .into_iter()
                .filter_map(|id| contents.entries.remove(&id))
                .collect::<Vec<_>>();
            for process in &processes {
                contents
                    .sessions
                    .get_mut(&process.session_id)
                    .expect("owned process has a registered Session")
                    .workers += 1;
            }
            self.reserved.fetch_add(processes.len(), Ordering::AcqRel);
            (
                processes,
                contents.children.values().cloned().collect::<Vec<_>>(),
            )
        };
        if let Some(host) = &self.host {
            for process in processes {
                self.release_detached(host.clone(), process.handle, process.session_id.clone());
            }
        }
        for child in children {
            child.abort();
        }
    }
    pub async fn close(&self) -> Result<(), Error> {
        self.close_attempt(false).await
    }
    /// Retry only retained exact identities, requiring fresh Host release ACKs.
    /// Ordinary close retains its historical result; explicit reconciliation can
    /// supersede a failed containment attempt after all ownership is discharged.
    pub async fn reconcile_close(&self) -> Result<(), Error> {
        self.close_attempt(true).await
    }
    async fn close_attempt(&self, reconcile: bool) -> Result<(), Error> {
        self.seal();
        let mut receiver = {
            let mut result = self.close_result.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(completion) = result.as_ref()
                && !(reconcile && matches!(*completion.completion.borrow(), Some(Err(_))))
                && (completion.completion.borrow().is_some()
                    || completion.completion.has_changed().is_ok())
            {
                completion.completion.clone()
            } else {
                let (sender, receiver) = tokio::sync::watch::channel(None);
                *result = Some(ProcessClose {
                    completion: receiver.clone(),
                });
                let owner = self
                    .self_ref
                    .upgrade()
                    .expect("a live registry has an Arc owner");
                tokio::spawn(async move {
                    let outcome = AssertUnwindSafe(owner.close_owned(reconcile))
                        .catch_unwind()
                        .await;
                    let outcome = match outcome {
                        Ok(result) => result.map_err(|error| error.tool_diagnostic()),
                        Err(_) => Err(Error::lifecycle("process containment cleanup panicked")
                            .tool_diagnostic()),
                    };
                    sender.send_replace(Some(outcome));
                });
                receiver
            }
        };
        loop {
            let result = receiver.borrow().clone();
            if let Some(result) = result {
                return result.map_err(Error::Diagnostic);
            }
            receiver.changed().await.map_err(|_| Error::Closed)?;
        }
    }
    fn close_owned(
        &self,
        reconcile: bool,
    ) -> futures_util::future::BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let _close = self.close_gate.lock().await;
            // An accepted start owns cleanup even when its original caller is gone.
            // Register first, then recheck, so the last acknowledgement cannot be lost.
            let (processes, children) = loop {
                let done = self.changed.notified();
                tokio::pin!(done);
                done.as_mut().enable();
                {
                    let contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
                    if self.reserved.load(Ordering::Acquire) == 0 {
                        break (
                            contents.entries.values().cloned().collect::<Vec<_>>(),
                            contents.children.values().cloned().collect::<Vec<_>>(),
                        );
                    }
                }
                done.await;
            };
            let mut failed = !reconcile && self.cleanup_failed.load(Ordering::Acquire);
            let mut first_error = None;
            let pending = self
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(host) = &self.host {
                for process in processes {
                    let ctx = self.context(CancellationToken::new());
                    if !reconcile {
                        let _ = AssertUnwindSafe(ProcessApi::kill_tree(
                            host.as_ref(),
                            &ctx,
                            process.handle,
                            true,
                        ))
                        .catch_unwind()
                        .await;
                    }
                    // The shared release worker keeps the mapping until Host ack.
                    // A concurrent successful release can already have removed it.
                    if let Err(error) = self
                        .source_release(
                            &process.session_id,
                            process.handle,
                            CancellationToken::new(),
                        )
                        .await
                    {
                        failed = true;
                        first_error.get_or_insert(error);
                    }
                }
                for (id, process) in pending {
                    if release_handle(host, process.handle).await {
                        let removed = self
                            .pending
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&id)
                            .is_some();
                        if removed {
                            self.refund_session(&process.session);
                        }
                    } else {
                        failed = true;
                    }
                }
            }
            for child in children {
                let result = if reconcile {
                    Box::pin(child.reconcile_close()).await
                } else {
                    Box::pin(child.close()).await
                };
                if let Err(error) = result {
                    failed = true;
                    first_error.get_or_insert(error);
                } else {
                    self.contents
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .children
                        .remove(&child.owner);
                }
            }
            // Host-wide unresolved startup/release acknowledgement is not an
            // ownership transfer to this registry. Conservatively report failed
            // cleanup rather than claiming containment has settled.
            if self
                .host
                .as_ref()
                .is_some_and(|host| ProcessApi::has_unresolved_cleanup(host.as_ref()))
            {
                failed = true;
            }
            self.signal();
            if failed {
                Err(first_error.unwrap_or_else(|| Error::tool("process containment cleanup failed; unresolved ownership or prior failure requires explicit reconciliation")))
            } else {
                if reconcile {
                    self.cleanup_failed.store(false, Ordering::Release);
                }
                Ok(())
            }
        })
    }
    fn context(&self, cancellation: CancellationToken) -> OpContext {
        OpContext {
            cancel: cancellation,
            principal: self.owner.clone(),
            ..OpContext::default()
        }
    }
    async fn status(
        &self,
        host: &Arc<dyn Host>,
        handle: ProcessHandle,
        capture: bool,
    ) -> Result<SessionProcessStatus, Error> {
        let ctx = self.context(CancellationToken::new());
        let process = ProcessApi::status(host.as_ref(), &ctx, handle)
            .await
            .map_err(host_error)?;
        let (state, exit_code) = match process.state {
            ProcessState::Running => (SessionProcessState::Running, None),
            ProcessState::Failed { reason } => (SessionProcessState::Failed { reason }, None),
            ProcessState::Exited { status } => {
                let state = if status.signal == Some(Signal::Kill) {
                    SessionProcessState::Killed
                } else if let Some(code) = status.code {
                    if code == 0 {
                        SessionProcessState::Exited { code }
                    } else {
                        SessionProcessState::Failed {
                            reason: format!("process exited with code {code}"),
                        }
                    }
                } else {
                    SessionProcessState::Failed {
                        reason: format!("process exited with signal {:?}", status.signal),
                    }
                };
                (state, status.code)
            }
        };
        let capture = if capture {
            ShellApi::capture_status(host.as_ref(), &ctx, handle)
                .await
                .map_err(host_error)?
        } else {
            let consumed = self.local().into_iter().find(|process| process.handle == handle).is_some_and(|process| {
                let stdout = process.stdout.try_lock();
                let stderr = process.stderr.try_lock();
                matches!((stdout, stderr), (Ok(stdout), Ok(stderr)) if stdout.eof && stderr.eof && stdout.pending.is_empty() && stderr.pending.is_empty())
            });
            ShellCaptureStatus {
                closed: consumed && !matches!(state, SessionProcessState::Running),
                cursor: ShellPosition::default(),
                stdout_bytes: 0,
                stderr_bytes: 0,
                truncated: false,
            }
        };
        Ok(SessionProcessStatus {
            pid: process.pid,
            state,
            exit_code,
            closed: capture.closed,
            cursor: capture.cursor,
            stdout_bytes: capture.stdout_bytes,
            stderr_bytes: capture.stderr_bytes,
            truncated: capture.truncated,
            elapsed_s: process.elapsed_s,
        })
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Launch observation keeps immutable provenance, Host admission and pending cleanup ownership explicit"
    )]
    async fn observe_launch(
        &self,
        session_id: &str,
        agent: &str,
        host: &Arc<dyn Host>,
        tool: &str,
        call_id: &str,
        request: Launch,
        cancellation: CancellationToken,
        pending: &mut Option<ProcessHandle>,
        launch_handle: &Mutex<Option<ProcessHandle>>,
    ) -> Result<ProcessHandle, Error> {
        let admission_token = CancellationToken::new();
        let session_stopping = self
            .contents
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sessions
            .get(session_id)
            .ok_or(Error::Closed)?
            .stopping
            .clone();
        let ctx = self.context(admission_token.clone());
        let capture = matches!(&request, Launch::Shell(_));
        let spawn = async {
            match request {
                Launch::Shell(mut request) => {
                    request.output_bytes =
                        request.output_bytes.min(self.limits.output_buffer_bytes);
                    ShellApi::launch(host.as_ref(), &ctx, request).await
                }
                Launch::Process(spec) => ProcessApi::spawn(host.as_ref(), &ctx, spec).await,
            }
        };
        tokio::pin!(spawn);
        let handle = tokio::select! {
            biased;
            _ = self.stopping.cancelled() => { admission_token.cancel(); spawn.await },
            _ = session_stopping.cancelled() => { admission_token.cancel(); spawn.await },
            _ = cancellation.cancelled() => { admission_token.cancel(); spawn.await },
            result = &mut spawn => result,
        }
        .map_err(host_error)?;
        *pending = Some(handle);
        *launch_handle.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                handle.0.0,
                PendingProcess {
                    handle,
                    session: session_id.into(),
                },
            );
        if self.stopping.is_cancelled()
            || session_stopping.is_cancelled()
            || cancellation.is_cancelled()
        {
            return Err(Error::Cancelled);
        }
        // Subscribe before the first status so completion cannot fall between them.
        let changes = ProcessApi::watch(host.as_ref(), handle).map_err(host_error)?;
        let status = self.status(host, handle, capture).await?;
        let id = ProcessId(handle.0.0);
        let process = Arc::new(OwnedProcess {
            process_id: id,
            tool_name: tool.into(),
            session_id: session_id.into(),
            agent: agent.into(),
            call_id: call_id.into(),
            registered_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            handle,
            pid: status.pid,
            capture,
            releasing: AtomicBool::new(false),
            release_completion: Mutex::new(None),
            bridged: AtomicBool::new(false),
            stdout: Default::default(),
            stderr: Default::default(),
            status: Mutex::new(status),
        });
        {
            let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
            if self.sealed.load(Ordering::Acquire)
                || contents
                    .sessions
                    .get(session_id)
                    .is_none_or(|budget| budget.sealed)
                || cancellation.is_cancelled()
            {
                return Err(Error::Cancelled);
            }
            if contents.entries.contains_key(&id) {
                return Err(Error::tool("duplicate Host process identity"));
            }
            contents.entries.insert(id, process.clone());
            self.pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&handle.0.0);
            pending.take();
        }
        self.watch(process, changes);
        self.signal();
        Ok(handle)
    }
    fn local(&self) -> Vec<Arc<OwnedProcess>> {
        self.contents
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .values()
            .cloned()
            .collect()
    }
    fn descendants(&self) -> Vec<Arc<ProcessRegistry>> {
        self.contents
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .children
            .values()
            .cloned()
            .collect()
    }
    fn owners(&self, owners: &mut BTreeSet<String>) -> bool {
        if !owners.insert(self.owner.clone()) {
            return false;
        }
        self.descendants().iter().all(|child| child.owners(owners))
    }
    fn hierarchy_root(&self) -> Arc<Self> {
        let mut root = self
            .self_ref
            .upgrade()
            .expect("a live registry has an Arc owner");
        loop {
            let parent = root
                .parent
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .and_then(Weak::upgrade);
            match parent {
                Some(parent) => root = parent,
                None => return root,
            }
        }
    }
    fn candidates(&self) -> Vec<(Arc<ProcessRegistry>, Arc<OwnedProcess>)> {
        let mut all = Vec::new();
        if let Some(owner) = self.self_ref.upgrade() {
            for process in self.local() {
                all.push((owner.clone(), process));
            }
        }
        for child in self.descendants() {
            all.extend(child.candidates());
        }
        all
    }
    fn update(&self, process: &OwnedProcess, status: SessionProcessStatus) {
        let changed = {
            let mut previous = process.status.lock().unwrap_or_else(|e| e.into_inner());
            let changed = previous.state != status.state || previous.closed != status.closed;
            *previous = status;
            changed
        };
        self.evict();
        if changed {
            self.signal();
        }
    }
    fn evict(&self) {
        let removed = {
            let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
            let closed = contents
                .entries
                .iter()
                .filter(|(_, j)| {
                    !j.releasing.load(Ordering::Acquire)
                        && j.status
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .status_projection()
                            .output_closed
                })
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            let remove = super::super::retention::RetentionPolicy::processes(&self.limits)
                .evict_count(closed.len());
            if remove > 0 {
                tracing::warn!(
                    evicted = remove,
                    retained_capacity = self.limits.max_retained_processes,
                    "retained process history exceeded its capacity; oldest closed entries were evicted"
                );
            }
            let removed = closed
                .into_iter()
                .take(remove)
                .filter_map(|id| contents.entries.remove(&id))
                .collect::<Vec<_>>();
            for process in &removed {
                contents
                    .sessions
                    .get_mut(&process.session_id)
                    .expect("owned process has a registered Session")
                    .workers += 1;
            }
            self.reserved.fetch_add(removed.len(), Ordering::AcqRel);
            removed
        };
        if let Some(host) = &self.host {
            for process in removed {
                self.release_detached(host.clone(), process.handle, process.session_id.clone());
            }
        }
    }
    fn release_detached(&self, host: Arc<dyn Host>, handle: ProcessHandle, session: String) {
        // Caller reserves cleanup before removing the entry. Keep a retryable
        // handle even if no runtime exists or shutdown drops an unpolled worker.
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                handle.0.0,
                PendingProcess {
                    handle,
                    session: session.clone(),
                },
            );
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                self.reserved.fetch_sub(1, Ordering::AcqRel);
                if let Some(budget) = self
                    .contents
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .sessions
                    .get_mut(&session)
                {
                    budget.workers -= 1;
                }
                self.signal();
                tracing::warn!(
                    "process cleanup needs an async runtime; await close for Host cleanup"
                );
                return;
            }
        };
        if let Some(owner) = self.self_ref.upgrade() {
            let admission = Admission {
                owner: owner.clone(),
                refund: None,
                worker_session: Some(session.clone()),
                launch_handle: None,
            };
            runtime.spawn(async move {
                if release_handle(&host, handle).await {
                    let removed = owner
                        .pending
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .remove(&handle.0.0);
                    if removed.is_some() {
                        owner.refund_session(&session);
                    }
                } else {
                    owner.cleanup_failed.store(true, Ordering::Release);
                }
                drop(admission);
            });
        } else {
            // During final Drop no Arc owner can retain the reservation. The task
            // owns the Host handle, but shutdown can still prevent best-effort I/O.
            self.reserved.fetch_sub(1, Ordering::AcqRel);
            runtime.spawn(async move {
                if !release_handle(&host, handle).await {
                    tracing::warn!("process destructor cleanup was not acknowledged");
                }
            });
        }
    }
    fn watch(&self, process: Arc<OwnedProcess>, mut changes: tokio::sync::watch::Receiver<u64>) {
        let weak = self.self_ref.clone();
        let Some(host) = self.host.clone() else {
            return;
        };
        let stopping = self.stopping.clone();
        let span = tracing::info_span!("process", tool_name = %process.tool_name, session_id = %process.session_id, process_id = process.process_id.0, pid = process.pid);
        tokio::spawn(
            async move {
                loop {
                    let Some(registry) = weak.upgrade() else {
                        break;
                    };
                    if registry.sealed.load(Ordering::Acquire)
                        || !registry
                            .local()
                            .iter()
                            .any(|entry| Arc::ptr_eq(entry, &process))
                    {
                        break;
                    }
                    if let Ok(status) = registry
                        .status(&host, process.handle, process.capture)
                        .await
                    {
                        registry.update(&process, status);
                    }
                    drop(registry);
                    tokio::select! {
                        biased;
                        _ = stopping.cancelled() => break,
                        result = changes.changed() => if result.is_err() { break; },
                    }
                }
            }
            .instrument(span),
        );
    }
}
/// Until a process receipt is accepted, cancellation/abort still owns cleanup.
struct LaunchReceipt {
    owner: Arc<ProcessRegistry>,
    handle: Option<ProcessHandle>,
}
impl LaunchReceipt {
    fn accept(mut self) -> ProcessHandle {
        self.handle.take().expect("receipt accepts once")
    }
}
impl Drop for LaunchReceipt {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.owner.abandon_process(handle);
        }
    }
}
struct Admission {
    owner: Arc<ProcessRegistry>,
    refund: Option<String>,
    worker_session: Option<String>,
    launch_handle: Option<Arc<Mutex<Option<ProcessHandle>>>>,
}
impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(session) = &self.refund {
            let handle = self
                .launch_handle
                .as_ref()
                .and_then(|handle| *handle.lock().unwrap_or_else(|e| e.into_inner()));
            let retained = handle.is_some_and(|handle| {
                let pending = self
                    .owner
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .contains_key(&handle.0.0);
                pending || self.owner.owned_process(session, handle).is_some()
            });
            if !retained {
                self.owner.refund_session(session);
            }
        }
        if let Some(session) = &self.worker_session
            && let Some(budget) = self
                .owner
                .contents
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .sessions
                .get_mut(session)
        {
            budget.workers -= 1;
        }
        self.owner.reserved.fetch_sub(1, Ordering::AcqRel);
        self.owner.signal();
    }
}

#[async_trait]
impl ProcessControl for ProcessRegistry {
    fn session_id(&self) -> &str {
        &self.owner
    }
    fn wakeup(&self) -> Arc<Notify> {
        self.changed.clone()
    }
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    fn has_running(&self) -> bool {
        self.has_running_scoped(None, true)
    }
    fn has_unclosed(&self) -> bool {
        self.has_unclosed_scoped(None, true)
    }
    fn projection(&self) -> Option<Value> {
        self.projection_scoped(None, true)
    }
    fn register_child(&self, child: Arc<ProcessRegistry>) -> Result<(), Error> {
        let _hierarchy = HIERARCHY.lock().unwrap_or_else(|e| e.into_inner());
        if self.sealed.load(Ordering::Acquire) || child.sealed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if child.limits.output_buffer_bytes > self.limits.output_buffer_bytes {
            return Err(Error::config(
                "child process capture budget exceeds parent capture budget",
            ));
        }
        if child
            .parent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
        {
            return Err(Error::config("child process registry already has a parent"));
        }
        let root = self.hierarchy_root();
        if root.sealed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let mut existing = BTreeSet::new();
        let mut incoming = BTreeSet::new();
        if !root.owners(&mut existing)
            || !child.owners(&mut incoming)
            || !existing.is_disjoint(&incoming)
        {
            return Err(Error::config(
                "child process ownership must be acyclic with distinct session ids",
            ));
        }
        let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
        if self.sealed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        *child.parent.lock().unwrap_or_else(|e| e.into_inner()) = Some(self.self_ref.clone());
        let mut observed_generation = child.generation();
        let child_wakeup = child.wakeup();
        let child_ref = Arc::downgrade(&child);
        contents.children.insert(child.owner.clone(), child);
        drop(contents);
        self.signal();
        let parent_ref = self.self_ref.clone();
        tokio::spawn(async move {
            loop {
                let next = child_wakeup.notified();
                tokio::pin!(next);
                next.as_mut().enable();
                let Some(child) = child_ref.upgrade() else {
                    break;
                };
                let current_generation = child.generation();
                drop(child);
                if current_generation == observed_generation {
                    next.await;
                }
                if let Some(child) = child_ref.upgrade() {
                    observed_generation = child.generation();
                }
                let Some(parent) = parent_ref.upgrade() else {
                    break;
                };
                parent.signal();
                if parent.sealed.load(Ordering::Acquire) {
                    break;
                }
            }
        });
        Ok(())
    }
    fn registry(&self) -> Arc<ProcessRegistry> {
        self.self_ref
            .upgrade()
            .expect("a live registry has an Arc owner")
    }
    fn scoped(&self, session_id: String, agent: String, is_main: bool) -> Arc<dyn ProcessControl> {
        let root = self
            .self_ref
            .upgrade()
            .expect("a live registry has an Arc owner");
        Arc::new(ScopedProcessControl::new(root, session_id, agent, is_main))
    }
    async fn launch_shell(
        &self,
        tool: &str,
        call_id: &str,
        request: ShellRequest,
        cancellation: CancellationToken,
    ) -> Result<ProcessHandle, Error> {
        self.launch_scoped(
            &self.owner,
            &self.owner,
            tool,
            call_id,
            Launch::Shell(request),
            cancellation,
        )
        .await
    }
    async fn launch_process(
        &self,
        tool: &str,
        call_id: &str,
        spec: ProcessSpec,
        cancellation: CancellationToken,
    ) -> Result<ProcessHandle, Error> {
        self.launch_scoped(
            &self.owner,
            &self.owner,
            tool,
            call_id,
            Launch::Process(spec),
            cancellation,
        )
        .await
    }
    fn owns_process(&self, handle: ProcessHandle) -> bool {
        self.owned_process(&self.owner, handle).is_some()
    }
    fn process_pid(&self, handle: ProcessHandle) -> Option<Option<u32>> {
        self.owned_process(&self.owner, handle)
            .map(|process| process.pid)
    }
    async fn release_process(
        &self,
        handle: ProcessHandle,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        self.release_process_scoped(&self.owner, handle, cancellation)
            .await
    }
    async fn read_process_pipe(
        &self,
        handle: ProcessHandle,
        stderr: bool,
        max: usize,
        cancellation: CancellationToken,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.read_pipe_scoped(&self.owner, handle, stderr, max, cancellation)
            .await
    }
}

impl ProcessRegistry {
    pub fn has_running(&self) -> bool {
        self.has_running_scoped(None, true)
    }

    pub fn has_unclosed(&self) -> bool {
        self.has_unclosed_scoped(None, true)
    }

    pub(crate) fn has_running_scoped(&self, target_session: Option<&str>, is_main: bool) -> bool {
        if is_main {
            self.candidates().iter().any(|(_, j)| {
                j.status
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .status_projection()
                    .is_pending()
            })
        } else {
            let session = target_session.unwrap_or(&self.owner);
            self.candidates().iter().any(|(_, j)| {
                j.session_id == session
                    && j.status
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .status_projection()
                        .is_pending()
            })
        }
    }

    pub(crate) fn has_unclosed_scoped(&self, target_session: Option<&str>, is_main: bool) -> bool {
        if is_main {
            self.candidates().iter().any(|(_, j)| {
                !j.status
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .status_projection()
                    .output_closed
            })
        } else {
            let session = target_session.unwrap_or(&self.owner);
            self.candidates().iter().any(|(_, j)| {
                j.session_id == session
                    && !j
                        .status
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .status_projection()
                        .output_closed
            })
        }
    }

    pub(crate) fn projection_scoped(
        &self,
        caller_session: Option<&str>,
        is_main: bool,
    ) -> Option<Value> {
        let mut values = serde_json::Map::new();
        for (_owner, process) in self.candidates() {
            if !is_main
                && let Some(target) = caller_session
                && process.session_id != target
            {
                continue;
            }
            let status = process
                .status
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let mut value = serde_json::to_value(status).unwrap_or(Value::Null);
            if let Some(obj) = value.as_object_mut() {
                obj.insert("tool_name".into(), json!(process.tool_name));
                obj.insert("session_id".into(), json!(process.session_id));
                obj.insert("agent".into(), json!(process.agent));
                obj.insert("process_id".into(), json!(process.process_id));
                obj.insert("call_id".into(), json!(process.call_id));
                obj.insert(
                    "registered_at_unix_ms".into(),
                    json!(process.registered_at_unix_ms),
                );
            }
            let key = if is_main && process.session_id != self.owner {
                format!("{}/{}", process.session_id, process.process_id.0)
            } else {
                process.process_id.0.to_string()
            };
            values.insert(key, value);
        }
        if values.is_empty() {
            None
        } else {
            Some(Value::Object(values))
        }
    }

    async fn launch_scoped(
        &self,
        session_id: &str,
        agent: &str,
        tool: &str,
        call_id: &str,
        request: Launch,
        cancellation: CancellationToken,
    ) -> Result<ProcessHandle, Error> {
        let host = self
            .host
            .clone()
            .ok_or_else(|| Error::tool("Host process capability is unavailable"))?;
        let session_stopping = {
            let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
            if self.sealed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let budget = contents
                .sessions
                .get_mut(session_id)
                .ok_or_else(|| Error::denied("unregistered process Session"))?;
            if budget.sealed {
                return Err(Error::Closed);
            }
            if budget.occupied >= budget.max_processes {
                return Err(Error::tool(
                    "process capacity reached; drain completed processes before starting another",
                ));
            }
            budget.occupied += 1;
            budget.workers += 1;
            self.reserved.fetch_add(1, Ordering::AcqRel);
            budget.stopping.clone()
        };
        let owner = self.registry();
        let launch_handle = Arc::new(Mutex::new(None));
        let mut admission = Admission {
            owner: owner.clone(),
            refund: Some(session_id.into()),
            worker_session: Some(session_id.into()),
            launch_handle: Some(launch_handle.clone()),
        };
        let token = cancellation.child_token();
        let _cancel = crate::utils::cancel::CancelOnDrop(token.clone());
        let (tool, call_id, session_id, agent) = (
            tool.to_owned(),
            call_id.to_owned(),
            session_id.to_owned(),
            agent.to_owned(),
        );
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let mut pending = None;
            let observed = AssertUnwindSafe(owner.observe_launch(
                &session_id,
                &agent,
                &host,
                &tool,
                &call_id,
                request,
                token.clone(),
                &mut pending,
                &launch_handle,
            ))
            .catch_unwind()
            .await;
            let mut result =
                observed.unwrap_or_else(|_| Err(Error::tool("process launch worker panicked")));
            if let Some(handle) = pending {
                if release_handle(&host, handle).await {
                    owner
                        .pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&handle.0.0);
                } else {
                    owner.cleanup_failed.store(true, Ordering::Release);
                    admission.refund = None; // unresolved handle retains this Session's slot
                    result = Err(Error::tool("process startup containment cleanup failed"));
                }
            }
            // The receipt owns cleanup even after send succeeds: aborting before
            // the receiver observes it drops the guard inside the channel.
            if result.is_ok() {
                admission.refund = None; // published ownership now carries the slot
            }
            let result = result.map(|handle| LaunchReceipt {
                owner: owner.clone(),
                handle: Some(handle),
            });
            drop(admission);
            let _ = sender.send(result);
        });
        let receipt = receiver
            .await
            .map_err(|_| Error::tool("process launch worker ended before returning a result"))??;
        // A scoped close may finish after publication while this caller has not
        // polled its receipt. Never return the handle already released by close.
        if cancellation.is_cancelled()
            || self.stopping.is_cancelled()
            || session_stopping.is_cancelled()
        {
            return Err(Error::Cancelled);
        }
        Ok(receipt.accept())
    }
    fn abandon_process(&self, handle: ProcessHandle) {
        let removed = {
            let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
            let removed = contents.entries.remove(&ProcessId(handle.0.0));
            if let Some(process) = &removed {
                contents
                    .sessions
                    .get_mut(&process.session_id)
                    .expect("owned process has a registered Session")
                    .workers += 1;
                self.reserved.fetch_add(1, Ordering::AcqRel);
            }
            removed
        };
        if let Some(process) = removed {
            if let Some(host) = &self.host {
                self.release_detached(host.clone(), handle, process.session_id.clone());
            }
            self.signal();
        }
    }
    fn owned_process(&self, session: &str, handle: ProcessHandle) -> Option<Arc<OwnedProcess>> {
        self.local()
            .into_iter()
            .find(|process| process.handle == handle && process.session_id == session)
    }
    async fn release_process_scoped(
        &self,
        session: &str,
        handle: ProcessHandle,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let owner = self.registry();
        let host = self
            .host
            .clone()
            .ok_or_else(|| Error::tool("Host process capability is unavailable"))?;
        let mut receiver = {
            let mut contents = self.contents.lock().unwrap_or_else(|e| e.into_inner());
            // Sealing prevents admission, while existing owners can still release.
            let process = contents
                .entries
                .get(&ProcessId(handle.0.0))
                .filter(|process| process.session_id == session)
                .ok_or_else(|| Error::denied("process belongs to another Session"))?
                .clone();
            let mut previous = process
                .release_completion
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(previous) = previous.as_ref()
                && previous.completion.borrow().is_none()
                && previous.completion.has_changed().is_ok()
            {
                previous.completion.clone()
            } else {
                let (sender, receiver) = tokio::sync::watch::channel(None);
                *previous = Some(ProcessRelease {
                    completion: receiver.clone(),
                });
                process.releasing.store(true, Ordering::Release);
                self.reserved.fetch_add(1, Ordering::AcqRel);
                contents
                    .sessions
                    .get_mut(session)
                    .expect("owned process has a registered Session")
                    .workers += 1;
                let admission = Admission {
                    owner: owner.clone(),
                    refund: None,
                    worker_session: Some(session.into()),
                    launch_handle: None,
                };
                let attempt = ProcessReleaseAttempt {
                    process: process.clone(),
                    completion: sender,
                };
                tokio::spawn(async move {
                    // Neither waiter cancellation nor timeout can erase this owner.
                    // Both guards are captured before spawn, including never-polled work.
                    let ctx = owner.context(CancellationToken::new());
                    let released =
                        AssertUnwindSafe(ProcessApi::release(host.as_ref(), &ctx, handle))
                            .catch_unwind()
                            .await;
                    let result = match released {
                        Ok(Ok(())) => {
                            let removed = owner
                                .contents
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .entries
                                .remove(&ProcessId(handle.0.0));
                            if let Some(process) = removed {
                                owner.refund_session(&process.session_id);
                            }
                            owner.signal();
                            Ok(())
                        }
                        Ok(Err(error)) => Err(host_error(error).tool_diagnostic()),
                        Err(_) => {
                            Err(Error::tool("process release worker panicked").tool_diagnostic())
                        }
                    };
                    if result.is_err() {
                        owner.cleanup_failed.store(true, Ordering::Release);
                    }
                    let completion = attempt.completion.clone();
                    drop(attempt);
                    drop(admission);
                    completion.send_replace(Some(result));
                });
                receiver
            }
        };
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result.map_err(Error::Diagnostic);
            }
            receiver.changed().await.map_err(|_| {
                Error::tool("process release worker ended before returning a result")
            })?;
        }
    }
    async fn read_pipe_scoped(
        &self,
        session: &str,
        handle: ProcessHandle,
        stderr: bool,
        max: usize,
        cancellation: CancellationToken,
    ) -> Result<Option<Vec<u8>>, Error> {
        let process = self
            .owned_process(session, handle)
            .ok_or_else(|| Error::denied("process belongs to another Session"))?;
        if process.capture {
            return Err(host_error(HostError::new(
                ErrorCategory::Conflict,
                "shell capture owns process pipes",
            )));
        }
        if max == 0 || max > self.limits.max_tool_output_bytes.min(65_536) {
            return Err(Error::tool("process pipe read byte budget exceeded"));
        }
        let pipe = if stderr {
            &process.stderr
        } else {
            &process.stdout
        };
        let mut pipe = tokio::select! { biased; _ = cancellation.cancelled() => return Err(Error::Cancelled), pipe = pipe.lock() => pipe };
        if !pipe.acquired {
            let host = self
                .host
                .as_ref()
                .ok_or_else(|| Error::tool("Host process capability is unavailable"))?;
            pipe.stream = Some(
                if stderr {
                    ProcessApi::stderr(host.as_ref(), handle)
                } else {
                    ProcessApi::stdout(host.as_ref(), handle)
                }
                .map_err(host_error)?,
            );
            pipe.acquired = true;
        }
        while pipe.pending.is_empty() && !pipe.eof {
            let stream = pipe
                .stream
                .as_mut()
                .expect("acquired pipe has a stream until EOF");
            let next = tokio::select! { biased; _ = cancellation.cancelled() => return Err(Error::Cancelled), next = stream.next() => next };
            match next {
                Some(Ok(bytes)) => pipe.pending = bytes,
                Some(Err(error)) => return Err(host_error(error)),
                None => {
                    pipe.eof = true;
                    pipe.stream = None;
                }
            }
        }
        let output = if pipe.pending.is_empty() {
            None
        } else {
            let count = max.min(pipe.pending.len());
            Some(pipe.pending.split_to(count).to_vec())
        };
        drop(pipe);
        // Never await after consuming bytes: an aborted caller must not lose a
        // chunk between extraction from retained pending bytes and its receipt.
        if output.is_none()
            && let Some(host) = &self.host
            && let Ok(status) = self.status(host, handle, false).await
        {
            self.update(&process, status);
        }
        Ok(output)
    }
}

#[derive(Clone)]
pub struct ScopedProcessControl {
    registry: Arc<ProcessRegistry>,
    session_id: String,
    agent: Arc<Mutex<String>>,
    is_main: bool,
}

impl ScopedProcessControl {
    pub fn new(
        registry: Arc<ProcessRegistry>,
        session_id: String,
        agent: String,
        is_main: bool,
    ) -> Self {
        Self {
            registry,
            session_id,
            agent: Arc::new(Mutex::new(agent)),
            is_main,
        }
    }
    pub fn set_agent(&self, agent: String) {
        *self.agent.lock().unwrap_or_else(|e| e.into_inner()) = agent;
    }
}

#[async_trait]
impl ProcessControl for ScopedProcessControl {
    fn session_id(&self) -> &str {
        &self.session_id
    }
    fn wakeup(&self) -> Arc<Notify> {
        self.registry.wakeup()
    }
    fn generation(&self) -> u64 {
        self.registry.generation()
    }
    fn has_running(&self) -> bool {
        self.registry
            .has_running_scoped(Some(&self.session_id), self.is_main)
    }
    fn has_unclosed(&self) -> bool {
        self.registry
            .has_unclosed_scoped(Some(&self.session_id), self.is_main)
    }
    fn projection(&self) -> Option<Value> {
        self.registry
            .projection_scoped(Some(&self.session_id), self.is_main)
    }
    fn register_child(&self, child: Arc<ProcessRegistry>) -> Result<(), Error> {
        self.registry.register_child(child)
    }
    fn registry(&self) -> Arc<ProcessRegistry> {
        self.registry.clone()
    }
    fn scoped(&self, session_id: String, agent: String, is_main: bool) -> Arc<dyn ProcessControl> {
        Arc::new(ScopedProcessControl::new(
            self.registry.clone(),
            session_id,
            agent,
            is_main,
        ))
    }
    async fn launch_shell(
        &self,
        tool: &str,
        call_id: &str,
        request: ShellRequest,
        cancellation: CancellationToken,
    ) -> Result<ProcessHandle, Error> {
        let agent = self.agent.lock().unwrap_or_else(|e| e.into_inner()).clone();
        self.registry
            .launch_scoped(
                &self.session_id,
                &agent,
                tool,
                call_id,
                Launch::Shell(request),
                cancellation,
            )
            .await
    }
    async fn launch_process(
        &self,
        tool: &str,
        call_id: &str,
        spec: ProcessSpec,
        cancellation: CancellationToken,
    ) -> Result<ProcessHandle, Error> {
        let agent = self.agent.lock().unwrap_or_else(|e| e.into_inner()).clone();
        self.registry
            .launch_scoped(
                &self.session_id,
                &agent,
                tool,
                call_id,
                Launch::Process(spec),
                cancellation,
            )
            .await
    }
    fn owns_process(&self, handle: ProcessHandle) -> bool {
        self.registry
            .owned_process(&self.session_id, handle)
            .is_some()
    }
    fn process_pid(&self, handle: ProcessHandle) -> Option<Option<u32>> {
        self.registry
            .owned_process(&self.session_id, handle)
            .map(|process| process.pid)
    }
    async fn release_process(
        &self,
        handle: ProcessHandle,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        self.registry
            .release_process_scoped(&self.session_id, handle, cancellation)
            .await
    }
    async fn read_process_pipe(
        &self,
        handle: ProcessHandle,
        stderr: bool,
        max: usize,
        cancellation: CancellationToken,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.registry
            .read_pipe_scoped(&self.session_id, handle, stderr, max, cancellation)
            .await
    }
}
fn host_error(error: HostError) -> Error {
    Error::from_host_error(ToolError::Host(error))
}
enum Launch {
    Shell(ShellRequest),
    Process(ProcessSpec),
}
#[derive(Default)]
struct RawPipe {
    stream: Option<BoxStream<Bytes>>,
    pending: Bytes,
    acquired: bool,
    eof: bool,
}
async fn release_handle(host: &Arc<dyn Host>, handle: ProcessHandle) -> bool {
    let ctx = OpContext::default();
    // Release owns kill/reap and retained-state cleanup by the Host contract.
    // A preliminary kill failure cannot invalidate its successful acknowledgement.
    let _ = AssertUnwindSafe(ProcessApi::kill_tree(host.as_ref(), &ctx, handle, true))
        .catch_unwind()
        .await;
    let released = AssertUnwindSafe(ProcessApi::release(host.as_ref(), &ctx, handle))
        .catch_unwind()
        .await;
    matches!(released, Ok(Ok(())))
}
impl Drop for ProcessRegistry {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_retention_uses_its_own_cap_and_preserves_open_output() {
        for (process_cap, execution_cap) in [(2, 0), (0, 2)] {
            let registry = ProcessRegistry::detached(Limits {
                max_retained_processes: process_cap,
                max_retained_executions: execution_cap,
                ..Limits::default()
            });
            for id in 1..=4 {
                let closed = id != 4;
                let status = SessionProcessStatus {
                    pid: Some(id as u32),
                    state: SessionProcessState::Exited { code: 0 },
                    exit_code: Some(0),
                    closed,
                    cursor: ShellPosition::default(),
                    stdout_bytes: usize::from(!closed),
                    stderr_bytes: 0,
                    truncated: false,
                    elapsed_s: 1,
                };
                let projection = status.status_projection();
                assert_eq!(
                    projection.kind,
                    crate::runtime::execution::ExecutionKind::Process
                );
                assert!(!projection.is_pending());
                // Terminal production and closed output are independent facts.
                assert_eq!(projection.output_closed, closed);
                registry.contents.lock().unwrap().entries.insert(
                    ProcessId(id),
                    Arc::new(OwnedProcess {
                        process_id: ProcessId(id),
                        tool_name: "shell".into(),
                        session_id: "detached".into(),
                        agent: "fixture".into(),
                        call_id: format!("call-{id}"),
                        registered_at_unix_ms: 0,
                        handle: ProcessHandle(microcrab_host_api::HandleId(id)),
                        pid: Some(id as u32),
                        capture: true,
                        releasing: AtomicBool::new(false),
                        release_completion: Mutex::new(None),
                        bridged: AtomicBool::new(false),
                        stdout: Default::default(),
                        stderr: Default::default(),
                        status: Mutex::new(status),
                    }),
                );
            }
            registry.evict();
            let contents = registry.contents.lock().unwrap();
            assert_eq!(contents.entries.len(), process_cap + 1);
            assert!(contents.entries.contains_key(&ProcessId(4)));
            assert!(!contents.entries.contains_key(&ProcessId(1)));
            if process_cap == 2 {
                assert!(contents.entries.contains_key(&ProcessId(2)));
                assert!(contents.entries.contains_key(&ProcessId(3)));
            }
        }
    }

    #[test]
    fn projection_preserves_host_process_duration_and_names_registration_time() {
        for pid in [Some(321), None] {
            let registry = ProcessRegistry::detached(Limits::default());
            let status = SessionProcessStatus {
                pid,
                state: SessionProcessState::Running,
                exit_code: None,
                closed: false,
                cursor: ShellPosition::default(),
                stdout_bytes: 0,
                stderr_bytes: 0,
                truncated: false,
                elapsed_s: 47,
            };
            registry.contents.lock().unwrap().entries.insert(
                ProcessId(1),
                Arc::new(OwnedProcess {
                    process_id: ProcessId(1),
                    tool_name: "shell".into(),
                    session_id: "detached".into(),
                    agent: "fixture".into(),
                    call_id: "invocation-1".into(),
                    registered_at_unix_ms: 1234,
                    handle: ProcessHandle(microcrab_host_api::HandleId(1)),
                    pid,
                    capture: true,
                    releasing: AtomicBool::new(false),
                    release_completion: Mutex::new(None),
                    bridged: AtomicBool::new(false),
                    stdout: Default::default(),
                    stderr: Default::default(),
                    status: Mutex::new(status),
                }),
            );
            let view = registry.projection_scoped(None, true).unwrap();
            assert_eq!(view["1"]["process_id"], 1);
            assert!(view["1"].get("job_id").is_none());
            assert_eq!(view["1"]["elapsed_s"], 47);
            assert_eq!(view["1"]["tool_name"], "shell");
            assert_eq!(view["1"]["registered_at_unix_ms"], 1234);
            assert_eq!(view["1"]["call_id"], "invocation-1");
            assert!(view["1"].get("tool_id").is_none());
            assert!(view["1"].get("started_at_unix_ms").is_none());
            assert_eq!(
                registry.process_pid(ProcessHandle(microcrab_host_api::HandleId(1))),
                Some(pid)
            );
            assert_eq!(
                registry.process_pid(ProcessHandle(microcrab_host_api::HandleId(99))),
                None
            );
            match pid {
                Some(native) => assert_eq!(view["1"]["pid"], native),
                None => assert!(view["1"].get("pid").is_none()),
            }
        }
    }
}

#[cfg(test)]
#[path = "quota_tests.rs"]
mod quota_tests;
