//! Functional calls borrow a frozen request; they never admit a model turn or tools.
mod auto_compaction;
pub(crate) mod one_shot;
use super::*;
use crate::runtime::subagents;
use microcrab_protocol::Response;

/// One independent helper; its worker never retains the Session owner.
pub(super) struct RenameTask {
    token: CancellationToken,
    abort: tokio::task::AbortHandle,
    done: tokio::sync::watch::Receiver<bool>,
}
impl RenameTask {
    pub(super) fn abort(self) {
        self.token.cancel();
        self.abort.abort();
    }
}
// Signals on normal return, panic and task destruction, including before poll.
struct RenameDone(tokio::sync::watch::Sender<bool>);
impl Drop for RenameDone {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

// Detached State deliberately cannot expose its Session-only VariableView.
// Copy the already projected values without retaining or bypassing an endpoint.
struct RenameVariables {
    visible: Value,
    all: Value,
}
impl microcrab_auditor::VariableView for RenameVariables {
    fn get(&self, name: &str) -> Option<Value> {
        self.visible.get(name).cloned()
    }
    fn get_all(&self, name: &str) -> Option<Value> {
        self.all.get(name).cloned()
    }
}

impl SessionInner {
    pub(super) async fn close_rename(&self) {
        let task = self.rename.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(mut task) = task {
            task.token.cancel();
            loop {
                let done = *task.done.borrow_and_update();
                if done || task.done.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    pub(crate) async fn rename_with_model(
        self: &Arc<Self>,
        user_input: String,
    ) -> Result<Response, Error> {
        self.ensure_open()?;
        if self.kind == crate::SessionKind::OneShot {
            return Err(Error::denied(
                "one-shot Sessions cannot recursively start functional operations",
            ));
        }
        let token = CancellationToken::new();
        let _cancel = CancelOnDrop(token.clone());
        let worker = {
            // Registration is ordered with close by closed + this slot. Helpers
            // never reserve foreground/storage or overwrite its interrupt token.
            let mut slot = self.rename.lock().unwrap_or_else(|e| e.into_inner());
            self.ensure_open()?;
            if self.kind == crate::SessionKind::OneShot {
                return Err(Error::denied(
                    "one-shot Sessions cannot recursively start functional operations",
                ));
            }
            if self.replacing.load(Ordering::Acquire) {
                return Err(Error::Busy);
            }
            if slot.as_ref().is_some_and(|task| !*task.done.borrow()) {
                return Err(Error::Busy);
            }
            // Keep Context immutable while capturing detached State. Its epoch
            // belongs to this issuing Context, never a later provider callback.
            let context = self.context.lock().unwrap_or_else(|e| e.into_inner());
            let state = self.state.preview();
            let limits = state.snapshot().limits;
            // Rename owns a Clean prompt and discards the issuer's conversation,
            // tools and system. Capture only its epoch and actual Context budget:
            // admitted live material may legitimately have pending tool results.
            let issued = crate::context::Content {
                epoch: context.epoch(),
                subagent: None,
                system: String::new(),
                items: vec![],
                tools: vec![],
                max_output_tokens: limits.max_output_tokens,
                max_bytes: context.measure()?.limit_bytes,
                turn_id: None,
                round: None,
            };
            state.update(|snapshot| snapshot.epoch_id = issued.epoch);
            let variables = RenameVariables {
                visible: state.model_variables()?,
                all: state.view_all()?,
            };
            drop(context);
            // Separate completion correlation and detached hook writes keep a
            // helper from consuming main call IDs or changing live variables.
            let auditor = Arc::new(crate::observation::hooks::BoundAuditor {
                inner: self.auditor.inner.clone(),
                state: state.clone(),
                completed: Mutex::new(None),
            });
            let client = self
                .route
                .as_ref()
                .map(|route| route.client(issued.epoch))
                .unwrap_or_else(|| self.client.clone());
            let cancellation = token.clone();
            let executor = self.one_shots.clone();
            let (done, receiver) = tokio::sync::watch::channel(false);
            let done = RenameDone(done);
            let worker = tokio::spawn(async move {
                let _done = done;
                AssertUnwindSafe(subagents::rename::run(
                    executor.as_ref(),
                    client,
                    auditor.inner.clone(),
                    Arc::new(variables),
                    &issued,
                    &user_input,
                    &limits,
                    &cancellation,
                ))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(Error::lifecycle("rename worker panicked")))
            });
            *slot = Some(RenameTask {
                token: token.clone(),
                abort: worker.abort_handle(),
                done: receiver,
            });
            worker
        };
        let response = worker.await.map_err(|_| Error::Cancelled)??;
        if token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        self.ensure_open()?;
        Ok(response)
    }

    pub(crate) async fn compact_with_model(
        self: &Arc<Self>,
        user_input: String,
        variables: Value,
        token: CancellationToken,
    ) -> Result<Response, Error> {
        self.ensure_open()?;
        if self.kind == crate::SessionKind::OneShot {
            return Err(Error::denied(
                "one-shot Sessions cannot recursively start functional operations",
            ));
        }
        crate::state::Variables::new(variables.clone())?;
        let _operation = self.reserve_replacement().await?;
        if token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        self.register_replacement_token(&token)?;
        let _active = TurnGuard {
            inner: self.clone(),
            clear: false,
        };
        let _cancel = CancelOnDrop(token.clone());
        self.initialize(&token).await?;
        let limits = self.state().limits;
        // Clone only committed Context. Do not synchronize variables, refresh
        // builtins, materialize a pending epoch or emit a functional lifecycle.
        let issued = self
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .content(limits.max_output_tokens)?;
        let response = subagents::compact::run(
            self.one_shots.as_ref(),
            self.client.clone(),
            self.auditor.inner.clone(),
            Arc::new(self.state.clone()),
            &issued,
            &user_input,
            &token,
        )
        .await?;
        if token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        self.ensure_open()?;
        // Only successful text reaches the existing mechanical installer. Its
        // validation and guarded publication are shared with manual compaction.
        self.compact_locked(response.content.clone(), variables)
            .await?;
        Ok(response)
    }
}
