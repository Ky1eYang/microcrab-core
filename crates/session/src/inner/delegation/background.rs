//! One parent execution retains the child until conversation and resources settle.
//! Session owns model admission; this worker only starts the initial turn and
//! observes committed replies, retirement and resource changes.
use super::*;
use crate::runtime::execution::{
    ExecutionController, ExecutionId, OutputOptions, OutputWriter, ToolFailure, ToolFailureKind,
    ToolOutput,
};
use futures_util::FutureExt;
use std::collections::BTreeMap;
use tokio::sync::{broadcast, watch};

struct ChildController {
    cancellation: CancellationToken,
    completion:
        watch::Receiver<Option<Result<(), crate::observation::diagnostics::ToolDiagnostic>>>,
    retained: Arc<tokio::sync::Mutex<Option<ChildSession>>>,
}
impl ChildController {
    async fn stop(&self, cancellation: CancellationToken) -> Result<(), Error> {
        self.cancellation.cancel();
        let mut completion = self.completion.clone();
        loop {
            let result = completion.borrow_and_update().clone();
            if let Some(result) = result {
                if result.is_err() {
                    let retained = self.retained.lock().await;
                    if let Some(child) = retained.as_ref() {
                        child.session.executions().close().await?;
                        child.auditor.flush().await.map_err(|error| {
                            Error::audit_with_private("child audit flush failed", error.to_string())
                        })?;
                    }
                }
                // Session close is a sticky acknowledged result. A later resource
                // retry cannot erase a failed journal/lifecycle close witness.
                return result.map_err(Error::Diagnostic);
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                result = completion.changed() => result.map_err(|_| Error::ExecutionInterrupted)?,
            }
        }
    }
}
#[async_trait]
impl ExecutionController for ChildController {
    async fn cancel(&self, cancellation: CancellationToken) -> Result<(), Error> {
        self.stop(cancellation).await
    }
    async fn release(&self, cancellation: CancellationToken) -> Result<(), Error> {
        self.stop(cancellation).await
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Delegation preserves separate target, mode, authority, focus and invocation owners"
)]
pub(super) async fn start(
    executor: &OwnedDelegateExecutor,
    target: &str,
    input: &str,
    mode: Option<&str>,
    context_mode: &str,
    timeout_ms: Option<u64>,
    interaction: crate::state::communication::SessionInteraction,
    focus: Vec<crate::state::reference::focus::FocusSnapshot>,
    invocation: ToolContext,
) -> Result<ToolOutput, Error> {
    // Capture authority before child registration or assembly can yield.
    let issuer_epoch = invocation.state.snapshot().epoch_id;
    let deadline = timeout_ms
        .map(|ms| {
            tokio::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(ms))
                .ok_or_else(|| Error::tool("delegate timeout is too large"))
        })
        .transpose()?;
    if invocation.cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    let owner_cancellation = CancellationToken::new();
    let completion = executor
        .children
        .register(owner_cancellation.clone())
        .await?;
    if invocation.cancellation.is_cancelled() {
        completion.finish(Ok(()));
        return Err(Error::Cancelled);
    }
    let retained = Arc::new(tokio::sync::Mutex::new(None));
    let (finished, receiver) = watch::channel(None);
    let controller = Arc::new(ChildController {
        cancellation: owner_cancellation.clone(),
        completion: receiver.clone(),
        retained: retained.clone(),
    });
    let (writer, output) = match invocation.open_output(
        OutputOptions {
            channels: vec!["reply".into(), "update".into()],
        },
        controller,
    ) {
        Ok(output) => output,
        Err(error) => {
            completion.finish(Ok(()));
            return Err(error);
        }
    };
    let factory = executor.factory.clone();
    let executor_parent = executor
        .parent
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    let target = target.to_owned();
    let input = input.to_owned();
    let mode = mode.map(str::to_owned);
    let context_mode = context_mode.to_owned();
    let output_budget = invocation.state.snapshot().limits.max_tool_output_bytes;
    tokio::spawn(async move {
        let mut child = None;
        let work = async {
            child = Some(
                factory
                    .create_for_epoch(
                        invocation.mode.as_deref(),
                        &target,
                        mode.as_deref(),
                        &context_mode,
                        invocation.processes.clone(),
                        issuer_epoch,
                    )
                    .await?,
            );
            let child = child.as_ref().expect("factory returned a child");
            if mode.as_deref() != Some(child.mode.as_str())
                || child.id != child.session.process_control().session_id()
            {
                return Err(Error::config(
                    "prepared child mode or Session identity differs from the admitted request",
                ));
            }
            child
                .session
                .inner
                .select_mode_now(child.mode.clone(), None)?;
            factory.configure(child).await?;
            *child
                .session
                .inner
                .delegation_completion
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(receiver.clone());
            if let Some(progress) = &invocation.execution_progress {
                progress.bind_delegate_child(crate::runtime::execution::DelegateChild {
                    session_id: child.id.clone(),
                    target: target.clone(),
                })?;
            }
            let parent = executor_parent
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
                .ok_or_else(|| Error::tool("delegation requires the issuing Session owner"))?;
            if invocation.state.snapshot().communication.is_some()
                && let Some(mut communication) = child.session.state().communication
            {
                communication.identity.interaction = interaction;
                communication.focus = focus;
                communication.validate()?;
                child.session.inner.configure_unstarted_conversation_tools(
                    Some(&communication.identity),
                    Some(&child.mode),
                )?;
                child
                    .session
                    .inner
                    .state
                    .install_focused_references(communication.focus.clone())?;
                child
                    .session
                    .inner
                    .state
                    .update(|snapshot| snapshot.communication = Some(communication));
            } else if interaction != crate::state::communication::SessionInteraction::Persistent
                || !focus.is_empty()
            {
                return Err(Error::denied(
                    "legacy child cannot accept new focused interaction",
                ));
            }
            parent.adopt_child(
                &target,
                mode.as_deref(),
                issuer_epoch,
                &child.session,
                &invocation,
            )?;
            child.session.start().await?;

            observe(
                child,
                input,
                &target,
                &writer,
                output_budget,
                invocation.execution_progress.as_ref(),
            )
            .await
        };
        let expired = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        let result = std::panic::AssertUnwindSafe(async {
            tokio::select! {
                biased;
                _ = owner_cancellation.cancelled() => Err(Error::Cancelled),
                _ = expired => Err(Error::ToolFailure(ToolFailure {
                    kind: ToolFailureKind::Timeout, code: "delegate_timeout".into(),
                    message: format!("delegate timed out after {}ms", timeout_ms.expect("deadline requires timeout")),
                    retryable: false,
                })),
                result = work => result,
            }
        }).catch_unwind().await.unwrap_or_else(|_| Err(Error::tool("child session worker panicked")));
        // A canceled observation never abandons the published child. Full close
        // keeps its ordinary destructive semantics, including retired resources.
        let child_id = child.as_ref().map(|child| child.id.clone());
        let cleanup = if let Some(child) = child {
            child.session.interrupt();
            let closed = child.session.close().await;
            let flushed = tokio::time::timeout(
                std::time::Duration::from_millis(child.session.state().limits.lifecycle_timeout_ms),
                child.auditor.flush(),
            )
            .await
            .map_err(|_| Error::audit("child audit flush deadline"))
            .and_then(|result| {
                result.map_err(|error| {
                    Error::audit_with_private("child audit flush failed", error.to_string())
                })
            });
            let close_succeeded = closed.is_ok() && flushed.is_ok();
            if let Err(error) = factory.finalize(&child, close_succeeded).await {
                tracing::warn!(reason = %error.diagnostic_reason(), "child display finalization failed");
            }
            let result = closed.and(flushed);
            if result.is_err() {
                *retained.lock().await = Some(child);
            }
            result
        } else {
            Ok(())
        };
        let cleanup_result = cleanup
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.tool_diagnostic());
        if let Some(parent) = executor_parent.as_ref().and_then(std::sync::Weak::upgrade)
            && let Some(id) = child_id
        {
            parent.child_settled(&id, &target, cleanup_result.clone());
        }
        let terminal = cleanup.and(result);
        // Publish cleanup ACK before the terminal buffer event: an explicit release
        // can immediately request controller cleanup and must not await itself.
        completion.finish(
            cleanup_result
                .clone()
                .map_err(|diagnostic| diagnostic.summary().to_owned()),
        );
        finished.send_replace(Some(cleanup_result));
        match terminal {
            Ok(()) => {
                let _ = writer.finish(Ok(()));
            }
            Err(Error::Cancelled) => {
                let _ = writer.confirm_cancelled();
            }
            Err(Error::ToolFailure(failure)) => {
                let _ = writer.finish(Err(failure));
            }
            Err(error) => {
                let _ = writer.finish(Err(ToolFailure::failed(
                    "delegate_failed",
                    error.tool_diagnostic().summary(),
                )));
            }
        }
    });
    Ok(ToolOutput::Buffered(output))
}

fn broadcast_error(error: broadcast::error::RecvError) -> Error {
    match error {
        broadcast::error::RecvError::Lagged(count) => Error::tool_with_private(
            "child observation lost committed events; continuity cannot be established",
            format!("child observation lost {count} committed events"),
        ),
        broadcast::error::RecvError::Closed => Error::ExecutionInterrupted,
    }
}

fn reply(
    child: &ChildSession,
    target: &str,
    outcome: crate::runtime::TurnOutcome,
    writer: &OutputWriter,
    budget: usize,
) -> Result<(), Error> {
    let text = bound_delegate_receipt(
        json!({
            "child_id": child.id, "subagent": target, "outcome": outcome,
            "usage": child.ledger.total(), "executions": [],
        }),
        budget,
    )?;
    writer.append(Some("reply"), &format!("{text}\n"))
}

async fn observe(
    child: &ChildSession,
    input: String,
    target: &str,
    writer: &OutputWriter,
    budget: usize,
    progress: Option<&crate::runtime::execution::ExecutionProgress>,
) -> Result<(), Error> {
    if child
        .session
        .communication_identity()
        .is_some_and(|identity| {
            identity.interaction == crate::state::communication::SessionInteraction::Return
        })
    {
        let outcome = child
            .session
            .inner
            .submit(SessionInput::text(input), None)
            .await?;
        reply(child, target, outcome, writer, budget)?;
        return Ok(());
    }
    let mut turns = child.session.subscribe_turns();
    let mut notices = child.session.subscribe_notices();
    let mut activity = child.session.subscribe_activity();
    let executions = child.session.executions();
    let mut changed = executions.subscribe();
    let mut cursors = BTreeMap::new();
    // A failed/canceled child turn pauses its own inbox. Keep this owner waiting
    // for explicit resume/cancel instead of reporting success or resubmitting it.
    if let Err(error) = child
        .session
        .inner
        .submit(SessionInput::text(input), None)
        .await
    {
        let text = serde_json::to_string(
            &json!({"child_id":child.id,"turn_error":error.tool_diagnostic().result_json(),"paused":true}),
        )?;
        writer.append(Some("update"), &format!("{text}\n"))?;
        if let Some(progress) = progress {
            progress.mark_waiting();
        }
    }
    loop {
        loop {
            match turns.try_recv() {
                Ok(turn) => reply(child, target, turn.outcome, writer, budget)?,
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(count)) => {
                    return Err(broadcast_error(broadcast::error::RecvError::Lagged(count)));
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Err(Error::ExecutionInterrupted);
                }
            }
        }
        drain_notices(child, writer, &mut notices)?;
        let snapshot = activity.borrow_and_update().clone();
        if let Some(progress) = progress {
            if snapshot.paused && !snapshot.retired {
                progress.mark_waiting();
            } else {
                progress.mark_running();
            }
        }
        if snapshot.retired {
            forward_retired_output(child, writer, &mut cursors, budget)?;
        }
        // A settled Turn leaves a delegated Session available for authorized
        // follow-up. Only explicit retirement/close, parent cleanup or deadline
        // settles this long-lived output owner.
        if snapshot.retired && !executions.has_pending() {
            return Ok(());
        }
        tokio::select! {
            turn = turns.recv() => reply(child, target, turn.map_err(broadcast_error)?.outcome, writer, budget)?,
            notice = notices.recv() => {
                let notice = notice.map_err(broadcast_error)?;
                let text = serde_json::to_string(&json!({"child_id":child.id,"notice":notice}))?;
                writer.append(Some("update"), &format!("{text}\n"))?;
            },
            result = activity.changed() => result.map_err(|_| Error::ExecutionInterrupted)?,
            result = changed.changed() => result.map_err(|_| Error::ExecutionInterrupted)?,
        }
    }
}

fn drain_notices(
    child: &ChildSession,
    writer: &OutputWriter,
    notices: &mut broadcast::Receiver<crate::StateNotice>,
) -> Result<(), Error> {
    loop {
        match notices.try_recv() {
            Ok(notice) => {
                let text = serde_json::to_string(&json!({"child_id":child.id,"notice":notice}))?;
                writer.append(Some("update"), &format!("{text}\n"))?;
            }
            Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
            Err(broadcast::error::TryRecvError::Lagged(count)) => {
                return Err(broadcast_error(broadcast::error::RecvError::Lagged(count)));
            }
            Err(broadcast::error::TryRecvError::Closed) => return Err(Error::ExecutionInterrupted),
        }
    }
}

/// Read through the retained, creator-owned Session, without widening builtin
/// job authority or consuming child buffer cursors. Parent output has its own
/// bounded cursor space and labels every original execution identity.
fn forward_retired_output(
    child: &ChildSession,
    writer: &OutputWriter,
    cursors: &mut BTreeMap<ExecutionId, u64>,
    budget: usize,
) -> Result<(), Error> {
    for row in child.session.execution_snapshots() {
        if row.output.is_none() {
            continue;
        }
        let cursor = cursors.entry(row.execution_id).or_default();
        loop {
            let page = child.session.read_execution_output(
                row.execution_id,
                *cursor,
                budget.clamp(512, 4096),
            )?;
            let next = page.next_cursor;
            if !page.chunks.is_empty() || page.start_cursor > *cursor {
                let text = serde_json::to_string(
                    &json!({"child_id":child.id,"execution_id":row.execution_id,"output":page}),
                )?;
                writer.append(Some("update"), &format!("{text}\n"))?;
            }
            if next <= *cursor {
                break;
            }
            *cursor = next;
        }
    }
    Ok(())
}
