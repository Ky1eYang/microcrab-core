//! Observe committed turns and retire only the model conversation. Resource
//! registries and their notification pumps stay alive until ordinary full close.
use super::*;

impl SessionInner {
    pub(crate) fn subscribe_activity(
        &self,
    ) -> tokio::sync::watch::Receiver<crate::SessionActivity> {
        self.observation.subscribe_activity()
    }
    pub(crate) fn subscribe_turns(&self) -> tokio::sync::broadcast::Receiver<crate::CompletedTurn> {
        self.observation.subscribe_turns()
    }
    pub(crate) fn subscribe_notices(&self) -> tokio::sync::broadcast::Receiver<crate::StateNotice> {
        let mut inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
        let receiver = self.inbox.notices_changed.subscribe();
        if inbox.retired {
            inbox.handoff_notices(&self.inbox.notices_changed);
        }
        receiver
    }
    pub(super) fn publish_activity(&self) {
        use crate::observation::activity::{ActivityProjection, ActivitySnapshot};
        // Publisher acquires its fence before this closure reads inbox. Return
        // owned scalar facts, releasing inbox before a watch reader can block us.
        let result = self.observation.publish(|| {
            let position = self.observation_position();
            let inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
            ActivityProjection::Snapshot(ActivitySnapshot {
                position,
                active: inbox.active,
                paused: inbox.paused,
                ready: inbox.ready(),
                retired: inbox.retired,
            })
        });
        debug_assert!(result.is_ok(), "snapshot projection cannot fail");
    }
    fn observation_position(&self) -> crate::observation::Position {
        self.state.observation_position()
    }
    pub(super) fn publish_completed(
        &self,
        outcome: &TurnOutcome,
        autonomous: bool,
    ) -> Result<(), Error> {
        // Foreground remains reserved. TurnEnded can clear active IDs, so use
        // the authoritative monotonic last-turn identity under publication fence.
        self.observation.publish(|| {
            let position = self.state.completion_position();
            crate::observation::activity::ActivityProjection::Completed {
                epoch: position.epoch,
                turn: position
                    .turn
                    .expect("completion position retains last turn"),
                outcome,
                autonomous,
            }
        })
    }
    pub(super) fn publish_inbox_failure(
        &self,
        error: &Error,
        interrupt_reason: Option<crate::InterruptReason>,
    ) {
        let result = self.observation.publish(|| {
            crate::observation::activity::ActivityProjection::InboxFailure {
                position: self.observation_position(),
                error,
                interrupt_reason,
                retry_ready: self.retry_ready(),
            }
        });
        debug_assert!(result.is_ok(), "failure projection cannot fail");
    }
    pub(super) fn ensure_model_open(&self) -> Result<(), Error> {
        self.ensure_open()?;
        let inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
        if inbox.retired || inbox.retiring {
            return Err(Error::config("Session conversation is retired or retiring"));
        }
        Ok(())
    }
    pub(crate) async fn retire_conversation(self: &Arc<Self>) -> Result<(), Error> {
        self.ensure_open()?;
        // Publish retirement intent before the interrupt. An admission that has
        // reserved foreground but not installed its token must reject itself.
        {
            let mut inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
            inbox.retiring = true;
            inbox.paused = true;
        }
        self.interrupt();
        let _foreground = self.foreground.clone().lock_owned().await;
        let _operation = self.operation.clone().lock_owned().await;
        self.ensure_open()?;
        {
            let mut inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
            inbox.retired = true;
            inbox.retiring = false;
            inbox.paused = true;
            inbox.handoff_notices(&self.inbox.notices_changed);
        }
        self.publish_activity();
        Ok(())
    }
    /// No row can become terminal without publishing its notice before the
    /// registry quiescence fence releases. This closes the final-reply/Wake race.
    pub(crate) async fn retire_if_quiescent(self: &Arc<Self>) -> Result<bool, Error> {
        self.ensure_open()?;
        let Ok(_foreground) = self.foreground.clone().try_lock_owned() else {
            return Ok(false);
        };
        let _operation = self.operation.clone().lock_owned().await;
        self.ensure_open()?;
        let retired = self.state.executions().with_quiescence(|no_pending| {
            let mut inbox = self.inbox.state.lock().unwrap_or_else(|e| e.into_inner());
            if inbox.retired {
                return no_pending;
            }
            if !no_pending || inbox.active || inbox.paused || inbox.ready() {
                return false;
            }
            inbox.retired = true;
            inbox.retiring = false;
            inbox.paused = true;
            inbox.handoff_notices(&self.inbox.notices_changed);
            true
        });
        self.publish_activity();
        Ok(retired)
    }
}
