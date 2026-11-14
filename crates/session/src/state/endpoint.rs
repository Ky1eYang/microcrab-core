//! Nongeneric variable mutation authority. Construction is synchronous; the first
//! sender starts the driver on Tokio. The driver retains only a weak endpoint
//! between commands, so detached State snapshots cannot keep a Session alive.
use super::{State, VariableCommit, VariableWitness};
use crate::{Error, VariablesChange};
use microcrab_protocol::VariableRole;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// Commands belong only to this queue driver. Acknowledgements report atomic
/// commits and fence admission, never plugin delivery.
enum VariableCommand {
    /// Acknowledge preceding writes, then pause until the guard releases the receiver.
    Fence {
        terminal: bool,
        reply: oneshot::Sender<Result<(), Error>>,
        release: oneshot::Receiver<()>,
    },
    Changed {
        change: VariablesChange,
        actor: VariableRole,
        expected: Option<VariableWitness>,
        reply: oneshot::Sender<Result<VariableCommit, Error>>,
    },
}

pub(crate) struct VariableEndpoint {
    state: State,
    closed: Arc<AtomicBool>,
    faulted: Arc<AtomicBool>,
    commands: mpsc::UnboundedSender<VariableCommand>,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<VariableCommand>>>,
    shutdown: CancellationToken,
}

/// Exclusive queue position for a coherent State/Context capture or replacement.
/// Drop releases later commands, including when the caller's future is cancelled.
pub(crate) struct VariableFence {
    release: Option<oneshot::Sender<()>>,
}
impl Drop for VariableFence {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl VariableEndpoint {
    pub(crate) fn new(
        state: State,
        closed: Arc<AtomicBool>,
        faulted: Arc<AtomicBool>,
    ) -> Arc<Self> {
        let (commands, receiver) = mpsc::unbounded_channel();
        Arc::new(Self {
            state,
            closed,
            faulted,
            commands,
            receiver: Mutex::new(Some(receiver)),
            shutdown: CancellationToken::new(),
        })
    }

    /// State writes may start the driver before Session initialization.
    fn sender(self: &Arc<Self>) -> mpsc::UnboundedSender<VariableCommand> {
        let receiver = self
            .receiver
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(receiver) = receiver {
            tokio::spawn(Self::run(
                Arc::downgrade(self),
                receiver,
                self.shutdown.clone(),
            ));
        }
        self.commands.clone()
    }

    pub(crate) async fn change(
        self: &Arc<Self>,
        change: VariablesChange,
        actor: VariableRole,
        expected: Option<VariableWitness>,
    ) -> Result<VariableCommit, Error> {
        self.ensure_open()?;
        let (reply, received) = oneshot::channel();
        self.sender()
            .send(VariableCommand::Changed {
                change,
                actor,
                expected,
                reply,
            })
            .map_err(|_| Error::Closed)?;
        received.await.map_err(|_| Error::Closed)?
    }

    /// Release before running hooks that may submit ordinary queued writes.
    /// All preceding commands finish before acknowledgement. Later commands stay
    /// queued until release; shutdown remains responsive while the fence is held.
    pub(crate) async fn fence(self: &Arc<Self>) -> Result<VariableFence, Error> {
        self.fence_inner(false).await
    }

    /// Only the close worker may use this after acquiring the operation lock.
    /// Final persistence still orders behind queued writes when closed/faulted;
    /// this does not reopen external mutation or bypass endpoint shutdown.
    pub(crate) async fn terminal_fence(self: &Arc<Self>) -> Result<VariableFence, Error> {
        self.fence_inner(true).await
    }

    async fn fence_inner(self: &Arc<Self>, terminal: bool) -> Result<VariableFence, Error> {
        self.ensure_fence(terminal)?;
        let (reply, received) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let fence = VariableFence {
            release: Some(release),
        };
        self.sender()
            .send(VariableCommand::Fence {
                terminal,
                reply,
                release: released,
            })
            .map_err(|_| Error::Closed)?;
        received.await.map_err(|_| Error::Closed)??;
        Ok(fence)
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
        self.state.clear_ephemeral_variables();
    }

    fn ensure_fence(&self, terminal: bool) -> Result<(), Error> {
        if terminal {
            if self.shutdown.is_cancelled() {
                Err(Error::Closed)
            } else {
                Ok(())
            }
        } else {
            self.ensure_open()
        }
    }

    pub(super) fn ensure_open(&self) -> Result<(), Error> {
        if self.shutdown.is_cancelled() || self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        if self.faulted.load(Ordering::SeqCst) {
            return Err(Error::config("session faulted; create a new session"));
        }
        Ok(())
    }

    async fn run(
        owner: Weak<Self>,
        mut receiver: mpsc::UnboundedReceiver<VariableCommand>,
        shutdown: CancellationToken,
    ) {
        loop {
            let command = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                command = receiver.recv() => match command {
                    Some(command) => command,
                    None => return,
                },
            };
            let Some(endpoint) = owner.upgrade() else {
                Self::reject(command);
                break;
            };
            match command {
                VariableCommand::Fence {
                    terminal,
                    reply,
                    release,
                } => {
                    let result = endpoint.ensure_fence(terminal);
                    let accepted = result.is_ok();
                    let acknowledged = reply.send(result).is_ok();
                    // Do not retain the endpoint while a caller holds a fence.
                    drop(endpoint);
                    if accepted && acknowledged {
                        tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => break,
                            _ = release => {},
                        }
                    }
                }
                VariableCommand::Changed {
                    change,
                    actor,
                    expected,
                    reply,
                } => {
                    let result = endpoint.ensure_open().and_then(|()| {
                        endpoint.state.apply_change_checked(change, actor, expected)
                    });
                    let _ = reply.send(result);
                }
            }
        }
        receiver.close();
        while let Some(command) = receiver.recv().await {
            Self::reject(command);
        }
    }

    fn reject(command: VariableCommand) {
        match command {
            VariableCommand::Fence { reply, .. } => {
                let _ = reply.send(Err(Error::Closed));
            }
            VariableCommand::Changed { reply, .. } => {
                let _ = reply.send(Err(Error::Closed));
            }
        }
    }
}

impl Drop for VariableEndpoint {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;
    use serde_json::{Map, json};

    fn change() -> VariablesChange {
        VariablesChange {
            scoped_set: vec![],
            set: Map::from_iter([("queued".into(), json!(true))]),
            remove: vec![],
        }
    }

    #[tokio::test]
    async fn shutdown_rejects_queued_writes_and_fences_while_a_fence_is_held() {
        let (state, endpoint) = State::new(Limits::default()).bind_writer(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let held = endpoint.fence().await.unwrap();
        let (write_reply, write_received) = oneshot::channel();
        let (fence_reply, fence_received) = oneshot::channel();
        let (_release, released) = oneshot::channel();
        endpoint
            .sender()
            .send(VariableCommand::Changed {
                change: change(),
                actor: VariableRole::Client,
                expected: None,
                reply: write_reply,
            })
            .unwrap();
        endpoint
            .sender()
            .send(VariableCommand::Fence {
                terminal: false,
                reply: fence_reply,
                release: released,
            })
            .unwrap();
        endpoint.shutdown();
        assert!(matches!(write_received.await.unwrap(), Err(Error::Closed)));
        assert!(matches!(fence_received.await.unwrap(), Err(Error::Closed)));
        assert_eq!(state.extract("queued"), None);
        drop(held);
    }

    #[tokio::test]
    async fn terminal_fence_drains_prior_commands_when_closed_and_faulted() {
        let closed = Arc::new(AtomicBool::new(false));
        let faulted = Arc::new(AtomicBool::new(false));
        let (state, endpoint) =
            State::new(Limits::default()).bind_writer(closed.clone(), faulted.clone());
        let held = endpoint.fence().await.unwrap();
        let (reply, received) = oneshot::channel();
        endpoint
            .sender()
            .send(VariableCommand::Changed {
                change: change(),
                actor: VariableRole::Client,
                expected: None,
                reply,
            })
            .unwrap();
        closed.store(true, Ordering::SeqCst);
        faulted.store(true, Ordering::SeqCst);
        assert!(matches!(endpoint.fence().await, Err(Error::Closed)));
        assert!(matches!(
            state.inject("external", json!(1)).await,
            Err(Error::Closed)
        ));
        drop(held);
        let terminal = endpoint.terminal_fence().await.unwrap();
        assert!(matches!(received.await.unwrap(), Err(Error::Closed)));
        assert_eq!(state.extract("queued"), None);
        state.apply_change(change()).unwrap();
        assert_eq!(state.extract("queued"), Some(json!(true)));
        drop(terminal);
        endpoint.shutdown();
        assert!(matches!(
            endpoint.terminal_fence().await,
            Err(Error::Closed)
        ));
    }

    #[tokio::test]
    async fn dropped_fence_waiter_and_write_ack_do_not_stall_the_queue() {
        let (state, endpoint) = State::new(Limits::default()).bind_writer(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let (reply, received) = oneshot::channel();
        let (_release, released) = oneshot::channel();
        drop(received);
        endpoint
            .sender()
            .send(VariableCommand::Fence {
                terminal: false,
                reply,
                release: released,
            })
            .unwrap();
        let (reply, received) = oneshot::channel();
        drop(received);
        endpoint
            .sender()
            .send(VariableCommand::Changed {
                change: change(),
                actor: VariableRole::Client,
                expected: None,
                reply,
            })
            .unwrap();
        let _settled = endpoint.fence().await.unwrap();
        assert_eq!(state.extract("queued"), Some(json!(true)));
    }
}
