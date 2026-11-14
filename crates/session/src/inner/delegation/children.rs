//! Parent close seals admission, drains every child owner and reports sticky cleanup errors.
use crate::Error;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Registry {
    closed: bool,
    settled: bool,
    failure: Option<String>,
    tasks: Vec<(CancellationToken, oneshot::Receiver<Result<(), String>>)>,
}

/// An owner must explicitly report cleanup. Panic, abort and forgotten completion
/// become errors rather than indistinguishable successful acknowledgements.
pub struct Completion(Option<oneshot::Sender<Result<(), String>>>);
impl Completion {
    pub fn finish(mut self, result: Result<(), String>) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(result);
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(Err("child owner ended without completing cleanup".into()));
        }
    }
}

#[derive(Clone, Default)]
pub struct Children {
    registry: Arc<Mutex<Registry>>,
    settled: Arc<Notify>,
}
impl Children {
    pub async fn register(&self, cancellation: CancellationToken) -> Result<Completion, Error> {
        let (completed, receiver) = oneshot::channel();
        let mut registry = self.registry.lock().await;
        if registry.closed {
            cancellation.cancel();
            return Err(Error::Closed);
        }
        let mut failure = None;
        registry
            .tasks
            .retain_mut(|(_, receiver)| match receiver.try_recv() {
                Err(oneshot::error::TryRecvError::Empty) => true,
                Ok(Ok(())) => false,
                Ok(Err(error)) => {
                    failure.get_or_insert(error);
                    false
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    failure.get_or_insert_with(|| "child cleanup completion channel closed".into());
                    false
                }
            });
        if registry.failure.is_none() {
            registry.failure = failure;
        }
        registry.tasks.push((cancellation, receiver));
        Ok(Completion(Some(completed)))
    }

    pub async fn close(&self) -> Result<(), Error> {
        loop {
            let changed = self.settled.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut registry = self.registry.lock().await;
            if !registry.closed {
                registry.closed = true;
                let tasks = std::mem::take(&mut registry.tasks);
                let state = self.registry.clone();
                let settled = self.settled.clone();
                // The cleanup owner outlives any individual close waiter. Aborting
                // an HTTP/Python future cannot drop admitted child acknowledgements.
                tokio::spawn(async move {
                    for (cancellation, _) in &tasks {
                        cancellation.cancel();
                    }
                    let mut failure = None;
                    for (_, completed) in tasks {
                        match completed.await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                failure.get_or_insert(error);
                            }
                            Err(_) => {
                                failure.get_or_insert_with(|| {
                                    "child cleanup completion channel closed".into()
                                });
                            }
                        }
                    }
                    let mut registry = state.lock().await;
                    if registry.failure.is_none() {
                        registry.failure = failure;
                    }
                    registry.settled = true;
                    settled.notify_waiters();
                });
            }
            if registry.settled {
                return match &registry.failure {
                    Some(error) => Err(Error::tool_with_private(
                        "child Session cleanup failed",
                        format!("child cleanup failed: {error}"),
                    )),
                    None => Ok(()),
                };
            }
            drop(registry);
            changed.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn close_seals_registration_and_waits_for_admitted_child_cleanup() {
        let children = Children::default();
        let cancellation = CancellationToken::new();
        let completed = children.register(cancellation.clone()).await.unwrap();
        let owner = children.clone();
        let close = tokio::spawn(async move { owner.close().await });
        cancellation.cancelled().await;
        assert!(!close.is_finished());
        let late = CancellationToken::new();
        assert!(matches!(
            children.register(late.clone()).await,
            Err(Error::Closed)
        ));
        assert!(late.is_cancelled());
        completed.finish(Ok(()));
        close.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cleanup_failure_stays_sticky_after_pruning_and_close_still_drains_suspended_owners() {
        let children = Children::default();
        let failed = children.register(CancellationToken::new()).await.unwrap();
        failed.finish(Err("child journal flush failed".into()));
        // A later admission prunes completed owners but must retain their error.
        let cancellation = CancellationToken::new();
        let suspended = children.register(cancellation.clone()).await.unwrap();
        let owner = children.clone();
        let close = tokio::spawn(async move { owner.close().await });
        cancellation.cancelled().await;
        assert!(!close.is_finished());
        suspended.finish(Ok(()));
        let error = close.await.unwrap().unwrap_err().to_string();
        assert!(error.contains("child journal flush failed"));
        assert!(
            children
                .close()
                .await
                .unwrap_err()
                .to_string()
                .contains("child journal flush failed")
        );
    }

    #[tokio::test]
    async fn cancelling_close_waiter_does_not_abandon_cleanup_or_hide_failure() {
        let children = Children::default();
        let cancellation = CancellationToken::new();
        let completion = children.register(cancellation.clone()).await.unwrap();
        let owner = children.clone();
        let first = tokio::spawn(async move { owner.close().await });
        cancellation.cancelled().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(!children.registry.lock().await.settled);
        let owner = children.clone();
        let second = tokio::spawn(async move { owner.close().await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        completion.finish(Err("delayed child close failed".into()));
        assert!(
            second
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("delayed child close failed")
        );
        assert!(
            children
                .close()
                .await
                .unwrap_err()
                .to_string()
                .contains("delayed child close failed")
        );
    }

    #[tokio::test]
    async fn dropping_child_owner_without_explicit_cleanup_reports_failure() {
        let children = Children::default();
        let completion = children.register(CancellationToken::new()).await.unwrap();
        drop(completion);
        assert!(
            children
                .close()
                .await
                .unwrap_err()
                .to_string()
                .contains("without completing cleanup")
        );
    }
}
