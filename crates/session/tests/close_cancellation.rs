//! Closing belongs to Session even when the first caller stops awaiting it.
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, Injection, Moment, VariableView,
};
use microcrab_protocol::{Response, VariablesChange};
use microcrab_session::context::Content;
use microcrab_session::runtime::{Runtime, tool::DenyAll};
use microcrab_session::{Client, Error, Session};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Answer;
#[async_trait::async_trait]
impl Client for Answer {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
    }
}
#[derive(Default)]
struct Closing {
    gate_round: bool,
    round_entered: Notify,
    terminal_entered: Notify,
    terminal_release: Notify,
    terminal_calls: AtomicUsize,
}
impl Auditor for Closing {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, _: AuditBody) {}
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        _: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            if moment == Moment::RoundCompleted && self.gate_round {
                self.round_entered.notify_one();
                std::future::pending::<()>().await;
            }
            if moment == Moment::SessionTerminated {
                self.terminal_calls.fetch_add(1, Ordering::SeqCst);
                self.terminal_entered.notify_one();
                self.terminal_release.notified().await;
                return Ok(Injection {
                    variables: VariablesChange {
                        scoped_set: vec![],
                        set: json!({"terminal":true}).as_object().unwrap().clone(),
                        remove: vec![],
                    },
                    ..Default::default()
                });
            }
            Ok(Injection::default())
        })
    }
}
fn session(auditor: Arc<Closing>) -> Session {
    Session::builder_arc(
        Arc::new(Answer),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .auditor(auditor)
    .build()
    .unwrap()
}

#[tokio::test]
async fn aborting_first_close_while_round_intervention_is_pending_preserves_cleanup() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let hooks = Arc::new(Closing {
            gate_round: true,
            ..Default::default()
        });
        let session = session(hooks.clone());
        session.start().await.unwrap();
        let running = session.clone();
        let turn = tokio::spawn(async move { running.submit("work").await });
        hooks.round_entered.notified().await;
        let closing = session.clone();
        let first = tokio::spawn(async move { closing.close().await });
        // Close cancels the active intervention, then its owned worker awaits
        // terminal intervention independently of any close waiter.
        hooks.terminal_entered.notified().await;
        assert!(matches!(session.start().await, Err(Error::Closed)));
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let closing = session.clone();
        let later = tokio::spawn(async move { closing.close().await });
        assert!(!later.is_finished());
        hooks.terminal_release.notify_one();
        assert!(matches!(turn.await.unwrap(), Err(Error::Cancelled)));
        later.await.unwrap().unwrap();
        assert_eq!(hooks.terminal_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            session.state_handle().extract("terminal"),
            Some(json!(true))
        );
    })
    .await
    .expect("close must cancel pending hooks and finish the owned terminal intervention");
}

#[tokio::test]
async fn aborting_first_close_during_terminal_intervention_allows_a_later_caller_to_join() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let hooks = Arc::new(Closing::default());
        let session = session(hooks.clone());
        session.start().await.unwrap();
        let closing = session.clone();
        let first = tokio::spawn(async move { closing.close().await });
        hooks.terminal_entered.notified().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let closing = session.clone();
        let later = tokio::spawn(async move { closing.close().await });
        assert!(matches!(
            session.state_handle().inject("global", json!(true)).await,
            Err(Error::Closed)
        ));
        assert!(session.state_handle().extract("terminal").is_none());
        assert!(!later.is_finished());
        hooks.terminal_release.notify_one();
        later.await.unwrap().unwrap();
        assert_eq!(hooks.terminal_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            session.state_handle().extract("terminal"),
            Some(json!(true))
        );
        session.close().await.unwrap();
    })
    .await
    .expect("a later close must join the original terminal hook exactly once");
}
