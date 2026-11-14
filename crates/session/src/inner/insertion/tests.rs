//! Framework insertion tests cover registry delivery, inbox lanes and round refresh.
mod buffered_delivery;
mod identity;
mod mutations;
use crate::admission::inbox::{
    InboxState, Lane, NoticeFact, NoticeKey, NoticeKind, StateDelivery, StateNotice,
};
use crate::context::{Content, ContentItem};
use crate::runtime::execution::{ExecutionId, ExecutionStatus};
use crate::runtime::{Runtime, tool::DenyAll};
use crate::{Auditor, Client, Limits, Session, State};
use microcrab_protocol::{Response, Usage};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

fn notice(delivery: StateDelivery, id: u64) -> StateNotice {
    StateNotice {
        delivery,
        key: NoticeKey::Execution {
            session_id: "test".into(),
            execution_id: ExecutionId(id),
            change: NoticeKind::Metadata,
        },
        fact: NoticeFact {
            status: ExecutionStatus::Running,
            failure: None,
            exit_code: None,
        },
    }
}
struct Notices {
    state: Mutex<Option<State>>,
    seen: Mutex<Vec<Content>>,
}
#[async_trait::async_trait]
impl Client for Notices {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let first = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(content.clone());
            seen.len() == 1
        };
        if first {
            self.state
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .post_notice(notice(StateDelivery::Continue, 1))
                .unwrap();
        }
        Ok(Response {
            provider_metadata: None,
            content: "final".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
fn session(cap: usize) -> (Session, Arc<Notices>) {
    let client = Arc::new(Notices {
        state: Mutex::new(None),
        seen: Mutex::new(vec![]),
    });
    let session = Session::builder_arc(
        client.clone(),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .limits(Limits {
        max_rounds: cap,
        ..Default::default()
    })
    .build()
    .unwrap();
    *client.state.lock().unwrap() = Some(session.state_handle());
    (session, client)
}
#[tokio::test]
async fn real_continue_sends_state_suffix_once_and_ending_assistant_is_not_resent() {
    let (session, client) = session(3);
    let result = session.submit("first").await.unwrap();
    assert_eq!(result.rounds, 2);
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(matches!(seen[1].items.last(), Some(ContentItem::State(_))));
        assert_eq!(seen[0].turn_id, seen[1].turn_id);
    }
    session.close().await.unwrap();
}
#[tokio::test]
async fn cap_downgrades_continue_to_observe_for_the_next_real_request() {
    let (session, client) = session(1);
    assert_eq!(session.submit("first").await.unwrap().rounds, 1);
    assert_eq!(client.seen.lock().unwrap().len(), 1);
    session.submit("second").await.unwrap();
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(
            serde_json::to_string(&seen[1].items)
                .unwrap()
                .contains("notices")
        );
    }
    session.close().await.unwrap();
}
struct RenewRounds(Mutex<Vec<crate::budget::RoundLimitRequest>>);
#[async_trait::async_trait]
impl crate::budget::RoundLimitCallback for RenewRounds {
    async fn request(
        &self,
        request: crate::budget::RoundLimitRequest,
        _: CancellationToken,
    ) -> Result<crate::budget::RoundLimitDecision, crate::Error> {
        self.0.lock().unwrap().push(request);
        Ok(crate::budget::RoundLimitDecision::Continue)
    }
}
#[tokio::test]
async fn framework_continue_survives_confirmation_and_starts_one_round() {
    let client = Arc::new(Notices {
        state: Mutex::new(None),
        seen: Mutex::new(vec![]),
    });
    let callback = Arc::new(RenewRounds(Mutex::new(vec![])));
    let session = Session::builder_arc(
        client.clone(),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .limits(Limits {
        max_rounds: 1,
        ..Default::default()
    })
    .round_limit_callback(callback.clone())
    .build()
    .unwrap();
    *client.state.lock().unwrap() = Some(session.state_handle());
    let result = session.submit("first").await.unwrap();
    assert_eq!(result.rounds, 2);
    assert_eq!(
        result.stop_reason,
        crate::runtime::TurnStopReason::ModelCompleted
    );
    {
        let requests = callback.0.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].used, 1);
    }
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[1].round, Some(2));
        assert!(matches!(seen[1].items.last(), Some(ContentItem::State(_))));
        assert_eq!(seen[0].turn_id, seen[1].turn_id);
    }
    session.close().await.unwrap();
}

#[test]
fn inbox_budgets_coalescing_and_snapshot_ack_are_independent() {
    let mut inbox = InboxState::default();
    for _ in 0..128 {
        inbox.enqueue("kept".into(), true).unwrap();
    }
    assert!(inbox.enqueue("overflow".into(), true).is_err());
    assert_eq!(
        inbox.next_turn().unwrap().input().unwrap(),
        crate::SessionInput::text("kept")
    );
    // Framework capacity is separate from a completely full user budget.
    inbox.active = true;
    inbox.post(notice(StateDelivery::Continue, 1)).unwrap();
    let snapshot = inbox.step(true, false);
    assert_eq!(snapshot.notices.len(), 1);
    inbox.post(notice(StateDelivery::Continue, 1)).unwrap();
    inbox.acknowledge(&[], &[snapshot.notices[0].id]);
    assert_eq!(inbox.step(true, false).notices.len(), 1);
    inbox.end_turn(true);
    assert!(inbox.paused);
    assert!(inbox.next_turn().is_none());
    assert_eq!(inbox.step(true, true).notices[0].lane, Lane::Observe);
}
#[test]
fn idle_framework_continue_is_observe_and_wake_is_next_turn() {
    let mut inbox = InboxState::default();
    inbox.post(notice(StateDelivery::Continue, 1)).unwrap();
    inbox.post(notice(StateDelivery::Wake, 2)).unwrap();
    assert!(inbox.step(true, false).notices.is_empty());
    assert!(inbox.next_turn().unwrap().framework());
    assert_eq!(inbox.step(true, true).notices.len(), 1);
}

#[tokio::test]
async fn idle_wake_is_a_typed_framework_turn_and_retired_notices_only_forward() {
    let (session, client) = session(1);
    session.submit("first").await.unwrap();
    let mut turns = session.subscribe_turns();
    let mut notices = session.subscribe_notices();
    session
        .state_handle()
        .post_notice(notice(StateDelivery::Wake, 2))
        .unwrap();
    let completed = tokio::time::timeout(std::time::Duration::from_secs(3), turns.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.sequence, 2);
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[1].items.iter().any(|item| matches!(
            item,
            ContentItem::User {
                source: Some(microcrab_transport::UserSource::Framework),
                ..
            }
        )));
        assert_ne!(seen[0].turn_id, seen[1].turn_id);
    }
    session
        .state_handle()
        .post_notice(notice(StateDelivery::Continue, 4))
        .unwrap();
    session.retire_conversation().await.unwrap();
    assert!(session.subscribe_activity().borrow().retired);
    assert!(session.submit("retired input").await.is_err());
    session
        .state_handle()
        .post_notice(notice(StateDelivery::Wake, 3))
        .unwrap();
    assert_eq!(
        notices.recv().await.unwrap().delivery,
        StateDelivery::Continue
    );
    assert_eq!(notices.recv().await.unwrap().delivery, StateDelivery::Wake);
    session.retire_conversation().await.unwrap();
    assert!(matches!(
        notices.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert!(session.retire_if_quiescent().await.unwrap());
    assert_eq!(client.seen.lock().unwrap().len(), 2);
    session.close().await.unwrap();
}

struct BufferedTool(Mutex<Option<crate::runtime::execution::OutputWriter>>);
#[async_trait::async_trait]
impl crate::runtime::tool::Tool for BufferedTool {
    fn definition(&self) -> microcrab_protocol::ToolDefinition {
        microcrab_protocol::ToolDefinition {
            name: "background".into(),
            description: "background".into(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        _: serde_json::Value,
        context: crate::runtime::tool::ToolContext,
    ) -> Result<crate::runtime::tool::ToolOutput, crate::Error> {
        let (writer, output) = context.open_output(
            Default::default(),
            Arc::new(crate::runtime::execution::CancellationController::new(
                CancellationToken::new(),
            )),
        )?;
        *self.0.lock().unwrap() = Some(writer);
        Ok(crate::runtime::tool::ToolOutput::Buffered(output))
    }
}
#[derive(Default)]
struct BackgroundModel(Mutex<Vec<Content>>);
#[async_trait::async_trait]
impl Client for BackgroundModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut seen = self.0.lock().unwrap();
        seen.push(content.clone());
        Ok(Response {
            provider_metadata: None,
            content: "reply".into(),
            usage: Usage::default(),
            tool_calls: if seen.len() == 1 {
                vec![microcrab_protocol::ToolCall {
                    id: "background-call".into(),
                    name: "background".into(),
                    arguments: serde_json::json!({}),
                    provider_metadata: None,
                }]
            } else {
                vec![]
            },
        })
    }
}
#[tokio::test]
async fn delivered_buffer_terminal_wakes_after_idle_metadata_downgrade() {
    use crate::runtime::{ToolRegistration, tool::AllowTools};
    let tool = Arc::new(BufferedTool(Mutex::new(None)));
    let model = Arc::new(BackgroundModel::default());
    let runtime = Runtime::new(
        vec![ToolRegistration::External(tool.clone())],
        Arc::new(AllowTools::new(["background"])),
    )
    .unwrap();
    let session = Session::builder_arc(model.clone(), runtime)
        .build()
        .unwrap();
    let mut turns = session.subscribe_turns();
    assert_eq!(session.submit("first").await.unwrap().rounds, 2);
    assert_eq!(turns.recv().await.unwrap().sequence, 1);
    assert!(!session.retire_if_quiescent().await.unwrap());
    let writer = tool.0.lock().unwrap().as_ref().unwrap().clone();
    writer.append(None, "metadata only while idle").unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), turns.recv())
            .await
            .is_err()
    );
    writer.finish(Ok(())).unwrap();
    let turn = tokio::time::timeout(std::time::Duration::from_secs(3), turns.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(turn.sequence, 2);
    assert!(
        model
            .0
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .items
            .iter()
            .any(|item| matches!(
                item,
                ContentItem::User {
                    source: Some(microcrab_transport::UserSource::Framework),
                    ..
                }
            ))
    );
    session.retire_conversation().await.unwrap();
    assert!(session.retire_if_quiescent().await.unwrap());
    session.close().await.unwrap();
}

#[tokio::test]
async fn retirement_keeps_pending_writer_and_quiescence_waits_for_its_terminal() {
    use crate::runtime::{ToolRegistration, tool::AllowTools};
    let tool = Arc::new(BufferedTool(Mutex::new(None)));
    let model = Arc::new(BackgroundModel::default());
    let runtime = Runtime::new(
        vec![ToolRegistration::External(tool.clone())],
        Arc::new(AllowTools::new(["background"])),
    )
    .unwrap();
    let session = Session::builder_arc(model.clone(), runtime)
        .build()
        .unwrap();
    session.submit("first").await.unwrap();
    let writer = tool.0.lock().unwrap().as_ref().unwrap().clone();
    let mut notices = session.subscribe_notices();
    session.retire_conversation().await.unwrap();
    assert!(!session.retire_if_quiescent().await.unwrap());
    writer.append(None, "after retirement").unwrap();
    writer.finish(Ok(())).unwrap();
    assert!(session.retire_if_quiescent().await.unwrap());
    assert!(notices.try_recv().is_ok());
    assert_eq!(model.0.lock().unwrap().len(), 2);
    assert_eq!(
        session
            .read_execution_output(writer.execution_id(), 0, 4096)
            .unwrap()
            .chunks[0]
            .content,
        "after retirement"
    );
    session.close().await.unwrap();
}
