//! Exercise producer observations through the real registry and Session notice port.
use super::*;
use crate::runtime::execution::{
    CancellationController, ExecutionNotice, ExecutionNoticeSink, ExecutionRegistry,
    ExecutionSnapshot, OutputWriter,
};
use crate::runtime::tool::ToolOutput;
use serde_json::json;

fn take_notice(inbox: &crate::admission::inbox::Inbox) -> Option<StateNotice> {
    let mut pending = inbox.state.lock().unwrap();
    let snapshot = pending.step(true, true);
    assert!(snapshot.notices.len() <= 1);
    let entry = snapshot.notices.first()?;
    let notice = entry.notice.clone();
    pending.acknowledge(&[], &[entry.id]);
    Some(notice)
}

#[tokio::test]
async fn registry_observations_classify_passive_bytes_and_terminal_without_clock_events() {
    let registry = ExecutionRegistry::new(Limits::default());
    let inbox = Arc::new(crate::admission::inbox::Inbox::default());
    let state = State::new(Limits::default());
    state.install_inbox(Arc::downgrade(&inbox));
    let sink: Arc<dyn ExecutionNoticeSink> = Arc::new(super::super::SessionNoticeSink {
        inbox: inbox.clone(),
        state,
    });
    registry.bind_notice_sink(Arc::downgrade(&sink));
    inbox.state.lock().unwrap().active = true;
    let owner = registry.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut handle = registry
        .start(
            ExecutionSnapshot::new(
                "owner".into(),
                "call".into(),
                "background".into(),
                "agent".into(),
                None,
            ),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = owner.open_output(
                    progress.execution_id(),
                    Default::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                sender
                    .send((writer, progress))
                    .map_err(|_| crate::Error::Closed)?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = handle.id;
    handle.activate().unwrap();
    handle.wait().await.unwrap();
    handle.detach();
    let (writer, progress) = receiver.await.unwrap();
    // No event before Context delivery, and the receipt is already the baseline.
    writer.mark_truncated().unwrap();
    assert!(take_notice(&inbox).is_none());
    registry.acknowledge_result_delivered(id);
    assert_eq!(
        take_notice(&inbox).unwrap().delivery,
        StateDelivery::Continue,
        "the explicit upstream-loss marker advances production cursors"
    );
    registry.with_quiescence(|pending| assert!(!pending));
    registry.get(id).unwrap();
    registry.snapshot();
    registry.with_quiescence(|_| {});
    assert!(
        take_notice(&inbox).is_none(),
        "observing clocks never posts a notice"
    );

    progress.mark_waiting();
    assert_eq!(take_notice(&inbox).unwrap().delivery, StateDelivery::Wake);
    progress.mark_running();
    assert_eq!(take_notice(&inbox).unwrap().delivery, StateDelivery::Wake);
    writer.append(Some("stderr"), "").unwrap();
    assert_eq!(
        take_notice(&inbox).unwrap().delivery,
        StateDelivery::Passive
    );
    writer
        .append(Some("stderr"), "body-stays-only-in-job")
        .unwrap();
    let bytes = take_notice(&inbox).unwrap();
    assert_eq!(bytes.delivery, StateDelivery::Continue);
    assert!(
        !serde_json::to_string(&bytes)
            .unwrap()
            .contains("body-stays-only-in-job")
    );
    let retained = serde_json::to_value(registry.read(id, 0, 4096).unwrap()).unwrap();
    assert_eq!(
        serde_json::to_value(registry.read(id, 0, 4096).unwrap()).unwrap(),
        retained
    );
    assert!(
        take_notice(&inbox).is_none(),
        "nonconsuming reads do not change production"
    );
    writer.finish(Ok(())).unwrap();
    assert_eq!(take_notice(&inbox).unwrap().delivery, StateDelivery::Wake);
    let terminal = serde_json::to_value(registry.read(id, 0, 4096).unwrap()).unwrap();
    assert_eq!(terminal["chunks"], retained["chunks"]);
    assert_eq!(terminal["start_cursor"], retained["start_cursor"]);
    assert_eq!(terminal["end_cursor"], retained["end_cursor"]);
    assert_eq!(terminal["closed"], true);
    registry.acknowledge_result_delivered(id);
    registry.with_quiescence(|pending| assert!(pending));
    assert!(
        take_notice(&inbox).is_none(),
        "terminal baseline must not duplicate completion"
    );
    registry.close().await.unwrap();
}

#[derive(Clone, Copy)]
enum ProducerChange {
    Metadata,
    Bytes,
    Terminal,
}
struct DuringRound {
    tool: Arc<BufferedTool>,
    change: ProducerChange,
    seen: Mutex<Vec<Content>>,
}
#[async_trait::async_trait]
impl Client for DuringRound {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let round = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(content.clone());
            seen.len()
        };
        if round == 2 {
            let writer: OutputWriter = self.tool.0.lock().unwrap().as_ref().unwrap().clone();
            match self.change {
                ProducerChange::Metadata => writer.mark_truncated().unwrap(),
                ProducerChange::Bytes => {
                    writer.append(None, "private-buffer-body").unwrap();
                    writer.mark_truncated().unwrap();
                }
                ProducerChange::Terminal => writer.finish(Ok(())).unwrap(),
            }
        }
        Ok(Response {
            provider_metadata: None,
            content: "reply".into(),
            usage: Usage::default(),
            tool_calls: if round == 1 {
                vec![microcrab_protocol::ToolCall {
                    id: "buffered-round-call".into(),
                    name: "background".into(),
                    arguments: json!({}),
                    provider_metadata: None,
                }]
            } else {
                vec![]
            },
        })
    }
}

#[tokio::test]
async fn active_passive_waits_for_real_round_but_bytes_and_completion_continue() {
    use crate::runtime::{ToolRegistration, tool::AllowTools};
    for (change, expected_rounds) in [
        (ProducerChange::Metadata, 3),
        (ProducerChange::Bytes, 3),
        (ProducerChange::Terminal, 3),
    ] {
        let tool = Arc::new(BufferedTool(Mutex::new(None)));
        let model = Arc::new(DuringRound {
            tool: tool.clone(),
            change,
            seen: Mutex::new(vec![]),
        });
        let runtime = Runtime::new(
            vec![ToolRegistration::External(tool)],
            Arc::new(AllowTools::new(["background"])),
        )
        .unwrap();
        let session = Session::builder_arc(model.clone(), runtime)
            .build()
            .unwrap();
        assert_eq!(
            session.submit("first").await.unwrap().rounds,
            expected_rounds
        );

        {
            let seen = model.seen.lock().unwrap();
            assert_eq!(seen.len(), 3);
            let latest = serde_json::to_string(&seen[2].items).unwrap();
            assert!(latest.contains("notices"));
            assert!(!latest.contains("private-buffer-body"));
            if matches!(change, ProducerChange::Metadata) {
                assert!(latest.contains("end_cursor"));
            }
        }
        session.close().await.unwrap();
    }
}

#[test]
fn unclaimed_wake_promotes_after_active_turn_but_passive_and_continue_do_not() {
    let mut inbox = InboxState::default();
    inbox.active = true;
    inbox.post(notice(StateDelivery::Passive, 1)).unwrap();
    inbox.post(notice(StateDelivery::Continue, 2)).unwrap();
    inbox.post(notice(StateDelivery::Wake, 3)).unwrap();
    inbox.end_turn(false);
    let next = inbox.next_turn().unwrap();
    assert!(next.framework());
    assert!(next.input().unwrap().parts.iter().any(|part| matches!(
        part, microcrab_protocol::InputPart::Text { text } if text.contains("execution_id\":3")
    )));
    inbox.activate_notices();
    let boundary = inbox.step(true, true);
    assert!(
        boundary
            .notices
            .iter()
            .any(|entry| entry.notice.delivery == StateDelivery::Wake
                && entry.lane == Lane::NextStep)
    );
    assert!(
        boundary
            .notices
            .iter()
            .filter(|entry| entry.notice.delivery != StateDelivery::Wake)
            .all(|entry| entry.lane == Lane::Observe)
    );
}

#[test]
fn passive_coalescing_preserves_pending_admission_and_replaces_ack_identity() {
    let mut inbox = InboxState::default();
    inbox.active = true;
    inbox.post(notice(StateDelivery::Continue, 1)).unwrap();
    let old = inbox.step(true, false).notices[0].id;
    let mut passive = notice(StateDelivery::Passive, 1);
    passive.fact.status = ExecutionStatus::Cancelling;
    inbox.post(passive).unwrap();
    inbox.acknowledge(&[], &[old]);
    let pending = inbox.step(true, false);
    assert_eq!(pending.notices.len(), 1);
    assert_ne!(pending.notices[0].id, old);
    assert_eq!(pending.notices[0].notice.delivery, StateDelivery::Continue);
    assert_eq!(
        pending.notices[0].notice.fact.status,
        ExecutionStatus::Cancelling
    );
    assert_eq!(pending.notices[0].lane, Lane::NextStep);
    inbox.downgrade_framework();
    inbox.post(notice(StateDelivery::Passive, 1)).unwrap();
    assert!(inbox.step(true, false).notices.is_empty());
    assert_eq!(
        inbox.step(true, true).notices[0].notice.delivery,
        StateDelivery::Passive
    );

    let mut idle = InboxState::default();
    idle.post(notice(StateDelivery::Wake, 2)).unwrap();
    idle.post(notice(StateDelivery::Passive, 2)).unwrap();
    assert!(idle.next_turn().unwrap().framework());
    idle.activate_notices();
    assert_eq!(
        idle.step(true, false).notices[0].notice.delivery,
        StateDelivery::Wake
    );
}

#[test]
fn range_and_production_changes_continue_even_when_retained_size_is_unchanged() {
    let inbox = Arc::new(crate::admission::inbox::Inbox::default());
    let state = State::new(Limits::default());
    state.install_inbox(Arc::downgrade(&inbox));
    let sink = super::super::SessionNoticeSink {
        inbox: inbox.clone(),
        state,
    };
    let mut previous = ExecutionSnapshot::new(
        "owner".into(),
        "call".into(),
        "background".into(),
        "agent".into(),
        None,
    );
    let mut empty_output = previous.clone();
    empty_output.output = Some(
        serde_json::from_value(json!({
            "channels":[],"bytes":0,"start_cursor":0,"end_cursor":0,
            "closed":false
        }))
        .unwrap(),
    );
    sink.changed(ExecutionNotice {
        row: empty_output,
        previous: Some(previous.clone()),
        result_delivered: true,
    });
    assert_eq!(
        take_notice(&inbox).unwrap().delivery,
        StateDelivery::Passive
    );
    previous.output = Some(
        serde_json::from_value(json!({
            "channels":[],"bytes":10,"start_cursor":0,"end_cursor":10,
            "closed":false
        }))
        .unwrap(),
    );
    let mut row = previous.clone();
    let output = row.output.as_mut().unwrap();
    output.start_cursor = 10;
    output.end_cursor = 20;
    output.bytes = 20;
    sink.changed(ExecutionNotice {
        row: row.clone(),
        previous: Some(previous),
        result_delivered: true,
    });
    assert_eq!(
        take_notice(&inbox).unwrap().delivery,
        StateDelivery::Continue
    );
    let previous = row.clone();
    row.output.as_mut().unwrap().start_cursor = 15;
    sink.changed(ExecutionNotice {
        row,
        previous: Some(previous),
        result_delivered: true,
    });
    assert_eq!(
        take_notice(&inbox).unwrap().delivery,
        StateDelivery::Continue
    );
}
