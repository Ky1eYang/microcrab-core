//! Turn positions are local to the selected epoch, including pending and restored history.
use crate::hooks;
use microcrab_protocol::Moment;
use microcrab_protocol::Response;
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::{Runtime, tool::DenyAll};
use microcrab_session::{Auditor, Client, Session};
use microcrab_transport::StateFrame;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Capture(Mutex<Vec<Content>>);
#[async_trait::async_trait]
impl Client for Capture {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.lock().unwrap().push(content.clone());
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
    }
}
fn runtime() -> Runtime {
    Runtime::new(vec![], Arc::new(DenyAll)).unwrap()
}
fn last_turn(state: &microcrab_session::SessionState) -> u64 {
    serde_json::to_value(state).unwrap()["last_turn_id"]
        .as_u64()
        .unwrap()
}

#[tokio::test]
async fn pending_epoch_resets_before_lifecycle_and_provider_observe_turn_one() {
    let client = Arc::new(Capture::default());
    let hooks = Arc::new(hooks::Recording::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .auditor(hooks.clone())
        .build()
        .unwrap();
    session.submit("epoch zero first").await.unwrap();
    session.submit("epoch zero second").await.unwrap();
    assert_eq!(last_turn(&session.state()), 2);
    session.compact("summary", json!({})).await.unwrap();
    let pending = session.checkpoint().await.unwrap();
    assert_eq!(pending.state.epoch_id, 0);
    assert_eq!(last_turn(&pending.state), 2);
    assert!(pending.context.pending_epoch);
    assert!(session.submit(" ").await.is_err());
    let rejected = session.checkpoint().await.unwrap();
    assert_eq!(
        serde_json::to_value(rejected).unwrap(),
        serde_json::to_value(&pending).unwrap()
    );

    hooks.clear();
    session.submit("epoch one first").await.unwrap();
    {
        let seen = hooks.seen.lock().unwrap();
        assert_eq!(
            seen.iter().map(|item| item.moment).collect::<Vec<_>>(),
            vec![Moment::InputAdmitted, Moment::RoundCompleted]
        );
        // InputAdmitted precedes epoch materialization; the completed model round
        // observes the new epoch and turn1, and so does its builtin variable view.
        let completed = seen
            .iter()
            .find(|item| item.moment == Moment::RoundCompleted)
            .unwrap();
        assert_eq!(
            (completed.epoch, completed.turn, completed.round),
            (1, Some(1), Some(1))
        );
        let builtin = completed.session.as_ref().unwrap();
        assert_eq!(builtin["epoch"], 1);
        assert_eq!(builtin["turn"], 1);
        assert_eq!(builtin["round"], 1);
    }
    let epoch_one_first = session.checkpoint().await.unwrap();
    assert_eq!(last_turn(&epoch_one_first.state), 1);
    session.submit("epoch one second").await.unwrap();
    assert_eq!(last_turn(&session.state()), 2);

    // Recovering selected epoch-one history resumes its own turn position.
    session.recover(epoch_one_first).await.unwrap();
    session.submit("epoch one branch second").await.unwrap();
    assert_eq!(
        (session.state().epoch_id, last_turn(&session.state())),
        (1, 2)
    );
    {
        let requests = client.0.lock().unwrap();
        let positions: Vec<_> = requests
            .iter()
            .map(|content| (content.epoch, content.turn_id))
            .collect();
        assert_eq!(
            positions,
            vec![
                (0, Some(1)),
                (0, Some(2)),
                (1, Some(1)),
                (1, Some(2)),
                (1, Some(2))
            ]
        );
        let session_patch = requests[2]
            .items
            .iter()
            .filter_map(|item| match item {
                ContentItem::State(StateFrame::StateSnapshot { state }) => state.get("session"),
                ContentItem::State(StateFrame::StatePatch { set, .. }) => set.get("session"),
                _ => None,
            })
            .next_back()
            .unwrap();
        assert_eq!(session_patch["epoch"], 1);
        assert_eq!(session_patch["turn"], 1);
        assert_eq!(session_patch["round"], 1);
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn pending_restore_and_rollback_retain_selected_epoch_turn_positions() {
    let client = Arc::new(Capture::default());
    let original = Session::builder_arc(client.clone(), runtime())
        .build()
        .unwrap();
    original.submit("first").await.unwrap();
    let epoch_zero_first = original.checkpoint().await.unwrap();
    original.submit("second").await.unwrap();
    original.compact("summary", json!({})).await.unwrap();
    let pending = original.checkpoint().await.unwrap();
    original.close().await.unwrap();
    let restored = Session::restore(
        Session::builder_arc(client.clone(), runtime()),
        pending.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        (restored.state().epoch_id, last_turn(&restored.state())),
        (0, 2)
    );
    assert!(restored.checkpoint().await.unwrap().context.pending_epoch);
    restored.submit("new epoch").await.unwrap();
    assert_eq!(
        (restored.state().epoch_id, last_turn(&restored.state())),
        (1, 1)
    );
    restored.recover(epoch_zero_first).await.unwrap();
    assert_eq!(
        (restored.state().epoch_id, last_turn(&restored.state())),
        (0, 1)
    );
    restored.submit("branch second").await.unwrap();
    assert_eq!(
        (restored.state().epoch_id, last_turn(&restored.state())),
        (0, 2)
    );
    restored.recover(pending).await.unwrap();
    assert_eq!(
        (restored.state().epoch_id, last_turn(&restored.state())),
        (0, 2)
    );
    restored.submit("new epoch after recover").await.unwrap();
    assert_eq!(
        (restored.state().epoch_id, last_turn(&restored.state())),
        (1, 1)
    );
    let positions: Vec<_> = client
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|content| (content.epoch, content.turn_id))
        .collect();
    assert_eq!(
        positions,
        vec![
            (0, Some(1)),
            (0, Some(2)),
            (1, Some(1)),
            (0, Some(2)),
            (1, Some(1))
        ]
    );
    restored.close().await.unwrap();
}

#[tokio::test]
async fn pending_epoch_ignores_old_epoch_turn_counter_overflow() {
    let original = Session::builder(Capture::default(), runtime())
        .build()
        .unwrap();
    original.start().await.unwrap();
    original.compact("summary", json!({})).await.unwrap();
    let mut fixture = serde_json::to_value(original.checkpoint().await.unwrap()).unwrap();
    fixture["state"]["last_turn_id"] = json!(u64::MAX);
    original.close().await.unwrap();
    let client = Arc::new(Capture::default());
    let restored = Session::restore(
        Session::builder_arc(client.clone(), runtime()),
        serde_json::from_value(fixture).unwrap(),
    )
    .await
    .unwrap();
    assert!(restored.submit(" ").await.is_err());
    assert_eq!(last_turn(&restored.state()), u64::MAX);
    assert!(restored.checkpoint().await.unwrap().context.pending_epoch);
    restored.submit("valid new epoch").await.unwrap();
    assert_eq!(
        (restored.state().epoch_id, last_turn(&restored.state())),
        (1, 1)
    );
    let request = client.0.lock().unwrap()[0].clone();
    assert_eq!(
        (request.epoch, request.turn_id, request.round),
        (1, Some(1), Some(1))
    );
    restored.close().await.unwrap();
}
