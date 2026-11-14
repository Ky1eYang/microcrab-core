//! Pending epochs belong to selected Context history and materialize at model admission.
use microcrab_protocol::Moment;
use microcrab_protocol::{ImageSource, InputPart, Response};
use microcrab_session::context::{Content, ContentItem, Context};
use microcrab_session::runtime::{Runtime, tool::DenyAll};
use microcrab_session::{Auditor, Client, Error, Limits, Session, SessionInput};
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

#[tokio::test]
async fn compact_checkpoint_reopen_and_recover_preserve_pending_until_one_valid_request() {
    let client = Arc::new(Capture::default());
    let original = Session::builder_arc(client.clone(), runtime())
        // The Context budget is the only content bound left, so an oversized
        // submission must be rejected against it rather than a per-input cap.
        .limits(Limits {
            max_context_bytes: 1024 * 1024,
            ..Limits::default()
        })
        .build()
        .unwrap();
    original.start().await.unwrap();
    let (sender, mut audit) = tokio::sync::mpsc::unbounded_channel();
    original
        .compact("first summary", json!({"phase":1}))
        .await
        .unwrap();
    original
        .compact("second summary", json!({"phase":2}))
        .await
        .unwrap();
    let pending = original.checkpoint().await.unwrap();
    assert_eq!(pending.state.epoch_id, 0);
    assert_eq!(pending.context.epoch, 0);
    assert!(pending.context.pending_epoch);
    assert!(client.0.lock().unwrap().is_empty());
    original.close().await.unwrap();

    let reopened = Session::restore(
        Session::builder_arc(client.clone(), runtime()).audit_sender(sender),
        pending.clone(),
    )
    .await
    .unwrap();
    reopened.start().await.unwrap();
    let restored = audit.try_recv().unwrap();
    assert_eq!(restored.event, Moment::SessionRestored);
    assert_eq!(restored.epoch, 0);
    assert!(audit.try_recv().is_err());
    reopened.recover(pending).await.unwrap();
    assert_eq!(audit.try_recv().unwrap().event, Moment::SessionRestored);
    assert!(audit.try_recv().is_err());

    for input in [
        SessionInput::text(" "),
        SessionInput::text("bad\0input"),
        SessionInput::text("x".repeat(2_000_000)),
    ] {
        assert!(reopened.submit(input).await.is_err());
        let saved = reopened.checkpoint().await.unwrap();
        assert_eq!(saved.state.epoch_id, 0);
        assert_eq!(saved.context.epoch, 0);
        assert!(saved.context.pending_epoch);
    }
    let mut invalid = SessionInput::text("reserved variable");
    invalid.variables.set.insert("session".into(), json!({}));
    assert!(reopened.submit(invalid).await.is_err());
    let mut oversized_variables = SessionInput::text("oversized variables");
    oversized_variables
        .variables
        .set
        .insert("large".into(), json!("x".repeat(3_000_000)));
    assert!(reopened.submit(oversized_variables).await.is_err());
    assert!(client.0.lock().unwrap().is_empty());
    assert!(audit.try_recv().is_err());
    reopened.submit("valid next request").await.unwrap();
    assert_eq!(reopened.state().epoch_id, 1);
    let saved = reopened.checkpoint().await.unwrap();
    assert!(!saved.context.pending_epoch);
    assert_eq!(saved.context.epoch, 1);
    reopened.submit("same baseline").await.unwrap();
    assert_eq!(reopened.state().epoch_id, 1);
    let events = std::iter::from_fn(|| audit.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .filter(|record| record.event == Moment::InputAdmitted)
            .count(),
        2
    );
    let completed = events
        .iter()
        .filter(|record| record.event == Moment::RoundCompleted)
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), 2);
    assert!(completed.iter().all(|record| record.epoch == 1));
    let requests = client.0.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|content| content.epoch == 1));
    let mut model_epoch = None;
    for item in &requests[0].items {
        if let ContentItem::State(frame) = item {
            match frame {
                StateFrame::StateSnapshot { state } => {
                    model_epoch = state
                        .get("session")
                        .and_then(|session| session.get("epoch"))
                        .cloned()
                }
                StateFrame::StatePatch { set, .. } => {
                    if let Some(session) = set.get("session") {
                        model_epoch = session.get("epoch").cloned();
                    }
                }
            }
        }
    }
    assert_eq!(model_epoch, Some(json!(1)));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn full_context_and_variable_budget_rejections_leave_pending_admission_reusable() {
    let client = Arc::new(Capture::default());
    let limits = microcrab_session::Limits {
        max_context_bytes: 4096,
        ..Default::default()
    };
    let session = Session::builder_arc(client.clone(), runtime())
        .limits(limits)
        .build()
        .unwrap();
    session.start().await.unwrap();
    session.compact("summary", json!({})).await.unwrap();
    let before = serde_json::to_value(session.checkpoint().await.unwrap()).unwrap();
    assert!(matches!(
        session.submit("x".repeat(10_000)).await,
        Err(Error::Limit(_))
    ));
    let mut input = SessionInput::text("large custom variable");
    input
        .variables
        .set
        .insert("large".into(), json!("x".repeat(10_000)));
    assert!(matches!(session.submit(input).await, Err(Error::Limit(_))));
    assert_eq!(
        serde_json::to_value(session.checkpoint().await.unwrap()).unwrap(),
        before
    );
    assert!(client.0.lock().unwrap().is_empty());
    session.submit("valid").await.unwrap();
    assert_eq!(session.state().epoch_id, 1);
    session.close().await.unwrap();
}

#[test]
fn identical_compaction_baseline_does_not_create_a_pending_epoch() {
    let mut context = Context::new("system".into(), vec![], 16_384).unwrap();
    context
        .compact("summary".into(), &json!({"phase":1}))
        .unwrap();
    let mut admitted_baseline = context.snapshot().unwrap();
    admitted_baseline.pending_epoch = false;
    let mut context = Context::from_snapshot(admitted_baseline).unwrap();
    context
        .compact("summary".into(), &json!({"phase":1}))
        .unwrap();
    assert!(!context.pending_epoch());
    assert_eq!(context.epoch(), 0);
    context
        .compact("summary".into(), &json!({"phase":2}))
        .unwrap();
    assert!(context.pending_epoch());
    let encoded = serde_json::to_value(context.snapshot().unwrap()).unwrap();
    let mut fixture = encoded.clone();
    fixture.as_object_mut().unwrap().remove("pending_epoch");
    let restored: microcrab_session::context::ContextSnapshot =
        serde_json::from_value(fixture).unwrap();
    assert!(!restored.pending_epoch);
    assert!(
        Context::from_snapshot(serde_json::from_value(encoded).unwrap())
            .unwrap()
            .pending_epoch()
    );
}

#[tokio::test]
async fn pending_epoch_overflow_rejects_before_host_input_reads_or_journal_publication() {
    let directory = tempfile::tempdir().unwrap();
    let host = Arc::new(
        microcrab_host_local::LocalHost::new(directory.path(), Default::default()).unwrap(),
    );
    let original = Session::builder(Capture::default(), runtime())
        .host(host.clone())
        .build()
        .unwrap();
    original.start().await.unwrap();
    original.compact("summary", json!({})).await.unwrap();
    let mut recover = original.checkpoint().await.unwrap();
    recover.state.epoch_id = u64::MAX;
    recover.context.epoch = u64::MAX;
    original.close().await.unwrap();
    let journal = Arc::new(microcrab_session::runtime::journal::MemoryJournal::default());
    let session = Session::restore(
        Session::builder(Capture::default(), runtime())
            .host(host)
            .journal_arc(journal.clone()),
        recover,
    )
    .await
    .unwrap();
    assert_eq!(session.state().epoch_id, u64::MAX);
    let mut input = SessionInput::text("read missing image");
    input.parts.push(InputPart::Image {
        source: ImageSource::Path {
            path: directory.path().join("missing.png"),
        },
    });
    assert!(
        matches!(session.submit(input).await, Err(Error::Limit(message)) if message.contains("epoch"))
    );
    assert!(journal.latest().is_none());
    assert!(session.checkpoint().await.unwrap().context.pending_epoch);
    assert_eq!(session.state().epoch_id, u64::MAX);
    session.close().await.unwrap();
}
