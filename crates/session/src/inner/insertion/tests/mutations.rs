//! CAS mutations against the real inbox owner and asynchronous admission claims.
use super::*;
use crate::{
    Error, ImageSource, InputPart, PendingInputError as Rejection, PendingInputMode as Mode,
    SessionInput,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

#[test]
fn pending_cas_budget_order_normalization_and_incarnation_are_atomic() {
    let port = crate::admission::inbox::Inbox::default();
    let token = port.state.lock().unwrap().snapshot().inbox_id;
    assert_ne!(
        token,
        crate::admission::inbox::Inbox::default()
            .state
            .lock()
            .unwrap()
            .snapshot()
            .inbox_id
    );
    let mut inbox = port.state.lock().unwrap();
    inbox.paused = true;
    inbox.enqueue("first".into(), false).unwrap();
    inbox.enqueue("second".into(), false).unwrap();
    let first = inbox.snapshot().items[0].clone();
    let second = inbox.snapshot().items[1].id;
    let changed = inbox
        .update(
            &token,
            first.id,
            0,
            Some("edited".into()),
            Some(Mode::Steer),
        )
        .unwrap();
    assert_eq!(changed.id, first.id);
    assert_eq!(changed.revision, 1);
    assert_eq!(changed.mode, Mode::Queue);
    assert!(inbox.paused);
    assert_eq!(inbox.snapshot().items[1].id, second);
    assert!(matches!(
        inbox.remove(&token, first.id, 0),
        Err(Error::PendingInput(Rejection::RevisionConflict))
    ));
    let before = serde_json::to_value(inbox.snapshot()).unwrap();
    assert!(
        inbox
            .update(
                &token,
                first.id,
                1,
                Some(SessionInput::text("x".repeat(1024 * 1024))),
                None
            )
            .is_err()
    );
    let mut invalid = SessionInput::text("invalid variables");
    invalid
        .variables
        .set
        .insert("session".into(), serde_json::json!({}));
    assert!(
        inbox
            .update(&token, first.id, 1, Some(invalid), None)
            .is_err()
    );
    assert_eq!(serde_json::to_value(inbox.snapshot()).unwrap(), before);
    assert!(matches!(
        inbox.remove("stale-incarnation", first.id, 1),
        Err(Error::PendingInput(Rejection::InboxChanged))
    ));
    let noop = inbox
        .update(&token, first.id, 1, None, Some(Mode::Queue))
        .unwrap();
    assert_eq!(noop.revision, 2);
    inbox.active = true;
    let steer = inbox
        .update(&token, first.id, 2, None, Some(Mode::Steer))
        .unwrap();
    assert_eq!(steer.mode, Mode::Steer);
    inbox.end_turn(false);
    assert_eq!(inbox.snapshot().items[0].revision, steer.revision + 1);
    assert!(matches!(
        inbox.remove(&token, first.id, steer.revision),
        Err(Error::PendingInput(Rejection::RevisionConflict))
    ));
    let bytes = inbox.snapshot().used_bytes;
    let removed = inbox.remove(&token, first.id, steer.revision + 1).unwrap();
    assert_eq!(removed.input, SessionInput::text("edited"));
    assert!(inbox.snapshot().used_bytes < bytes);
    assert_eq!(inbox.snapshot().items[0].id, second);
}

struct WaitingImage {
    entered: Semaphore,
    release: Semaphore,
}
impl microcrab_host_api::ProcessApi for WaitingImage {}
impl microcrab_host_api::FsApi for WaitingImage {}
impl microcrab_host_api::NetApi for WaitingImage {}
impl microcrab_host_api::ShellApi for WaitingImage {}
impl microcrab_host_api::HostAPI for WaitingImage {}
#[async_trait::async_trait]
impl microcrab_host_api::Host for WaitingImage {
    async fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
        token: CancellationToken,
    ) -> Result<Vec<u8>, microcrab_host_api::ToolError> {
        self.entered.add_permits(1);
        tokio::select! {
            _ = token.cancelled() => Err(microcrab_host_api::ToolError::Cancelled),
            permit = self.release.acquire() => {
                permit.unwrap().forget();
                Err(microcrab_host_api::ToolError::InvalidRequest("synthetic unavailable image".into()))
            }
        }
    }
}
struct ControlledModel {
    calls: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
}
#[async_trait::async_trait]
impl Client for ControlledModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.add_permits(1);
        tokio::select! {
            _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
            permit = self.release.acquire() => permit.unwrap().forget(),
        }
        Ok(Response {
            provider_metadata: None,
            content: "settled".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
async fn permit(semaphore: &Semaphore) {
    tokio::time::timeout(std::time::Duration::from_secs(5), semaphore.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
#[tokio::test]
async fn next_turn_and_step_claim_before_host_io_reject_stale_edits_and_release_on_failure() {
    for steer in [false, true] {
        let host = Arc::new(WaitingImage {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        let client = Arc::new(ControlledModel {
            calls: AtomicUsize::new(0),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        let session = Session::builder_arc(
            client.clone(),
            Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
        )
        .host(host.clone())
        .build()
        .unwrap();
        let mut stream = if steer {
            Some(session.stream("foreground").await.unwrap())
        } else {
            None
        };
        if steer {
            permit(&client.entered).await;
        }
        let image = SessionInput {
            parts: vec![InputPart::Image {
                source: ImageSource::Path {
                    path: "image.png".into(),
                },
            }],
            variables: Default::default(),
        };
        if steer {
            session.steer(image).unwrap();
            client.release.add_permits(1);
        } else {
            session.queue(image).unwrap();
        }
        permit(&host.entered).await;
        let claimed = session.inbox_snapshot();
        let item = &claimed.items[0];
        assert!(item.claimed);
        assert!(matches!(
            session.update_pending_input(
                &claimed.inbox_id,
                item.id,
                item.revision,
                Some("must not execute".into()),
                None
            ),
            Err(Error::PendingInput(Rejection::Claimed))
        ));
        assert!(matches!(
            session.remove_pending_input(&claimed.inbox_id, item.id, item.revision),
            Err(Error::PendingInput(Rejection::Claimed))
        ));
        if steer {
            session.interrupt();
        } else {
            host.release.add_permits(1);
        }
        if let Some(stream) = stream.as_mut() {
            while let Some(event) = stream.recv().await {
                if event.is_err() || matches!(event, Ok(crate::SessionEvent::Finished { .. })) {
                    break;
                }
            }
        }
        let mut activity = session.subscribe_activity();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let snapshot = session.inbox_snapshot();
                if !snapshot.active && !snapshot.items[0].claimed { break; }
                tokio::select! { changed = activity.changed() => changed.unwrap(), _ = tokio::task::yield_now() => {} }
            }
        }).await.unwrap();
        let pending = session.inbox_snapshot();
        assert!(pending.paused);
        let entry = &pending.items[0];
        let edited = session
            .update_pending_input(
                &pending.inbox_id,
                entry.id,
                entry.revision,
                Some("repaired".into()),
                Some(Mode::Steer),
            )
            .unwrap();
        assert_eq!(edited.mode, Mode::Queue);
        assert!(session.inbox_snapshot().paused);
        let removed = session
            .remove_pending_input(&pending.inbox_id, edited.id, edited.revision)
            .unwrap();
        assert_eq!(removed.input, SessionInput::text("repaired"));
        assert!(session.inbox_snapshot().items.is_empty());
        assert_eq!(client.calls.load(Ordering::SeqCst), usize::from(steer));
        session.close().await.unwrap();
    }
}

#[test]
fn shared_inbox_admission_rejects_before_identity_or_retention_mutation() {
    let mut inbox = crate::admission::inbox::InboxState::default();
    let before = serde_json::to_value(inbox.snapshot()).unwrap();
    let mut invalid = SessionInput::text("invalid custom write");
    invalid
        .variables
        .set
        .insert("session".into(), serde_json::json!(1));
    assert!(inbox.enqueue(invalid, false).is_err());
    let error = inbox
        .enqueue(SessionInput::text("x".repeat(1024 * 1024)), false)
        .unwrap_err();
    let Error::Limit(limit) = error else {
        panic!("expected inbox retention limit")
    };
    assert_eq!(limit.kind, crate::LimitKind::InboxBytes);
    assert!(!limit.is_pre_effect());
    assert_eq!(serde_json::to_value(inbox.snapshot()).unwrap(), before);
    inbox.enqueue("first accepted".into(), false).unwrap();
    assert_eq!(inbox.snapshot().items[0].id, 1);
    assert_eq!(inbox.snapshot().items[0].revision, 0);
}

#[test]
fn exact_pending_claim_never_selects_framework_and_keeps_pause_until_execution() {
    let port = crate::admission::inbox::Inbox::default();
    let mut inbox = port.state.lock().unwrap();
    inbox.post(notice(StateDelivery::Wake, 1)).unwrap();
    inbox.paused = true;
    inbox.enqueue("user target".into(), false).unwrap();
    let before = inbox.snapshot();
    let first = &before.items[0];
    let entry = inbox
        .claim_pending_resume(&before.inbox_id, first.id, first.revision)
        .unwrap();
    assert_eq!(entry.id, first.id);
    assert_eq!(entry.input, SessionInput::text("user target"));
    assert!(inbox.snapshot().items[0].claimed);
    assert!(inbox.paused);
    assert!(matches!(
        inbox.claim_pending_resume(&before.inbox_id, first.id, first.revision),
        Err(Error::PendingInput(Rejection::Claimed))
    ));
    inbox.release_claims(&[first.id]);
    inbox.active = true;
    assert!(matches!(
        inbox.claim_pending_resume(&before.inbox_id, first.id, first.revision),
        Err(Error::Busy)
    ));
    assert!(!inbox.snapshot().items[0].claimed);
}

#[tokio::test]
async fn explicit_pending_resume_host_cancellation_retains_exact_input_and_repauses() {
    let host = Arc::new(WaitingImage {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let client = Arc::new(ControlledModel {
        calls: AtomicUsize::new(0),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let session = Session::builder_arc(
        client.clone(),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .host(host.clone())
    .build()
    .unwrap();
    session.interrupt();
    let input = SessionInput {
        parts: vec![InputPart::Image {
            source: ImageSource::Path {
                path: "image.png".into(),
            },
        }],
        variables: Default::default(),
    };
    session.queue(input.clone()).unwrap();
    let before = session.inbox_snapshot();
    let first = &before.items[0];
    let cancellation = CancellationToken::new();
    let (mut stream, settlement) = session
        .resume_pending_input_with_settlement(
            &before.inbox_id,
            first.id,
            first.revision,
            cancellation.clone(),
        )
        .await
        .unwrap();
    permit(&host.entered).await;
    assert!(session.inbox_snapshot().items[0].claimed);
    assert!(matches!(
        session.remove_pending_input(&before.inbox_id, first.id, first.revision),
        Err(Error::PendingInput(Rejection::Claimed))
    ));
    assert!(matches!(
        session.update_pending_input(
            &before.inbox_id,
            first.id,
            first.revision,
            Some("stale copy".into()),
            None
        ),
        Err(Error::PendingInput(Rejection::Claimed))
    ));
    assert!(matches!(
        session
            .resume_pending_input_with_settlement(
                &before.inbox_id,
                first.id,
                first.revision,
                CancellationToken::new(),
            )
            .await,
        Err(Error::Busy)
    ));
    cancellation.cancel();
    let terminal: Result<(), Error> =
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(event) = stream.recv().await {
                event?;
            }
            panic!("resume must publish cancellation");
        })
        .await
        .unwrap();
    assert!(matches!(terminal, Err(Error::Cancelled)));
    drop(settlement);
    let pending = session.inbox_snapshot();
    assert!(!pending.active);
    assert!(pending.paused);
    assert_eq!(pending.items.len(), 1);
    assert_eq!(pending.items[0].id, first.id);
    assert_eq!(pending.items[0].revision, first.revision);
    assert_eq!(pending.items[0].input, input);
    assert!(!pending.items[0].claimed);
    assert_eq!(client.calls.load(Ordering::SeqCst), 0);
    session.close().await.unwrap();
}
