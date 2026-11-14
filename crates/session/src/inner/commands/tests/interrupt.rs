use super::*;

fn public_session(model: Arc<PendingModel>) -> crate::Session {
    crate::Session::builder_arc(model, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
        .build()
        .unwrap()
}
async fn cancelled_stream(stream: &mut crate::SessionStream) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match stream.recv().await {
                Some(Err(Error::Cancelled)) => break,
                Some(Ok(_)) => {}
                other => panic!("expected cancelled terminal event, got {other:?}"),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stream_interrupt_first_cause_is_operation_scoped_and_parent_token_survives() {
    let model = Arc::new(PendingModel::default());
    let session = public_session(model.clone());
    let parent = CancellationToken::new();
    let mut first = session
        .stream_with_cancellation("first", parent.clone())
        .await
        .unwrap();
    model.entered.notified().await;
    let cause = crate::InterruptReason::HostDetached {
        code: crate::HostDetachCode::Replaced,
    };
    session.interrupt_with_reason(cause);
    session.interrupt();
    cancelled_stream(&mut first).await;
    assert_eq!(first.interrupt_reason(), Some(cause));
    assert!(!parent.is_cancelled());
    drop(first);

    let mut second = session.stream("second").await.unwrap();
    model.entered.notified().await;
    assert_eq!(second.interrupt_reason(), None);
    session.interrupt();
    cancelled_stream(&mut second).await;
    assert_eq!(
        second.interrupt_reason(),
        Some(crate::InterruptReason::UserRequested)
    );
    drop(second);
    session.close().await.unwrap();
}

#[tokio::test]
async fn caller_cancelled_stream_does_not_invent_a_user_interrupt_cause() {
    let model = Arc::new(PendingModel::default());
    let session = public_session(model.clone());
    let parent = CancellationToken::new();
    let mut stream = session
        .stream_with_cancellation("caller", parent.clone())
        .await
        .unwrap();
    model.entered.notified().await;
    parent.cancel();
    session.interrupt_with_reason(crate::InterruptReason::SessionClosed);
    cancelled_stream(&mut stream).await;
    assert_eq!(stream.interrupt_reason(), None);
    drop(stream);
    session.close().await.unwrap();
}

#[tokio::test]
async fn autonomous_failure_retains_its_interrupt_cause_after_a_later_operation() {
    let model = Arc::new(PendingModel::default());
    let session = public_session(model.clone());
    let mut activity = session.subscribe_activity();
    session.queue_and_activate("queued").unwrap();
    model.entered.notified().await;
    session.interrupt_with_reason(crate::InterruptReason::ClientToolDisconnected);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let settled = {
                let snapshot = activity.borrow_and_update();
                !snapshot.active && snapshot.latest_inbox_failure.is_some()
            };
            if settled {
                break;
            }
            activity.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let check = || {
        assert!(matches!(
            activity
                .borrow()
                .latest_inbox_failure
                .as_ref()
                .unwrap()
                .kind,
            crate::InboxFailureKind::Cancelled {
                reason: Some(crate::InterruptReason::ClientToolDisconnected)
            }
        ));
    };
    check();
    let mut next = session.stream("next").await.unwrap();
    model.entered.notified().await;
    assert_eq!(next.interrupt_reason(), None);
    session.interrupt();
    cancelled_stream(&mut next).await;
    check();
    drop(next);
    session.close().await.unwrap();
}

#[tokio::test]
async fn compact_waits_for_the_embedding_settlement_ack_before_replacing_context() {
    let model = Arc::new(PendingModel::default());
    let session = public_session(model.clone());
    let (mut stream, settlement) = session
        .stream_with_settlement("old input", CancellationToken::new())
        .await
        .unwrap();
    model.entered.notified().await;
    let owner = session.clone();
    let mut replacement = tokio::spawn(async move {
        owner
            .compact("settled summary", serde_json::json!({}))
            .await
    });
    cancelled_stream(&mut stream).await;
    assert_eq!(
        stream.interrupt_reason(),
        Some(crate::InterruptReason::StateReplaced)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut replacement)
            .await
            .is_err(),
        "replacement must wait for the retained receipt guard"
    );
    drop(settlement);
    tokio::time::timeout(Duration::from_secs(3), replacement)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(session.context_snapshot().unwrap().pending_epoch);
    assert!(!session.has_continuation());
    session.close().await.unwrap();
}

#[tokio::test]
async fn recovery_interrupts_an_active_provider_and_keeps_the_session_reusable() {
    let model = Arc::new(PendingModel::default());
    let session = public_session(model.clone());
    session.start().await.unwrap();
    let previous = session.checkpoint().await.unwrap();
    let frozen = serde_json::to_value(&previous).unwrap();
    let mut stream = session.stream("discard this suffix").await.unwrap();
    model.entered.notified().await;
    let owner = session.clone();
    let recovered = tokio::spawn(async move { owner.recover(previous).await });
    cancelled_stream(&mut stream).await;
    tokio::time::timeout(Duration::from_secs(3), recovered)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(session.checkpoint().await.unwrap()).unwrap(),
        frozen
    );
    let mut next = session.stream("new suffix").await.unwrap();
    model.entered.notified().await;
    session.interrupt();
    cancelled_stream(&mut next).await;
    session.close().await.unwrap();
}

#[tokio::test]
async fn replacement_closes_the_reserved_before_worker_registration_gap() {
    let model = Arc::new(PendingModel::default());
    let session = public_session(model.clone());
    session.start().await.unwrap();
    let mut stream = session.stream("not yet dispatched").await.unwrap();
    // On this current-thread executor the newly spawned worker has not polled yet.
    session
        .compact("replacement", serde_json::json!({}))
        .await
        .unwrap();
    cancelled_stream(&mut stream).await;
    assert_eq!(
        stream.interrupt_reason(),
        Some(crate::InterruptReason::StateReplaced)
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    session.close().await.unwrap();
}
#[tokio::test]
async fn crash_after_route_admission_keeps_input_once_without_fabricating_an_unprepared_retry() {
    struct StopAfterAdmission(Arc<PendingModel>);
    #[async_trait::async_trait]
    impl crate::TurnRouting for StopAfterAdmission {
        async fn prepare(&self) -> Result<Option<crate::ConfigurationEffect>, Error> {
            Ok(None)
        }
        fn apply(&self, _: u64) -> Result<(), Error> {
            Ok(())
        }
        fn client(&self, _: u64) -> Arc<dyn crate::Client> {
            self.0.clone()
        }
        async fn before_checkpoint(
            &self,
            _: &crate::runtime::journal::Recover,
        ) -> Result<(), Error> {
            Ok(())
        }
        async fn checkpoint_committed(
            &self,
            recovery: &crate::runtime::journal::Recover,
        ) -> Result<(), Error> {
            if recovery
                .context
                .items
                .iter()
                .any(|item| matches!(item, microcrab_transport::ContentItem::User { .. }))
            {
                Err(Error::lifecycle(
                    "injected stop after admission Journal acknowledgement",
                ))
            } else {
                Ok(())
            }
        }
        async fn restore(&self, _: &crate::runtime::journal::Recover) -> Result<(), Error> {
            Ok(())
        }
    }
    let model = Arc::new(PendingModel::default());
    let journal = crate::runtime::journal::MemoryJournal::new();
    let first = crate::Session::builder_arc(
        model.clone(),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .journal(journal.clone())
    .turn_routing(Arc::new(StopAfterAdmission(model.clone())))
    .build()
    .unwrap();
    assert!(first.submit("durable first input").await.is_err());
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        0,
        "admission checkpoint precedes provider dispatch"
    );
    let saved = journal.latest().unwrap();
    assert!(saved.continuation.is_none(), "no request has been prepared");
    assert_eq!(saved.state.last_turn_id, 1);
    assert!(saved.state.turn_id.is_none() && saved.state.round_id.is_none());
    let input_count = |items: &[microcrab_transport::ContentItem]| {
        items.iter().filter(|item| matches!(item,
        microcrab_transport::ContentItem::User { parts, .. } if parts.iter().any(|part| matches!(part, microcrab_protocol::InputPart::Text { text } if text == "durable first input"))
    )).count()
    };
    assert_eq!(input_count(&saved.context.items), 1);
    let _ = first.close().await;
    let restored = crate::Session::restore(
        crate::Session::builder_arc(
            model.clone(),
            Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
        ),
        saved,
    )
    .await
    .unwrap();
    assert!(!restored.has_continuation());
    assert!(!restored.retry_ready());
    let mut next = restored.stream("same-model fresh Turn").await.unwrap();
    model.entered.notified().await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    restored.interrupt();
    cancelled_stream(&mut next).await;
    assert_eq!(input_count(&restored.context_snapshot().unwrap().items), 1);
    assert_eq!(restored.state().last_turn_id, 2);
    restored.close().await.unwrap();
}
