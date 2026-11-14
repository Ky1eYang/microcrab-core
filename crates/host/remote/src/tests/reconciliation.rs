//! Unknown original effects fail terminally; transport remains available for inspection.
use super::*;
use microcrab_host_protocol::{ErrorCode, WireError};

async fn respond(
    host: &RemoteHost,
    generation: u64,
    receiver: &mut mpsc::Receiver<HostFrame>,
    value: serde_json::Value,
) -> HostRequest {
    let HostFrame::Request {
        request_id,
        operation_id,
        method,
        ..
    } = next_request(receiver).await
    else {
        panic!("expected request")
    };
    host.receive(
        generation,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(value),
        },
    )
    .unwrap();
    method
}

#[tokio::test]
async fn missing_original_is_failed_without_replay_and_next_write_is_admitted() {
    missing_original(scope()).await;
}
#[tokio::test]
async fn conversation_unknown_mutation_reconnect_never_replays_original_effect() {
    missing_original(BindingScope::conversation(
        "s".into(),
        "b".into(),
        "conversation".into(),
    ))
    .await;
}

async fn missing_original(expected_scope: BindingScope) {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready_with_scope(&host, 1, expected_scope.clone()).await;
    let write = {
        let host = host.clone();
        tokio::spawn(async move {
            Host::write(
                host.as_ref(),
                WriteRequest {
                    path: "original".into(),
                    content: "private request body".into(),
                    precondition: WritePrecondition::Missing,
                },
                CancellationToken::new(),
            )
            .await
        })
    };
    let HostFrame::Request {
        operation_id: original,
        method,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected original write")
    };
    let digest = payload_digest(&method).unwrap();
    host.detach(1);
    assert!(write.await.unwrap().is_err());
    let (sender, mut receiver) = mpsc::channel(32);
    host.attach(sender, 2).unwrap();
    let reconcile = {
        let host = host.clone();
        tokio::spawn(async move { host.reconcile().await })
    };
    let HostFrame::Status {
        request_id,
        operation_id,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("original must only be queried")
    };
    assert_eq!(operation_id, original);
    host.receive(
        2,
        HostFrame::Receipt {
            request_id,
            receipt: None,
        },
    )
    .unwrap();
    let terminal = OperationReceipt {
        operation_id: original.clone(),
        payload_digest: digest.clone(),
        scope: expected_scope.clone(),
        state: ReceiptState::FailedUnknown {
            reason: WireError::new(
                ErrorCode::OutcomeUnknown,
                "receipt_missing: original durable receipt is absent",
            ),
            effects_may_have_occurred: true,
        },
    };
    let method = respond(&host, 2, &mut receiver, json!(terminal)).await;
    assert!(
        matches!(method,HostRequest::SettleUnknown {operation_id,payload_digest,scope: actual} if operation_id==original && payload_digest==digest && actual==expected_scope)
    );
    assert!(matches!(
        respond(&host, 2, &mut receiver, json!([terminal])).await,
        HostRequest::Recovery
    ));
    reconcile.await.unwrap().unwrap();
    assert!(host.is_ready());
    assert!(host.unresolved_operations().is_empty());
    let records = host.operations(100);
    assert_eq!(records[0]["state"], "failed_unknown");
    assert_eq!(records[0]["effects_may_have_occurred"], true);
    assert!(
        records[0]["reason"]["message"]
            .as_str()
            .unwrap()
            .contains("receipt_missing")
    );
    assert!(!json!(records).to_string().contains("private request body"));
    let next = {
        let host = host.clone();
        tokio::spawn(async move {
            Host::write(
                host.as_ref(),
                WriteRequest {
                    path: "next".into(),
                    content: "safe".into(),
                    precondition: WritePrecondition::Missing,
                },
                CancellationToken::new(),
            )
            .await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Write(request),
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected new write, no original replay")
    };
    assert_ne!(operation_id, original);
    assert_eq!(request.path, std::path::PathBuf::from("next"));
    host.receive(
        2,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(json!({"version":"hash","bytes":4})),
        },
    )
    .unwrap();
    next.await.unwrap().unwrap();
    host.detach(2);
}

#[tokio::test]
async fn not_ready_route_allows_status_recovery_and_rejects_stale_reconcile() {
    let host = RemoteHost::new(descriptor()).unwrap();
    host.set_scope(scope()).unwrap();
    let (sender, mut receiver) = mpsc::channel(32);
    host.attach(sender, 2).unwrap();
    assert!(!host.is_ready());
    assert!(host.reconcile_for_generation(1).await.is_err());
    assert!(host.is_connected());
    let status = {
        let host = host.clone();
        tokio::spawn(async move { host.status_for_generation("inspect", None).await })
    };
    let HostFrame::Status { request_id, .. } = next_request(&mut receiver).await else {
        panic!("status must remain usable")
    };
    host.receive(
        2,
        HostFrame::Receipt {
            request_id,
            receipt: None,
        },
    )
    .unwrap();
    assert!(status.await.unwrap().unwrap().is_none());
    let recovery = {
        let host = host.clone();
        tokio::spawn(async move {
            host.call(HostRequest::Recovery, CancellationToken::new())
                .await
        })
    };
    assert!(matches!(
        respond(&host, 2, &mut receiver, json!([])).await,
        HostRequest::Recovery
    ));
    recovery.await.unwrap().unwrap();
    assert!(!host.is_ready());
    host.detach(2);
}

#[tokio::test]
async fn mismatched_record_is_isolated_and_only_our_original_is_failed() {
    let host = RemoteHost::new(descriptor()).unwrap();
    host.set_scope(scope()).unwrap();
    host.unresolved
        .lock()
        .unwrap()
        .insert("collision".into(), "a".repeat(64));
    let (sender, mut receiver) = mpsc::channel(32);
    host.attach(sender, 1).unwrap();
    let reconcile = {
        let host = host.clone();
        tokio::spawn(async move { host.reconcile().await })
    };
    let HostFrame::Status { request_id, .. } = next_request(&mut receiver).await else {
        panic!("expected status")
    };
    let foreign = OperationReceipt {
        operation_id: "collision".into(),
        payload_digest: "b".repeat(64),
        scope: scope(),
        state: ReceiptState::Indeterminate,
    };
    host.receive(
        1,
        HostFrame::Receipt {
            request_id,
            receipt: Some(foreign.clone()),
        },
    )
    .unwrap();
    assert!(matches!(
        respond(&host, 1, &mut receiver, json!([foreign])).await,
        HostRequest::Recovery
    ));
    reconcile.await.unwrap().unwrap();
    assert!(host.is_ready());
    assert!(host.unresolved_operations().is_empty());
    let records = host.operations(100);
    assert_eq!(records[0]["payload_digest"], "a".repeat(64));
    assert_eq!(records[0]["state"], "failed_unknown");
    assert_eq!(records[0]["reason"]["code"], "operation_conflict");
    host.detach(1);
}

#[test]
fn history_is_bounded_and_completed_result_bodies_are_omitted() {
    let host = RemoteHost::new(descriptor()).unwrap();
    for n in 0..110 {
        host.remember_receipt(&OperationReceipt {
            operation_id: format!("op-{n}"),
            payload_digest: "a".repeat(64),
            scope: scope(),
            state: ReceiptState::Completed(Ok(json!({"secret":"private completed result"}))),
        });
    }
    let records = host.operations(usize::MAX);
    assert_eq!(records.len(), 100);
    assert!(
        !json!(records)
            .to_string()
            .contains("private completed result")
    );
}
