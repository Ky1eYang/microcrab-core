//! Readiness evidence and transitions, including late completion across replacement.
use super::*;
use microcrab_host_protocol::{ErrorCode, WireError};

async fn start_call(
    host: &Arc<RemoteHost>,
    method: HostRequest,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<Result<serde_json::Value, ToolError>> {
    let host = host.clone();
    tokio::spawn(async move { host.call(method, cancel).await })
}

#[tokio::test]
async fn unknown_wire_causes_keep_exact_code_message_and_identity() {
    for code in [
        ErrorCode::OutcomeUnknown,
        ErrorCode::Disconnected,
        ErrorCode::LeaseExpired,
    ] {
        let host = RemoteHost::new(descriptor()).unwrap();
        let mut receiver = ready(&host, 1).await;
        let changes = host.watch_readiness();
        let task = start_call(
            &host,
            HostRequest::Write(WriteRequest {
                path: "private-path".into(),
                content: "private-body".into(),
                precondition: WritePrecondition::Missing,
            }),
            CancellationToken::new(),
        )
        .await;
        let HostFrame::Request {
            request_id,
            operation_id,
            ..
        } = next_request(&mut receiver).await
        else {
            panic!("expected write")
        };
        let message = "original_worker_active: exact bridge error";
        host.receive(
            1,
            HostFrame::Response {
                request_id: request_id.clone(),
                operation_id: operation_id.clone(),
                result: Err(WireError::new(code.clone(), message)),
            },
        )
        .unwrap();
        // The validated response changes readiness before its caller resumes.
        assert!(changes.has_changed().unwrap());
        assert!(task.await.unwrap().is_err());
        let snapshot = host.readiness();
        assert_eq!(snapshot["connected"], true);
        assert_eq!(snapshot["ready"], false);
        assert_eq!(snapshot["reconciling"], false);
        assert_eq!(snapshot["phase"], "unready");
        assert_eq!(
            snapshot["cause"],
            json!({"code":code,"message":message,
            "operation_id":operation_id,"request_id":request_id})
        );
        assert_eq!(host.reconciliation_error(), Some(snapshot["cause"].clone()));
        assert!(!snapshot.to_string().contains("private-body"));
        assert!(!snapshot.to_string().contains("private-path"));
        host.detach(1);
    }
}

#[tokio::test]
async fn watch_resamples_committed_readiness_without_heartbeat_traffic() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut changes = host.watch_readiness();
    assert!(!changes.has_changed().unwrap());

    let (sender, _outbound) = mpsc::channel(32);
    host.attach(sender, 1).unwrap();
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    let attached = host.readiness();
    assert_eq!(attached["connection_generation"], "1");
    assert_eq!(attached["connected"], true);
    assert_eq!(attached["phase"], "awaiting_reconciliation");

    host.heartbeat_warning(1, true);
    host.heartbeat_warning(1, false);
    assert!(!changes.has_changed().unwrap());

    let (pass, revision) = host.begin_reconciliation(1).unwrap();
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    assert_eq!(host.readiness()["phase"], "reconciling");
    assert!(host.finish_reconciliation(1, pass, revision, &Ok(())));
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    assert_eq!(host.readiness()["phase"], "ready");

    host.record_cleanup_failure(1, &unavailable("process cleanup failed"));
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    assert_eq!(host.readiness()["cause"]["code"], "process_cleanup_failed");

    let (pass, revision) = host.request_call_recovery(1).unwrap();
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    assert_eq!(host.readiness()["phase"], "reconciling");
    assert!(host.finish_call_recovery(1, pass, revision, &Ok(())));
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    assert_eq!(host.readiness()["phase"], "ready");

    let (pass, _) = host.begin_reconciliation(1).unwrap();
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    host.abandon_reconciliation(1, pass);
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    assert_eq!(host.readiness()["phase"], "unready");
    assert_eq!(
        host.readiness()["cause"]["code"],
        "reconciliation_cancelled"
    );

    host.detach(1);
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    let detached = host.readiness();
    assert_eq!(detached["connection_generation"], "1");
    assert_eq!(detached["connected"], false);
    assert_eq!(detached["phase"], "disconnected");
}

#[tokio::test]
async fn old_pass_success_and_failure_cannot_publish_on_replacement() {
    for fail in [false, true] {
        let host = RemoteHost::new(descriptor()).unwrap();
        let mut receiver = ready(&host, 1).await;
        let old = {
            let host = host.clone();
            tokio::spawn(async move { host.reconcile_for_generation(1).await })
        };
        let HostFrame::Request {
            request_id,
            operation_id,
            ..
        } = next_request(&mut receiver).await
        else {
            panic!("expected recovery")
        };
        host.receive(
            1,
            HostFrame::Response {
                request_id,
                operation_id,
                result: if fail {
                    Err(WireError::new(
                        ErrorCode::OutcomeUnknown,
                        "old pass failure",
                    ))
                } else {
                    Ok(json!([]))
                },
            },
        )
        .unwrap();
        // No yield between delivery and replacement: completion resumes on the new route.
        let (sender, mut replacement) = mpsc::channel(32);
        host.attach(sender, 2).unwrap();
        let changes = host.watch_readiness();
        let expected = host.readiness();
        assert_eq!(expected["phase"], "awaiting_reconciliation");
        assert_eq!(expected["connection_generation"], "2");
        assert_eq!(expected["reconciling"], false);
        assert_eq!(expected["cause"], serde_json::Value::Null);
        assert!(old.await.unwrap().is_err());
        assert_eq!(host.readiness(), expected);
        assert!(!changes.has_changed().unwrap());
        let new = {
            let host = host.clone();
            tokio::spawn(async move { host.reconcile_for_generation(2).await })
        };
        let HostFrame::Request {
            request_id,
            operation_id,
            ..
        } = next_request(&mut replacement).await
        else {
            panic!("expected replacement recovery")
        };
        host.receive(
            2,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Ok(json!([])),
            },
        )
        .unwrap();
        new.await.unwrap().unwrap();
        assert!(host.is_ready());
        host.detach(2);
    }
}
