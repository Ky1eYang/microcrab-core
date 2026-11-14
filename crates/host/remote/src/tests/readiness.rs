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
        let expected = host.readiness();
        assert_eq!(expected["phase"], "awaiting_reconciliation");
        assert_eq!(expected["connection_generation"], "2");
        assert_eq!(expected["reconciling"], false);
        assert_eq!(expected["cause"], serde_json::Value::Null);
        assert!(old.await.unwrap().is_err());
        assert_eq!(host.readiness(), expected);
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
