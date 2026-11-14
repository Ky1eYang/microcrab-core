//! Unadmitted dispatch may join one active pass, but never cross a route/pass fence.
use super::*;
use std::time::Duration;

fn write() -> HostRequest {
    HostRequest::Write(WriteRequest {
        path: "a".into(),
        content: "private-body".into(),
        precondition: WritePrecondition::Missing,
    })
}
async fn call(
    host: &Arc<RemoteHost>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<Result<serde_json::Value, ToolError>> {
    let host = host.clone();
    tokio::spawn(async move { host.call(write(), cancel).await })
}
async fn recovering(
    host: &Arc<RemoteHost>,
    receiver: &mut mpsc::Receiver<HostFrame>,
) -> (
    tokio::task::JoinHandle<Result<(), ToolError>>,
    String,
    String,
) {
    let host_task = host.clone();
    let task = tokio::spawn(async move { host_task.reconcile().await });
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_request(receiver).await
    else {
        panic!("expected recovery")
    };
    (task, request_id, operation_id)
}
async fn no_admission(
    host: &RemoteHost,
    receiver: &mut mpsc::Receiver<HostFrame>,
    task: &tokio::task::JoinHandle<Result<serde_json::Value, ToolError>>,
) {
    assert!(
        tokio::time::timeout(Duration::from_millis(20), next_request(receiver))
            .await
            .is_err()
    );
    assert!(
        !task.is_finished(),
        "active reconciliation must not reject the call"
    );
    assert!(host.unresolved_operations().is_empty());
    assert_eq!(
        host.pending.lock().unwrap().len(),
        1,
        "only Recovery is pending"
    );
}

#[tokio::test]
async fn active_recovery_delays_unadmitted_mutation_then_sends_once() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let (recovery, request_id, operation_id) = recovering(&host, &mut receiver).await;
    let task = call(&host, CancellationToken::new()).await;
    no_admission(&host, &mut receiver, &task).await;
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(json!([])),
        },
    )
    .unwrap();
    recovery.await.unwrap().unwrap();
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Write(_),
        lease_generation,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected one write after ready")
    };
    assert_eq!(lease_generation, "lease-1");
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(json!({"written":true})),
        },
    )
    .unwrap();
    assert_eq!(task.await.unwrap().unwrap(), json!({"written":true}));
    assert!(host.unresolved_operations().is_empty());
    assert!(receiver.try_recv().is_err());
    host.detach(1);
}
