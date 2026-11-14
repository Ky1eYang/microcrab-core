//! Reconciliation inventory and process starts share one admission mutex.
use super::*;
use std::{future::Future, task::Poll, time::Duration};

fn spawn_request() -> HostRequest {
    HostRequest::ProcessSpawn(microcrab_host_api::process::ProcessSpec {
        program: "owned-process".into(),
        ..Default::default()
    })
}

#[tokio::test]
async fn process_start_waits_outside_inventory_mutex_then_admits_once() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let reconcile = {
        let host = host.clone();
        tokio::spawn(async move { host.reconcile().await })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected recovery")
    };
    let start = {
        let host = host.clone();
        tokio::spawn(async move {
            host.start_remote_process(spawn_request(), CancellationToken::new())
                .await
        })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(20), next_request(&mut receiver))
            .await
            .is_err()
    );
    assert!(!start.is_finished());
    assert!(host.unresolved_operations().is_empty());
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(json!([])),
        },
    )
    .unwrap();
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessInventory,
        ..
    } = tokio::time::timeout(Duration::from_secs(1), next_request(&mut receiver))
        .await
        .expect("process start must not hold inventory mutex while awaiting reconciliation")
    else {
        panic!("inventory must precede the process start")
    };
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(json!([])),
        },
    )
    .unwrap();
    reconcile.await.unwrap().unwrap();
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessSpawn(_),
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected one process start")
    };
    let handle = relay_handle(7001);
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::to_value(ProcessStarted {
                handle,
                pid: Some(7011),
                revision: 0,
            })
            .unwrap()),
        },
    )
    .unwrap();
    assert_eq!(start.await.unwrap().unwrap(), handle);
    assert_eq!(host.processes.entries.lock().unwrap().len(), 1);
    assert!(host.unresolved_operations().is_empty());
    assert!(receiver.try_recv().is_err());
    host.detach(1);
}

#[tokio::test]
async fn pass_started_after_preflight_is_rejected_without_waiting_under_process_mutex() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let admission = host.process_start_admission.lock().await;
    let start = host.start_remote_process(spawn_request(), CancellationToken::new());
    tokio::pin!(start);
    // Drive exactly through successful readiness preflight into the held mutex.
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(start.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    let (pass, _) = host.begin_reconciliation(1).unwrap();
    drop(admission);
    let error = tokio::time::timeout(Duration::from_secs(1), &mut start)
        .await
        .expect("process admission must never await reconciliation under the inventory mutex")
        .unwrap_err();
    assert!(error.to_string().contains("reconciliation is in progress"));
    assert!(host.pending.lock().unwrap().is_empty());
    assert!(host.unresolved_operations().is_empty());
    assert!(host.processes.starts.lock().unwrap().is_empty());
    while let Ok(frame) = receiver.try_recv() {
        assert!(matches!(frame, HostFrame::Ping { .. }));
    }
    assert!(host.process_start_admission.try_lock().is_ok());
    host.abandon_reconciliation(1, pass);
    host.detach(1);
}
