//! A late response restarts receipt observation after a missing-receipt pass.
//! Only the exact terminal receipt may resolve uncertainty and own start cleanup.
use super::*;
use microcrab_host_api::{HandleId, OpContext, ShellRequest, process::ProcessSpec};
use microcrab_host_protocol::{ErrorCode, HostReply, ProcessStarted, WireError};

async fn next_control(receiver: &mut mpsc::Receiver<HostFrame>) -> HostFrame {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let frame = next_request(receiver).await;
            if !matches!(frame, HostFrame::Cancel { .. }) {
                return frame;
            }
        }
    })
    .await
    .expect("automatic recovery control must arrive without replay")
}

async fn wait_finished(host: &RemoteHost) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let changed = host.readiness_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !host.is_reconciling() {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("answered automatic recovery must finish");
}

async fn answer_status(
    host: &RemoteHost,
    receiver: &mut mpsc::Receiver<HostFrame>,
    original: &OperationReceipt,
    receipt: Option<OperationReceipt>,
) {
    let HostFrame::Status {
        request_id,
        operation_id,
        ..
    } = next_control(receiver).await
    else {
        panic!("expected original receipt query, never replay or settlement")
    };
    assert_eq!(operation_id, original.operation_id);
    host.receive(
        1,
        HostFrame::Receipt {
            request_id,
            receipt,
        },
    )
    .unwrap();
}

async fn answer_recovery(host: &RemoteHost, receiver: &mut mpsc::Receiver<HostFrame>) {
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_control(receiver).await
    else {
        panic!("expected Recovery after terminal receipt")
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
}

async fn start_followup(spawn: bool, success: bool) {
    let mut value = descriptor();
    value.supported_primitives.extend(
        ["process.spawn", "shell.launch", "process.release"]
            .into_iter()
            .map(str::to_owned),
    );
    value
        .tool_capabilities
        .extend(["process".into(), "shell".into()]);
    value.descriptor_digest = descriptor_fingerprint(&value).unwrap();
    let host = RemoteHost::new(value).unwrap();
    let mut receiver = ready(&host, 1).await;
    let cancel = CancellationToken::new();
    let start = {
        let host = host.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let ctx = OpContext {
                cancel,
                ..OpContext::default()
            };
            if spawn {
                ProcessApi::spawn(
                    host.as_ref(),
                    &ctx,
                    ProcessSpec {
                        program: "owned-process".into(),
                        ..ProcessSpec::default()
                    },
                )
                .await
            } else {
                host.launch(
                    &ctx,
                    ShellRequest {
                        command: "owned-process".into(),
                        cwd: None,
                        output_bytes: 1024,
                    },
                )
                .await
            }
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method,
        ..
    } = next_control(&mut receiver).await
    else {
        panic!("expected start")
    };
    assert!(matches!(
        (&method, spawn),
        (HostRequest::ProcessSpawn(_), true) | (HostRequest::ShellLaunch(_), false)
    ));
    let mut receipt = OperationReceipt {
        operation_id,
        payload_digest: payload_digest(&method).unwrap(),
        scope: scope(),
        state: ReceiptState::Intent,
    };
    cancel.cancel();
    assert!(start.await.unwrap().is_err());
    answer_status(&host, &mut receiver, &receipt, None).await;
    wait_finished(&host).await;
    assert!(!host.is_ready());
    assert_eq!(
        host.unresolved_operations(),
        vec![receipt.operation_id.clone()]
    );
    assert!(host.has_unresolved_cleanup());

    let started = ProcessStarted {
        handle: microcrab_host_api::process::ProcessHandle(HandleId(1701)),
        pid: Some(4321),
        revision: 0,
    };
    let result: HostReply = if success {
        Ok(serde_json::to_value(&started).unwrap())
    } else {
        Err(WireError::new(
            ErrorCode::Denied,
            "start denied by exact workspace policy",
        ))
    };
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id: receipt.operation_id.clone(),
            result: result.clone(),
        },
    )
    .unwrap();
    // The previous worker is finished: this response must own a new pass.
    // Even a known error cannot erase the cancelled start before Status proves it.
    assert!(host.is_reconciling());
    assert!(!host.is_ready());
    assert_eq!(
        host.unresolved_operations(),
        vec![receipt.operation_id.clone()]
    );
    {
        let starts = host.processes.starts.lock().unwrap();
        let owner = starts.get(&receipt.operation_id).unwrap();
        assert!(owner.cancelled);
        assert!(owner.receipt.is_none());
    }
    assert!(host.processes.entries.lock().unwrap().is_empty());
    assert!(host.has_unresolved_cleanup());
    assert!(!host.operations(100).iter().any(|item| {
        item["operation_id"] == receipt.operation_id && item["state"] == "completed"
    }));

    receipt.state = ReceiptState::Completed(result);
    answer_status(&host, &mut receiver, &receipt, Some(receipt.clone())).await;
    answer_recovery(&host, &mut receiver).await;
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessInventory,
        ..
    } = next_control(&mut receiver).await
    else {
        panic!("expected ProcessInventory before readiness")
    };
    assert!(!host.is_ready());
    assert!(host.unresolved_operations().is_empty());
    if success {
        assert!(
            host.processes
                .entries
                .lock()
                .unwrap()
                .get(&started.handle)
                .unwrap()
                .release_pending
        );
        assert!(host.has_unresolved_cleanup());
    } else {
        assert!(host.processes.starts.lock().unwrap().is_empty());
        assert!(host.processes.entries.lock().unwrap().is_empty());
    }
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(if success { json!([started]) } else { json!([]) }),
        },
    )
    .unwrap();
    if success {
        let HostFrame::Request {
            request_id,
            operation_id,
            method: HostRequest::ProcessRelease { handle },
            ..
        } = next_control(&mut receiver).await
        else {
            panic!("expected exact cancelled-start release")
        };
        assert_eq!(handle, started.handle);
        assert!(!host.is_ready());
        assert!(host.has_unresolved_cleanup());
        host.receive(
            1,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Ok(json!(null)),
            },
        )
        .unwrap();
    }
    wait_finished(&host).await;
    assert!(host.is_ready());
    assert!(host.unresolved_operations().is_empty());
    assert!(host.processes.starts.lock().unwrap().is_empty());
    assert!(host.processes.entries.lock().unwrap().is_empty());
    assert!(!host.has_unresolved_cleanup());
    assert!(!host.has_running_processes());
    while let Ok(frame) = receiver.try_recv() {
        assert!(matches!(frame, HostFrame::Ping { .. }));
    }
    host.detach(1);
}

#[tokio::test]
async fn shell_launch_late_success_after_missing_receipt_owns_followup_cleanup() {
    start_followup(false, true).await;
}

#[tokio::test]
async fn process_spawn_late_success_after_missing_receipt_owns_followup_cleanup() {
    start_followup(true, true).await;
}
