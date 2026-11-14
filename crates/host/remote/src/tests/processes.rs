use super::*;
mod admission;
mod followup;
use microcrab_host_api::process::ProcessHandle;
use microcrab_host_api::{HandleId, OpContext, ShellRequest};
use microcrab_host_protocol::ProcessStarted;
fn relay_handle(value: u64) -> ProcessHandle {
    ProcessHandle(HandleId(value))
}
fn stored_pid(host: &RemoteHost, handle: ProcessHandle) -> Option<u32> {
    host.processes
        .entries
        .lock()
        .unwrap()
        .get(&handle)
        .unwrap()
        .pid
}
fn request() -> ShellRequest {
    ShellRequest {
        command: "owned-process".into(),
        cwd: None,
        output_bytes: 1024,
    }
}
fn process_descriptor() -> HostDescriptor {
    let mut value = descriptor();
    value.supported_primitives.extend(
        [
            "process.spawn",
            "shell.launch",
            "process.status",
            "process.watch",
            "process.release",
        ]
        .into_iter()
        .map(str::to_owned),
    );
    value
        .tool_capabilities
        .extend(["process".into(), "shell".into()]);
    value.descriptor_digest = descriptor_fingerprint(&value).unwrap();
    value
}
async fn admit(
    host: &Arc<RemoteHost>,
    receiver: &mut mpsc::Receiver<HostFrame>,
    id: u64,
    pid: u32,
) -> ProcessHandle {
    admit_with_pid(host, receiver, id, Some(pid)).await
}
async fn admit_with_pid(
    host: &Arc<RemoteHost>,
    receiver: &mut mpsc::Receiver<HostFrame>,
    id: u64,
    pid: Option<u32>,
) -> ProcessHandle {
    let task = {
        let host = host.clone();
        tokio::spawn(async move { host.launch(&OpContext::default(), request()).await })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ShellLaunch(_),
        ..
    } = next_request(receiver).await
    else {
        panic!("start request")
    };
    // Completion can overtake delivery of the admission response.
    host.receive(
        1,
        HostFrame::ProcessChanged {
            lease_generation: "lease-1".into(),
            handle: relay_handle(id),
            revision: 1,
        },
    )
    .unwrap();
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::to_value(ProcessStarted {
                handle: relay_handle(id),
                pid,
                revision: 0,
            })
            .unwrap()),
        },
    )
    .unwrap();
    task.await.unwrap().unwrap()
}
#[tokio::test]
async fn actual_receipt_watch_and_route_loss_preserve_owned_handle() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    assert!(host.tool_capabilities().contains(&"process".into()));
    let mut receiver = ready(&host, 1).await;
    let handle = admit(&host, &mut receiver, 17, 4321).await;
    assert!(!host.has_unresolved_cleanup());
    assert_eq!(stored_pid(&host, handle), Some(4321));
    assert_eq!(handle, relay_handle(17));
    assert!(host.watch(handle).unwrap().has_changed().unwrap());
    let mut changes = host.watch(handle).unwrap().clone();
    changes.borrow_and_update();
    assert!(
        host.receive(
            1,
            HostFrame::ProcessChanged {
                lease_generation: "wrong".into(),
                handle,
                revision: 2
            }
        )
        .is_err()
    );
    assert!(!changes.has_changed().unwrap());
    host.receive(
        1,
        HostFrame::ProcessChanged {
            lease_generation: "lease-1".into(),
            handle,
            revision: 2,
        },
    )
    .unwrap();
    changes.changed().await.unwrap();
    changes.borrow_and_update();
    host.detach(1);
    changes.changed().await.unwrap();
    assert!(host.has_running_processes());
    assert!(
        ProcessApi::status(host.as_ref(), &OpContext::default(), handle)
            .await
            .is_err()
    );
    assert!(host.processes.entries.lock().unwrap().contains_key(&handle));
    assert!(!host.has_unresolved_cleanup());
    let forged = relay_handle(99999);
    assert!(host.check_process(forged).is_err());
}
#[tokio::test]
async fn independent_owned_jobs_overlap_and_unknown_drain_fences_only_its_output() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let a = admit(&host, &mut receiver, 21, 1234).await;
    let b = admit(&host, &mut receiver, 22, 1235).await;
    let drain = {
        let host = host.clone();
        tokio::spawn(async move { host.drain(&OpContext::default(), a, 1024).await })
    };
    let HostFrame::Request {
        request_id: first_request,
        operation_id: first_operation,
        method: HostRequest::ShellDrain { .. },
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("drain request")
    };
    let kill = {
        let host = host.clone();
        tokio::spawn(async move { host.kill_tree(&OpContext::default(), b, true).await })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessKill { .. },
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("concurrent kill request")
    };
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::Value::Null),
        },
    )
    .unwrap();
    kill.await.unwrap().unwrap();
    host.receive(
        1,
        HostFrame::Response {
            request_id: first_request.clone(),
            operation_id: first_operation.clone(),
            result: Err(microcrab_host_protocol::WireError::new(
                microcrab_host_protocol::ErrorCode::OutcomeUnknown,
                "unknown drain",
            )),
        },
    )
    .unwrap();
    assert!(drain.await.unwrap().is_err());
    assert!(host.is_ready());
    assert_eq!(host.unresolved_operations(), vec![first_operation]);
    assert!(host.drain(&OpContext::default(), a, 1024).await.is_err());
    let before = host.processes.entries.lock().unwrap().len();
    host.detach(1);
    assert_eq!(host.processes.entries.lock().unwrap().len(), before);
}

async fn next_rpc(receiver: &mut mpsc::Receiver<HostFrame>) -> HostFrame {
    loop {
        let frame = next_request(receiver).await;
        if matches!(frame, HostFrame::Request { .. } | HostFrame::Status { .. }) {
            return frame;
        }
    }
}
async fn answer_inventory(
    host: &RemoteHost,
    generation: u64,
    receiver: &mut mpsc::Receiver<HostFrame>,
    inventory: Vec<ProcessStarted>,
) {
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessInventory,
        ..
    } = next_rpc(receiver).await
    else {
        panic!("explicit inventory")
    };
    host.receive(
        generation,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::to_value(inventory).unwrap()),
        },
    )
    .unwrap();
}
#[tokio::test]
async fn cancelled_lost_start_is_reconciled_then_released_without_start_replay() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut first = ready(&host, 1).await;
    let cancel = CancellationToken::new();
    let start = {
        let host = host.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            host.launch(
                &OpContext {
                    cancel,
                    ..OpContext::default()
                },
                request(),
            )
            .await
        })
    };
    let HostFrame::Request {
        operation_id: original,
        method,
        ..
    } = next_rpc(&mut first).await
    else {
        panic!("start")
    };
    let digest = payload_digest(&method).unwrap();
    cancel.cancel();
    assert!(start.await.unwrap().is_err());
    host.detach(1);
    assert!(host.has_running_processes());
    assert!(
        host.processes
            .starts
            .lock()
            .unwrap()
            .get(&original)
            .unwrap()
            .cancelled
    );
    assert!(host.has_unresolved_cleanup());
    let (outbound, mut second) = mpsc::channel(32);
    host.attach_with_lease(outbound, 2, "lease-2".into())
        .unwrap();
    let reconcile = {
        let host = host.clone();
        tokio::spawn(async move { host.reconcile().await })
    };
    let HostFrame::Status {
        request_id,
        operation_id,
        ..
    } = next_rpc(&mut second).await
    else {
        panic!("original receipt query, never repeated start")
    };
    assert_eq!(operation_id, original);
    host.receive(
        2,
        HostFrame::Receipt {
            request_id,
            receipt: Some(OperationReceipt {
                operation_id,
                payload_digest: digest,
                scope: scope(),
                state: ReceiptState::Completed(Ok(serde_json::to_value(ProcessStarted {
                    handle: relay_handle(707),
                    pid: Some(6543),
                    revision: 0,
                })
                .unwrap())),
            }),
        },
    )
    .unwrap();
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_rpc(&mut second).await
    else {
        panic!("recovery")
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
    answer_inventory(
        &host,
        2,
        &mut second,
        vec![ProcessStarted {
            handle: relay_handle(707),
            pid: Some(6543),
            revision: 0,
        }],
    )
    .await;
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessRelease { handle: handle_id },
        ..
    } = next_rpc(&mut second).await
    else {
        panic!("explicit owned cleanup")
    };
    assert_eq!(handle_id, relay_handle(707));
    assert!(
        host.operations(100)
            .iter()
            .any(|item| item["state"] == "cleanup_pending" && item["pid"] == 6543)
    );
    host.receive(
        2,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::Value::Null),
        },
    )
    .unwrap();
    reconcile.await.unwrap().unwrap();
    assert!(host.is_ready());
    assert!(host.processes.starts.lock().unwrap().is_empty());
    assert!(host.processes.entries.lock().unwrap().is_empty());
    assert!(!host.has_running_processes());
    assert!(!host.has_unresolved_cleanup());
    host.detach(2);
}
#[tokio::test]
async fn cancelled_start_late_receipt_releases_the_identified_process() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let cancel = CancellationToken::new();
    let start = {
        let host = host.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            host.launch(
                &OpContext {
                    cancel,
                    ..OpContext::default()
                },
                request(),
            )
            .await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id: original,
        method,
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("start")
    };
    let started = ProcessStarted {
        handle: relay_handle(909),
        pid: Some(8765),
        revision: 0,
    };
    let result = Ok(serde_json::to_value(&started).unwrap());
    let receipt = OperationReceipt {
        operation_id: original.clone(),
        payload_digest: payload_digest(&method).unwrap(),
        scope: scope(),
        state: ReceiptState::Completed(result.clone()),
    };
    cancel.cancel();
    assert!(start.await.unwrap().is_err());
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id: original.clone(),
            result,
        },
    )
    .unwrap();
    assert_eq!(host.unresolved_operations(), vec![original.clone()]);
    assert!(host.has_unresolved_cleanup());
    assert!(host.processes.entries.lock().unwrap().is_empty());
    {
        let starts = host.processes.starts.lock().unwrap();
        let owner = starts.get(&original).unwrap();
        assert!(owner.cancelled);
        assert!(owner.receipt.is_none());
    }
    let HostFrame::Status {
        request_id,
        operation_id,
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("exact terminal receipt query before cleanup")
    };
    assert_eq!(operation_id, original);
    host.receive(
        1,
        HostFrame::Receipt {
            request_id,
            receipt: Some(receipt),
        },
    )
    .unwrap();
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("recovery before process inventory")
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
    answer_inventory(&host, 1, &mut receiver, vec![started]).await;
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessRelease { handle: handle_id },
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("owned cleanup")
    };
    assert_eq!(handle_id, relay_handle(909));
    assert!(!host.is_ready());
    assert!(host.has_unresolved_cleanup());
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::Value::Null),
        },
    )
    .unwrap();
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
    .expect("answered automatic cleanup must finish");
    assert!(host.is_ready());
    assert!(host.unresolved_operations().is_empty());
    assert!(!host.has_unresolved_cleanup());
    assert!(!host.has_running_processes());
    assert!(host.processes.entries.lock().unwrap().is_empty());
    assert!(host.processes.starts.lock().unwrap().is_empty());
    host.detach(1);
}
#[tokio::test]
async fn reconnect_readiness_wakes_terminal_refresh_without_replayed_notification() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut first = ready(&host, 1).await;
    let handle = admit(&host, &mut first, 111, 4321).await;
    let mut changes = host.watch(handle).unwrap().clone();
    changes.borrow_and_update();
    host.detach(1);
    changes.changed().await.unwrap();
    changes.borrow_and_update();
    assert!(
        ProcessApi::status(host.as_ref(), &OpContext::default(), handle)
            .await
            .is_err()
    );
    let (outbound, mut second) = mpsc::channel(32);
    host.attach_with_lease(outbound, 2, "lease-2".into())
        .unwrap();
    // Consume the attach/unavailable wake. Only successful reconciliation can
    // supply the next change; the bridge sends no ProcessChanged in this test.
    changes.borrow_and_update();
    let reconcile = {
        let host = host.clone();
        tokio::spawn(async move { host.reconcile().await })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_rpc(&mut second).await
    else {
        panic!("recovery")
    };
    assert!(!changes.has_changed().unwrap());
    host.receive(
        2,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(json!([])),
        },
    )
    .unwrap();
    answer_inventory(
        &host,
        2,
        &mut second,
        vec![ProcessStarted {
            handle,
            pid: stored_pid(&host, handle),
            revision: 0,
        }],
    )
    .await;
    reconcile.await.unwrap().unwrap();
    assert!(host.is_ready());
    assert!(!host.has_unresolved_cleanup());
    changes.changed().await.unwrap();
    let status = {
        let host = host.clone();
        tokio::spawn(async move {
            ProcessApi::status(host.as_ref(), &OpContext::default(), handle).await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessStatus { handle: handle_id },
        ..
    } = next_rpc(&mut second).await
    else {
        panic!("metadata refresh")
    };
    assert_eq!(handle_id, handle);
    host.receive(2, HostFrame::Response { request_id, operation_id, result: Ok(json!({"pid":stored_pid(&host, handle),"state":"exited","status":{"code":0,"signal":null},"elapsed_s":1})) }).unwrap();
    assert!(matches!(
        status.await.unwrap().unwrap().state,
        microcrab_host_api::process::ProcessState::Exited {
            status: microcrab_host_api::process::ExitStatus { code: Some(0), .. }
        }
    ));
    assert!(!host.has_running_processes());
    assert!(!host.has_unresolved_cleanup());
    host.detach(2);
}
#[tokio::test]
async fn disconnected_release_retains_cancellation_intent_and_never_claims_cleanup() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let handle = admit(&host, &mut receiver, 808, 7654).await;
    host.detach(1);
    host.release_remote_process(handle);
    assert!(host.release(&OpContext::default(), handle).await.is_err());
    assert!(host.has_running_processes());
    assert!(
        host.processes
            .entries
            .lock()
            .unwrap()
            .get(&handle)
            .unwrap()
            .release_pending
    );
    assert!(host.has_unresolved_cleanup());
    assert!(
        host.operations(100)
            .iter()
            .any(|item| item["state"] == "cleanup_pending" && item["pid"] == 7654)
    );
}

#[tokio::test]
async fn structured_raw_stream_admits_exactly_one_bounded_read_per_consumer_poll() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let spawn = {
        let host = host.clone();
        tokio::spawn(async move {
            host.spawn(
                &OpContext::default(),
                microcrab_host_api::process::ProcessSpec {
                    program: "native".into(),
                    ..Default::default()
                },
            )
            .await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessSpawn(_),
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("structured spawn")
    };
    let handle = relay_handle(5001);
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::to_value(ProcessStarted {
                handle,
                pid: Some(9876),
                revision: 0,
            })
            .unwrap()),
        },
    )
    .unwrap();
    assert_eq!(spawn.await.unwrap().unwrap(), handle);
    let mut stream = host.stdout(handle).unwrap();
    assert!(
        matches!(host.stdout(handle), Err(error) if error.category == microcrab_host_api::ErrorCategory::Conflict)
    );
    let reading =
        tokio::spawn(async move { std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await });
    let HostFrame::Request {
        request_id,
        operation_id,
        method:
            HostRequest::ProcessStreamOpen {
                handle: actual,
                stderr: false,
            },
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("exclusive raw pipe take")
    };
    assert_eq!(actual, handle);
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::Value::Null),
        },
    )
    .unwrap();
    let HostFrame::Request {
        request_id,
        operation_id,
        method:
            HostRequest::ProcessStreamRead {
                handle: actual,
                stderr: false,
                max_bytes,
            },
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("one bounded raw read")
    };
    assert_eq!(actual, handle);
    assert_eq!(max_bytes as usize, microcrab_host_protocol::MAX_CHUNK_BYTES);
    assert!(!reading.is_finished());
    // Raw bytes survive JSON transport, including non-UTF-8 data.
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(json!({"data":[0,255,128],"eof":false})),
        },
    )
    .unwrap();
    assert_eq!(
        reading.await.unwrap().unwrap().unwrap().as_ref(),
        &[0, 255, 128]
    );
    // The route heartbeat is independent of consumer polling. Reject every
    // other queued frame so an extra raw-read RPC still fails this invariant.
    while let Ok(frame) = receiver.try_recv() {
        assert!(
            matches!(frame, HostFrame::Ping { .. }),
            "unpolled output must never start a background read pump: {frame:?}"
        );
    }
    host.detach(1);
}
