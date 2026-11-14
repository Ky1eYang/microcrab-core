use super::*;

#[tokio::test]
async fn virtual_process_has_no_os_pid_and_rejects_fabricated_pid_changes() {
    let mut descriptor = process_descriptor();
    descriptor.platform = "browser".into();
    descriptor.info.os_family = microcrab_host_api::OsFamily::Other;
    descriptor.info.os = "browser".into();
    descriptor.info.arch = "wasm32".into();
    descriptor.info.shell = "wasmsh".into();
    descriptor
        .supported_primitives
        .retain(|name| name != "process.spawn" && name != "process.pid");
    descriptor.descriptor_digest = descriptor_fingerprint(&descriptor).unwrap();
    let host = RemoteHost::new(descriptor).unwrap();
    let mut receiver = ready(&host, 1).await;
    let handle = admit_with_pid(&host, &mut receiver, 2001, None).await;
    assert_eq!(stored_pid(&host, handle), None);
    for fabricated in [false, true] {
        let status = {
            let host = host.clone();
            tokio::spawn(async move {
                ProcessApi::status(host.as_ref(), &OpContext::default(), handle).await
            })
        };
        let HostFrame::Request {
            request_id,
            operation_id,
            method: HostRequest::ProcessStatus { handle: actual },
            ..
        } = next_rpc(&mut receiver).await
        else {
            panic!("virtual status request")
        };
        assert_eq!(actual, handle);
        let mut reply = json!({"state":"running", "elapsed_s":1});
        if fabricated {
            reply["pid"] = json!(1234);
        }
        host.receive(
            1,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Ok(reply),
            },
        )
        .unwrap();
        let result = status.await.unwrap();
        if fabricated {
            assert!(
                matches!(result, Err(error) if error.category == microcrab_host_api::ErrorCategory::Io)
            );
        } else {
            assert_eq!(result.unwrap().pid, None);
        }
    }
    let error = ProcessApi::pid(host.as_ref(), &OpContext::default(), handle)
        .await
        .unwrap_err();
    assert_eq!(
        error.category,
        microcrab_host_api::ErrorCategory::Unsupported
    );
    assert_eq!(stored_pid(&host, handle), None);
    let unknown = ProcessApi::pid(host.as_ref(), &OpContext::default(), relay_handle(999999))
        .await
        .unwrap_err();
    assert_eq!(
        unknown.category,
        microcrab_host_api::ErrorCategory::NotFound
    );
    while let Ok(frame) = receiver.try_recv() {
        assert!(
            matches!(frame, HostFrame::Ping { .. }),
            "unsupported PID must not send an RPC"
        );
    }
    host.detach(1);
}

#[tokio::test]
async fn native_pid_rpc_retains_the_receipted_identity() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let handle = admit(&host, &mut receiver, 2002, 4321).await;
    for returned in [4321, 4322] {
        let query = {
            let host = host.clone();
            tokio::spawn(async move {
                ProcessApi::pid(host.as_ref(), &OpContext::default(), handle).await
            })
        };
        let HostFrame::Request {
            request_id,
            operation_id,
            method: HostRequest::ProcessPid { handle: actual },
            ..
        } = next_rpc(&mut receiver).await
        else {
            panic!("native PID request")
        };
        assert_eq!(actual, handle);
        host.receive(
            1,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Ok(json!(returned)),
            },
        )
        .unwrap();
        let result = query.await.unwrap();
        if returned == 4321 {
            assert_eq!(result.unwrap(), returned);
        } else {
            assert!(
                matches!(result, Err(error) if error.category == microcrab_host_api::ErrorCategory::Io)
            );
        }
    }
    assert_eq!(stored_pid(&host, handle), Some(4321));
    host.detach(1);
}

#[tokio::test]
async fn inventory_waits_for_delivered_start_handle_installation() {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let ctx = OpContext::default();
    let start = host.launch(&ctx, request());
    tokio::pin!(start);
    assert!(
        poll_fn(|cx| Poll::Ready(start.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ShellLaunch(_),
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("start")
    };
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::to_value(ProcessStarted {
                handle: relay_handle(1401),
                pid: Some(4567),
                revision: 0,
            })
            .unwrap()),
        },
    )
    .unwrap();
    // Deliberately do not poll the caller: the receipt has arrived but its
    // Session-facing handle has not yet been installed in processes.entries.
    assert!(host.processes.entries.lock().unwrap().is_empty());
    let reconcile = host.reconcile();
    tokio::pin!(reconcile);
    assert!(
        poll_fn(|cx| Poll::Ready(reconcile.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("recovery")
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
    assert!(
        poll_fn(|cx| Poll::Ready(reconcile.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert!(
        receiver.try_recv().is_err(),
        "inventory must wait for the start owner"
    );
    let handle = start.await.unwrap();
    assert_eq!(stored_pid(&host, handle), Some(4567));
    assert!(
        poll_fn(|cx| Poll::Ready(reconcile.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    answer_inventory(
        &host,
        1,
        &mut receiver,
        vec![ProcessStarted {
            handle,
            pid: stored_pid(&host, handle),
            revision: 0,
        }],
    )
    .await;
    reconcile.await.unwrap();
    assert!(host.is_ready());
    assert!(host.check_process(handle).is_ok());
    assert!(!host.has_unresolved_cleanup());
    assert!(
        receiver.try_recv().is_err(),
        "legitimate startup must never emit orphan release"
    );
    host.detach(1);
}

#[tokio::test]
async fn confirmed_release_waits_for_real_ack_and_clears_cleanup_state() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let handle = admit(&host, &mut receiver, 1201, 3210).await;
    // Session close has already joined kill; release acknowledgement is a
    // separate lifecycle fact and cannot be reported complete at RPC admission.
    let release = {
        let host = host.clone();
        tokio::spawn(async move {
            ProcessApi::release(host.as_ref(), &OpContext::default(), handle).await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessRelease { handle: handle_id },
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("release")
    };
    assert_eq!(handle_id, handle);
    assert!(!release.is_finished());
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
    release.await.unwrap().unwrap();
    assert!(!host.has_unresolved_cleanup());
    assert!(!host.has_running_processes());
    assert!(host.processes.entries.lock().unwrap().is_empty());
    host.detach(1);
}

#[tokio::test]
async fn dropped_confirmed_release_waiter_keeps_its_cleanup_owner() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let handle = admit(&host, &mut receiver, 1202, 3211).await;
    let waiter = {
        let host = host.clone();
        tokio::spawn(async move {
            ProcessApi::release(host.as_ref(), &OpContext::default(), handle).await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessRelease { .. },
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("release")
    };
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::Value::Null),
        },
    )
    .unwrap();
    // Join the independently retained owner without a timer or polling loop.
    host.cleanup_releases().await.unwrap();
    assert!(!host.has_unresolved_cleanup());
    host.detach(1);
}

#[tokio::test]
async fn recreated_backend_discovers_orphan_and_requires_release_before_ready() {
    let descriptor = process_descriptor();
    let previous = RemoteHost::new(descriptor.clone()).unwrap();
    let mut old_route = ready(&previous, 1).await;
    let handle = admit(&previous, &mut old_route, 9_007_199_254_740_991, 6543).await;
    let id = handle;
    let pid = stored_pid(&previous, handle);
    previous.detach(1);
    drop(previous);
    // A bridge survives the backend, including its Completed start receipt and
    // live handle. Recovery excludes Completed starts, so inventory is required.
    let replacement = RemoteHost::new(descriptor).unwrap();
    replacement.set_scope(scope()).unwrap();
    let (outbound, mut receiver) = mpsc::channel(32);
    replacement
        .attach_with_lease(outbound, 2, "lease-2".into())
        .unwrap();
    let reconcile = {
        let host = replacement.clone();
        tokio::spawn(async move { host.reconcile().await })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("recovery")
    };
    replacement
        .receive(
            2,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Ok(json!([])),
            },
        )
        .unwrap();
    assert!(!replacement.is_ready());
    answer_inventory(
        &replacement,
        2,
        &mut receiver,
        vec![ProcessStarted {
            handle: id,
            pid,
            revision: 0,
        }],
    )
    .await;
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessRelease { handle: handle_id },
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("orphan release, never repeated start")
    };
    assert_eq!(handle_id, id);
    assert!(!replacement.is_ready());
    assert!(replacement.has_unresolved_cleanup());
    assert!(
        replacement
            .operations(100)
            .iter()
            .any(|item| item["state"] == "cleanup_pending"
                && item["pid"] == serde_json::to_value(pid).unwrap())
    );
    replacement
        .receive(
            2,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Ok(serde_json::Value::Null),
            },
        )
        .unwrap();
    reconcile.await.unwrap().unwrap();
    assert!(replacement.is_ready());
    assert!(!replacement.has_unresolved_cleanup());
    assert!(!replacement.has_running_processes());
    replacement.detach(2);
}

#[tokio::test]
async fn unknown_inventory_release_failure_retains_orphan_and_blocks_readiness() {
    let host = RemoteHost::new(process_descriptor()).unwrap();
    host.set_scope(scope()).unwrap();
    let (outbound, mut receiver) = mpsc::channel(32);
    host.attach_with_lease(outbound, 1, "lease-1".into())
        .unwrap();
    let reconcile = {
        let host = host.clone();
        tokio::spawn(async move { host.reconcile().await })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("recovery")
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
    answer_inventory(
        &host,
        1,
        &mut receiver,
        vec![ProcessStarted {
            handle: relay_handle(1203),
            pid: Some(3212),
            revision: 0,
        }],
    )
    .await;
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::ProcessRelease { .. },
        ..
    } = next_rpc(&mut receiver).await
    else {
        panic!("release")
    };
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Err(microcrab_host_protocol::WireError::new(
                microcrab_host_protocol::ErrorCode::OutcomeUnknown,
                "release receipt unavailable",
            )),
        },
    )
    .unwrap();
    assert!(reconcile.await.unwrap().is_err());
    assert!(!host.is_ready());
    assert!(host.has_unresolved_cleanup());
    assert!(host.has_running_processes());
    assert!(
        host.processes
            .entries
            .lock()
            .unwrap()
            .get(&relay_handle(1203))
            .unwrap()
            .release_pending
    );
    host.detach(1);
}
