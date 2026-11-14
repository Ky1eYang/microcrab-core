use super::*;
mod admission;
mod automatic_recovery;
mod automatic_recovery_followup;
mod diagnostics;
mod failure_causes;
mod heartbeat;
mod mutations;
mod processes;
mod readiness;
mod reconciliation;

#[tokio::test]
async fn typed_glob_preserves_directory_opt_in_kinds_and_truncation() {
    use microcrab_host_api::{GlobEntry, GlobEntryKind, GlobOutput, GlobRequest};
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let task = {
        let host = host.clone();
        tokio::spawn(async move {
            host.glob(
                GlobRequest {
                    root: ".".into(),
                    pattern: "*".into(),
                    include_hidden: false,
                    include_ignored: false,
                    include_directories: true,
                    max_results: 3,
                },
                CancellationToken::new(),
            )
            .await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Glob(request),
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected typed glob request")
    };
    assert!(request.include_directories);
    assert_eq!(request.max_results, 3);
    let expected = GlobOutput {
        entries: vec![
            GlobEntry {
                path: "file".into(),
                kind: GlobEntryKind::File,
            },
            GlobEntry {
                path: "dir".into(),
                kind: GlobEntryKind::Directory,
            },
            GlobEntry {
                path: "link".into(),
                kind: GlobEntryKind::Symlink,
            },
        ],
        truncated: true,
    };
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::to_value(&expected).unwrap()),
        },
    )
    .unwrap();
    assert_eq!(task.await.unwrap().unwrap(), expected);
    assert!(host.is_ready());
    assert!(host.unresolved.lock().unwrap().is_empty());
    host.detach(1);
}

#[tokio::test]
async fn known_shell_failures_remain_operation_scoped_and_actionable() {
    use microcrab_host_api::{HandleId, process::ProcessHandle};
    use microcrab_host_protocol::{ErrorCode, WireError};
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    for code in [
        ErrorCode::Denied,
        ErrorCode::Timeout,
        ErrorCode::Cancelled,
        ErrorCode::Conflict,
        ErrorCode::OperationConflict,
        ErrorCode::NotFound,
        ErrorCode::Unsupported,
        ErrorCode::InvalidRequest,
        ErrorCode::Limit,
        ErrorCode::Io,
    ] {
        let detail = format!("shell_drain: {code:?}: process 17 could not provide retained output");
        let task = {
            let host = host.clone();
            tokio::spawn(async move {
                host.call(
                    HostRequest::ShellDrain {
                        handle: ProcessHandle(HandleId(17)),
                        max_bytes: 1024,
                    },
                    CancellationToken::new(),
                )
                .await
            })
        };
        let HostFrame::Request {
            request_id,
            operation_id,
            ..
        } = next_request(&mut receiver).await
        else {
            panic!("expected shell drain")
        };
        host.receive(
            1,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Err(WireError::new(code.clone(), detail.clone())),
            },
        )
        .unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains(&detail), "{error}");
        if code == ErrorCode::Limit {
            assert!(matches!(error, ToolError::OperationLimit(_)));
        }
        assert!(host.is_connected());
        assert!(host.is_ready(), "known {code:?} failure detached route");
        assert_eq!(host.readiness()["cause"], serde_json::Value::Null);
        assert_eq!(host.readiness()["phase"], "ready");
        assert!(host.unresolved.lock().unwrap().is_empty());
    }
    host.detach(1);
}
#[test]
fn conversation_receipts_cannot_borrow_native_or_other_conversation_authority() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let expected =
        BindingScope::conversation("backend".into(), "binding".into(), "conversation".into());
    host.set_scope(expected.clone()).unwrap();
    let mut receipt = OperationReceipt {
        operation_id: "op".into(),
        payload_digest: "a".repeat(64),
        scope: expected.clone(),
        state: ReceiptState::Intent,
    };
    assert!(host.valid_receipt_scope(&receipt));
    receipt.scope =
        BindingScope::project("backend".into(), "binding".into(), "conversation".into());
    assert!(!host.valid_receipt_scope(&receipt));
    receipt.scope = BindingScope::conversation("backend".into(), "binding".into(), "other".into());
    assert!(!host.valid_receipt_scope(&receipt));
    assert!(host.set_scope(receipt.scope).is_err());
    assert!(host.set_scope(expected).is_ok());
}
#[tokio::test]
async fn cancelled_late_reply_is_discarded_without_losing_route() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let cancel = CancellationToken::new();
    let read = {
        let host = host.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            host.read_text(
                ReadRequest {
                    path: "a".into(),
                    offset_line: 1,
                    max_lines: 1,
                    max_bytes: 8,
                },
                cancel,
            )
            .await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected read")
    };
    cancel.cancel();
    assert!(read.await.unwrap().is_err());
    let reference = microcrab_host_protocol::TransferRef {
        transfer_id: "retired-transfer".into(),
        byte_length: 0,
        sha256: microcrab_host_protocol::sha256(b""),
        kind: "bytes".into(),
    };
    host.receive(
        1,
        HostFrame::Response {
            request_id: request_id.clone(),
            operation_id: operation_id.clone(),
            result: Ok(serde_json::to_value(reference).unwrap()),
        },
    )
    .unwrap();
    host.receive(
        1,
        HostFrame::TransferChunk {
            transfer_id: "retired-transfer".into(),
            sequence: 0,
            data: vec![],
            eof: true,
        },
    )
    .unwrap();
    let diagnostics = host.diagnostics(64);
    assert_eq!(diagnostics[0]["reason"], "late_response");
    assert_eq!(diagnostics[0]["request_id"], request_id);
    assert_eq!(diagnostics[0]["operation_id"], operation_id);
    assert_eq!(diagnostics[0]["connection_generation"], "1");
    assert!(host.is_ready());
    host.detach(1);
}
#[test]
fn duplicate_transfer_declarations_cannot_grow_pending_map() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let (sender, _receiver) = oneshot::channel();
    host.pending.lock().unwrap().insert(
        "request".into(),
        Pending {
            raw_read: false,
            readiness_operation: false,
            operation_id: "operation".into(),
            generation: 1,
            sender,
        },
    );
    let reference = |id: &str| microcrab_host_protocol::TransferRef {
        transfer_id: id.into(),
        byte_length: 0,
        sha256: microcrab_host_protocol::sha256(b""),
        kind: "bytes".into(),
    };
    host.begin_transfer("request".into(), "operation".into(), 1, reference("first"))
        .unwrap();
    assert!(
        host.begin_transfer("request".into(), "operation".into(), 1, reference("second"))
            .is_err()
    );
    assert_eq!(host.transfers.lock().unwrap().len(), 1);
}
use microcrab_host_api::{OsFamily, ReadRequest, WritePrecondition, WriteRequest};
use microcrab_host_protocol::{
    BindingScope, HostFrame, HostRequest, OperationReceipt, ReceiptState, descriptor_fingerprint,
    payload_digest,
};
use serde_json::json;
fn descriptor() -> HostDescriptor {
    let mut descriptor = HostDescriptor {
        protocol_version: microcrab_host_protocol::PROTOCOL_VERSION,
        host_id: "host-1".into(),
        workspace_id: "workspace-1".into(),
        bridge_instance_id: "instance-1".into(),
        workspace_root: "/remote/workspace".into(),
        platform: "linux".into(),
        info: HostInfo {
            os_family: OsFamily::Unix,
            os: "linux".into(),
            arch: "x86_64".into(),
            shell: String::new(),
            path_separator: "/".into(),
            newline: "\n".into(),
            capabilities: vec!["read".into(), "write".into()],
            resources: None,
            virtual_shell: None,
            description: None,
        },
        tool_capabilities: vec!["read".into(), "write".into()],
        supported_primitives: vec!["filesystem.stat".into()],
        descriptor_digest: String::new(),
    };
    descriptor.descriptor_digest = descriptor_fingerprint(&descriptor).unwrap();
    descriptor
}
fn scope() -> BindingScope {
    BindingScope::project("s".into(), "b".into(), "p".into())
}
async fn next_request(receiver: &mut mpsc::Receiver<HostFrame>) -> HostFrame {
    loop {
        let frame = receiver.recv().await.unwrap();
        if !matches!(frame, HostFrame::Ping { .. }) {
            return frame;
        }
    }
}
async fn ready(host: &Arc<RemoteHost>, generation: u64) -> mpsc::Receiver<HostFrame> {
    ready_with_scope(host, generation, scope()).await
}
async fn ready_with_scope(
    host: &Arc<RemoteHost>,
    generation: u64,
    scope: BindingScope,
) -> mpsc::Receiver<HostFrame> {
    let (outbound, mut receiver) = mpsc::channel(32);
    host.set_scope(scope).unwrap();
    host.attach_with_lease(outbound, generation, format!("lease-{generation}"))
        .unwrap();
    let task = {
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
    host.receive(
        generation,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(json!([])),
        },
    )
    .unwrap();
    if host
        .descriptor
        .supported_primitives
        .iter()
        .any(|name| name == "process.spawn" || name == "shell.launch")
    {
        let HostFrame::Request {
            request_id,
            operation_id,
            method: HostRequest::ProcessInventory,
            ..
        } = next_request(&mut receiver).await
        else {
            panic!("expected job inventory")
        };
        host.receive(
            generation,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Ok(json!([])),
            },
        )
        .unwrap();
    }
    task.await.unwrap().unwrap();
    assert!(host.is_ready());
    receiver
}
#[tokio::test]
async fn reconnect_fences_stale_routes_and_requires_reconciliation() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut first = ready(&host, 1).await;
    let read = {
        let host = host.clone();
        tokio::spawn(async move {
            host.read_text(
                ReadRequest {
                    path: "a".into(),
                    offset_line: 1,
                    max_lines: 1,
                    max_bytes: 20,
                },
                CancellationToken::new(),
            )
            .await
        })
    };
    let request = next_request(&mut first).await;
    host.detach(1);
    assert!(!host.is_ready());
    assert!(read.await.unwrap().is_err());
    let _second = ready(&host, 2).await;
    let HostFrame::Request {
        request_id,
        operation_id,
        ..
    } = request
    else {
        panic!("expected read")
    };
    assert!(
        host.receive(
            1,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Ok(json!(null))
            }
        )
        .is_err()
    );
    host.detach(1);
    assert!(host.is_ready());
    host.detach(2);
}
#[tokio::test]
async fn lost_write_response_is_queried_without_replaying_effect() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let write = {
        let host = host.clone();
        tokio::spawn(async move {
            Host::write(
                host.as_ref(),
                WriteRequest {
                    path: "a".into(),
                    content: "new".into(),
                    precondition: WritePrecondition::Missing,
                },
                CancellationToken::new(),
            )
            .await
        })
    };
    let HostFrame::Request {
        operation_id,
        method,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected write")
    };
    host.detach(1);
    assert!(write.await.unwrap().is_err());
    assert_eq!(host.unresolved_operations(), vec![operation_id.clone()]);
    let (sender, mut receiver) = mpsc::channel(32);
    host.attach_with_lease(sender, 2, "new-lease".into())
        .unwrap();
    assert!(!host.is_ready());
    let reconciliation = {
        let host = host.clone();
        tokio::spawn(async move { host.reconcile().await })
    };
    let HostFrame::Status {
        request_id,
        operation_id: queried,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("mutation replayed instead of queried")
    };
    assert_eq!(queried, operation_id);
    host.receive(
        2,
        HostFrame::Receipt {
            request_id,
            receipt: Some(OperationReceipt {
                operation_id,
                payload_digest: payload_digest(&method).unwrap(),
                scope: scope(),
                state: ReceiptState::Completed(Ok(json!({"version":"hash","bytes":3}))),
            }),
        },
    )
    .unwrap();
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::Recovery,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected crash recovery query")
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
    reconciliation.await.unwrap().unwrap();
    assert!(host.unresolved_operations().is_empty());
    host.detach(2);
}
#[tokio::test]
async fn unsettled_original_worker_fences_execution_but_keeps_route() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let write = {
        let host = host.clone();
        tokio::spawn(async move {
            Host::write(
                host.as_ref(),
                WriteRequest {
                    path: "a".into(),
                    content: "new".into(),
                    precondition: WritePrecondition::Missing,
                },
                CancellationToken::new(),
            )
            .await
        })
    };
    let _ = next_request(&mut receiver).await;
    host.detach(1);
    assert!(write.await.unwrap().is_err());
    let (sender, mut receiver) = mpsc::channel(32);
    host.attach_with_lease(sender, 2, "2".into()).unwrap();
    let reconciliation = {
        let host = host.clone();
        tokio::spawn(async move { host.reconcile().await })
    };
    let HostFrame::Status { request_id, .. } = next_request(&mut receiver).await else {
        panic!("expected status")
    };
    host.receive(
        2,
        HostFrame::Receipt {
            request_id,
            receipt: None,
        },
    )
    .unwrap();
    let HostFrame::Request {
        request_id,
        operation_id,
        method: HostRequest::SettleUnknown { .. },
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected explicit settlement without replay")
    };
    host.receive(
        2,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Err(microcrab_host_protocol::WireError::new(
                microcrab_host_protocol::ErrorCode::OutcomeUnknown,
                "original_worker_active",
            )),
        },
    )
    .unwrap();
    assert!(reconciliation.await.unwrap().is_err());
    assert!(!host.is_ready());
    assert!(host.is_connected());
    assert!(
        host.reconciliation_error().unwrap()["message"]
            .as_str()
            .unwrap()
            .contains("original_worker_active")
    );
    host.detach(2);
}
#[tokio::test]
async fn transfer_header_retired_after_response_validation_cannot_create_orphan() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let cancel = CancellationToken::new();
    let read = {
        let host = host.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            host.read_text(
                ReadRequest {
                    path: "a".into(),
                    offset_line: 1,
                    max_lines: 1,
                    max_bytes: 8,
                },
                cancel,
            )
            .await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected read")
    };
    // Simulate receive's successful initial validation, then cancellation before admission.
    assert!(host.pending.lock().unwrap().contains_key(&request_id));
    cancel.cancel();
    assert!(read.await.unwrap().is_err());
    assert!(host.pending.lock().unwrap().is_empty());
    let reference = microcrab_host_protocol::TransferRef {
        transfer_id: "cancel-gap-transfer".into(),
        byte_length: 0,
        sha256: microcrab_host_protocol::sha256(b""),
        kind: "bytes".into(),
    };
    host.begin_transfer(
        request_id.clone(),
        operation_id.clone(),
        1,
        reference.clone(),
    )
    .unwrap();
    assert!(host.transfers.lock().unwrap().is_empty());
    host.receive(
        1,
        HostFrame::TransferChunk {
            transfer_id: reference.transfer_id.clone(),
            sequence: 0,
            data: vec![],
            eof: true,
        },
    )
    .unwrap();
    assert!(host.is_ready());
    assert!(
        host.begin_transfer(
            "unsolicited".into(),
            operation_id.clone(),
            1,
            reference.clone()
        )
        .is_err()
    );
    assert!(
        host.begin_transfer(
            request_id.clone(),
            "wrong-operation".into(),
            1,
            reference.clone()
        )
        .is_err()
    );
    assert!(
        host.begin_transfer(request_id, operation_id, 2, reference)
            .is_err()
    );
    assert!(
        host.receive(
            1,
            HostFrame::TransferChunk {
                transfer_id: "unsolicited-transfer".into(),
                sequence: 0,
                data: vec![],
                eof: true,
            }
        )
        .is_err()
    );
    assert!(host.transfers.lock().unwrap().is_empty());
    host.detach(1);
}

#[tokio::test]
async fn pending_admission_rejects_detached_and_replaced_route_snapshots() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let (sender, _receiver) = mpsc::channel(8);
    host.attach(sender, 1).unwrap();
    let (_, old_generation, _) = host.route_parts().unwrap();
    host.detach(1);
    let (sender, _) = oneshot::channel();
    assert!(
        host.register_pending(
            "old".into(),
            Pending {
                raw_read: false,
                readiness_operation: false,
                operation_id: "operation".into(),
                generation: old_generation,
                sender,
            }
        )
        .is_err()
    );
    assert!(host.pending.lock().unwrap().is_empty());
    let (sender, _receiver) = mpsc::channel(8);
    host.attach(sender, 2).unwrap();
    let (sender, _) = oneshot::channel();
    assert!(
        host.register_pending(
            "old".into(),
            Pending {
                raw_read: false,
                readiness_operation: false,
                operation_id: "operation".into(),
                generation: old_generation,
                sender,
            }
        )
        .is_err()
    );
    assert!(host.pending.lock().unwrap().is_empty());
    assert!(host.is_connected());
    host.detach(2);
}

#[tokio::test]
async fn cache_points_validate_fresh_content_and_do_not_request_workspace_scans() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    for valid in [false, true] {
        let task = {
            let host = host.clone();
            tokio::spawn(async move {
                host.cache_point(
                    microcrab_host_api::CachePointRequest {
                        path: "selected".into(),
                        max_bytes: 16,
                    },
                    CancellationToken::new(),
                )
                .await
            })
        };
        let HostFrame::Request {
            request_id,
            operation_id,
            method: HostRequest::CachePoint(request),
            ..
        } = next_request(&mut receiver).await
        else {
            panic!("only selected-path capture may be requested")
        };
        assert_eq!(request.path, std::path::PathBuf::from("selected"));
        assert_eq!(request.max_bytes, 16);
        host.receive(1, HostFrame::Response { request_id, operation_id,
            result: Ok(json!({"path":"selected", "data":[97], "version": if valid {microcrab_host_protocol::sha256(b"a")} else {"0".repeat(64)}})),
        }).unwrap();
        let result = task.await.unwrap();
        if valid {
            assert_eq!(result.unwrap().data, Some(vec![97]));
        } else {
            assert!(result.is_err());
        }
        assert!(host.is_ready());
    }
    host.detach(1);
}
