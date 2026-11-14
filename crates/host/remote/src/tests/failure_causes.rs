use super::*;
use microcrab_host_api::{ErrorCategory, FailureReason, HostError};
use microcrab_host_protocol::{ErrorCode, HostCause, WireError};

#[test]
fn wire_causes_keep_closed_distinctions_and_sanitize_concrete_details() {
    for error in [
        ToolError::PathOutsideRoot,
        ToolError::CwdOutsideRoot,
        ToolError::Timeout,
        ToolError::OperationDeadline,
        ToolError::WorkspaceConflict,
        ToolError::FileConflict,
        ToolError::FileLimit,
        ToolError::OutputLimit,
    ] {
        let expected = error.public_reason().map(str::to_owned);
        let cause = HostCause::from_public_reason(expected.as_deref().unwrap()).unwrap();
        let wire =
            WireError::new(ErrorCode::Denied, "token=private-host-secret").with_cause(Some(cause));
        let wire: WireError = serde_json::from_str(&serde_json::to_string(&wire).unwrap()).unwrap();
        let result = calls::wire_error(wire);
        let expected = match &result {
            ToolError::PathOutsideRoot | ToolError::CwdOutsideRoot => expected,
            _ => expected.map(|reason| format!("{reason}; token=[redacted]")),
        };
        assert_eq!(result.public_reason(), expected.as_deref());
        let reason = result.public_reason().unwrap();
        assert!(!reason.contains("[redacted]]"));
        let repeated = FailureReason::from(reason);
        assert_eq!(repeated.public_reason(), Some(reason));
        assert!(!format!("{result:?}").contains("private-host-secret"));
    }
    let result = calls::wire_error(WireError::new(
        ErrorCode::InvalidRequest,
        "concrete request failure; token=private-host-secret",
    ));
    assert_eq!(
        result.public_reason(),
        Some("concrete request failure; token=[redacted]")
    );
    assert!(!format!("{result:?}").contains("private-host-secret"));
    assert!(
        serde_json::from_value::<WireError>(
            serde_json::json!({"code":"denied","message":"body","cause":"private-host-secret"})
        )
        .is_err()
    );
}

#[test]
fn receipt_failure_retains_concrete_os_detail_without_duplicate_label_or_secret() {
    for prefix in ["", "cannot persist durable receipt: "] {
        let wire = WireError::new(
            ErrorCode::Io,
            format!(
                "{prefix}No space left on device (os error 28); kind=StorageFull; token=private-host-secret"
            ),
        )
        .with_cause(Some(HostCause::ReceiptStoreUnavailable));
        let wire: WireError = serde_json::from_str(&serde_json::to_string(&wire).unwrap()).unwrap();
        assert_eq!(wire.cause, Some(HostCause::ReceiptStoreUnavailable));
        let result = calls::wire_error(wire);
        let reason = result.public_reason().unwrap();
        assert!(reason.contains("No space left on device (os error 28)"));
        assert!(reason.contains("kind=StorageFull"));
        assert_eq!(reason.matches("cannot persist durable receipt").count(), 1);
        assert!(reason.contains("token=[redacted]"));
        assert!(!reason.contains("private-host-secret"));
        assert!(!format!("{result:?}").contains("private-host-secret"));
        let ToolError::Host(host) = result else {
            panic!("receipt failure must retain Host error category")
        };
        assert_eq!(host.category, ErrorCategory::Io);
    }
    let result = calls::wire_error(WireError::new(ErrorCode::OutcomeUnknown, ""));
    assert_eq!(
        result
            .public_reason()
            .unwrap()
            .matches("do not replay automatically")
            .count(),
        1
    );
}

#[test]
fn native_conversion_keeps_all_closed_distinctions_and_opaque_reasons() {
    for error in [
        ToolError::PathOutsideRoot,
        ToolError::CwdOutsideRoot,
        ToolError::Timeout,
        ToolError::OperationDeadline,
        ToolError::WorkspaceConflict,
        ToolError::FileConflict,
        ToolError::FileLimit,
        ToolError::OutputLimit,
        ToolError::invalid_request("expected process admission"),
        ToolError::operation_limit("bounded capacity reached"),
        ToolError::worker("file worker stopped"),
        ToolError::unsupported("process.spawn"),
    ] {
        let expected = error.public_reason().map(str::to_owned);
        let result = error.into_host_error();
        assert_eq!(result.public_reason, expected);
    }
    let opaque = ToolError::Worker(FailureReason::from("private-host-secret"));
    assert_eq!(
        opaque.into_host_error().public_reason.as_deref(),
        Some("private-host-secret")
    );
    let error = HostError::new(ErrorCategory::Io, "private-host-secret")
        .with_public_reason("file worker stopped");
    let result = ToolError::Host(error).into_host_error();
    assert_eq!(result.public_reason.as_deref(), Some("file worker stopped"));
    assert!(!format!("{result:?}").contains("private-host-secret"));
}

#[tokio::test]
async fn invalid_read_payload_roundtrip_reports_safe_missing_field_and_type() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    for (payload, expected) in [
        (
            serde_json::json!({"private-host-secret": "private-value-secret"}),
            "Host result is missing required field: content",
        ),
        (
            serde_json::json!({"content":false,"start_line":1,"next_line":null,"truncated":false,"version":"private-host-secret"}),
            "Host result expected a string",
        ),
    ] {
        let task = {
            let host = host.clone();
            tokio::spawn(async move {
                host.read_text(
                    microcrab_host_api::ReadRequest {
                        path: "file".into(),
                        offset_line: 1,
                        max_lines: 1,
                        max_bytes: 8,
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
            panic!("expected read request")
        };
        host.receive(
            1,
            HostFrame::Response {
                request_id,
                operation_id,
                result: Ok(payload),
            },
        )
        .unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.public_reason(), Some(expected));
        assert!(!format!("{error:?}").contains("private-host-secret"));
        assert!(host.is_ready());
        assert!(host.unresolved_operations().is_empty());
    }
    host.detach(1);
}

#[test]
fn response_validation_retains_serde_acceptance_and_nested_closed_causes() {
    use microcrab_host_api::{ReadOutput, ShellOutput};
    let value = serde_json::json!({"content":"ok","start_line":1,"next_line":null,"truncated":false,"version":"v"});
    let output: ReadOutput = payload::decode(value).unwrap();
    assert_eq!(output.content, "ok");
    let value = serde_json::json!({"stdout":"","stderr":"","next":{"line":1},"cursor":{"line":1,"column":0},"expired":null,"truncated":false});
    let error = payload::decode::<ShellOutput>(value).unwrap_err();
    assert_eq!(
        error.public_reason(),
        Some("Host result is missing required field: column")
    );
    let error = payload::decode::<microcrab_host_api::process::ProcessState>(
        serde_json::json!({"private-host-secret":{}}),
    )
    .unwrap_err();
    assert!(!format!("{error:?}").contains("private-host-secret"));
    assert!(error.public_reason().is_some());
}

#[tokio::test]
async fn carrier_failure_completes_exact_request_and_retires_partial_transfer() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let call = {
        let host = host.clone();
        tokio::spawn(async move {
            host.call(
                HostRequest::ReadFile {
                    path: "a".into(),
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
        panic!("read required")
    };
    host.fail_request(2, &request_id, &operation_id, "stale generation".into());
    host.fail_request(1, &request_id, "wrong-operation", "wrong invocation".into());
    assert!(host.pending.lock().unwrap().contains_key(&request_id));
    let reference = microcrab_host_protocol::TransferRef {
        transfer_id: "partial-transfer".into(),
        byte_length: 3,
        sha256: microcrab_host_protocol::sha256(b"abc"),
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
    host.fail_request(
        1,
        &request_id,
        &operation_id,
        "gRPC reset after transfer declaration".into(),
    );
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("gRPC reset after transfer declaration")
    );
    assert!(host.pending.lock().unwrap().is_empty());
    assert!(host.transfers.lock().unwrap().is_empty());
    host.receive(
        1,
        HostFrame::TransferChunk {
            transfer_id: "partial-transfer".into(),
            sequence: 0,
            data: b"abc".to_vec(),
            eof: true,
        },
    )
    .unwrap();
    assert!(host.is_ready());
    host.detach(1);
}

#[tokio::test]
async fn carrier_failure_keeps_consumption_uncertain_and_queries_only_original_receipt() {
    use microcrab_host_api::{HandleId, process::ProcessHandle};
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let call = {
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
        panic!("drain required")
    };
    host.fail_request(
        1,
        &request_id,
        &operation_id,
        "gRPC drain stream reset".into(),
    );
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("gRPC drain stream reset"));
    assert!(
        host.is_ready(),
        "uncertainty belongs to one output, not workspace readiness"
    );
    assert_eq!(host.unresolved_operations(), vec![operation_id.clone()]);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            match next_request(&mut receiver).await {
                HostFrame::Cancel {
                    request_id: cancel, ..
                } => assert_eq!(cancel, request_id),
                HostFrame::Status {
                    request_id: status,
                    operation_id: observed,
                    ..
                } => {
                    assert_eq!(observed, operation_id);
                    host.receive(
                        1,
                        HostFrame::Receipt {
                            request_id: status,
                            receipt: None,
                        },
                    )
                    .unwrap();
                    break;
                }
                other => panic!("receipt recovery must never replay the drain: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(host.unresolved_operations(), vec![operation_id]);
    assert!(host.is_ready());
    host.detach(1);
}
