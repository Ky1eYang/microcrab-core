use super::*;
use microcrab_host_protocol::{BridgeDiagnostic, BridgeDiagnosticPhase, ErrorCode};

fn event() -> BridgeDiagnostic {
    BridgeDiagnostic {
        event_id: uuid::Uuid::new_v4().to_string(),
        timestamp_ms: 123,
        bridge_instance_id: "instance-1".into(),
        request_id: Some(uuid::Uuid::new_v4().to_string()),
        operation_id: Some(uuid::Uuid::new_v4().to_string()),
        lease_generation: "lease-1".into(),
        method: Some("shell_launch".into()),
        phase: BridgeDiagnosticPhase::OperationFailed,
        error_code: Some(ErrorCode::Io),
        elapsed_ms: Some(5),
    }
}

#[tokio::test]
async fn diagnostic_frames_are_route_fenced_safe_and_bounded() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let _receiver = ready(&host, 1).await;
    let diagnostic = event();
    let encoded = serde_json::to_value(HostFrame::Diagnostic {
        event: diagnostic.clone(),
    })
    .unwrap();
    let frame: HostFrame = serde_json::from_value(encoded.clone()).unwrap();
    host.receive(1, frame).unwrap();
    assert_eq!(
        host.diagnostics(64)[0]["event"]["event_id"],
        diagnostic.event_id
    );
    assert!(
        host.is_ready(),
        "evidence must not alter execution readiness"
    );

    let mut extra = encoded;
    extra["event"]["message"] = json!("private native text");
    assert!(serde_json::from_value::<HostFrame>(extra).is_err());
    let mut invalid_events = Vec::new();
    let mut invalid = diagnostic.clone();
    invalid.lease_generation = "lease-0".into();
    invalid_events.push(invalid);
    let mut invalid = diagnostic.clone();
    invalid.bridge_instance_id = "other-instance".into();
    invalid_events.push(invalid);
    let mut invalid = diagnostic.clone();
    invalid.method = Some("private command /path".into());
    invalid_events.push(invalid);
    let mut invalid = diagnostic.clone();
    invalid.request_id = Some("x".repeat(4096));
    invalid_events.push(invalid);
    let mut invalid = diagnostic.clone();
    invalid.event_id = "malformed".into();
    invalid_events.push(invalid);
    for invalid in invalid_events {
        assert!(
            host.receive(1, HostFrame::Diagnostic { event: invalid })
                .is_err()
        );
    }
    assert!(
        host.receive(
            0,
            HostFrame::Diagnostic {
                event: diagnostic.clone()
            }
        )
        .is_err()
    );
    assert_eq!(host.diagnostics(64).len(), 1);
    for _ in 0..300 {
        host.receive(1, HostFrame::Diagnostic { event: event() })
            .unwrap();
    }
    assert_eq!(host.diagnostics.lock().unwrap().len(), 256);
    assert_eq!(host.diagnostics(usize::MAX).len(), 64);
    assert!(host.is_ready());
    host.detach(1);
    let _receiver = ready(&host, 2).await;
    assert!(
        host.receive(
            1,
            HostFrame::Diagnostic {
                event: diagnostic.clone()
            }
        )
        .is_err()
    );
    assert!(
        host.receive(2, HostFrame::Diagnostic { event: diagnostic })
            .is_err()
    );
    host.detach(2);
}

#[tokio::test]
async fn not_ready_rejection_records_generation_without_inventing_admission_identity() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let (outbound, mut receiver) = mpsc::channel(32);
    host.attach_with_lease(outbound, 1, "lease-1".into())
        .unwrap();
    assert!(
        host.call(
            HostRequest::Normalize {
                path: "private-path".into()
            },
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    let diagnostics = host.diagnostics(64);
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0]["reason"], "rejected_not_ready");
    assert_eq!(diagnostics[0]["connection_generation"], "1");
    assert!(diagnostics[0]["request_id"].is_null());
    assert!(diagnostics[0]["operation_id"].is_null());
    assert!(host.unresolved.lock().unwrap().is_empty());
    assert!(
        !serde_json::to_string(&diagnostics)
            .unwrap()
            .contains("private-path")
    );
    while let Ok(frame) = receiver.try_recv() {
        assert!(matches!(frame, HostFrame::Ping { .. }));
    }
    assert!(host.pending.lock().unwrap().is_empty());
    host.detach(1);
}
