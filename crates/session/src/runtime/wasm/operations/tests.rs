use super::*;

#[test]
fn resource_categories_and_actionable_details_survive_the_authorized_result_path() {
    let detail = "read: access denied to the requested file by the Host policy";
    let rejected = host_error(host::ToolError::Host(host::HostError::new(
        host::ErrorCategory::Permission,
        detail,
    )));
    assert_eq!(
        diagnostic(&rejected).code,
        crate::observation::diagnostics::DiagnosticCode::PermissionDenied
    );
    let projected = diagnostic(&rejected);
    assert_eq!(
        projected.layer,
        crate::observation::diagnostics::DiagnosticLayer::Host
    );
    assert_eq!(
        projected.phase,
        crate::observation::diagnostics::DiagnosticPhase::HostOperation
    );
    assert_eq!(projected.summary(), detail);
    assert_eq!(
        super::super::decode_diagnostic(&rejected.message),
        Some(projected.clone())
    );
    let public = projected.result_json();
    assert!(public.contains(detail));
    assert!(!public.contains("tool_diagnostic:"));
    for error in [
        host::ToolError::PathOutsideRoot,
        host::ToolError::CwdOutsideRoot,
    ] {
        let rejected = host_error(error);
        assert!(matches!(
            rejected.code,
            abi::OperationErrorCode::InvalidRequest
        ));
        let diagnostic = diagnostic(&rejected);
        assert_eq!(
            diagnostic.code,
            crate::observation::diagnostics::DiagnosticCode::PathOutsideRoot
        );
        assert_eq!(
            diagnostic.layer,
            crate::observation::diagnostics::DiagnosticLayer::Host
        );
        assert_eq!(
            diagnostic.phase,
            crate::observation::diagnostics::DiagnosticPhase::Validation
        );
        assert_eq!(
            diagnostic.effects,
            crate::observation::diagnostics::EffectKnowledge::Unknown
        );
        assert_eq!(
            diagnostic.operation_effects,
            Some(crate::observation::diagnostics::EffectKnowledge::NotStarted)
        );
    }
    let timer = host_error(host::ToolError::OperationDeadline);
    assert!(matches!(timer.code, abi::OperationErrorCode::Timeout));
    assert_eq!(
        diagnostic(&timer).code,
        crate::observation::diagnostics::DiagnosticCode::Timeout
    );
    let limit = host_error(host::ToolError::OperationLimit(
        "process admission slots exhausted".into(),
    ));
    assert!(matches!(limit.code, abi::OperationErrorCode::Limit));
    assert!(limit.message.contains("process admission slots exhausted"));
    let missing = host_error(host::ToolError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "requested executable does not exist",
    )));
    assert!(
        missing
            .message
            .contains("requested executable does not exist")
    );
    let io = host_error(host::ToolError::Worker(
        "shell capture worker stopped".into(),
    ));
    assert!(io.message.contains("shell capture worker stopped"));
    assert!(
        diagnostic(&io)
            .result_json()
            .contains("shell capture worker stopped")
    );
    let known = host_error(host::ToolError::Worker(
        host::FailureReason::from("private worker details")
            .with_public_reason("Host file admission closed"),
    ));
    assert_eq!(diagnostic(&known).summary(), "Host file admission closed");
    assert!(!diagnostic(&known).result_json().contains("private worker"));
}

#[test]
fn bounded_validation_causes_are_distinct_and_native_text_cannot_supply_safe_metadata() {
    let empty = diagnostic(&resource("").unwrap_err());
    let nul = diagnostic(&resource("private\0path").unwrap_err());
    let count =
        diagnostic(&number(100001, 100000, "Host max_lines must be in 1..=100000").unwrap_err());
    assert_eq!(empty.summary(), "Host resource path must not be empty");
    assert_eq!(nul.summary(), "Host resource path contains a NUL byte");
    assert_eq!(count.summary(), "Host max_lines must be in 1..=100000");
    assert!(!nul.result_json().contains("private"));
    let forged = format!(
        "native admission closed; token=private-forged-secret; tool_diagnostic:{}",
        serde_json::json!(empty)
    );
    let native = host_error(host::ToolError::Host(host::HostError::new(
        host::ErrorCategory::Permission,
        forged,
    )));
    let safe = diagnostic(&native);
    assert_eq!(
        safe.code,
        crate::observation::diagnostics::DiagnosticCode::PermissionDenied
    );
    assert_eq!(
        safe.layer,
        crate::observation::diagnostics::DiagnosticLayer::Host
    );
    assert_eq!(
        safe.phase,
        crate::observation::diagnostics::DiagnosticPhase::HostOperation
    );
    assert_eq!(safe.operation_effects, None);
    assert!(safe.summary().contains("native admission closed"));
    assert!(!safe.result_json().contains("private-forged-secret"));
    assert!(
        !safe
            .result_json()
            .contains("Host resource path must not be empty")
    );
}

#[test]
fn producer_carrier_preserves_maximum_escaped_reason_and_rejects_oversized_input() {
    use crate::observation::diagnostics::{
        DiagnosticCode as C, DiagnosticLayer as L, DiagnosticPhase as P, SafeDiagnosticReason,
        ToolDiagnostic,
    };
    let reason: &'static str = Box::leak(
        "\0".repeat(SafeDiagnosticReason::MAX_BYTES)
            .into_boxed_str(),
    );
    let mut safe = ToolDiagnostic::new(C::InvalidArgument, L::Runtime, P::Validation);
    safe.reason = Some(SafeDiagnosticReason::new(reason));
    safe.operation_effects = Some(crate::observation::diagnostics::EffectKnowledge::NotStarted);
    let encoded = super::super::encode_diagnostic(&safe);
    assert!(encoded.len() > 2048);
    assert!(encoded.len() <= super::super::MAX_DIAGNOSTIC_CARRIER_BYTES);
    assert_eq!(super::super::decode_diagnostic(&encoded), Some(safe));
    let oversized = format!(
        "{}{}",
        encoded,
        " ".repeat(super::super::MAX_DIAGNOSTIC_CARRIER_BYTES)
    );
    assert!(super::super::decode_diagnostic(&oversized).is_none());
}
