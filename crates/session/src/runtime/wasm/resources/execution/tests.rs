//! Process imports preserve ownership, confinement and original admission.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
struct NoHost;
impl host::ProcessApi for NoHost {}
impl host::FsApi for NoHost {}
impl host::NetApi for NoHost {}
impl host::shell::ShellApi for NoHost {}
impl host::HostAPI for NoHost {}
#[async_trait::async_trait]
impl host::Host for NoHost {}

#[test]
fn typed_host_category_and_retryability_survive_process_diagnostic_roundtrip() {
    use host::ErrorCategory as H;
    for category in [
        H::NotFound,
        H::Permission,
        H::Timeout,
        H::Cancelled,
        H::Conflict,
        H::Unsupported,
        H::Io,
    ] {
        for retryable in [false, true] {
            let cause = format!(
                "backend pipe {} became unavailable",
                if retryable { "temporary" } else { "permanent" }
            );
            let mut source = host::HostError::new(
                category,
                format!(
                    "{cause}; token=fixture-token-secret; response_body=fixture-response-secret"
                ),
            );
            source.retryable = retryable;
            let projected =
                session_error(crate::Error::from_host_error(host::ToolError::Host(source)));
            assert_eq!(projected.category, category);
            assert_eq!(projected.retryable, retryable);
            assert!(projected.message.contains(&cause));
            for secret in ["fixture-token-secret", "fixture-response-secret"] {
                assert!(!projected.message.contains(secret));
            }
            let mut wire = host_error(projected.wire);
            wire.authoritative = projected.authoritative;
            assert_eq!(wire.retryable, retryable);
            assert!(wire.message.contains(&cause));
            for secret in ["fixture-token-secret", "fixture-response-secret"] {
                assert!(!wire.message.contains(secret));
            }
            let mut state = super::super::tests::runtime_state(true);
            let result: Result<(), ImportFailure<types::HostError>> = Err(wire);
            assert!(finish(&mut state, result).unwrap().0.is_err());
            assert_eq!(
                state.operation_diagnostic.as_ref().unwrap().0.retryable,
                Some(retryable)
            );
            let reason = state
                .operation_diagnostic
                .as_ref()
                .unwrap()
                .0
                .reason
                .as_ref()
                .unwrap()
                .as_str();
            assert!(reason.contains(&cause));
            for secret in ["fixture-token-secret", "fixture-response-secret"] {
                assert!(!reason.contains(secret));
            }
            if category == H::Cancelled {
                assert!(state.operation_cancelled);
            }
        }
    }
}

#[test]
fn cwd_boundary_precheck_preserves_typed_validation_without_starting_process_work() {
    use crate::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticPhase, EffectKnowledge,
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    for prior_write in [false, true] {
        let mut state = super::super::tests::runtime_state(true);
        state.host = Some(Arc::new(
            microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap(),
        ));
        state.runtime = Some(runtime.handle().clone());
        state.known_write_committed = prior_write;
        let called = Arc::new(AtomicBool::new(false));
        let backend_called = called.clone();
        let result: Result<(), ImportFailure<types::HostError>> = operation(
            &mut state,
            0,
            true,
            vec!["../outside".into()],
            json!({"operation":"shell.launch","command":"must-not-run","cwd":"../outside"}),
            move |_, _, _| {
                backend_called.store(true, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            },
        );
        assert!(matches!(
            result.as_ref().unwrap_err().category,
            types::ErrorCategory::Conflict
        ));
        assert!(!called.load(Ordering::SeqCst));
        assert!(finish(&mut state, result).unwrap().0.is_err());
        let diagnostic = &state.operation_diagnostic.as_ref().unwrap().0;
        assert_eq!(diagnostic.code, DiagnosticCode::PathOutsideRoot);
        assert_eq!(diagnostic.layer, DiagnosticLayer::Host);
        assert_eq!(diagnostic.phase, DiagnosticPhase::Validation);
        assert_eq!(
            diagnostic.operation_effects,
            Some(EffectKnowledge::NotStarted)
        );
        assert_eq!(
            diagnostic.effects,
            if prior_write {
                EffectKnowledge::Committed
            } else {
                EffectKnowledge::Unknown
            }
        );
        assert!(state.confinement_diagnostic.is_some());
        assert!(!diagnostic.result_json().contains("../outside"));
    }
}

#[test]
fn missing_or_rebound_proof_prevents_any_process_work() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for mismatch in ["missing", "call", "session", "tool"] {
        let mut state = super::super::tests::runtime_state(true);
        state.host = Some(Arc::new(NoHost));
        state.runtime = Some(runtime.handle().clone());
        let context = state.tool_context.as_mut().unwrap();
        match mismatch {
            "missing" => context.admission = None,
            "call" => context.call_id.push_str("other"),
            "session" => context.session_id.push_str("other"),
            _ => context.tool.push_str("other"),
        }
        let called = Arc::new(AtomicBool::new(false));
        let observed = called.clone();
        let result: Result<(), ImportFailure<types::HostError>> =
            operation(&mut state, 0, false, vec![], json!({}), move |_, _, _| {
                observed.store(true, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            });
        assert!(result.is_err(), "{mismatch}");
        assert!(!called.load(Ordering::SeqCst));
    }
}

#[test]
fn foreign_process_handle_is_rejected_even_with_valid_invocation_proof() {
    let state = super::super::tests::runtime_state(true);
    let unknown = process::ProcessHandle {
        id: 77,
        pid: Some(99),
    };
    let failure = checked(&state, &unknown).unwrap_err();
    assert!(failure.message.contains("does not belong to this Session"));
}

#[test]
fn process_session_errors_keep_marked_reason_and_omit_private_native_body() {
    let source = crate::Error::tool_with_private(
        "process belongs to another Session",
        "private-native-secret",
    );
    let wire = host_error(session_error(source).wire);
    let mut state = super::super::tests::runtime_state(true);
    assert!(finish::<()>(&mut state, Err(wire)).unwrap().0.is_err());
    let diagnostic = &state.operation_diagnostic.as_ref().unwrap().0;
    assert_eq!(diagnostic.summary(), "process belongs to another Session");
    assert!(!diagnostic.result_json().contains("private-native-secret"));
}
