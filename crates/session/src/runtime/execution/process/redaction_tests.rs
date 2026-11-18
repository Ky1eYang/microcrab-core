//! Actual Host failures cross the process State/watch boundary already masked.
use super::*;
use crate::{Limits, State, runtime::execution::InvocationRedaction};
use async_trait::async_trait;
use microcrab_host_api::{
    ErrorCategory, FsApi, HandleId, Host, HostAPI, HostError, NetApi, OpContext, ProcessApi,
    ShellApi, ShellCaptureStatus, ShellRequest,
    process::{ProcessHandle, ProcessState, ProcessStatus},
};
use std::sync::{Arc, atomic::AtomicBool};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const PRIVATE: &str = "old-value-with-quote\"你好";
struct FailureHost {
    changes: watch::Sender<u64>,
}
#[async_trait]
impl ProcessApi for FailureHost {
    fn watch(&self, _: ProcessHandle) -> Result<watch::Receiver<u64>, HostError> {
        Ok(self.changes.subscribe())
    }
    async fn status(&self, _: &OpContext, _: ProcessHandle) -> Result<ProcessStatus, HostError> {
        Ok(ProcessStatus {
            pid: None,
            state: ProcessState::Failed {
                reason: format!("failed {PRIVATE}"),
            },
            elapsed_s: 7,
        })
    }
    async fn release(&self, _: &OpContext, _: ProcessHandle) -> Result<(), HostError> {
        Err(HostError::new(
            ErrorCategory::Io,
            format!("release failed {PRIVATE}"),
        ))
    }
}
#[async_trait]
impl ShellApi for FailureHost {
    async fn launch(&self, _: &OpContext, _: ShellRequest) -> Result<ProcessHandle, HostError> {
        Ok(ProcessHandle(HandleId(1)))
    }
    async fn capture_status(
        &self,
        _: &OpContext,
        _: ProcessHandle,
    ) -> Result<ShellCaptureStatus, HostError> {
        Ok(ShellCaptureStatus {
            closed: true,
            cursor: ShellPosition::default(),
            stdout_bytes: 0,
            stderr_bytes: 0,
            truncated: false,
        })
    }
}
impl FsApi for FailureHost {}
impl NetApi for FailureHost {}
impl HostAPI for FailureHost {}
impl Host for FailureHost {}

#[tokio::test]
async fn actual_secret_state_masks_initial_process_status_and_release_watch_after_rotation() {
    let (state, _endpoint) = State::new(Limits::default()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    state
        .prepare_secret_write(std::collections::BTreeMap::from([(
            "token".into(),
            PRIVATE.into(),
        )]))
        .await
        .unwrap()
        .commit()
        .unwrap();
    let redaction = InvocationRedaction::from_snapshot(state.secret_snapshot()).unwrap();
    let host = Arc::new(FailureHost {
        changes: watch::channel(0).0,
    });
    let registry = ProcessRegistry::new("owner".into(), Some(host), Limits::default());
    let scoped = registry.scoped("owner".into(), "agent".into(), true);
    let port = scoped.for_invocation(redaction).unwrap();
    assert_eq!(port.session_id(), scoped.session_id());
    let handle = port
        .launch_shell(
            "shell",
            "call",
            ShellRequest {
                command: "controlled".into(),
                cwd: None,
                output_bytes: 1024,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let projection = port.projection().unwrap();
    assert_eq!(projection["1"]["reason"], "failed {{secret:token}}");
    assert_eq!(projection["1"]["agent"], "agent");
    assert!(!projection.to_string().contains(PRIVATE));
    state
        .prepare_secret_write(std::collections::BTreeMap::from([(
            "token".into(),
            "rotated-current-value".into(),
        )]))
        .await
        .unwrap()
        .commit()
        .unwrap();
    let error = port
        .release_process(handle, CancellationToken::new())
        .await
        .unwrap_err();
    let diagnostic = error.tool_diagnostic();
    use crate::observation::diagnostics::{DiagnosticCode, DiagnosticLayer, DiagnosticPhase};
    assert_eq!(diagnostic.code, DiagnosticCode::Io);
    assert_eq!(diagnostic.layer, DiagnosticLayer::Host);
    assert_eq!(diagnostic.phase, DiagnosticPhase::HostOperation);
    assert!(diagnostic.summary().starts_with("release failed "));
    assert_eq!(diagnostic.retryable, Some(false));
    assert!(!diagnostic.result_json().contains("old-value"));
    assert!(!port.projection().unwrap().to_string().contains("old-value"));
    // Failed cleanup does not relinquish the actual process or authority.
    assert!(port.owns_process(handle));
}
