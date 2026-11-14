//! Public consumers share the native Process owner with shell capture and Session jobs.
use crate::execution_fixture;
use microcrab_host_api::{
    ErrorCategory, OpContext, ProcessApi, ShellApi, ShellRequest,
    process::{ProcessHandle, ProcessState, Signal},
};
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_session::Limits;
use std::{sync::Arc, time::Duration};

fn echo_input() -> &'static str {
    if cfg!(windows) {
        "$line = [Console]::ReadLine(); [Console]::Out.WriteLine($line); [Console]::Error.Write('stderr-proof'); exit 7"
    } else {
        "IFS= read -r line; printf '%s\\n' \"$line\"; printf 'stderr-proof' >&2; exit 7"
    }
}

async fn capture_closed(host: &LocalHost, handle: ProcessHandle) {
    // Subscribe before inspecting closure; a fast exit cannot lose its notification.
    let mut changes = ProcessApi::watch(host, handle).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            changes.borrow_and_update();
            if ShellApi::capture_status(host, &OpContext::default(), handle)
                .await
                .unwrap()
                .closed
            {
                break;
            }
            changes
                .changed()
                .await
                .expect("capture owner must retain its watch sender");
        }
    })
    .await
    .expect("process exit must close both capture pipes");
}

#[tokio::test]
async fn shell_launch_process_wait_and_capture_share_one_retained_identity() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    let handle = ShellApi::launch(
        &host,
        &ctx,
        ShellRequest {
            command: echo_input().into(),
            cwd: None,
            output_bytes: 4096,
        },
    )
    .await
    .unwrap();
    let status = ProcessApi::status(&host, &ctx, handle).await.unwrap();
    assert!(matches!(status.state, ProcessState::Running));
    assert!(status.pid.is_some_and(|pid| pid > 0));
    assert_eq!(
        Some(ProcessApi::pid(&host, &ctx, handle).await.unwrap()),
        status.pid
    );
    assert_eq!(
        ProcessApi::stdout(&host, handle).err().unwrap().category,
        ErrorCategory::Conflict
    );
    assert_eq!(
        ProcessApi::stderr(&host, handle).err().unwrap().category,
        ErrorCategory::Conflict
    );
    ShellApi::write(
        &host,
        &ctx,
        handle,
        "interoperability-中\n".as_bytes().to_vec(),
        true,
    )
    .await
    .unwrap();
    let exit = tokio::time::timeout(
        Duration::from_secs(15),
        ProcessApi::wait(&host, &ctx, handle),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(exit.code, Some(7));
    assert_eq!(exit.signal, None);
    assert_eq!(ProcessApi::wait(&host, &ctx, handle).await.unwrap(), exit);
    assert!(
        !ShellApi::capture_status(&host, &ctx, handle)
            .await
            .unwrap()
            .closed
    );
    let output = ShellApi::read(&host, &ctx, handle, None).await.unwrap();
    assert_eq!(output.stdout.trim_end(), "interoperability-中");
    assert_eq!(output.stderr, "stderr-proof");
    let repeated = ShellApi::read(&host, &ctx, handle, None).await.unwrap();
    assert_eq!(repeated.stdout, output.stdout);
    assert_eq!(repeated.cursor, output.cursor);
    let drained = ShellApi::drain(&host, &ctx, handle, 4096).await.unwrap();
    assert_eq!(drained.stdout, output.stdout);
    capture_closed(&host, handle).await;
    assert!(
        ShellApi::read(&host, &ctx, handle, None)
            .await
            .unwrap()
            .stdout
            .is_empty()
    );
    assert_eq!(
        Some(ProcessApi::pid(&host, &ctx, handle).await.unwrap()),
        status.pid
    );
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
    assert_eq!(
        ProcessApi::status(&host, &ctx, handle)
            .await
            .err()
            .unwrap()
            .category,
        ErrorCategory::NotFound
    );
}

#[tokio::test]
async fn public_execution_identity_is_independent_of_the_owned_native_handle() {
    let root = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let held = if cfg!(windows) {
        "$null = [Console]::ReadLine()"
    } else {
        "IFS= read -r line"
    };
    // Occupy a Host handle before Session admission to prove distinct namespaces.
    let sentinel = ShellApi::launch(
        local.as_ref(),
        &OpContext::default(),
        ShellRequest {
            command: held.into(),
            cwd: None,
            output_bytes: 4096,
        },
    )
    .await
    .unwrap();
    let fixture = execution_fixture::Fixture::new("same-owner", local.clone(), Limits::default());
    let receipt = fixture.start(held, 0).await.unwrap();
    let public_id = receipt["execution_id"].as_u64().unwrap();
    let handle = fixture.handles.lock().unwrap()[0];
    assert_ne!(public_id, handle.0.0);
    assert!(receipt.get("pid").is_none() && receipt.get("job_id").is_none());
    let pid = ProcessApi::pid(local.as_ref(), &OpContext::default(), handle)
        .await
        .unwrap();
    assert!(pid > 0);
    ProcessApi::signal(local.as_ref(), &OpContext::default(), handle, Signal::Kill)
        .await
        .unwrap();
    let exit = tokio::time::timeout(
        Duration::from_secs(15),
        ProcessApi::wait(local.as_ref(), &OpContext::default(), handle),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(exit.signal, Some(Signal::Kill));
    let completed = fixture.terminal(execution_fixture::id(&receipt)).await;
    assert_eq!(completed.execution_id.0, public_id);
    assert!(!completed.status.is_pending());
    assert!(
        completed.failure.is_none(),
        "Host process state alone is not a tool-reported exception"
    );
    fixture.close().await.unwrap();
    assert_eq!(
        ProcessApi::status(local.as_ref(), &OpContext::default(), handle)
            .await
            .err()
            .unwrap()
            .category,
        ErrorCategory::NotFound
    );
    ProcessApi::release(local.as_ref(), &OpContext::default(), sentinel)
        .await
        .unwrap();
}
