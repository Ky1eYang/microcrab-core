//! Real process/shell interoperability through one native owner.
use microcrab_host_api::{
    BoxStream, Bytes, ErrorCategory, HostAPI, OpContext, ProcessApi, ShellApi, ShellPosition,
    ShellRequest,
    process::{ProcessHandle, ProcessState},
};
use microcrab_host_local::{LocalHost, ShellLimits};
use std::time::Duration;

fn request(command: &str, output_bytes: usize) -> ShellRequest {
    ShellRequest {
        command: command.into(),
        cwd: None,
        output_bytes,
    }
}
fn sleeping() -> &'static str {
    #[cfg(windows)]
    {
        "Start-Sleep -Seconds 30"
    }
    #[cfg(unix)]
    {
        "sleep 30"
    }
}
async fn collect(mut stream: BoxStream<Bytes>) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(chunk) = std::future::poll_fn(|ctx| stream.as_mut().poll_next(ctx)).await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    bytes
}
async fn settled(host: &LocalHost, handle: ProcessHandle) {
    tokio::time::timeout(
        Duration::from_secs(10),
        ProcessApi::wait(host, &OpContext::default(), handle),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn raw_process_exec_and_take_once_pipes_are_implemented() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    assert!(host.supports("process.spawn"));
    assert!(host.supports("shell.launch"));
    assert!(!host.capabilities().process_pty);
    assert!(!host.supports("process.resize-pty"));
    #[cfg(windows)]
    let command = "[Console]::Out.Write('raw'); [Console]::Error.Write('error'); exit 7";
    #[cfg(unix)]
    let command = "printf raw; printf error >&2; exit 7";
    let handle = ProcessApi::spawn(&host, &ctx, ProcessApi::shell_spec(&host, command).unwrap())
        .await
        .unwrap();
    let pid = ProcessApi::pid(&host, &ctx, handle).await.unwrap();
    assert!(pid > 0);
    assert!(handle.0.0 > 0 && handle.0.0 < (1 << 53));
    let stdout = ProcessApi::stdout(&host, handle).unwrap();
    let stderr = ProcessApi::stderr(&host, handle).unwrap();
    assert_eq!(
        ProcessApi::stdout(&host, handle).err().unwrap().category,
        ErrorCategory::Conflict
    );
    assert_eq!(
        ShellApi::capture_status(&host, &ctx, handle)
            .await
            .unwrap_err()
            .category,
        ErrorCategory::Conflict
    );
    ProcessApi::close_stdin(&host, &ctx, handle).await.unwrap();
    let (stdout, stderr, status) = tokio::join!(
        collect(stdout),
        collect(stderr),
        ProcessApi::wait(&host, &ctx, handle)
    );
    assert_eq!(stdout, b"raw");
    assert_eq!(stderr, b"error");
    assert_eq!(status.unwrap().code, Some(7));
    assert_eq!(ProcessApi::pid(&host, &ctx, handle).await.unwrap(), pid);
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
    assert_eq!(
        ProcessApi::status(&host, &ctx, handle)
            .await
            .unwrap_err()
            .category,
        ErrorCategory::NotFound
    );
    let output = ProcessApi::exec(&host, &ctx, command).await.unwrap();
    assert_eq!(output.stdout, &b"raw"[..]);
    assert_eq!(output.stderr, &b"error"[..]);
    assert_eq!(output.status.code, Some(7));
}

#[tokio::test]
async fn shell_pid_is_the_same_process_and_capture_owns_both_pipes() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    #[cfg(windows)]
    let command = "[Console]::Out.Write($PID)";
    #[cfg(unix)]
    let command = "printf '%s' $$";
    let handle = ShellApi::launch(&host, &ctx, request(command, 1024))
        .await
        .unwrap();
    let pid = ProcessApi::pid(&host, &ctx, handle).await.unwrap();
    assert_eq!(
        ProcessApi::stdout(&host, handle).err().unwrap().category,
        ErrorCategory::Conflict
    );
    assert_eq!(
        ProcessApi::stderr(&host, handle).err().unwrap().category,
        ErrorCategory::Conflict
    );
    settled(&host, handle).await;
    let first = ShellApi::read(&host, &ctx, handle, None).await.unwrap();
    assert_eq!(first.stdout.parse::<u32>().unwrap(), pid);
    let second = ShellApi::read(&host, &ctx, handle, None).await.unwrap();
    assert_eq!(first.stdout, second.stdout);
    let mut changes = ProcessApi::watch(&host, handle).unwrap();
    changes.borrow_and_update();
    ShellApi::drain(&host, &ctx, handle, 1024).await.unwrap();
    assert!(changes.has_changed().unwrap());
    assert!(
        ShellApi::capture_status(&host, &ctx, handle)
            .await
            .unwrap()
            .closed
    );
    assert!(
        ShellApi::read(&host, &ctx, handle, None)
            .await
            .unwrap()
            .stdout
            .is_empty()
    );
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
}

#[tokio::test]
async fn stdin_and_eof_work_through_both_ports() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    #[cfg(windows)]
    let command = "[Console]::Out.Write([Console]::In.ReadToEnd())";
    #[cfg(unix)]
    let command = "cat";
    let raw = ProcessApi::spawn(&host, &ctx, ProcessApi::shell_spec(&host, command).unwrap())
        .await
        .unwrap();
    let stdout = ProcessApi::stdout(&host, raw).unwrap();
    ProcessApi::write_stdin(&host, &ctx, raw, Bytes::from_static("écho\n".as_bytes()))
        .await
        .unwrap();
    ProcessApi::close_stdin(&host, &ctx, raw).await.unwrap();
    let (output, status) = tokio::join!(collect(stdout), ProcessApi::wait(&host, &ctx, raw));
    assert_eq!(output, "écho\n".as_bytes());
    assert_eq!(status.unwrap().code, Some(0));
    ProcessApi::release(&host, &ctx, raw).await.unwrap();
    let shell = ShellApi::launch(&host, &ctx, request(command, 1024))
        .await
        .unwrap();
    // Raw stdin is the same pipe used by ShellApi::write.
    ProcessApi::write_stdin(&host, &ctx, shell, Bytes::from_static(b"prefix "))
        .await
        .unwrap();
    ShellApi::write(&host, &ctx, shell, "écho\n".as_bytes().to_vec(), true)
        .await
        .unwrap();
    settled(&host, shell).await;
    assert_eq!(
        ShellApi::read(&host, &ctx, shell, None)
            .await
            .unwrap()
            .stdout,
        "prefix écho\n"
    );
    assert_eq!(
        ShellApi::write(&host, &ctx, shell, Vec::new(), false)
            .await
            .unwrap_err()
            .category,
        ErrorCategory::Conflict
    );
    ProcessApi::release(&host, &ctx, shell).await.unwrap();
}

#[tokio::test]
async fn process_kill_preserves_captured_shell_output_and_wait_cancellation_never_kills() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    #[cfg(windows)]
    let command =
        "[Console]::Out.Write('retained'); [Console]::Out.Flush(); Start-Sleep -Seconds 30";
    #[cfg(unix)]
    let command = "printf retained; sleep 30";
    let handle = ShellApi::launch(&host, &ctx, request(command, 1024))
        .await
        .unwrap();
    // Observation timeouts are caller policy, not an execution timer in Host.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            ProcessApi::wait(&host, &ctx, handle)
        )
        .await
        .is_err()
    );
    let cancelled = OpContext::default();
    cancelled.cancel.cancel();
    assert_eq!(
        ProcessApi::wait(&host, &cancelled, handle)
            .await
            .unwrap_err()
            .category,
        ErrorCategory::Cancelled
    );
    assert!(matches!(
        ProcessApi::status(&host, &ctx, handle).await.unwrap().state,
        ProcessState::Running
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while ShellApi::read(&host, &ctx, handle, None)
            .await
            .unwrap()
            .stdout
            != "retained"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    ProcessApi::kill_tree(&host, &ctx, handle, true)
        .await
        .unwrap();
    assert!(matches!(
        ProcessApi::status(&host, &ctx, handle).await.unwrap().state,
        ProcessState::Exited { .. }
    ));
    assert_eq!(
        ShellApi::drain(&host, &ctx, handle, 1024)
            .await
            .unwrap()
            .stdout,
        "retained"
    );
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
}

#[tokio::test]
async fn receipt_survives_admission_token_cancellation_release_cleans_and_capacity_returns() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(
        root.path(),
        ShellLimits {
            max_concurrent: 1,
            ..ShellLimits::default()
        },
    )
    .unwrap();
    let ctx = OpContext::default();
    let admission = OpContext::default();
    let handle = ShellApi::launch(&host, &admission, request(sleeping(), 128))
        .await
        .unwrap();
    admission.cancel.cancel();
    assert!(ProcessApi::has_running_processes(&host));
    assert!(matches!(
        ProcessApi::status(&host, &ctx, handle).await.unwrap().state,
        ProcessState::Running
    ));
    let pending = tokio::spawn({
        let host = host.clone();
        async move {
            ShellApi::launch(
                &host,
                &OpContext::default(),
                request("echo forbidden > orphan", 128),
            )
            .await
        }
    });
    tokio::task::yield_now().await;
    pending.abort();
    let _ = pending.await;
    // Releasing a running process kills and reaps its tree before returning.
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
    assert!(!ProcessApi::has_running_processes(&host));
    assert!(!root.path().join("orphan").exists());
    let next = ShellApi::launch(&host, &ctx, request("echo capacity", 128))
        .await
        .unwrap();
    settled(&host, next).await;
    ProcessApi::release(&host, &ctx, next).await.unwrap();
}

#[tokio::test]
async fn giant_unterminated_capture_is_bounded_and_nonzero_exit_is_process_status() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    #[cfg(windows)]
    let command = "[Console]::Out.Write(('x' * 10000)); [Console]::Error.Write('problem'); exit 7";
    #[cfg(unix)]
    let command = "printf '%010000d' 0 | tr 0 x; printf problem >&2; exit 7";
    let handle = ShellApi::launch(&host, &ctx, request(command, 64))
        .await
        .unwrap();
    settled(&host, handle).await;
    assert!(
        matches!(ProcessApi::status(&host, &ctx, handle).await.unwrap().state, ProcessState::Exited { status } if status.code == Some(7))
    );
    let capture = ShellApi::capture_status(&host, &ctx, handle).await.unwrap();
    assert!(capture.stdout_bytes + capture.stderr_bytes <= 64);
    assert!(capture.truncated);
    let output = ShellApi::read(
        &host,
        &ctx,
        handle,
        Some(ShellPosition { line: 1, column: 0 }),
    )
    .await
    .unwrap();
    assert!(output.expired.unwrap().through_line > 1);
    ShellApi::drain(&host, &ctx, handle, 1024).await.unwrap();
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
}

#[tokio::test]
async fn dropping_last_host_stops_owned_process_tree() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    #[cfg(windows)]
    let command = "Set-Content -Path ready -Value yes; Start-Sleep -Seconds 1; Set-Content -Path survived -Value yes";
    #[cfg(unix)]
    let command = "touch ready; sleep 1; touch survived";
    let handle = ShellApi::launch(&host, &OpContext::default(), request(command, 128))
        .await
        .unwrap();
    let mut changes = ProcessApi::watch(&host, handle).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.path().join("ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(host);
    tokio::time::timeout(Duration::from_secs(5), changes.changed())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(!root.path().join("survived").exists());
}
