//! Shell capture admission and real interpreter behavior.
use microcrab_host_api::{ErrorCategory, OpContext, ProcessApi, ShellApi, ShellRequest};
use microcrab_host_local::{LocalHost, ShellLimits};
use std::{path::PathBuf, time::Duration};
fn request(command: &str) -> ShellRequest {
    ShellRequest {
        command: command.into(),
        cwd: None,
        output_bytes: 1024,
    }
}

#[tokio::test]
async fn rejects_parent_absolute_and_symlink_cwd_escape() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    for cwd in [PathBuf::from(".."), outside.path().to_path_buf()] {
        let mut req = request("echo must-not-run");
        req.cwd = Some(cwd);
        assert_eq!(
            ShellApi::launch(&host, &ctx, req)
                .await
                .unwrap_err()
                .category,
            ErrorCategory::Permission
        );
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
        let mut req = request("echo must-not-run");
        req.cwd = Some("escape".into());
        assert_eq!(
            ShellApi::launch(&host, &ctx, req)
                .await
                .unwrap_err()
                .category,
            ErrorCategory::Permission
        );
    }
}

#[tokio::test]
async fn validates_command_and_capture_budget_before_admission() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(
        root.path(),
        ShellLimits {
            max_output_bytes: 16,
            max_command_bytes: 256,
            ..Default::default()
        },
    )
    .unwrap();
    let ctx = OpContext::default();
    for script in ["", "nul\0byte"] {
        assert_eq!(
            ShellApi::launch(&host, &ctx, request(script))
                .await
                .unwrap_err()
                .category,
            ErrorCategory::Conflict
        );
    }
    for budget in [0, 17] {
        let mut req = request("echo must-not-run");
        req.output_bytes = budget;
        assert_eq!(
            ShellApi::launch(&host, &ctx, req)
                .await
                .unwrap_err()
                .category,
            ErrorCategory::Conflict
        );
    }
    assert!(!ProcessApi::has_running_processes(&host));
}

#[cfg(windows)]
#[tokio::test]
async fn windows_shell_preserves_unicode_on_both_pipes() {
    let root = tempfile::tempdir().unwrap();
    // A workspace executable with the interpreter basename must never be selected.
    std::fs::write(root.path().join("pwsh.exe"), b"not an interpreter").unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    let script = "[Console]::Out.Write('中文输出'); [Console]::Error.Write('错误详情'); if ([Console]::OutputEncoding.WebName -ne 'utf-8' -or $OutputEncoding.WebName -ne 'utf-8') { exit 9 }";
    let handle = ShellApi::launch(&host, &ctx, request(script))
        .await
        .unwrap();
    assert_eq!(
        ProcessApi::wait(&host, &ctx, handle).await.unwrap().code,
        Some(0)
    );
    let output = ShellApi::drain(&host, &ctx, handle, 1024).await.unwrap();
    assert_eq!(output.stdout, "中文输出");
    assert_eq!(output.stderr, "错误详情");
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
}

#[tokio::test]
async fn bounded_drain_rejection_preserves_capture_and_running_drain_consumes_only_when_it_fits() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    #[cfg(windows)]
    let script = "[Console]::Out.Write('prefix'); [Console]::Out.Flush(); $line = [Console]::In.ReadLine(); [Console]::Out.Write($line)";
    #[cfg(unix)]
    let script = "printf prefix; IFS= read -r line; printf '%s' \"$line\"";
    let handle = ShellApi::launch(&host, &ctx, request(script))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while ShellApi::read(&host, &ctx, handle, None)
            .await
            .unwrap()
            .stdout
            != "prefix"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let before = ShellApi::capture_status(&host, &ctx, handle).await.unwrap();
    assert_eq!(
        ShellApi::drain(&host, &ctx, handle, 5)
            .await
            .unwrap_err()
            .category,
        ErrorCategory::Conflict
    );
    let after = ShellApi::capture_status(&host, &ctx, handle).await.unwrap();
    assert_eq!(after.cursor, before.cursor);
    assert_eq!(after.stdout_bytes, before.stdout_bytes);
    assert_eq!(after.stderr_bytes, before.stderr_bytes);
    assert_eq!(after.closed, before.closed);
    assert_eq!(
        ShellApi::read(&host, &ctx, handle, None)
            .await
            .unwrap()
            .stdout,
        "prefix"
    );
    assert_eq!(
        ShellApi::drain(&host, &ctx, handle, 6)
            .await
            .unwrap()
            .stdout,
        "prefix"
    );
    assert!(
        !ShellApi::capture_status(&host, &ctx, handle)
            .await
            .unwrap()
            .closed
    );
    ShellApi::write(&host, &ctx, handle, b"tail\n".to_vec(), true)
        .await
        .unwrap();
    assert_eq!(
        ProcessApi::wait(&host, &ctx, handle).await.unwrap().code,
        Some(0)
    );
    assert_eq!(
        ShellApi::drain(&host, &ctx, handle, 4)
            .await
            .unwrap()
            .stdout,
        "tail"
    );
    assert!(
        ShellApi::capture_status(&host, &ctx, handle)
            .await
            .unwrap()
            .closed
    );
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
}

#[tokio::test]
async fn aborting_exec_caller_keeps_cleanup_owned_and_releases_execution_capacity() {
    let root = tempfile::tempdir().unwrap();
    let mut limits = ShellLimits::default();
    assert_eq!(limits.max_concurrent, 32);
    limits.max_concurrent = 1;
    let host = LocalHost::new(root.path(), limits).unwrap();
    #[cfg(windows)]
    let script = "Set-Content -Path exec-ready -Value yes; Start-Sleep -Seconds 2; Set-Content -Path exec-forbidden -Value yes; Start-Sleep -Seconds 30";
    #[cfg(unix)]
    let script = "touch exec-ready; sleep 2; touch exec-forbidden; sleep 30";
    let execution = tokio::spawn({
        let host = host.clone();
        async move { ProcessApi::exec(&host, &OpContext::default(), script).await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.path().join("exec-ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    execution.abort();
    assert!(execution.await.unwrap_err().is_cancelled());
    // The first process owned the sole permit. Admission proves its detached
    // cleanup worker terminated the tree and relinquished execution resources.
    let ctx = OpContext::default();
    let next = tokio::time::timeout(
        Duration::from_secs(5),
        ShellApi::launch(&host, &ctx, request("echo capacity")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        ProcessApi::wait(&host, &ctx, next).await.unwrap().code,
        Some(0)
    );
    ProcessApi::release(&host, &ctx, next).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2300)).await;
    assert!(!root.path().join("exec-forbidden").exists());
    assert!(!ProcessApi::has_running_processes(&host));
    assert!(!ProcessApi::has_unresolved_cleanup(&host));
}

#[tokio::test]
async fn process_kill_reaps_descendants_before_delayed_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let host = LocalHost::new(root.path(), ShellLimits::default()).unwrap();
    let ctx = OpContext::default();
    #[cfg(windows)]
    let script = "$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes('Start-Sleep -Seconds 2; Set-Content -Path descendant-survived -Value yes')); $p = Start-Process pwsh -ArgumentList @('-NoProfile','-EncodedCommand',$encoded) -PassThru; Set-Content -Path descendant-ready -Value yes; Wait-Process -Id $p.Id";
    #[cfg(unix)]
    let script = "(sleep 2; touch descendant-survived) & touch descendant-ready; wait";
    let handle = ShellApi::launch(&host, &ctx, request(script))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.path().join("descendant-ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    ProcessApi::kill_tree(&host, &ctx, handle, true)
        .await
        .unwrap();
    assert_eq!(
        ProcessApi::wait(&host, &ctx, handle).await.unwrap().signal,
        Some(microcrab_host_api::process::Signal::Kill)
    );
    tokio::time::sleep(Duration::from_millis(2300)).await;
    assert!(!root.path().join("descendant-survived").exists());
    ProcessApi::release(&host, &ctx, handle).await.unwrap();
}
