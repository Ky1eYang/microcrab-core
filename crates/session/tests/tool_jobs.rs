//! Real Host resource adapters behind generic Session execution identities.
use crate::execution_fixture;
use execution_fixture::{Fixture, channel, id, sleeping, waiting};
use microcrab_host_api::{Host, ShellRequest};
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_session::{
    Error, Limits,
    execution::process::{ProcessControl, ProcessRegistry},
    runtime::execution::ExecutionStatus,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
#[test]
fn limits_decode_legacy_keys_and_serialize_only_current_owner_names() {
    let mut legacy = serde_json::to_value(Limits::default()).unwrap();
    let fields = legacy.as_object_mut().unwrap();
    for name in [
        "max_pending_executions",
        "max_retained_executions",
        "max_processes",
        "max_execution_buffers",
        "max_retained_processes",
        "output_buffer_bytes",
    ] {
        fields.remove(name);
    }
    fields.insert("max_tool_tasks".into(), json!(3));
    fields.insert("max_closed_tool_tasks".into(), json!(5));
    fields.insert("max_jobs".into(), json!(7));
    fields.insert("max_closed_jobs".into(), json!(11));
    fields.insert("job_output_bytes".into(), json!(12345));
    let decoded: Limits = serde_json::from_value(legacy).unwrap();
    assert_eq!(decoded.max_pending_executions, 3);
    assert_eq!(decoded.max_retained_executions, 5);
    assert_eq!(decoded.max_processes, 7);
    assert_eq!(decoded.max_execution_buffers, 7);
    assert_eq!(decoded.max_retained_processes, 11);
    assert_eq!(decoded.output_buffer_bytes, 12345);

    let mut current = serde_json::to_value(decoded).unwrap();
    for name in [
        "max_tool_tasks",
        "max_closed_tool_tasks",
        "max_jobs",
        "max_closed_jobs",
        "job_output_bytes",
    ] {
        assert!(
            current.get(name).is_none(),
            "retired key serialized: {name}"
        );
    }
    assert_eq!(current["max_pending_executions"], 3);
    assert_eq!(current["max_retained_executions"], 5);
    assert_eq!(current["max_retained_processes"], 11);
    assert_eq!(current["output_buffer_bytes"], 12345);
    current["max_processes"] = json!(2);
    current["max_execution_buffers"] = json!(13);
    let separate: Limits = serde_json::from_value(current).unwrap();
    assert_eq!(separate.max_processes, 2);
    assert_eq!(separate.max_execution_buffers, 13);
}

fn request(command: &str) -> ShellRequest {
    ShellRequest {
        command: command.into(),
        cwd: None,
        output_bytes: ShellLimits::default().max_output_bytes,
    }
}
fn host(root: &TempDir) -> Arc<LocalHost> {
    Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap())
}
fn fixture(root: &TempDir, owner: &str, limits: Limits) -> Arc<Fixture> {
    Arc::new(Fixture::new(owner, host(root), limits))
}
fn failing() -> &'static str {
    #[cfg(windows)]
    {
        "$line = [Console]::ReadLine(); [Console]::Out.WriteLine($line); [Console]::Error.WriteLine('failure detail'); exit 7"
    }
    #[cfg(unix)]
    {
        "IFS= read -r line; printf '%s\\n' \"$line\"; printf 'failure detail\\n' >&2; exit 7"
    }
}

#[tokio::test]
async fn yield_zero_keeps_one_resource_and_repeated_reads_until_explicit_release() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "owner", Limits::default());
    let receipt = f.start(waiting(), 0).await.unwrap();
    assert!(receipt.get("pid").is_none() && receipt.get("job_id").is_none());
    let address = id(&receipt);
    assert!(f.read(address, 0, 65536).unwrap().chunks.is_empty());
    let completed = f.finish(address, "same process").await;
    assert_eq!(completed.status, ExecutionStatus::Succeeded);
    assert_eq!(f.handles.lock().unwrap().len(), 1);
    let first = f.read(address, 0, 65536).unwrap();
    let second = f.read(address, 0, 65536).unwrap();
    assert_eq!(channel(&first, "stdout").trim_end(), "same process");
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(second).unwrap()
    );
    let reread = f
        .session
        .executions()
        .read(completed.execution_id, 0, 65536)
        .unwrap();
    assert_eq!(channel(&reread, "stdout"), channel(&first, "stdout"));
    assert_eq!(f.session.executions().model_projection().len(), 1);
    f.action(address, "release", json!({})).await.unwrap();
    assert!(f.session.executions().model_projection().is_empty());
    assert_eq!(f.session.execution_snapshots().len(), 1);
    assert!(
        f.action(address, "write", json!({"data":"must fail"}))
            .await
            .is_err()
    );
    f.close().await.unwrap();
}

#[tokio::test]
async fn nonzero_process_exit_preserves_both_streams_without_inventing_a_tool_exception() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "failure-owner", Limits::default());
    let receipt = f.start(failing(), 0).await.unwrap();
    let completed = f.finish(id(&receipt), "out detail").await;
    assert_eq!(completed.status, ExecutionStatus::Succeeded);
    assert!(
        completed.failure.is_none(),
        "only an explicit tool failure marks generic execution failed"
    );
    let output = f.read(id(&receipt), 0, 65536).unwrap();
    assert_eq!(channel(&output, "stdout").trim_end(), "out detail");
    assert_eq!(channel(&output, "stderr").trim_end(), "failure detail");
    assert!(output.closed);
    f.action(id(&receipt), "release", json!({})).await.unwrap();
    assert_eq!(
        f.session
            .executions()
            .get(completed.execution_id)
            .unwrap()
            .status,
        ExecutionStatus::Succeeded
    );
    f.close().await.unwrap();
}

#[tokio::test]
async fn terminal_unreleased_resources_hold_capacity_and_released_history_is_evicted_on_admission()
{
    let root = tempfile::tempdir().unwrap();
    let f = fixture(
        &root,
        "bounded",
        Limits {
            max_processes: 1,
            max_execution_buffers: 1,
            max_pending_executions: 1,
            max_retained_executions: 1,
            ..Limits::default()
        },
    );
    let first = f.start(waiting(), 0).await.unwrap();
    f.finish(id(&first), "first").await;
    let rejected = f
        .start(waiting(), 0)
        .await
        .expect_err("unreleased Host resource still owns capacity");
    assert!(
        matches!(&rejected, Error::Tool(_)),
        "capacity rejection must settle as a tool failure before another invocation: {rejected}"
    );
    f.action(id(&first), "release", json!({})).await.unwrap();
    let second = f.start(waiting(), 0).await.unwrap();
    f.finish(id(&second), "second").await;
    f.action(id(&second), "release", json!({})).await.unwrap();
    let third = f.start(waiting(), 0).await.unwrap();
    assert!(
        f.session
            .executions()
            .get(f.id(id(&first)).unwrap())
            .is_err()
    );
    f.session
        .cancel_execution(f.id(id(&third)).unwrap())
        .await
        .unwrap();
    f.close().await.unwrap();
}

#[tokio::test]
async fn builtin_read_retains_terminal_output_and_explicit_release_reuses_buffer_capacity() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(
        &root,
        "explicit-release",
        Limits {
            max_execution_buffers: 1,
            max_processes: 1,
            ..Limits::default()
        },
    );
    for index in 0..3 {
        let receipt = f.start(waiting(), 0).await.unwrap();
        let address = id(&receipt);
        let expected = format!("complete output {index}");
        f.finish(address, &expected).await;
        let observed = f.read(address, 0, 65536).unwrap();
        assert_eq!(channel(&observed, "stdout").trim_end(), expected);
        assert_eq!(f.session.executions().model_projection().len(), 1);

        // Exercise the advertised builtin, including its cleanup acknowledgment,
        // rather than the fixture's direct-registry action adapter.
        let result = f
            .invoke("job", json!({"id":address,"action":"read"}))
            .await
            .unwrap();
        let page: microcrab_session::runtime::execution::OutputPage =
            serde_json::from_str(&result).unwrap();
        assert_eq!(channel(&page, "stdout").trim_end(), expected);
        assert!(page.closed);
        assert_eq!(page.next_cursor, page.end_cursor);
        assert_eq!(
            f.session.executions().model_projection().len(),
            1,
            "closed complete reads retain buffer capacity"
        );
        assert_eq!(
            channel(&f.read(address, 0, 65536).unwrap(), "stdout").trim_end(),
            expected
        );
        f.invoke("job", json!({"id":address,"action":"release"}))
            .await
            .unwrap();
        let mut changed = f.session.executions().subscribe();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !f.session.executions().model_projection().is_empty() {
                changed.changed().await.unwrap();
            }
        })
        .await
        .expect("explicit terminal release must reclaim capacity after Host cleanup");
        let history = f.session.executions().get(f.id(address).unwrap()).unwrap();
        assert_eq!(history.status, ExecutionStatus::Succeeded);
        let output = history.output.unwrap();
        assert_eq!(output.start_cursor, output.end_cursor);
    }
    assert_eq!(f.handles.lock().unwrap().len(), 3);
    f.close().await.unwrap();
}

#[tokio::test]
async fn job_builtin_requires_exact_owner_execution_address_without_pid_or_name_fallback() {
    let root = tempfile::tempdir().unwrap();
    let parent = fixture(&root, "parent", Limits::default());
    let child = fixture(&root, "child", Limits::default());
    let own = parent.start(waiting(), 0).await.unwrap();
    let theirs = child.start(waiting(), 0).await.unwrap();
    let output: Value = serde_json::from_str(
        &parent
            .invoke("job", json!({"id":id(&own),"action":"status"}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(output["execution_id"], own["execution_id"]);
    for address in [
        id(&theirs).to_owned(),
        "fixture_run".into(),
        "child".into(),
        own["execution_id"].to_string(),
        "87654321".into(),
    ] {
        let output: Value = serde_json::from_str(
            &parent
                .invoke("job", json!({"id":address,"action":"status"}))
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(
            output.get("error").is_some(),
            "noncanonical/foreign handles must fail: {output}"
        );
    }
    parent.close().await.unwrap();
    child.close().await.unwrap();
}

#[tokio::test]
async fn child_execution_changes_do_not_fabricate_parent_history_rows() {
    let root = tempfile::tempdir().unwrap();
    let parent = fixture(&root, "parent", Limits::default());
    let child = fixture(&root, "child", Limits::default());
    let receipt = child.start(waiting(), 0).await.unwrap();
    let generation = parent.session.executions().generation();
    child.finish(id(&receipt), "finished").await;
    assert_eq!(parent.session.executions().generation(), generation);
    assert!(parent.session.execution_snapshots().is_empty());
    assert_eq!(child.session.execution_snapshots().len(), 1);
    parent.close().await.unwrap();
    child.close().await.unwrap();
}

#[tokio::test]
async fn interrupt_keeps_buffered_work_alive_and_close_reaps_only_owned_resources() {
    let root = tempfile::tempdir().unwrap();
    let shared = host(&root);
    let f = Fixture::new("session", shared.clone(), Limits::default());
    let other = Fixture::new("other", shared.clone(), Limits::default());
    let own = f.start(sleeping(), 0).await.unwrap();
    let theirs = other.start(waiting(), 0).await.unwrap();
    f.session.interrupt();
    assert_eq!(
        f.action(id(&own), "status", json!({})).await.unwrap()["status"],
        "running"
    );
    f.close().await.unwrap();
    assert!(!f.session.executions().has_pending());
    assert!(f.start(waiting(), 0).await.is_err());
    assert_eq!(
        other
            .action(id(&theirs), "status", json!({}))
            .await
            .unwrap()["status"],
        "running"
    );
    other.close().await.unwrap();
    std::fs::write(root.path().join("host-readable.txt"), b"available").unwrap();
    assert_eq!(
        shared
            .read_file("host-readable.txt".into(), 64, CancellationToken::new())
            .await
            .unwrap(),
        b"available"
    );
}

#[tokio::test]
async fn execution_projection_is_builtin_metadata_and_never_a_custom_variable() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "projection", Limits::default());
    let state = f.session.state_handle();
    assert!(
        state
            .inject("executions", json!([{"forged":true}]))
            .await
            .is_err()
    );
    state.inject("custom", json!(17)).await.unwrap();
    let receipt = f.start(waiting(), 0).await.unwrap();
    f.session.submit("refresh executions").await.unwrap();
    assert_eq!(state.variables(), json!({"custom":17}));
    assert!(state.extract("executions").is_none());
    let view = f.session.state_view().unwrap();
    assert_eq!(
        view["executions"][0]["execution_id"],
        receipt["execution_id"]
    );
    let durable = f.session.checkpoint().await.unwrap();
    assert_eq!(durable.state.executions.len(), 1);
    let frames = serde_json::to_string(&f.session.messages()).unwrap();
    assert!(frames.contains("executions"));
    assert!(!frames.contains("\"pid\""));
    f.close().await.unwrap();
}

#[tokio::test]
async fn shared_capture_budget_reports_expired_byte_cursor_without_failing_execution() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(
        &root,
        "capture",
        Limits {
            output_buffer_bytes: 64,
            ..Limits::default()
        },
    );
    let script = if cfg!(windows) {
        "$null = [Console]::ReadLine(); for ($i = 0; $i -lt 100; $i++) { [Console]::Out.WriteLine('stdout-0123456789'); [Console]::Error.WriteLine('stderr-0123456789') }"
    } else {
        "IFS= read -r line; i=0; while [ \"$i\" -lt 100 ]; do printf 'stdout-0123456789\\n'; printf 'stderr-0123456789\\n' >&2; i=$((i+1)); done"
    };
    let receipt = f.start(script, 0).await.unwrap();
    let completed = f.finish(id(&receipt), "go").await;
    assert_eq!(completed.status, ExecutionStatus::Succeeded);
    let metadata = completed.output.unwrap();
    assert!(metadata.end_cursor - metadata.start_cursor <= 64);

    let output = f.read(id(&receipt), 0, 65536).unwrap();

    assert!(
        output.start_cursor > 0
            || output
                .chunks
                .iter()
                .any(|chunk| chunk.channel.is_none() && chunk.content.contains("[upstream lost]")),
        "Host truncation remains explicit even if before Runtime cursor origin"
    );
    assert!(
        f.read(id(&receipt), output.next_cursor, 65536)
            .unwrap()
            .chunks
            .is_empty()
    );
    f.close().await.unwrap();
}

#[tokio::test]
async fn cancellation_confirms_terminal_state_and_keeps_bounded_tombstone_readable() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "cancel", Limits::default());
    let receipt = f.start(sleeping(), 0).await.unwrap();
    f.session
        .cancel_execution(f.id(id(&receipt)).unwrap())
        .await
        .unwrap();
    let cancelled = f.terminal(id(&receipt)).await;
    assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
    assert!(!f.session.executions().has_pending());
    assert!(f.read(id(&receipt), 0, 65536).unwrap().closed);
    f.action(id(&receipt), "release", json!({})).await.unwrap();
    assert_eq!(
        f.session
            .executions()
            .get(cancelled.execution_id)
            .unwrap()
            .status,
        ExecutionStatus::Cancelled
    );
    f.close().await.unwrap();
}
async fn ready(root: &TempDir) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !root.path().join("pending-start-ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("foreground process must start before exercising close");
}
fn pending_script() -> &'static str {
    #[cfg(windows)]
    {
        "Set-Content -Path pending-start-ready -Value yes; Start-Sleep -Seconds 30"
    }
    #[cfg(unix)]
    {
        "printf yes > pending-start-ready; sleep 30"
    }
}

#[tokio::test]
async fn close_cancels_and_reaps_a_tool_still_observing_its_own_startup_window() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "pending", Limits::default());
    let running = f.clone();
    let start = tokio::spawn(async move { running.start(pending_script(), 60000).await });
    ready(&root).await;
    tokio::time::timeout(Duration::from_secs(10), f.close())
        .await
        .unwrap()
        .unwrap();
    assert!(start.await.unwrap().is_err());
    assert!(!f.session.executions().has_pending());
}
#[tokio::test]
async fn dropping_a_start_caller_still_acknowledges_cleanup_before_close_returns() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "dropped", Limits::default());
    let running = f.clone();
    let start = tokio::spawn(async move { running.start(pending_script(), 60000).await });
    ready(&root).await;
    start.abort();
    assert!(start.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(10), f.close())
        .await
        .unwrap()
        .unwrap();
    assert!(!f.session.executions().has_pending());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_reciprocal_child_registration_cannot_create_a_cycle() {
    let a = ProcessRegistry::detached(Limits::default());
    let b = ProcessRegistry::new("other".into(), None, Limits::default());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let register = |parent: Arc<ProcessRegistry>,
                    child: Arc<ProcessRegistry>,
                    barrier: Arc<tokio::sync::Barrier>| {
        tokio::spawn(async move {
            barrier.wait().await;
            parent.register_child(child)
        })
    };
    let first = register(a.clone(), b.clone(), barrier.clone());
    let second = register(b.clone(), a.clone(), barrier);
    let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
        (first.await.unwrap(), second.await.unwrap())
    })
    .await
    .unwrap();
    assert_ne!(
        first.is_ok(),
        second.is_ok(),
        "exactly one edge may be attached"
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn child_attachment_rejects_multiple_parents_and_duplicate_nested_owner_ids() {
    let root = ProcessRegistry::new("root".into(), None, Limits::default());
    let other = ProcessRegistry::new("other".into(), None, Limits::default());
    let child = ProcessRegistry::new("child".into(), None, Limits::default());
    root.register_child(child.clone()).unwrap();
    assert!(other.register_child(child.clone()).is_err());
    let duplicate = ProcessRegistry::new("root".into(), None, Limits::default());
    assert!(child.register_child(duplicate.clone()).is_err());
    root.close().await.unwrap();
    other.close().await.unwrap();
    duplicate.close().await.unwrap();
}

#[tokio::test]
async fn process_admission_uses_process_capacity_independently_of_execution_buffers() {
    let root = tempfile::tempdir().unwrap();
    let processes = ProcessRegistry::new(
        "independent-capacity".into(),
        Some(host(&root)),
        Limits {
            max_processes: 2,
            max_execution_buffers: 1,
            ..Limits::default()
        },
    );
    for call in ["first", "second"] {
        processes
            .launch_shell(
                "fixture",
                call,
                request(waiting()),
                CancellationToken::new(),
            )
            .await
            .unwrap();
    }
    let rejected = processes
        .launch_shell(
            "fixture",
            "third",
            request(waiting()),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(rejected, Error::Tool(message) if message.contains("process capacity")));
    assert_eq!(
        processes.projection().unwrap().as_object().unwrap().len(),
        2
    );
    processes.close().await.unwrap();
}

#[tokio::test]
async fn parent_capture_reservation_cannot_be_smaller_than_its_childs_capture() {
    let parent = ProcessRegistry::new(
        "parent".into(),
        None,
        Limits {
            output_buffer_bytes: 64,
            ..Limits::default()
        },
    );
    let child = ProcessRegistry::new(
        "child".into(),
        None,
        Limits {
            output_buffer_bytes: 128,
            ..Limits::default()
        },
    );
    let error = parent.register_child(child.clone()).unwrap_err();
    assert!(matches!(error, Error::Config(message) if message.contains("capture budget")));
    assert!(!parent.has_unclosed());
    parent.close().await.unwrap();
    child.close().await.unwrap();
}

#[tokio::test]
async fn context_preflight_rejects_release_before_discarding_retained_output() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(
        &root,
        "preflight",
        Limits {
            max_context_bytes: 64 * 1024,
            max_tool_output_bytes: 1024,
            ..Limits::default()
        },
    );
    let receipt = f.start(waiting(), 0).await.unwrap();
    f.finish(id(&receipt), "preserved output").await;
    let before = f.read(id(&receipt), 0, 65536).unwrap();
    f.session
        .state_handle()
        .inject("context_pressure", json!("x".repeat(63 * 1024)))
        .await
        .unwrap();
    assert!(matches!(
        f.invoke("job", json!({"id":id(&receipt),"action":"release"}))
            .await,
        Err(Error::Limit(_))
    ));
    let after = f.read(id(&receipt), 0, 65536).unwrap();
    assert_eq!(
        after
            .chunks
            .iter()
            .map(|c| c.content.as_str())
            .collect::<String>(),
        before
            .chunks
            .iter()
            .map(|c| c.content.as_str())
            .collect::<String>()
    );
    assert_eq!(after.cursor, before.cursor);
    f.close().await.unwrap();
}

#[tokio::test]
async fn owned_resource_cleanup_failure_is_reported_without_skipping_other_resources() {
    use microcrab_host_api::{
        ErrorCategory, HostError, OpContext, ProcessApi, ShellApi, ShellCaptureStatus, ShellOutput,
        ShellPosition,
        process::{ProcessHandle, ProcessStatus},
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    };
    struct FailingCleanup {
        inner: Arc<LocalHost>,
        fail: AtomicU64,
        killed: Mutex<Vec<u64>>,
        released: Mutex<Vec<u64>>,
    }
    #[async_trait::async_trait]
    impl ProcessApi for FailingCleanup {
        fn watch(
            &self,
            handle: ProcessHandle,
        ) -> Result<tokio::sync::watch::Receiver<u64>, HostError> {
            ProcessApi::watch(self.inner.as_ref(), handle)
        }
        async fn status(
            &self,
            ctx: &OpContext,
            handle: ProcessHandle,
        ) -> Result<ProcessStatus, HostError> {
            ProcessApi::status(self.inner.as_ref(), ctx, handle).await
        }
        async fn kill_tree(
            &self,
            ctx: &OpContext,
            handle: ProcessHandle,
            force: bool,
        ) -> Result<(), HostError> {
            ProcessApi::kill_tree(self.inner.as_ref(), ctx, handle, force).await?;
            self.killed.lock().unwrap().push(handle.0.0);
            Ok(())
        }
        async fn release(&self, ctx: &OpContext, handle: ProcessHandle) -> Result<(), HostError> {
            self.released.lock().unwrap().push(handle.0.0);
            if handle.0.0 == self.fail.load(Ordering::Acquire) {
                // Withhold release acknowledgement and retain the actual Host
                // handle, so registry ownership must survive this failed attempt.
                let mut error = HostError::new(
                    ErrorCategory::Io,
                    "injected process release failure; token=cleanup-token-secret; response_body=cleanup-response-secret",
                );
                error.retryable = true;
                Err(error)
            } else {
                ProcessApi::release(self.inner.as_ref(), ctx, handle).await
            }
        }
    }
    #[async_trait::async_trait]
    impl ShellApi for FailingCleanup {
        async fn launch(
            &self,
            ctx: &OpContext,
            request: ShellRequest,
        ) -> Result<ProcessHandle, HostError> {
            let fail = request.command.contains("pending-start-ready");
            let handle = ShellApi::launch(self.inner.as_ref(), ctx, request).await?;
            if fail {
                self.fail.store(handle.0.0, Ordering::Release);
            }
            Ok(handle)
        }
        async fn capture_status(
            &self,
            ctx: &OpContext,
            handle: ProcessHandle,
        ) -> Result<ShellCaptureStatus, HostError> {
            ShellApi::capture_status(self.inner.as_ref(), ctx, handle).await
        }
        async fn read(
            &self,
            ctx: &OpContext,
            handle: ProcessHandle,
            from: Option<ShellPosition>,
        ) -> Result<ShellOutput, HostError> {
            ShellApi::read(self.inner.as_ref(), ctx, handle, from).await
        }
    }
    impl microcrab_host_api::FsApi for FailingCleanup {}
    impl microcrab_host_api::NetApi for FailingCleanup {}
    impl microcrab_host_api::HostAPI for FailingCleanup {}
    impl Host for FailingCleanup {}
    let root = tempfile::tempdir().unwrap();
    let local = host(&root);
    let injected = Arc::new(FailingCleanup {
        inner: local.clone(),
        fail: AtomicU64::new(0),
        killed: Mutex::new(vec![]),
        released: Mutex::new(vec![]),
    });
    let processes = ProcessRegistry::new(
        "cleanup-failure".into(),
        Some(injected.clone()),
        Limits::default(),
    );
    processes
        .launch_shell(
            "fixture",
            "first",
            request(sleeping()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    processes
        .launch_shell(
            "fixture",
            "second",
            request(sleeping()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    processes
        .launch_shell(
            "fixture",
            "third",
            request(pending_script()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    ready(&root).await;
    let closed = tokio::time::timeout(Duration::from_secs(10), processes.close())
        .await
        .unwrap();
    let error = closed.expect_err("the exact withheld Host release ACK must fail close");
    assert!(matches!(&error, Error::Diagnostic(_)));
    let diagnostic = error.tool_diagnostic();
    use microcrab_session::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticPhase,
    };
    assert_eq!(diagnostic.code, DiagnosticCode::Io);
    assert_eq!(diagnostic.layer, DiagnosticLayer::Host);
    assert_eq!(diagnostic.phase, DiagnosticPhase::HostOperation);
    assert_eq!(diagnostic.retryable, Some(true));
    let reason = diagnostic.reason.as_ref().unwrap().as_str();
    assert!(reason.contains("injected process release failure"));
    let public = serde_json::to_string(&diagnostic).unwrap();
    for secret in ["cleanup-token-secret", "cleanup-response-secret"] {
        assert!(!public.contains(secret));
        assert!(!diagnostic.summary().contains(secret));
    }
    let repeated = processes
        .close()
        .await
        .expect_err("repeat close must retain the failed outcome");
    assert_eq!(repeated.tool_diagnostic(), diagnostic);
    let killed = injected.killed.lock().unwrap().clone();
    let released = injected.released.lock().unwrap().clone();
    assert_eq!(
        killed.len(),
        3,
        "cleanup must attempt the two other processes: {killed:?}"
    );
    assert_eq!(
        released.len(),
        3,
        "release failure must not skip other handle cleanup: {released:?}"
    );
    for id in &killed {
        assert!(released.contains(id));
        let handle = ProcessHandle(microcrab_host_api::HandleId(*id));
        assert_eq!(
            processes.owns_process(handle),
            *id == injected.fail.load(Ordering::Acquire),
            "only the failed release must retain registry ownership"
        );
    }
    let retained = ProcessHandle(microcrab_host_api::HandleId(
        injected.fail.load(Ordering::Acquire),
    ));
    ProcessApi::status(local.as_ref(), &OpContext::default(), retained)
        .await
        .expect("failed release must retain the actual Host handle for retry");
    // Sealing blocks admission, not cleanup. The caller can retry a retained
    // release without losing the root close's stable failure outcome.
    injected.fail.store(0, Ordering::Release);
    processes
        .release_process(retained, CancellationToken::new())
        .await
        .unwrap();
    assert!(!processes.owns_process(retained));
    assert!(
        ProcessApi::status(local.as_ref(), &OpContext::default(), retained)
            .await
            .is_err()
    );
    assert_eq!(
        processes
            .close()
            .await
            .expect_err("manual handle cleanup cannot rewrite sticky root close failure")
            .tool_diagnostic(),
        diagnostic
    );
    assert_eq!(
        injected.released.lock().unwrap().len(),
        4,
        "only the explicit retained-handle retry adds a release attempt"
    );
    assert!(
        !local
            .read_file("pending-start-ready".into(), 64, CancellationToken::new())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn current_state_removes_each_released_buffer_then_final_key_and_replays_exactly() {
    use microcrab_session::context::ContentItem;
    use microcrab_transport::{StateFrame, apply_state_patch};
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "state-removal", Limits::default());
    let first = f.start(waiting(), 0).await.unwrap();
    let second = f.start(waiting(), 0).await.unwrap();
    f.finish(id(&first), "retained first").await;
    f.finish(id(&second), "retained second").await;
    f.session.submit("observe terminal buffers").await.unwrap();
    let before = f.session.messages();
    let readable = f.session.state_view().unwrap();
    assert_eq!(readable["executions"].as_array().unwrap().len(), 2);
    assert_eq!(f.session.state().executions.len(), 2);
    assert!(
        f.session
            .state()
            .executions
            .iter()
            .all(|row| row.result.is_none())
    );
    // Reading all terminal bytes never releases the owner's retained output.
    assert!(f.read(id(&first), 0, 65536).unwrap().closed);
    assert_eq!(f.session.state().executions.len(), 2);
    f.invoke("job", json!({"id":id(&first),"action":"release"}))
        .await
        .unwrap();
    let after_first = f.session.state_view().unwrap();
    assert_eq!(after_first["executions"].as_array().unwrap().len(), 1);
    assert_eq!(
        after_first["executions"][0]["execution_id"],
        second["execution_id"]
    );
    assert!(f.session.messages().iter().any(|item| matches!(item,
        ContentItem::State(StateFrame::StatePatch { execution_delta: Some(delta), .. })
        if delta["remove"].as_array().unwrap().iter().any(|row| row["execution_id"] == first["execution_id"])
    )));
    f.invoke("job", json!({"id":id(&second),"action":"release"}))
        .await
        .unwrap();
    assert!(f.session.state_view().unwrap().get("executions").is_none());
    assert!(f.session.state().executions.is_empty());
    assert!(
        f.session.execution_snapshots().len() >= 4,
        "retained history includes release calls"
    );
    let after = f.session.messages();
    assert_eq!(
        serde_json::to_value(&after[..before.len()]).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    assert!(after.iter().any(|item| matches!(item,
        ContentItem::State(StateFrame::StatePatch { remove, .. }) if remove.iter().any(|key| key == "executions")
    )));
    let mut replayed = json!({});
    for item in &after {
        if let ContentItem::State(frame) = item {
            apply_state_patch(&mut replayed, frame).unwrap();
        }
    }
    assert!(replayed.get("executions").is_none());
    let frozen = serde_json::to_value(f.session.context_snapshot().unwrap()).unwrap();
    let restored =
        microcrab_session::context::Context::from_snapshot(f.session.context_snapshot().unwrap())
            .unwrap();
    assert_eq!(
        serde_json::to_value(restored.snapshot().unwrap()).unwrap(),
        frozen
    );
    f.close().await.unwrap();
}
