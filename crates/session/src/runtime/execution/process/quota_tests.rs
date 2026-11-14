//! Deterministic shared-registry admission and acknowledged-refund regressions.
use super::*;
use microcrab_host_api::{FsApi, HandleId, HostAPI, NetApi};
use tokio::sync::{Semaphore, watch};

struct QuotaHost {
    next: AtomicU64,
    launch_entered: Semaphore,
    launch_allowed: Semaphore,
    release_entered: Semaphore,
    release_allowed: Semaphore,
    fail_launch: AtomicBool,
    fail_kill: AtomicBool,
    fail_release: AtomicBool,
    fail_release_once: AtomicBool,
    changes: watch::Sender<u64>,
}
impl QuotaHost {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next: AtomicU64::new(1),
            launch_entered: Semaphore::new(0),
            launch_allowed: Semaphore::new(0),
            release_entered: Semaphore::new(0),
            release_allowed: Semaphore::new(0),
            fail_launch: AtomicBool::new(false),
            fail_kill: AtomicBool::new(false),
            fail_release: AtomicBool::new(false),
            fail_release_once: AtomicBool::new(false),
            changes: watch::channel(0).0,
        })
    }
}
#[async_trait]
impl ProcessApi for QuotaHost {
    fn watch(&self, _: ProcessHandle) -> Result<watch::Receiver<u64>, HostError> {
        Ok(self.changes.subscribe())
    }
    async fn status(
        &self,
        _: &OpContext,
        handle: ProcessHandle,
    ) -> Result<microcrab_host_api::process::ProcessStatus, HostError> {
        Ok(microcrab_host_api::process::ProcessStatus {
            pid: Some(handle.0.0 as u32),
            state: ProcessState::Running,
            elapsed_s: 0,
        })
    }
    async fn kill_tree(&self, _: &OpContext, _: ProcessHandle, _: bool) -> Result<(), HostError> {
        if self.fail_kill.load(Ordering::Acquire) {
            Err(HostError::new(ErrorCategory::Io, "injected kill failure"))
        } else {
            Ok(())
        }
    }
    async fn release(&self, _: &OpContext, _: ProcessHandle) -> Result<(), HostError> {
        self.release_entered.add_permits(1);
        self.release_allowed.acquire().await.unwrap().forget();
        if self.fail_release.load(Ordering::Acquire)
            || self.fail_release_once.swap(false, Ordering::AcqRel)
        {
            return Err(HostError::new(
                ErrorCategory::Io,
                "injected release failure",
            ));
        }
        Ok(())
    }
}
#[async_trait]
impl ShellApi for QuotaHost {
    async fn launch(&self, ctx: &OpContext, _: ShellRequest) -> Result<ProcessHandle, HostError> {
        self.launch_entered.add_permits(1);
        tokio::select! {
            _ = ctx.cancel.cancelled() => return Err(HostError::new(ErrorCategory::Cancelled, "cancelled launch")),
            permit = self.launch_allowed.acquire() => permit.unwrap().forget(),
        }
        if self.fail_launch.load(Ordering::Acquire) {
            return Err(HostError::new(ErrorCategory::Io, "injected launch failure"));
        }
        Ok(ProcessHandle(HandleId(
            self.next.fetch_add(1, Ordering::AcqRel),
        )))
    }
    async fn capture_status(
        &self,
        _: &OpContext,
        _: ProcessHandle,
    ) -> Result<ShellCaptureStatus, HostError> {
        Ok(ShellCaptureStatus {
            closed: false,
            cursor: ShellPosition::default(),
            stdout_bytes: 0,
            stderr_bytes: 0,
            truncated: false,
        })
    }
}
impl FsApi for QuotaHost {}
impl NetApi for QuotaHost {}
impl HostAPI for QuotaHost {}
impl Host for QuotaHost {}

fn limits() -> Limits {
    Limits {
        max_processes: 1,
        ..Limits::default()
    }
}
async fn launch(control: Arc<dyn ProcessControl>) -> Result<ProcessHandle, Error> {
    control
        .launch_shell(
            "fixture",
            "call",
            ShellRequest {
                command: "controlled".into(),
                cwd: None,
                output_bytes: 32,
            },
            CancellationToken::new(),
        )
        .await
}
async fn entered(gate: &Semaphore, count: u32) {
    tokio::time::timeout(Duration::from_secs(5), gate.acquire_many(count))
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[tokio::test]
async fn parent_and_two_children_have_independent_exact_inflight_quotas() {
    let host = QuotaHost::new();
    let registry = ProcessRegistry::new("parent".into(), Some(host.clone()), limits());
    for child in ["first", "second"] {
        registry.register_session(child, &limits()).unwrap();
    }
    let controls = [
        registry.scoped("parent".into(), "main".into(), true),
        registry.scoped("first".into(), "child".into(), false),
        registry.scoped("second".into(), "child".into(), false),
    ];
    let mut starts = Vec::new();
    for control in &controls {
        starts.push(tokio::spawn(launch(control.clone())));
    }
    entered(&host.launch_entered, 3).await;
    for control in &controls {
        assert!(
            matches!(launch(control.clone()).await, Err(Error::Tool(message)) if message.contains("process capacity"))
        );
    }
    host.launch_allowed.add_permits(3);
    for start in starts {
        start.await.unwrap().unwrap();
    }
    assert_eq!(registry.local().len(), 3);
    assert_eq!(
        registry
            .contents
            .lock()
            .unwrap()
            .sessions
            .values()
            .map(|budget| budget.occupied)
            .sum::<usize>(),
        3
    );
    host.release_allowed.add_permits(3);
    registry.close_session("first").await.unwrap();
    assert_eq!(registry.local().len(), 2);
    assert!(matches!(
        launch(controls[1].clone()).await,
        Err(Error::Closed)
    ));
    host.launch_allowed.add_permits(1);
    // A saturated sibling stays saturated after the other child closes.
    assert!(matches!(
        launch(controls[2].clone()).await,
        Err(Error::Tool(_))
    ));
    registry.close().await.unwrap();
    assert_eq!(
        registry
            .contents
            .lock()
            .unwrap()
            .sessions
            .values()
            .map(|budget| budget.occupied)
            .sum::<usize>(),
        0
    );
}

#[tokio::test]
async fn releasing_process_keeps_slot_until_ack_and_failed_release_retains_owner() {
    let host = QuotaHost::new();
    let registry = ProcessRegistry::new("parent".into(), Some(host.clone()), limits());
    host.launch_allowed.add_permits(1);
    let handle = launch(registry.clone()).await.unwrap();
    let owner = registry.clone();
    let release = tokio::spawn(async move {
        owner
            .release_process(handle, CancellationToken::new())
            .await
    });
    entered(&host.release_entered, 1).await;
    assert!(matches!(
        launch(registry.clone()).await,
        Err(Error::Tool(_))
    ));
    host.fail_release.store(true, Ordering::Release);
    host.release_allowed.add_permits(1);
    assert!(release.await.unwrap().is_err());
    assert!(registry.owns_process(handle));
    assert_eq!(
        registry.contents.lock().unwrap().sessions["parent"].occupied,
        1
    );
    host.fail_release.store(false, Ordering::Release);
    host.release_allowed.add_permits(1);
    registry
        .release_process(handle, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        registry.contents.lock().unwrap().sessions["parent"].occupied,
        0
    );
    host.launch_allowed.add_permits(1);
    let next = launch(registry.clone()).await.unwrap();
    host.release_allowed.add_permits(1);
    registry
        .release_process(next, CancellationToken::new())
        .await
        .unwrap();
    // A historical failed cleanup remains reportable by the root close contract.
    assert!(registry.close().await.is_err());
}

#[tokio::test]
async fn launch_failure_refunds_and_unregistered_scopes_cannot_create_budgets() {
    let host = QuotaHost::new();
    let registry = ProcessRegistry::new("parent".into(), Some(host.clone()), limits());
    let arbitrary = registry.scoped("arbitrary".into(), "child".into(), false);
    assert!(matches!(launch(arbitrary).await, Err(Error::Denied(_))));
    assert!(
        !registry
            .contents
            .lock()
            .unwrap()
            .sessions
            .contains_key("arbitrary")
    );
    host.fail_launch.store(true, Ordering::Release);
    host.launch_allowed.add_permits(1);
    assert!(launch(registry.clone()).await.is_err());
    assert_eq!(
        registry.contents.lock().unwrap().sessions["parent"].occupied,
        0
    );
    host.fail_launch.store(false, Ordering::Release);
    host.launch_allowed.add_permits(1);
    let handle = launch(registry.clone()).await.unwrap();
    host.release_allowed.add_permits(1);
    registry
        .release_process(handle, CancellationToken::new())
        .await
        .unwrap();
    registry.close().await.unwrap();
}

#[tokio::test]
async fn scoped_close_cancels_only_its_inflight_launch_and_registration_is_immutable() {
    let host = QuotaHost::new();
    let registry = ProcessRegistry::new("parent".into(), Some(host.clone()), limits());
    registry.register_session("child", &limits()).unwrap();
    assert!(
        registry
            .register_session(
                "child",
                &Limits {
                    max_processes: 2,
                    ..limits()
                }
            )
            .is_err()
    );
    let child = registry.scoped("child".into(), "child".into(), false);
    let start = tokio::spawn(launch(child));
    entered(&host.launch_entered, 1).await;
    tokio::time::timeout(Duration::from_secs(5), registry.close_session("child"))
        .await
        .unwrap()
        .unwrap();
    assert!(start.await.unwrap().is_err());
    assert_eq!(
        registry.contents.lock().unwrap().sessions["child"].occupied,
        0
    );
    assert!(matches!(
        registry.register_session("child", &limits()),
        Err(Error::Closed)
    ));
    host.launch_allowed.add_permits(1);
    let parent = launch(registry.clone()).await.unwrap();
    host.release_allowed.add_permits(1);
    registry
        .release_process(parent, CancellationToken::new())
        .await
        .unwrap();
    registry.close().await.unwrap();
}

#[tokio::test]
async fn scoped_close_rejects_a_published_launch_receipt_before_caller_acceptance() {
    let host = QuotaHost::new();
    let registry = ProcessRegistry::new("parent".into(), Some(host.clone()), limits());
    registry.register_session("child", &limits()).unwrap();
    let child = registry.scoped("child".into(), "child".into(), false);
    let mut pending_launch = Box::pin(launch(child));
    // Poll only through spawning the owner. Keep the caller parked even after
    // the worker publishes its receipt so close deterministically wins acceptance.
    assert!(futures_util::poll!(&mut pending_launch).is_pending());
    entered(&host.launch_entered, 1).await;
    host.launch_allowed.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let changed = registry.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if registry.contents.lock().unwrap().sessions["child"].workers == 0 {
                break;
            }
            changed.await;
        }
    })
    .await
    .unwrap();
    assert_eq!(registry.local().len(), 1);
    host.release_allowed.add_permits(1);
    registry.close_session("child").await.unwrap();
    assert!(registry.local().is_empty());
    assert!(matches!(pending_launch.await, Err(Error::Cancelled)));
    assert_eq!(host.release_entered.available_permits(), 1);
    assert_eq!(
        registry.contents.lock().unwrap().sessions["child"].occupied,
        0
    );
    // Child closure never seals the parent or creates a second cleanup attempt.
    host.launch_allowed.add_permits(1);
    let parent = launch(registry.clone()).await.unwrap();
    host.release_allowed.add_permits(1);
    registry
        .release_process(parent, CancellationToken::new())
        .await
        .unwrap();
    registry.close().await.unwrap();
}

#[tokio::test]
async fn acknowledged_release_refunds_pending_cleanup_even_when_preliminary_kill_fails() {
    let host = QuotaHost::new();
    let registry = ProcessRegistry::new("parent".into(), Some(host.clone()), limits());
    host.launch_allowed.add_permits(1);
    let handle = launch(registry.clone()).await.unwrap();
    host.fail_kill.store(true, Ordering::Release);
    // The dropped launch receipt path transfers the same slot into detached
    // pending cleanup. Release, not its best-effort preliminary kill, owns reap.
    registry.abandon_process(handle);
    entered(&host.release_entered, 1).await;
    assert!(matches!(
        launch(registry.clone()).await,
        Err(Error::Tool(_))
    ));
    host.release_allowed.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let done = registry.changed.notified();
            tokio::pin!(done);
            done.as_mut().enable();
            if registry.contents.lock().unwrap().sessions["parent"].occupied == 0 {
                break;
            }
            done.await;
        }
    })
    .await
    .unwrap();
    assert!(registry.pending.lock().unwrap().is_empty());
    assert!(!registry.cleanup_failed.load(Ordering::Acquire));
    registry.register_session("child", &limits()).unwrap();
    host.launch_allowed.add_permits(2);
    launch(registry.clone()).await.unwrap();
    launch(registry.scoped("child".into(), "child".into(), false))
        .await
        .unwrap();
    host.release_allowed.add_permits(2);
    registry.close_session("child").await.unwrap();
    assert_eq!(
        registry.contents.lock().unwrap().sessions["child"].occupied,
        0
    );
    registry.close().await.unwrap();
    assert_eq!(
        registry.contents.lock().unwrap().sessions["parent"].occupied,
        0
    );
}

#[tokio::test]
async fn scoped_close_attempts_other_resources_after_one_release_fails() {
    let host = QuotaHost::new();
    let registry = ProcessRegistry::new("parent".into(), Some(host.clone()), limits());
    registry
        .register_session(
            "child",
            &Limits {
                max_processes: 2,
                ..limits()
            },
        )
        .unwrap();
    let child = registry.scoped("child".into(), "child".into(), false);
    host.launch_allowed.add_permits(3);
    let parent_handle = launch(registry.clone()).await.unwrap();
    launch(child.clone()).await.unwrap();
    launch(child).await.unwrap();
    host.fail_release_once.store(true, Ordering::Release);
    // Two first-pass attempts, one child retry and the final parent release.
    host.release_allowed.add_permits(4);
    assert!(registry.close_session("child").await.is_err());
    assert_eq!(host.release_entered.available_permits(), 2);
    let processes = registry.local();
    assert_eq!(
        processes
            .iter()
            .filter(|process| process.session_id == "child")
            .count(),
        1
    );
    assert!(
        processes
            .iter()
            .any(|process| process.session_id == "parent" && process.handle == parent_handle)
    );
    {
        let contents = registry.contents.lock().unwrap();
        assert_eq!(contents.sessions["child"].occupied, 1);
        assert_eq!(contents.sessions["parent"].occupied, 1);
    }
    registry.close_session("child").await.unwrap();
    assert_eq!(
        registry.contents.lock().unwrap().sessions["child"].occupied,
        0
    );
    assert_eq!(registry.local().len(), 1);
    assert_eq!(
        registry.contents.lock().unwrap().sessions["parent"].occupied,
        1
    );
    // Root close preserves the earlier release failure even after scoped retry
    // succeeds; its error must not prevent the remaining parent resource cleanup.
    assert!(registry.close().await.is_err());
    assert!(registry.local().is_empty());
    assert_eq!(
        registry.contents.lock().unwrap().sessions["parent"].occupied,
        0
    );
    assert_eq!(
        registry.contents.lock().unwrap().sessions["child"].occupied,
        0
    );
    assert!(registry.close().await.is_err());
}

#[tokio::test]
async fn replacement_quiesce_waits_for_ack_and_reopens_only_the_exact_session() {
    let host = QuotaHost::new();
    let registry = ProcessRegistry::new("parent".into(), Some(host.clone()), limits());
    registry.register_session("child", &limits()).unwrap();
    let parent = registry.scoped("parent".into(), "main".into(), true);
    let child = registry.scoped("child".into(), "child".into(), false);
    host.launch_allowed.add_permits(2);
    let old = launch(parent.clone()).await.unwrap();
    let sibling = launch(child.clone()).await.unwrap();
    let mut quiesce = Box::pin(registry.quiesce_session_for_restore("parent"));
    assert!(futures_util::poll!(quiesce.as_mut()).is_pending());
    entered(&host.release_entered, 1).await;
    assert!(parent.owns_process(old));
    assert!(child.owns_process(sibling));
    assert!(matches!(launch(parent.clone()).await, Err(Error::Closed)));
    host.release_allowed.add_permits(1);
    quiesce.await.unwrap();
    assert!(!parent.owns_process(old));
    assert!(child.owns_process(sibling));
    host.launch_allowed.add_permits(1);
    let new = launch(parent.clone()).await.unwrap();
    assert_ne!(new, old);
    host.release_allowed.add_permits(2);
    registry.close().await.unwrap();
}
#[tokio::test]
async fn replacement_quiesce_cancels_reserved_start_and_preserves_failed_cleanup_owner() {
    let host = QuotaHost::new();
    let registry = ProcessRegistry::new("parent".into(), Some(host.clone()), limits());
    let control = registry.scoped("parent".into(), "main".into(), true);
    let start = tokio::spawn(launch(control.clone()));
    entered(&host.launch_entered, 1).await;
    registry
        .quiesce_session_for_restore("parent")
        .await
        .unwrap();
    assert!(start.await.unwrap().is_err());
    host.launch_allowed.add_permits(1);
    let handle = launch(control.clone()).await.unwrap();
    host.fail_release_once.store(true, Ordering::Release);
    host.release_allowed.add_permits(1);
    assert!(
        registry
            .quiesce_session_for_restore("parent")
            .await
            .is_err()
    );
    assert!(control.owns_process(handle));
    host.release_allowed.add_permits(1);
    registry
        .quiesce_session_for_restore("parent")
        .await
        .unwrap();
    assert!(!control.owns_process(handle));
    host.launch_allowed.add_permits(1);
    launch(control).await.unwrap();
    host.release_allowed.add_permits(1);
    registry.reconcile_close().await.unwrap();
}
