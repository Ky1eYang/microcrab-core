use super::*;
use std::sync::Arc;
use wasmtime::{AsContextMut, Engine, Store};

fn store(
    root: &Path,
    unrestricted: bool,
    runtime: &tokio::runtime::Runtime,
) -> Store<RuntimeState> {
    let mut state = super::super::tests::runtime_state(true);
    state.host = Some(Arc::new(
        microcrab_host_local::LocalHost::new_filesystem_with_scope(
            root,
            Default::default(),
            unrestricted,
        )
        .unwrap(),
    ));
    state.runtime = Some(runtime.handle().clone());
    state.input_bytes_remaining = 1_048_576;
    Store::new(&Engine::default(), state)
}
fn opened(store: &mut Store<RuntimeState>, path: &str) -> u64 {
    open(store.as_context_mut(), (path.into(), fs::OpenFlags::READ))
        .unwrap()
        .0
        .unwrap()
        .id
}
#[test]
fn flags_and_foreign_handles_fail_before_host_access() {
    for flags in [
        fs::OpenFlags::empty(),
        fs::OpenFlags::READ | fs::OpenFlags::WRITE,
        fs::OpenFlags::READ | fs::OpenFlags::CREATE,
        fs::OpenFlags::READ | fs::OpenFlags::TRUNCATE,
        fs::OpenFlags::READ | fs::OpenFlags::APPEND,
        fs::OpenFlags::READ | fs::OpenFlags::EXCLUSIVE,
    ] {
        assert!(matches!(
            read_only(flags).unwrap_err().category,
            types::ErrorCategory::Permission
        ));
    }
    let mut store = Store::new(&Engine::default(), super::super::tests::runtime_state(true));
    assert!(matches!(
        read(store.as_context_mut(), (fs::FileHandle { id: 999 }, 0, 1))
            .unwrap()
            .0
            .unwrap_err()
            .category,
        types::ErrorCategory::Permission
    ));
    assert!(matches!(
        close(store.as_context_mut(), (fs::FileHandle { id: 999 },))
            .unwrap()
            .0
            .unwrap_err()
            .category,
        types::ErrorCategory::Permission
    ));
}
#[test]
fn aggregate_and_small_read_limits_are_separate_from_text_and_reset_safely() {
    let mut budget = Reads::default();
    for index in 0..(crate::admission::image::MAX_IMAGE_BYTES / CHUNK_BYTES) {
        budget
            .reserve_read((index * CHUNK_BYTES) as u64, CHUNK_BYTES)
            .unwrap();
    }
    budget
        .reserve_read(crate::admission::image::MAX_IMAGE_BYTES as u64, 1)
        .unwrap();
    assert!(budget.reserve_read(0, 1).is_err());
    budget.reset();
    for _ in 0..READ_CALLS {
        budget.reserve_read(0, 1).unwrap();
    }
    assert!(budget.reserve_read(0, 1).is_err());
    budget.reset();
    assert!(budget.reserve_read(0, CHUNK_BYTES + 1).is_err());
    assert!(budget.reserve_read(u64::MAX, 1).is_err());
    assert!(budget.reserve_read(0, 0).is_err());
}
#[test]
fn opaque_handles_expire_and_cleanup_acks_before_new_scope() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("image.bin"), [0, 0xff, 3, 4]).unwrap();
    let mut store = store(root.path(), false, &runtime);
    let id = opened(&mut store, "image.bin");
    assert!(
        open(
            store.as_context_mut(),
            ("image.bin".into(), fs::OpenFlags::READ)
        )
        .unwrap()
        .0
        .is_err()
    );
    let text_capacity = store.data().output_bytes_remaining;
    assert_eq!(
        read(store.as_context_mut(), (fs::FileHandle { id }, 0, 4))
            .unwrap()
            .0
            .unwrap(),
        [0, 0xff, 3, 4]
    );
    assert_eq!(store.data().output_bytes_remaining, text_capacity);
    cleanup(store.data_mut()).unwrap();
    assert!(
        runtime
            .block_on(
                store
                    .data()
                    .file_reads
                    .owner
                    .as_ref()
                    .unwrap()
                    .entries
                    .lock()
            )
            .is_empty()
    );
    store.data_mut().file_reads.reset();
    let fresh = opened(&mut store, "image.bin");
    assert_ne!(fresh, id);
    assert!(
        read(store.as_context_mut(), (fs::FileHandle { id }, 0, 1))
            .unwrap()
            .0
            .is_err()
    );
    close(store.as_context_mut(), (fs::FileHandle { id: fresh },))
        .unwrap()
        .0
        .unwrap();
}
#[test]
fn cancellation_prevents_reads_but_allows_owned_close() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("image.bin"), b"pixels").unwrap();
    let mut store = store(root.path(), false, &runtime);
    let id = opened(&mut store, "image.bin");
    store.data().cancellation.cancel();
    assert!(matches!(
        read(store.as_context_mut(), (fs::FileHandle { id }, 0, 1))
            .unwrap()
            .0
            .unwrap_err()
            .category,
        types::ErrorCategory::Cancelled
    ));
    close(store.as_context_mut(), (fs::FileHandle { id },))
        .unwrap()
        .0
        .unwrap();
    assert!(
        runtime
            .block_on(
                store
                    .data()
                    .file_reads
                    .owner
                    .as_ref()
                    .unwrap()
                    .entries
                    .lock()
            )
            .is_empty()
    );
}
#[test]
fn host_owns_confined_and_explicit_unrestricted_paths() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = tempfile::tempdir().unwrap();
    let base = fixture.path().canonicalize().unwrap();
    let root = base.join("workspace");
    let external = base.join("outside");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&external).unwrap();
    let target = external.join("outside.bin");
    std::fs::write(&target, b"outside").unwrap();
    let path = target.to_str().unwrap();
    let mut confined = store(&root, false, &runtime);
    assert!(
        open(
            confined.as_context_mut(),
            (path.into(), fs::OpenFlags::READ)
        )
        .unwrap()
        .0
        .is_err()
    );
    assert!(confined.data().confinement_diagnostic.is_some());
    let mut unrestricted = store(&root, true, &runtime);
    let id = opened(&mut unrestricted, path);
    assert_eq!(
        read(unrestricted.as_context_mut(), (fs::FileHandle { id }, 0, 7))
            .unwrap()
            .0
            .unwrap(),
        b"outside"
    );
    cleanup(unrestricted.data_mut()).unwrap();
}

struct RetryClose(std::sync::atomic::AtomicUsize);
impl host::ProcessApi for RetryClose {}
impl host::NetApi for RetryClose {}
impl host::shell::ShellApi for RetryClose {}
impl host::HostAPI for RetryClose {}
#[async_trait::async_trait]
impl host::Host for RetryClose {}
#[async_trait::async_trait]
impl FsApi for RetryClose {
    async fn close(
        &self,
        _: &host::OpContext,
        handle: host::filesystem::FileHandle,
    ) -> Result<(), host::HostError> {
        assert_eq!(handle.0.0, 91);
        if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            return Err(host::HostError::new(
                host::ErrorCategory::Io,
                "fixture close transport failed",
            ));
        }
        Ok(())
    }
}
#[test]
fn execution_owner_retains_failed_close_after_store_detach_and_retries_ack() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut state = super::super::tests::runtime_state(true);
    state.runtime = Some(runtime.handle().clone());
    let invocation = state.tool_context.clone().unwrap();
    let retained = owner(&mut state).unwrap();
    let backend = Arc::new(RetryClose(std::sync::atomic::AtomicUsize::new(0)));
    runtime.block_on(async {
        retained.entries.lock().await.insert(
            4,
            Entry {
                handle: host::filesystem::FileHandle(host::HandleId(91)),
                context: host::OpContext {
                    principal: "fixture".into(),
                    ..Default::default()
                },
                backend: backend.clone(),
            },
        );
    });
    let failure = cleanup(&mut state).unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("fixture close transport failed")
    );
    assert_eq!(runtime.block_on(retained.entries.lock()).len(), 1);
    state.file_reads.detach();
    drop(state);
    // The existing execution receipt still holds the same owner after Store loss.
    let weak = Arc::downgrade(&retained);
    drop(retained);
    let retained = weak
        .upgrade()
        .expect("execution owns unacknowledged cleanup");
    runtime
        .block_on(retained.release(CancellationToken::new()))
        .unwrap();
    assert!(runtime.block_on(retained.entries.lock()).is_empty());
    assert_eq!(backend.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    drop(invocation);
}
