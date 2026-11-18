use super::*;
fn row() -> ExecutionSnapshot {
    ExecutionSnapshot::new(
        "owner".into(),
        "call".into(),
        "resource".into(),
        "agent".into(),
        None,
    )
}
#[tokio::test]
async fn reserved_execution_cannot_replace_primary_buffer_or_cleanup_receipts() {
    for held in ["controller", "buffer", "cleanup"] {
        let registry = ExecutionRegistry::new(Limits::default());
        let id = registry
            .admit_detached(row(), CancellationToken::new())
            .unwrap();
        let controller: Arc<dyn ExecutionController> = Arc::new(
            super::super::CancellationController::new(CancellationToken::new()),
        );
        {
            let mut state = registry.state.lock().unwrap();
            let entry = state.entries.get_mut(&id).unwrap();
            match held {
                "controller" => entry.controller = Some(controller.clone()),
                "buffer" => {
                    entry.buffer = Some(Buffer::new(OutputOptions::default(), 128).unwrap())
                }
                "cleanup" => {
                    let (_, receipt) = watch::channel(Some(Ok(())));
                    entry.cleanup = Some(receipt);
                }
                _ => unreachable!(),
            }
        }
        let mut replacement = row();
        replacement.execution_id = id;
        let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = effects.clone();
        let result = registry.start(replacement, CancellationToken::new(), move |_| async move {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ToolOutput::text("replacement"))
        });
        assert!(matches!(result, Err(Error::Tool(_))), "{held}");
        assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 0);
        {
            let state = registry.state.lock().unwrap();
            let entry = &state.entries[&id];
            assert!(entry.worker.is_none());
            match held {
                "controller" => {
                    assert!(Arc::ptr_eq(entry.controller.as_ref().unwrap(), &controller))
                }
                "buffer" => assert!(entry.buffer.is_some()),
                "cleanup" => assert!(entry.cleanup.is_some()),
                _ => unreachable!(),
            }
        }
        registry.close().await.unwrap();
    }
}
#[tokio::test]
async fn reserved_execution_transfers_subordinate_cleanup_without_replacing_it() {
    let registry = ExecutionRegistry::new(Limits::default());
    let id = registry
        .admit_detached(row(), CancellationToken::new())
        .unwrap();
    let token = CancellationToken::new();
    let controller: Arc<dyn ExecutionController> =
        Arc::new(super::super::CancellationController::new(token.clone()));
    registry.retain_cleanup(id, controller).unwrap();
    let mut replacement = row();
    replacement.execution_id = id;
    let mut execution = registry
        .start(replacement, CancellationToken::new(), |_| async {
            Ok(ToolOutput::text("done"))
        })
        .unwrap();
    execution.activate().unwrap();
    assert_eq!(execution.wait().await.unwrap(), "done");
    assert!(
        token.is_cancelled(),
        "retained subordinate must acknowledge release"
    );
    execution.detach();
    registry.close().await.unwrap();
}
