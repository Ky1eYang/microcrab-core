use super::*;
use std::sync::Mutex;

#[derive(Default)]
struct Sink(Mutex<Vec<ExecutionNotice>>);
impl ExecutionNoticeSink for Sink {
    fn changed(&self, notice: ExecutionNotice) {
        self.0.lock().unwrap().push(notice);
    }
}

#[tokio::test]
async fn delegate_child_binding_is_immutable_and_absent_from_ordinary_legacy_bytes() {
    let ordinary = row("ordinary");
    let bytes = serde_json::to_value(&ordinary).unwrap();
    assert!(bytes.get("delegate_child").is_none());
    let restored: ExecutionSnapshot = serde_json::from_value(bytes).unwrap();
    assert!(restored.delegate_child.is_none());
    let registry = ExecutionRegistry::new(Limits::default());
    let mut handle = registry
        .start(
            row("delegate"),
            CancellationToken::new(),
            |progress| async move {
                let binding = DelegateChild {
                    session_id: "actual-child".into(),
                    target: "review".into(),
                };
                progress.bind_delegate_child(binding.clone())?;
                progress.bind_delegate_child(binding)?;
                assert!(
                    progress
                        .bind_delegate_child(DelegateChild {
                            session_id: "another-child".into(),
                            target: "review".into(),
                        })
                        .is_err()
                );
                Ok(ToolOutput::text("settled"))
            },
        )
        .unwrap();
    let id = handle.id;
    handle.activate().unwrap();
    handle.wait().await.unwrap();
    handle.detach();
    let snapshot = registry.get(id).unwrap();
    assert_eq!(
        snapshot.delegate_child.as_ref().unwrap().session_id,
        "actual-child"
    );
    let receipt: serde_json::Value =
        serde_json::from_str(&registry.receipt(id, false).unwrap()).unwrap();
    assert_eq!(receipt["delegate_child"]["session_id"], "actual-child");
    assert_eq!(receipt["delegate_child"]["target"], "review");
    registry.close().await.unwrap();
}

struct FinishDuringReceipt(Mutex<Option<OutputWriter>>);
#[async_trait::async_trait]
impl ExecutionController for FinishDuringReceipt {
    fn capabilities(&self) -> ExecutionCapabilities {
        if let Some(writer) = self.0.lock().unwrap().take() {
            writer
                .append(None, "completed during receipt capture")
                .unwrap();
            writer.finish(Ok(())).unwrap();
        }
        ExecutionCapabilities::default()
    }
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
}

#[tokio::test]
async fn receipt_and_initial_page_share_the_terminal_notice_baseline() {
    let registry = ExecutionRegistry::new(Limits::default());
    let sink = Arc::new(Sink::default());
    let endpoint: Arc<dyn ExecutionNoticeSink> = sink.clone();
    registry.bind_notice_sink(Arc::downgrade(&endpoint));
    let owner = registry.clone();
    let mut handle = registry
        .start(
            row("receipt-race"),
            CancellationToken::new(),
            move |progress| async move {
                let controller = Arc::new(FinishDuringReceipt(Mutex::new(None)));
                let (writer, output) = owner.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    controller.clone(),
                )?;
                // Controller capability observation is deliberately outside the registry
                // lock. Complete there to force a mutation immediately before the locked
                // receipt capture, without timing or scheduling assumptions.
                *controller.0.lock().unwrap() = Some(writer);
                Ok(ToolOutput::Buffered(output.with_initial_output(true)))
            },
        )
        .unwrap();
    let id = handle.id;
    handle.activate().unwrap();
    let receipt: serde_json::Value = serde_json::from_str(&handle.wait().await.unwrap()).unwrap();
    handle.detach();
    assert_eq!(receipt["status"], "succeeded");
    assert_eq!(receipt["output"]["closed"], true);
    assert_eq!(receipt["initial"]["closed"], true);
    assert!(
        receipt["initial"]["chunks"][0]["content"]
            .as_str()
            .unwrap()
            .contains("completed during receipt")
    );
    registry.acknowledge_result_delivered(id);
    assert!(
        sink.0.lock().unwrap().is_empty(),
        "terminal status already in the tool receipt must not wake another round"
    );
    registry.close().await.unwrap();
}

#[tokio::test]
async fn notices_wait_for_context_delivery_then_reconcile_fast_terminal_once() {
    let registry = ExecutionRegistry::new(Limits::default());
    let sink = Arc::new(Sink::default());
    let endpoint: Arc<dyn ExecutionNoticeSink> = sink.clone();
    registry.bind_notice_sink(Arc::downgrade(&endpoint));
    let owner = registry.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut handle = registry
        .start(
            row("stream"),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = owner.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    Arc::new(CancellationController::new(CancellationToken::new())),
                )?;
                sender.send(writer).map_err(|_| Error::Closed)?;
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = handle.id;
    handle.activate().unwrap();
    handle.wait().await.unwrap();
    handle.detach();
    let writer = receiver.await.unwrap();
    writer
        .append(None, "never-copy-this-body-to-state")
        .unwrap();
    writer.finish(Ok(())).unwrap();
    assert!(sink.0.lock().unwrap().is_empty());
    registry.acknowledge_result_delivered(id);
    {
        let notices = sink.0.lock().unwrap();
        assert_eq!(notices.len(), 1);
        assert_eq!(
            notices[0].previous.as_ref().unwrap().status,
            ExecutionStatus::Running
        );
        assert_eq!(notices[0].row.status, ExecutionStatus::Succeeded);
        assert!(notices[0].row.result.is_none());
        assert!(notices[0].row.invocation_result.is_none());
        assert!(notices[0].row.elapsed_ms.is_none());
        assert!(
            !serde_json::to_string(&notices[0].row)
                .unwrap()
                .contains("never-copy-this-body")
        );
    }
    registry.acknowledge_result_delivered(id);
    registry.with_quiescence(|no_pending| {
        assert!(no_pending);
        assert_eq!(sink.0.lock().unwrap().len(), 1);
    });
    registry.close().await.unwrap();
}
