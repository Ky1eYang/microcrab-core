//! Deterministic scheduler and cleanup ownership checks using real batch dispatch.
use super::{arguments, batch};
use crate::{
    Error, Limits, State,
    runtime::{
        execution::{
            ExecutionController, ExecutionId, ExecutionSnapshot, OutputAvailability, OutputOptions,
            OutputWriter, ToolOutput, process::ProcessRegistry,
        },
        tool::ToolContext,
    },
};
use futures_util::{pin_mut, poll};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

struct CleanupGate {
    entered: Semaphore,
    allowed: Semaphore,
    calls: AtomicUsize,
}
impl CleanupGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            allowed: Semaphore::new(0),
            calls: AtomicUsize::new(0),
        })
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.acquire())
            .await
            .expect("controller cleanup must start")
            .unwrap()
            .forget();
    }
}
#[async_trait::async_trait]
impl ExecutionController for CleanupGate {
    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }
    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.add_permits(1);
        self.allowed.acquire().await.unwrap().forget();
        Ok(())
    }
}

pub(super) fn context() -> ToolContext {
    let limits = Limits {
        max_execution_buffers: 16,
        max_pending_executions: 16,
        lifecycle_timeout_ms: 60_000,
        ..Limits::default()
    };
    let state = State::new(limits.clone());
    let mut context = ToolContext::new(
        state,
        ProcessRegistry::detached(limits),
        CancellationToken::new(),
        "batch-call".into(),
        "job".into(),
        "owner".into(),
        "agent".into(),
        None,
    );
    crate::runtime::wasm::test_admission::admit(&mut context, None);
    context
}

async fn buffered(
    context: &ToolContext,
    gate: Arc<CleanupGate>,
    terminal: bool,
) -> (ExecutionId, OutputWriter) {
    let registry = context.state.executions();
    let owner = registry.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut execution = registry
        .start(
            ExecutionSnapshot::new(
                "owner".into(),
                "producer-call".into(),
                "producer".into(),
                "agent".into(),
                None,
            ),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) =
                    owner.open_output(progress.execution_id(), OutputOptions::default(), gate)?;
                writer.append(None, "retained output")?;
                if terminal {
                    writer.finish(Ok(()))?;
                }
                assert!(sender.send(writer).is_ok());
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let id = execution.id;
    execution.activate().unwrap();
    execution.wait().await.unwrap();
    execution.detach();
    (id, receiver.await.unwrap())
}

async fn execute(context: ToolContext, ids: &[ExecutionId], action: &str) -> Value {
    let arguments = json!({
        "id": ids.iter().map(|id| format!("owner/{}", id.0)).collect::<Vec<_>>(),
        "action": action,
    });
    let output = batch::execute(arguments::validate(&arguments).unwrap(), context)
        .await
        .unwrap();
    match output {
        ToolOutput::Final(output) => serde_json::from_str(&output.content).unwrap(),
        ToolOutput::Buffered(_) => panic!("job does not create buffered output"),
    }
}

#[tokio::test]
async fn insufficient_batch_envelopes_reject_before_read_or_release_effects() {
    let mut context = context();
    context.state = State::new(Limits {
        max_tool_output_bytes: 1024,
        max_execution_buffers: 2,
        lifecycle_timeout_ms: 60_000,
        ..Limits::default()
    });
    crate::runtime::wasm::test_admission::admit(&mut context, None);
    let registry = context.state.executions();
    let mut ids = Vec::new();
    let mut gates = Vec::new();
    for _ in 0..2 {
        let gate = CleanupGate::new();
        ids.push(buffered(&context, gate.clone(), true).await.0);
        gates.push(gate);
    }
    for action in ["read", "release"] {
        let arguments = json!({"id":ids.iter().map(|id| format!("owner/{}", id.0)).collect::<Vec<_>>(),"action":action});
        let result =
            batch::execute(arguments::validate(&arguments).unwrap(), context.clone()).await;
        assert!(matches!(result, Err(Error::Limit(_))));
        for (id, gate) in ids.iter().zip(&gates) {
            let row = registry.get(*id).unwrap();
            assert_eq!(row.output_availability, Some(OutputAvailability::Retained));
            assert_eq!(row.output.unwrap().start_cursor, 0);
            assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
        }
    }
    for gate in gates {
        gate.allowed.add_permits(1);
    }
    registry.close().await.unwrap();
}

#[tokio::test]
async fn release_overlaps_eight_cleanup_owners_and_admits_ninth_only_after_one_settles() {
    let context = context();
    let registry = context.state.executions();
    let mut gates = Vec::new();
    let mut ids = Vec::new();
    for _ in 0..9 {
        let gate = CleanupGate::new();
        ids.push(buffered(&context, gate.clone(), true).await.0);
        gates.push(gate);
    }
    let expected = ids.clone();
    let task = tokio::spawn(async move { execute(context, &ids, "release").await });
    for gate in &gates[..8] {
        gate.entered().await;
    }
    // All eight owners entered while none could finish. The bounded scheduler
    // cannot poll the ninth operation until one of those slots is released.
    assert_eq!(gates[8].calls.load(Ordering::SeqCst), 0);
    assert!(!task.is_finished());
    gates[0].allowed.add_permits(1);
    gates[8].entered().await;
    assert!(
        !task.is_finished(),
        "the other cleanup acknowledgments are still withheld"
    );
    for gate in &gates[1..] {
        gate.allowed.add_permits(1);
    }
    let response = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("all acknowledged cleanups must settle")
        .unwrap();
    for (row, id) in response["results"].as_array().unwrap().iter().zip(expected) {
        assert_eq!(row["id"], format!("owner/{}", id.0));
        assert_eq!(row["ok"], true);
        assert_eq!(
            registry.get(id).unwrap().output_availability,
            Some(OutputAvailability::Released)
        );
    }
    assert!(
        gates
            .iter()
            .all(|gate| gate.calls.load(Ordering::SeqCst) == 1)
    );
    registry.close().await.unwrap();
}

#[tokio::test]
async fn wait_all_captures_ninth_baseline_before_it_can_enter_the_scheduler() {
    let context = context();
    let registry = context.state.executions();
    let mut ids = Vec::new();
    let mut writers = Vec::new();
    let mut gates = Vec::new();
    for _ in 0..9 {
        let gate = CleanupGate::new();
        let (id, writer) = buffered(&context, gate.clone(), false).await;
        ids.push(id);
        writers.push(writer);
        gates.push(gate);
    }
    let operation = execute(context, &ids, "wait");
    pin_mut!(operation);
    // Explicit polling establishes admission and the first eight pending waits;
    // it also captures the ninth target's baseline before that target is polled.
    assert!(poll!(operation.as_mut()).is_pending());
    writers[8]
        .append(None, "ninth changed while queued")
        .unwrap();
    assert!(poll!(operation.as_mut()).is_pending());
    writers[0].append(None, "first changed").unwrap();
    assert!(
        poll!(operation.as_mut()).is_pending(),
        "wait-all must retain the seven unchanged targets"
    );
    // The ninth remains running and gets no further revision changes. A scheduler
    // that captures its baseline only when its slot opens would wait forever.
    for writer in &writers[1..8] {
        writer.append(None, "changed").unwrap();
    }
    let response = tokio::time::timeout(Duration::from_secs(5), operation.as_mut())
        .await
        .expect("the queued ninth revision change must satisfy its admission baseline");
    for (row, id) in response["results"].as_array().unwrap().iter().zip(&ids) {
        assert_eq!(row["id"], format!("owner/{}", id.0));
        assert_eq!(row["ok"], true);
        assert_eq!(row["result"]["execution"]["execution_id"], id.0);
        assert_eq!(row["result"]["execution"]["status"], "running");
    }
    for writer in writers {
        writer.finish(Ok(())).unwrap();
    }
    for gate in gates {
        gate.allowed.add_permits(1);
    }
    registry.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_batch_waiter_preserves_started_cleanup_for_a_later_release() {
    let context = context();
    let cancellation = context.cancellation.clone();
    let registry = context.state.executions();
    let mut ids = Vec::new();
    let mut gates = Vec::new();
    for _ in 0..2 {
        let gate = CleanupGate::new();
        ids.push(buffered(&context, gate.clone(), true).await.0);
        gates.push(gate);
    }
    let task_ids = ids.clone();
    let task = tokio::spawn(async move { execute(context, &task_ids, "release").await });
    for gate in &gates {
        gate.entered().await;
    }
    cancellation.cancel();
    let response = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("cancelled waiters must settle independently of cleanup acknowledgment")
        .unwrap();
    for row in response["results"].as_array().unwrap() {
        assert_eq!(row["ok"], false);
    }
    for (id, gate) in ids.into_iter().zip(gates) {
        assert_eq!(
            registry.get(id).unwrap().output_availability,
            Some(OutputAvailability::Retained)
        );
        gate.allowed.add_permits(1);
        tokio::time::timeout(
            Duration::from_secs(5),
            registry.release(id, CancellationToken::new()),
        )
        .await
        .expect("later release must observe the retained cleanup owner")
        .unwrap();
        assert_eq!(
            gate.calls.load(Ordering::SeqCst),
            1,
            "cleanup must be reused rather than replayed"
        );
        assert_eq!(
            registry.get(id).unwrap().output_availability,
            Some(OutputAvailability::Released)
        );
    }
    registry.close().await.unwrap();
}

#[tokio::test]
async fn unadmitted_batch_and_changed_whole_call_never_release_a_target() {
    use crate::runtime::tool::Tool;
    let mut context = context();
    let gate = CleanupGate::new();
    let (id, _) = buffered(&context, gate.clone(), true).await;
    let arguments = json!({"id":[format!("owner/{}", id.0)],"action":"release"});
    context.admission = None;
    assert!(
        batch::execute(arguments::validate(&arguments).unwrap(), context.clone())
            .await
            .is_err()
    );
    let status = json!({"id":format!("owner/{}", id.0),"action":"status"});
    crate::runtime::wasm::test_admission::admit_arguments(&mut context, None, status);
    assert!(
        super::Job
            .execute(arguments, context.clone())
            .await
            .is_err()
    );
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        context
            .state
            .executions()
            .get(id)
            .unwrap()
            .output_availability,
        Some(OutputAvailability::Retained)
    );
    gate.allowed.add_permits(1);
    context.state.executions().close().await.unwrap();
}

#[tokio::test]
async fn direct_identical_job_dispatch_consumes_proof_once_without_replaying_cleanup() {
    use crate::runtime::tool::Tool;
    let mut context = context();
    let gate = CleanupGate::new();
    let (id, _) = buffered(&context, gate.clone(), true).await;
    let arguments = json!({"id":format!("owner/{}", id.0),"action":"release"});
    crate::runtime::wasm::test_admission::admit_arguments(&mut context, None, arguments.clone());
    gate.allowed.add_permits(1);
    super::Job
        .execute(arguments.clone(), context.clone())
        .await
        .unwrap();
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert!(
        super::Job
            .execute(arguments, context.clone())
            .await
            .is_err()
    );
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    context.state.executions().close().await.unwrap();
}
