#![cfg(feature = "wasm")]
//! Init is an independently admitted lifecycle invocation, fenced by the Journal.
use crate::typed_agent_fixture::{READY_INIT_BODY, component_with_init};
use microcrab_protocol::{Response, ToolCall, Usage};
use microcrab_session::{
    Auditor, Client, Error, Session, SessionBuilder,
    context::{Content, ContentItem},
    policy::{Action, InvocationSubject, PermissionConfig},
    runtime::{
        Runtime, ToolRegistration,
        journal::{AgentInitStatus, Journal, MemoryJournal, Recover},
        tool::AllowTools,
        wasm::{WasmAgent, WasmLimits},
    },
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Model {
    calls: AtomicUsize,
    invoke: bool,
    prefix: String,
    outputs: Mutex<Vec<String>>,
    captured: Mutex<std::collections::BTreeSet<String>>,
}
#[async_trait::async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        for item in &content.items {
            if let ContentItem::Tool { call_id, content, .. } = item
                && call_id.starts_with(&format!("{}-", self.prefix))
                && self.captured.lock().unwrap().insert(call_id.clone())
            {
                self.outputs.lock().unwrap().push(content.clone());
            }
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: if self.invoke && call.is_multiple_of(2) {
                vec![ToolCall {
                    provider_metadata: None,
                    id: format!("{}-{call}", self.prefix),
                    name: "echo".into(),
                    arguments: serde_json::json!({}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}
#[derive(Default)]
struct RecordingJournal {
    memory: MemoryJournal,
    commits: Mutex<Vec<Recover>>,
}
#[async_trait::async_trait]
impl Journal for RecordingJournal {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.memory.commit(recover.clone()).await?;
        self.commits.lock().unwrap().push(recover);
        Ok(())
    }
}
fn bytes(init: &str) -> Vec<u8> {
    let catalog = r#"[{"name":"init-fixture","modes":[{"name":"execute","role":"both"}],"variables":{},"tools":[{"name":"echo","description":"fixture","parameters":{"type":"object"}}]}]"#;
    let escaped: String = catalog
        .bytes()
        .map(|byte| format!("\\{byte:02x}"))
        .collect();
    component_with_init(
        &format!(
            r#"(component
      (core module $m
        (memory (export "memory") 2)
        (global $heap (mut i32) (i32.const 4096))
        (global $counter (mut i32) (i32.const 0))
        (data (i32.const 128) "{escaped}")
        (data (i32.const 2048) "missing_feature")
        (data (i32.const 2080) "Host lacks required setup")
        (func (export "realloc") (param i32 i32 i32 i32) (result i32)
          (local $old i32) global.get $heap local.tee $old
          local.get 3 i32.add global.set $heap local.get $old)
        (func (export "describe") (param i32 i32) (result i32)
          i32.const 0 i32.const 0 i32.store
          i32.const 4 i32.const 128 i32.store
          i32.const 8 i32.const {length} i32.store i32.const 0)
        (func (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32)
          global.get $counter i32.const 1 i32.add global.set $counter
          i32.const 3072 global.get $counter i32.const 48 i32.add i32.store8
          i32.const 16 i32.const 0 i32.store
          i32.const 20 i32.const 3072 i32.store
          i32.const 24 i32.const 1 i32.store i32.const 16))
      (core instance $i (instantiate $m))
      (func (export "describe") (param "host-info" string) (result (result string (error string)))
        (canon lift (core func $i "describe") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (func (export "invoke") (param "host-info" string) (param "name" string) (param "arguments" string)
        (result (result string (error string)))
        (canon lift (core func $i "invoke") (memory (core memory $i "memory")) (realloc (core func $i "realloc")))))"#,
            length = catalog.len()
        ),
        init,
    )
}
fn builder(bytes: &[u8], model: Arc<Model>, journal: Arc<RecordingJournal>) -> SessionBuilder {
    let host = Arc::new(
        microcrab_host_local::LocalHost::new_filesystem(
            std::env::current_dir().unwrap(),
            Default::default(),
        )
        .unwrap(),
    );
    let agent = WasmAgent::load(bytes, WasmLimits::default(), host).unwrap();
    let permission: Arc<dyn microcrab_session::policy::SessionPermission> =
        Arc::new(AllowTools::new(["echo"]));
    let permissions = PermissionConfig {
        initialization: Action::Allow,
        overrides: permission.initial_overrides(),
        ..Default::default()
    };
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        permission,
    )
    .unwrap();
    Session::builder_arc(model, runtime)
        .permissions(permissions)
        .journal_arc(journal)
}

#[tokio::test]
async fn intent_precedes_ready_and_repeated_start_does_not_repeat_init() {
    let journal = Arc::new(RecordingJournal::default());
    let model = Arc::new(Model::default());
    let session = builder(&bytes(READY_INIT_BODY), model.clone(), journal.clone())
        .build()
        .unwrap();
    session.start().await.unwrap();
    session.start().await.unwrap();
    {
        let commits = journal.commits.lock().unwrap();
        let evidence: Vec<_> = commits
            .iter()
            .filter_map(|recover| recover.state.agent_init.first())
            .collect();
        assert!(matches!(evidence[0].status, AgentInitStatus::Intent));
        assert!(matches!(
            evidence.last().unwrap().status,
            AgentInitStatus::Ready
        ));
        assert!(evidence.iter().all(|record| record.attempt == 1));
    }
    let rows = session.execution_snapshots();
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].subject,
        InvocationSubject::AgentInit { attempt: 1, .. }
    ));
    assert!(rows[0].tool.is_empty());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    session.close().await.unwrap();
}

#[tokio::test]
async fn typed_failure_blocks_model_and_automatic_retry() {
    let body = r#"
      i32.const 0 i32.const 1 i32.store
      i32.const 4 i32.const 2048 i32.store
      i32.const 8 i32.const 15 i32.store
      i32.const 12 i32.const 2080 i32.store
      i32.const 16 i32.const 25 i32.store
      i32.const 0"#;
    let model = Arc::new(Model::default());
    let session = builder(
        &bytes(body),
        model.clone(),
        Arc::new(RecordingJournal::default()),
    )
    .build()
    .unwrap();
    for _ in 0..2 {
        let Error::ToolFailure(failure) = session.start().await.unwrap_err() else {
            panic!("typed init failure")
        };
        assert_eq!(failure.code, "missing_feature");
        assert_eq!(failure.message, "Host lacks required setup");
    }
    assert!(session.submit("must not reach the model").await.is_err());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(session.state().agent_init[0].attempt, 1);
    assert!(session.retry_agent_init().await.is_err());
    assert_eq!(session.state().agent_init[0].attempt, 2);
    session.close().await.unwrap();
}

#[tokio::test]
async fn recovered_unknown_intent_requires_explicit_retry() {
    let component = bytes(READY_INIT_BODY);
    let journal = Arc::new(RecordingJournal::default());
    let original = builder(&component, Arc::new(Model::default()), journal.clone())
        .build()
        .unwrap();
    original.start().await.unwrap();
    let mut recover = journal.memory.latest().unwrap();
    recover.state.agent_init[0].status = AgentInitStatus::Intent;
    original.close().await.unwrap();
    let model = Arc::new(Model::default());
    let restored = Session::restore(
        builder(
            &component,
            model.clone(),
            Arc::new(RecordingJournal::default()),
        ),
        recover,
    )
    .await
    .unwrap();
    let Error::ToolFailure(failure) = restored.start().await.unwrap_err() else {
        panic!("unknown init must block")
    };
    assert_eq!(failure.code, "init_outcome_unknown");
    assert_eq!(restored.state().agent_init[0].attempt, 1);
    restored.retry_agent_init().await.unwrap();
    assert_eq!(restored.state().agent_init[0].attempt, 2);
    assert!(matches!(
        restored.state().agent_init[0].status,
        AgentInitStatus::Ready
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    restored.close().await.unwrap();
}

#[tokio::test]
async fn trapped_init_is_failed_and_cannot_be_reentered_automatically() {
    let session = builder(
        &bytes("unreachable"),
        Arc::new(Model::default()),
        Arc::new(RecordingJournal::default()),
    )
    .build()
    .unwrap();
    assert!(session.start().await.is_err());
    assert!(matches!(
        session.state().agent_init[0].status,
        AgentInitStatus::Failed { .. }
    ));
    assert!(session.start().await.is_err());
    assert_eq!(session.state().agent_init[0].attempt, 1);
    session.close().await.unwrap();
}

fn counting_init() -> String {
    READY_INIT_BODY.replacen(
        "(local $result i32)",
        "(local $result i32) i32.const 0 i32.const 0 i32.const 0 i32.const 0 i32.const 0 i32.const 0 call $invoke drop",
        1,
    )
}
fn invoking_model() -> Arc<Model> {
    static NEXT: AtomicUsize = AtomicUsize::new(1);
    Arc::new(Model {
        invoke: true,
        prefix: format!("fixture-{}", NEXT.fetch_add(1, Ordering::SeqCst)),
        ..Model::default()
    })
}
#[tokio::test]
async fn init_runs_once_for_session_not_epoch_recovery_or_restore_but_fork_is_new() {
    let component = bytes(&counting_init());
    let model = invoking_model();
    let journal = Arc::new(RecordingJournal::default());
    let source = builder(&component, model.clone(), journal.clone())
        .session_id("init-source")
        .build()
        .unwrap();
    source.start().await.unwrap();
    source.submit("initial instance").await.unwrap();
    let saved = source.checkpoint().await.unwrap();
    source
        .compact("compacted conversation", serde_json::json!({}))
        .await
        .unwrap();
    source
        .submit("new epoch without repeated setup")
        .await
        .unwrap();
    source.recover(saved.clone()).await.unwrap();
    source
        .submit("history selection without repeated setup")
        .await
        .unwrap();
    assert_eq!(*model.outputs.lock().unwrap(), vec!["2", "1", "1"]);
    assert_eq!(source.state().agent_init[0].attempt, 1);

    let restored_model = invoking_model();
    let restored = Session::restore(
        builder(
            &component,
            restored_model.clone(),
            Arc::new(RecordingJournal::default()),
        )
        .session_id("init-restored"),
        saved,
    )
    .await
    .unwrap();
    restored.retry_agent_init().await.unwrap();
    restored.start().await.unwrap();
    restored
        .submit("restored session without repeated setup")
        .await
        .unwrap();
    assert_eq!(*restored_model.outputs.lock().unwrap(), vec!["1"]);
    assert_eq!(restored.state().agent_init[0].attempt, 1);

    let fork_model = invoking_model();
    let fork = source
        .fork(
            builder(
                &component,
                fork_model.clone(),
                Arc::new(RecordingJournal::default()),
            )
            .session_id("init-fork"),
        )
        .await
        .unwrap();
    assert!(fork.state().agent_init.is_empty());
    let fork_checkpoint = fork.checkpoint().await.unwrap();
    assert!(fork_checkpoint.state.agent_initialization_pending);
    fork.start().await.unwrap();
    assert!(!fork.state().agent_initialization_pending);
    fork.start().await.unwrap();
    fork.submit("destination Session owns its own setup")
        .await
        .unwrap();
    assert_eq!(*fork_model.outputs.lock().unwrap(), vec!["2"]);
    assert_eq!(fork.state().agent_init[0].attempt, 1);

    let mut empty = journal
        .commits
        .lock()
        .unwrap()
        .iter()
        .find(|recover| recover.state.agent_init.is_empty())
        .unwrap()
        .clone();
    empty.state.agent_initialization_pending = false;
    assert!(
        serde_json::to_value(&empty.state)
            .unwrap()
            .get("agent_initialization_pending")
            .is_none()
    );
    let old_model = invoking_model();
    let old = Session::restore(
        builder(
            &component,
            old_model.clone(),
            Arc::new(RecordingJournal::default()),
        )
        .session_id("init-old"),
        empty,
    )
    .await
    .unwrap();
    old.start().await.unwrap();
    old.submit("historical state without init evidence does not trigger replay")
        .await
        .unwrap();
    assert_eq!(*old_model.outputs.lock().unwrap(), vec!["1"]);
    assert!(old.state().agent_init.is_empty());
    old.close().await.unwrap();
    fork.close().await.unwrap();
    restored.close().await.unwrap();
    source.close().await.unwrap();
}
#[tokio::test]
async fn cold_explicit_retry_performs_one_initial_attempt() {
    let session = builder(
        &bytes("unreachable"),
        Arc::new(Model::default()),
        Arc::new(RecordingJournal::default()),
    )
    .build()
    .unwrap();
    assert!(session.retry_agent_init().await.is_err());
    assert_eq!(session.state().agent_init[0].attempt, 1);
    assert!(matches!(
        session.state().agent_init[0].status,
        AgentInitStatus::Failed { .. }
    ));
    session.close().await.unwrap();
}

#[tokio::test]
async fn checkpoint_before_first_component_intent_requires_explicit_retry_after_restore() {
    let component = bytes(&counting_init());
    let fresh = builder(
        &component,
        invoking_model(),
        Arc::new(RecordingJournal::default()),
    )
    .session_id("pending-source")
    .build()
    .unwrap();
    // Even checkpoint-before-start must retain the new initial setup obligation.
    assert!(!fresh.agent_initialization_complete());
    let initial = fresh.checkpoint().await.unwrap();
    assert!(initial.state.agent_initialization_pending);
    assert!(initial.state.agent_init.is_empty());
    let encoded = serde_json::to_value(&initial).unwrap();
    assert_eq!(encoded["state"]["agent_initialization_pending"], true);
    let initial: Recover = serde_json::from_value(encoded).unwrap();
    initial.validate().unwrap();
    fresh.close().await.unwrap();

    let model = invoking_model();
    let recovered = Session::restore(
        builder(
            &component,
            model.clone(),
            Arc::new(RecordingJournal::default()),
        )
        .session_id("pending-restored"),
        initial,
    )
    .await
    .unwrap();
    let Error::ToolFailure(failure) = recovered.start().await.unwrap_err() else {
        panic!("unfinished initial setup must block")
    };
    assert_eq!(failure.code, "init_outcome_unknown");
    assert!(
        recovered
            .submit("must not silently skip initial setup")
            .await
            .is_err()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(
        recovered.state().agent_init.is_empty(),
        "ordinary restore must not enter guest init"
    );
    recovered.retry_agent_init().await.unwrap();
    assert!(!recovered.state().agent_initialization_pending);
    assert_eq!(recovered.state().agent_init[0].attempt, 1);
    recovered
        .submit("explicit initial setup ran exactly once")
        .await
        .unwrap();
    assert_eq!(*model.outputs.lock().unwrap(), vec!["2"]);
    let complete = recovered.checkpoint().await.unwrap();
    assert!(!complete.state.agent_initialization_pending);
    assert!(
        serde_json::to_value(&complete.state)
            .unwrap()
            .get("agent_initialization_pending")
            .is_none()
    );
    recovered.close().await.unwrap();

    let complete_model = invoking_model();
    let complete_restore = Session::restore(
        builder(
            &component,
            complete_model.clone(),
            Arc::new(RecordingJournal::default()),
        )
        .session_id("pending-complete"),
        complete.clone(),
    )
    .await
    .unwrap();
    complete_restore.start().await.unwrap();
    complete_restore
        .submit("completed initial setup is never replayed")
        .await
        .unwrap();
    assert_eq!(*complete_model.outputs.lock().unwrap(), vec!["1"]);
    complete_restore.close().await.unwrap();

    // A crash after all individual Ready records but before the batch completion
    // ACK is still unfinished. Retry settles the batch without replaying Ready.
    let mut unacknowledged_batch = complete;
    unacknowledged_batch.state.agent_initialization_pending = true;
    let batch_model = invoking_model();
    let batch = Session::restore(
        builder(
            &component,
            batch_model.clone(),
            Arc::new(RecordingJournal::default()),
        )
        .session_id("pending-batch"),
        unacknowledged_batch,
    )
    .await
    .unwrap();
    assert!(batch.start().await.is_err());
    batch.retry_agent_init().await.unwrap();
    assert!(!batch.state().agent_initialization_pending);
    assert_eq!(batch.state().agent_init[0].attempt, 1);
    batch
        .submit("settled Ready components do not run again")
        .await
        .unwrap();
    assert_eq!(*batch_model.outputs.lock().unwrap(), vec!["1"]);
    batch.close().await.unwrap();
}
