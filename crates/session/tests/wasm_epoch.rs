#![cfg(feature = "wasm")]

//! Source-only Component Model fixtures exercise the public Session tool path.
use crate::typed_agent_fixture;
use microcrab_protocol::{Response, ToolCall, Usage};
use microcrab_session::{
    Auditor, Client, Session, SessionState,
    context::{Content, ContentItem},
    policy::{Action, PermissionConfig},
    runtime::{
        Runtime, ToolRegistration,
        tool::{AllowTools, Tool, ToolContext},
        wasm::{WasmAgent, WasmLimits},
    },
};
use serde_json::{Value, json};
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use typed_agent_fixture::{InitializedTool as _, final_text};

fn fixture_host() -> Arc<dyn microcrab_host_api::Host> {
    Arc::new(
        microcrab_host_local::LocalHost::new_filesystem(
            std::env::current_dir().unwrap(),
            Default::default(),
        )
        .unwrap(),
    )
}

fn component_text() -> String {
    let catalog = serde_json::to_string(&json!([{
        "name":"epoch", "modes":[{"name":"execute","role":"both"}], "variables":{},
        "tools":(["count_a", "count_b", "error", "trap", "fuel"].map(
            |name| json!({"name":name,"description":"Epoch counter","parameters":{"type":"object"}})))
    }]))
    .unwrap();
    let escaped: String = catalog
        .bytes()
        .map(|byte| format!("\\{byte:02x}"))
        .collect();
    let text = format!(
        r#"(component
      (core module $m
        (memory (export "memory") 2 2)
        (global $counter (mut i32) (i32.const 0))
        (global $heap (mut i32) (i32.const 4096))
        (global $busy (mut i32) (i32.const 0))
        (data (i32.const 128) "{escaped}")
        (data (i32.const 2048) "ordinary guest error")
        (func (export "realloc") (param i32 i32 i32 i32) (result i32)
          (local $ptr i32)
          global.get $heap local.tee $ptr
          local.get 3 i32.add global.set $heap
          global.get $heap i32.const 131072 i32.gt_u if unreachable end
          local.get $ptr)
        (func (export "describe") (param i32 i32) (result i32)
          ;; Discovery state must never become the actual epoch instance.
          global.get $counter i32.const 5 i32.add global.set $counter
          i32.const 0 i32.const 0 i32.store
          i32.const 4 i32.const 128 i32.store
          i32.const 8 i32.const {length} i32.store i32.const 0)
        (func $private_secret_guest_name (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32)
          (local $remaining i32)
          local.get 2 i32.load8_u i32.const 102 i32.eq if
            i32.const 100000 local.set $remaining
            (loop $burn
              local.get $remaining i32.const 1 i32.sub local.tee $remaining
              br_if $burn)
          end
          ;; Both successful and ordinary-error returns require post-return.
          global.get $busy if unreachable end
          i32.const 1 global.set $busy
          global.get $counter i32.const 1 i32.add global.set $counter
          local.get 2 i32.load8_u i32.const 116 i32.eq if unreachable end
          i32.const 16 i32.const 0 i32.store
          local.get 2 i32.load8_u i32.const 101 i32.eq if
            i32.const 16 i32.const 1 i32.store
            i32.const 20 i32.const 2048 i32.store
            i32.const 24 i32.const 20 i32.store
          else
            ;; Each test stays below ten calls per instance: one JSON digit.
            i32.const 3072 global.get $counter i32.const 48 i32.add i32.store8
            i32.const 20 i32.const 3072 i32.store
            i32.const 24 i32.const 1 i32.store
          end
          i32.const 16)
        (func (export "post-return") (param i32)
          i32.const 0 global.set $busy
          i32.const 4096 global.set $heap))
      (core instance $i (instantiate $m))
      (func (export "describe") (param "host-info" string) (result (result string (error string)))
        (canon lift (core func $i "describe") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (func (export "invoke") (param "host-info" string) (param "name" string) (param "arguments" string)
        (result (result string (error string)))
        (canon lift (core func $i "invoke") (memory (core memory $i "memory"))
          (realloc (core func $i "realloc"))
          (post-return (core func $i "post-return")))))"#,
        length = catalog.len(),
    );
    text
}

fn component() -> Vec<u8> {
    typed_agent_fixture::component(&component_text())
}

fn tools() -> Vec<Arc<dyn Tool>> {
    WasmAgent::load(
        &component(),
        WasmLimits {
            memory_bytes: 128 * 1024,
            ..Default::default()
        },
        fixture_host(),
    )
    .unwrap()
    .tools()
}

struct Script {
    responses: Mutex<VecDeque<Response>>,
    outputs: Mutex<Vec<Value>>,
    captured: Mutex<BTreeSet<String>>,
    seen: Mutex<Vec<Content>>,
}
impl Script {
    // An empty batch ends a turn; nonempty batches can contain multiple tool calls.
    fn new(batches: &[&[&str]]) -> Arc<Self> {
        Self::with_arguments(batches, json!({}))
    }
    fn with_arguments(batches: &[&[&str]], arguments: Value) -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(
                batches
                    .iter()
                    .enumerate()
                    .map(|(round, names)| Response {
                        provider_metadata: None,
                        content: if names.is_empty() {
                            "done".into()
                        } else {
                            String::new()
                        },
                        tool_calls: names
                            .iter()
                            .enumerate()
                            .map(|(index, name)| ToolCall {
                                provider_metadata: None,
                                id: format!("epoch-{round}-{index}"),
                                name: (*name).into(),
                                arguments: arguments.clone(),
                            })
                            .collect(),
                        usage: Usage::default(),
                    })
                    .collect(),
            ),
            outputs: Mutex::new(vec![]),
            captured: Mutex::new(BTreeSet::new()),
            seen: Mutex::new(vec![]),
        })
    }
    fn results(&self) -> Vec<Value> {
        self.outputs.lock().unwrap().clone()
    }
}
#[async_trait::async_trait]
impl Client for Script {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        for item in &content.items {
            if let ContentItem::Tool { call_id, content, .. } = item
                && self.captured.lock().unwrap().insert(call_id.clone())
            {
                self.outputs.lock().unwrap().push(
                    serde_json::from_str(content).expect("tool output must be structured JSON"),
                );
            }
        }
        self.seen.lock().unwrap().push(content.clone());
        Ok(self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted model request"))
    }
}

fn session(tools: &[Arc<dyn Tool>], script: Arc<Script>) -> Session {
    let permission: Arc<dyn microcrab_session::policy::SessionPermission> = Arc::new(
        AllowTools::new(tools.iter().map(|tool| tool.definition().name)),
    );
    let permissions = PermissionConfig {
        initialization: Action::Allow,
        overrides: permission.initial_overrides(),
        ..Default::default()
    };
    let runtime = Runtime::new(
        tools.iter().cloned().map(ToolRegistration::Wasm).collect(),
        permission,
    )
    .unwrap();
    Session::builder_arc(script, runtime)
        .permissions(permissions)
        .build()
        .unwrap()
}

#[tokio::test]
async fn retained_instance_resets_fuel_for_each_invocation() {
    let tools = WasmAgent::load(
        &component(),
        WasmLimits {
            fuel: 2_000_000,
            memory_bytes: 128 * 1024,
            ..Default::default()
        },
        fixture_host(),
    )
    .unwrap()
    .tools();
    let script = Script::new(&[
        &["fuel", "fuel"],
        &["fuel", "fuel"],
        &["fuel", "fuel"],
        &["fuel", "fuel"],
        &[],
    ]);
    let session = session(&tools, script.clone());
    session
        .submit("independent bounded fuel per call")
        .await
        .unwrap();
    assert_eq!(
        script.results(),
        (1..=8).map(|n| json!(n)).collect::<Vec<_>>()
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn same_compiled_component_shares_memory_across_tool_names_calls_and_turns() {
    let tools = tools();
    let script = Script::new(&[
        &["count_a", "count_b"],
        &["count_a"],
        &[],
        &["count_b"],
        &[],
    ]);
    let session = session(&tools, script.clone());
    session.submit("first turn").await.unwrap();
    session.submit("second turn").await.unwrap();
    assert_eq!(
        script.results(),
        vec![json!(1), json!(2), json!(3), json!(4)]
    );
    {
        let seen = script.seen.lock().unwrap();
        assert!(seen.iter().all(|request| request.epoch == 0));
        assert_ne!(seen.first().unwrap().turn_id, seen.last().unwrap().turn_id);
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn identical_arc_tool_handles_do_not_share_guest_memory_between_sessions() {
    let tools = tools();
    let first_script = Script::new(&[&["count_a"], &[], &["count_b"], &[]]);
    let second_script = Script::new(&[&["count_b"], &[], &["count_a"], &[]]);
    let first = session(&tools, first_script.clone());
    let second = session(&tools, second_script.clone());
    first.submit("first owner").await.unwrap();
    second.submit("second owner").await.unwrap();
    first.submit("first owner again").await.unwrap();
    second.submit("second owner again").await.unwrap();
    assert_eq!(first_script.results(), vec![json!(1), json!(2)]);
    assert_eq!(second_script.results(), vec![json!(1), json!(2)]);
    first.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
async fn compaction_defers_epoch_transition_until_valid_submit_then_resets_guest() {
    let tools = tools();
    let script = Script::new(&[&["count_a"], &[], &["count_b"], &[], &["count_a"], &[]]);
    let session = session(&tools, script.clone());
    session.submit("first").await.unwrap();
    session.submit("same epoch").await.unwrap();
    assert_eq!(script.results(), vec![json!(1), json!(2)]);
    let epoch = session.state().epoch_id;
    session
        .compact("counter history summary", json!({}))
        .await
        .unwrap();
    assert_eq!(session.state().epoch_id, epoch);
    assert!(session.checkpoint().await.unwrap().context.pending_epoch);
    assert!(session.submit(" ").await.is_err());
    assert_eq!(session.state().epoch_id, epoch);
    assert!(session.checkpoint().await.unwrap().context.pending_epoch);
    assert_eq!(script.results(), vec![json!(1), json!(2)]);
    session.submit("admit next epoch").await.unwrap();
    assert_eq!(session.state().epoch_id, epoch + 1);
    assert_eq!(script.results(), vec![json!(1), json!(2), json!(1)]);
    assert!(!session.checkpoint().await.unwrap().context.pending_epoch);
    session.close().await.unwrap();
}

#[tokio::test]
async fn live_recovery_of_same_epoch_preserves_history_but_discards_guest_memory() {
    let tools = tools();
    let script = Script::new(&[&["count_a"], &[], &["count_b"], &[], &["count_a"], &[]]);
    let session = session(&tools, script.clone());
    session.submit("saved conversation").await.unwrap();
    let saved = session.checkpoint().await.unwrap();
    let history = session.messages();
    let epoch = saved.state.epoch_id;
    session.submit("discard this branch").await.unwrap();
    assert_eq!(script.results(), vec![json!(1), json!(2)]);
    session.recover(saved).await.unwrap();
    assert_eq!(session.state().epoch_id, epoch);
    assert_eq!(session.messages(), history);
    session
        .submit("new branch retains conversation only")
        .await
        .unwrap();
    assert_eq!(session.state().epoch_id, epoch);
    assert_eq!(script.results(), vec![json!(1), json!(2), json!(1)]);
    session.close().await.unwrap();
}

#[tokio::test]
async fn ordinary_guest_error_keeps_mutated_memory_and_completes_post_return() {
    let tools = tools();
    let script = Script::new(&[&["count_a"], &["error"], &["count_b"], &[]]);
    let session = session(&tools, script.clone());
    session.submit("ordinary guest error").await.unwrap();
    assert_eq!(
        script.results(),
        vec![
            json!(1),
            json!({"content":"", "failure": microcrab_session::runtime::execution::ToolFailure::failed(
                "fixture_failed", "ordinary guest error"
            )}),
            json!(3)
        ]
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn actual_traps_replace_only_the_store_and_leave_context_epoch_unchanged() {
    let tools = tools();
    let script = Script::new(&[
        &["count_a"],
        &["trap"],
        &["count_b"],
        &[],
        &["count_a"],
        &[],
        &["count_b"],
        &["trap"],
        &["count_a"],
        &[],
    ]);
    let session = session(&tools, script.clone());
    session.submit("trap then try same epoch").await.unwrap();
    session.submit("use the same replacement next turn").await.unwrap();
    let outputs = script.results();
    assert_eq!(outputs.len(), 4);
    assert_eq!(outputs[0], json!(1));
    assert_eq!(outputs[1]["diagnostic"]["code"], "wasm_trap");
    assert_eq!(outputs[1]["diagnostic"]["layer"], "guest");
    assert_eq!(outputs[1]["diagnostic"]["phase"], "guest_execution");
    assert_eq!(outputs[1]["diagnostic"]["effects"], "unknown");
    assert_eq!(
        outputs[1]["diagnostic"]["wasm_failure"],
        json!({"cause":"unreachable"})
    );
    let evidence_id = outputs[1]["diagnostic"]["evidence_id"].as_u64().unwrap();
    assert_eq!(outputs[2], json!(1));
    assert_eq!(outputs[3], json!(2));
    let epoch = session.state().epoch_id;
    assert_eq!(epoch, 0);
    assert!(
        script
            .seen
            .lock()
            .unwrap()
            .iter()
            .all(|content| content.epoch == epoch)
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut invocation = ToolContext::detached(session.state(), cancellation);
    invocation.state = session.state_handle();
    let cancelled = tools[0].execute(json!({}), invocation).await;
    assert!(matches!(
        cancelled,
        Err(microcrab_session::Error::Cancelled)
    ));
    assert_eq!(session.state().epoch_id, epoch);
    session
        .compact("leave poisoned epoch", json!({}))
        .await
        .unwrap();
    assert_eq!(session.state().epoch_id, epoch);
    assert!(session.checkpoint().await.unwrap().context.pending_epoch);
    session.submit("new epoch is usable").await.unwrap();
    assert_eq!(session.state().epoch_id, epoch + 1);
    let outputs = script.results();
    assert_eq!(outputs[4], json!(1));
    let new_id = outputs[5]["diagnostic"]["evidence_id"].as_u64().unwrap();
    assert_ne!(new_id, evidence_id);
    assert_eq!(outputs[6], json!(1));
    session.close().await.unwrap();
}

#[tokio::test]
async fn recurring_fuel_traps_are_fresh_failures_without_weakening_the_budget() {
    // The guest's finite loop fits under 2,000,000 fuel but not this non-default
    // allowance, which must be reported exactly rather than guessing the default.
    let tools = WasmAgent::load(
        &component(),
        WasmLimits {
            fuel: 123_456,
            memory_bytes: 128 * 1024,
            ..Default::default()
        },
        fixture_host(),
    )
    .unwrap()
    .tools();
    let script = Script::with_arguments(
        &[
            &["count_a"],
            &["fuel"],
            &["count_b"],
            &[],
            &["fuel"],
            &["count_a"],
            &[],
        ],
        json!({"body":"PRIVATE_PAYLOAD", "path":"/private/endpoint"}),
    );
    let session = session(&tools, script.clone());
    session
        .submit("exhaust fuel after successful call")
        .await
        .unwrap();
    session.submit("try again in same epoch").await.unwrap();
    let outputs = script.results();
    assert_eq!(outputs.len(), 5);
    assert_eq!(outputs[0], json!(1));
    let failure = &outputs[1]["diagnostic"];
    assert_eq!(failure["code"], "wasm_trap");
    assert_eq!(failure["effects"], "unknown");
    let id = failure["evidence_id"].as_u64().unwrap();
    let cause = json!({"cause":"out_of_fuel", "fuel_limit":123_456});
    assert_eq!(failure["wasm_failure"], cause);
    assert_eq!(outputs[2], json!(1));
    assert_eq!(outputs[3]["diagnostic"]["code"], "wasm_trap");
    assert_eq!(outputs[3]["diagnostic"]["effects"], "unknown");
    assert_eq!(outputs[3]["diagnostic"]["wasm_failure"], cause);
    assert_ne!(outputs[3]["diagnostic"]["evidence_id"], id);
    assert_eq!(outputs[4], json!(1));
    for output in &outputs[1..] {
        let text = output.to_string();
        for private in [
            "PRIVATE_PAYLOAD",
            "/private/endpoint",
            "private_secret_guest_name",
            "wasm_frames",
            "backtrace",
        ] {
            assert!(!text.contains(private), "leaked {private}");
        }
    }
    assert_eq!(session.state().epoch_id, 0);
    session.close().await.unwrap();
}

#[tokio::test]
async fn persistent_instance_reclaims_argument_allocations_after_every_return() {
    let tools = tools();
    // Eight allocations exceed the fixed 128 KiB memory without post-return reuse.
    let script = Script::with_arguments(
        &[
            &["count_a"],
            &["count_b"],
            &["count_a"],
            &["count_b"],
            &["count_a"],
            &["count_b"],
            &["count_a"],
            &["count_b"],
            &[],
        ],
        json!({"payload":"x".repeat(24 * 1024)}),
    );
    let session = session(&tools, script.clone());
    session
        .submit("reuse bounded guest allocation")
        .await
        .unwrap();
    assert_eq!(
        script.results(),
        (1..=8).map(|counter| json!(counter)).collect::<Vec<_>>()
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn direct_execute_remains_fresh_without_an_attached_session() {
    let tools = tools();
    for index in [0, 1, 0] {
        let output = tools[index]
            .execute_initialized(
                json!({}),
                ToolContext::detached(
                    SessionState::new(Default::default()),
                    CancellationToken::new(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(final_text(output), "1");
    }
}

#[tokio::test]
async fn retained_state_handle_cannot_recreate_guest_after_session_close() {
    let tools = tools();
    let script = Script::new(&[&["count_a"], &[]]);
    let session = session(&tools, script.clone());
    session.submit("instantiate before closing").await.unwrap();
    let state = session.state_handle();
    let snapshot = session.state();
    session.close().await.unwrap();
    let mut invocation = ToolContext::detached(snapshot, CancellationToken::new());
    invocation.state = state;
    let result = tools[0].execute(json!({}), invocation).await;
    assert!(
        result.is_err(),
        "closed Session must reject attached execution"
    );
    assert_eq!(script.results(), vec![json!(1)]);
}

// Minimal subscriber keeps boundary-event tests independent of embedding log setup.
#[derive(Clone, Default)]
struct EvidenceCapture(Arc<Mutex<Vec<CapturedEvidence>>>);
struct CapturedEvidence {
    level: tracing::Level,
    fields: std::collections::BTreeMap<String, String>,
}
impl tracing::Subscriber for EvidenceCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "microcrab_session::runtime::wasm"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(std::collections::BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().into(), format!("{value:?}"));
            }
        }
        let mut fields = Fields(Default::default());
        event.record(&mut fields);
        self.0.lock().unwrap().push(CapturedEvidence {
            level: *event.metadata().level(),
            fields: fields.0,
        });
    }
}

#[tokio::test]
async fn direct_trap_logs_error_with_typed_secret_free_evidence_and_rejects_replay() {
    use tracing::instrument::WithSubscriber;
    let tools = tools();
    let trap = tools
        .iter()
        .find(|tool| tool.definition().name == "trap")
        .unwrap();
    let capture = EvidenceCapture::default();
    let mut invocation = ToolContext::detached(
        SessionState::new(Default::default()),
        CancellationToken::new(),
    );
    invocation.call_id = "private-call-secret /private/call".into();
    invocation.session_id = "private-session-secret /private/session".into();
    let arguments = json!({"password":"private-body-secret", "path":"/private/guest"});
    let admitted = trap.admitted_context(arguments.clone(), invocation)
        .with_subscriber(capture.clone()).await.unwrap();
    let first = trap.execute(arguments.clone(), admitted.clone())
        .with_subscriber(capture.clone()).await.err().unwrap().tool_diagnostic();
    let replay = trap.execute(arguments.clone(), admitted)
        .with_subscriber(capture.clone()).await.err().unwrap().tool_diagnostic();
    assert_eq!(first.code, microcrab_session::observation::diagnostics::DiagnosticCode::WasmTrap);
    assert_eq!(first.effects, microcrab_session::observation::diagnostics::EffectKnowledge::Unknown);
    assert!(first.evidence_id.is_some());
    assert_ne!(replay.code, microcrab_session::observation::diagnostics::DiagnosticCode::WasmTrap);
    assert_eq!(capture.0.lock().unwrap().len(), 1, "a replay must not enter the guest");
    let fresh = trap
        .execute_initialized(
            arguments,
            ToolContext::detached(
                SessionState::new(Default::default()),
                CancellationToken::new(),
            ),
        )
        .with_subscriber(capture.clone())
        .await
        .err()
        .unwrap()
        .tool_diagnostic();
    assert_ne!(fresh.evidence_id, first.evidence_id);
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 2);
    for (event, id) in events.iter().zip([first.evidence_id, fresh.evidence_id]) {
        assert_eq!(event.level, tracing::Level::ERROR);
        assert_eq!(event.fields["cause"], "\"unreachable\"");
        assert_eq!(event.fields["phase"], "\"invoke\"");
        assert_eq!(event.fields["evidence_id"], id.unwrap().to_string());
        assert_ne!(event.fields["wasm_frames"], "[]");
        assert_eq!(event.fields["frames_truncated"], "false");
        assert!(event.fields.contains_key("component_sha256"));
        assert!(event.fields.contains_key("session_sha256"));
        assert!(event.fields.contains_key("call_sha256"));
        assert!(event.fields.contains_key("tool_sha256"));
        assert!(event.fields.contains_key("epoch"));
        assert_ne!(event.fields["execution_id"], "None");
        let encoded = format!("{:?}", event.fields);
        for secret in [
            "private",
            "secret",
            "password",
            "guest_name",
            "ordinary guest error",
        ] {
            assert!(!encoded.contains(secret), "unexpected disclosure: {secret}");
            assert!(!first.result_json().contains(secret));
        }
    }
}

#[test]
fn compile_describe_and_instantiation_failures_log_safe_boundary_phases() {
    let capture = EvidenceCapture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let describe = component_text().replace(
        "global.get $counter i32.const 5 i32.add global.set $counter",
        "unreachable",
    );
    let instantiate = component_text().replace(
        "i32.const 4096 global.set $heap))",
        "i32.const 4096 global.set $heap) (func $private_start_secret unreachable) (start $private_start_secret))",
    );
    let bad_components = [
        b"invalid private-native-secret /private/component".to_vec(),
        typed_agent_fixture::component(&describe),
        typed_agent_fixture::component(&instantiate),
    ];
    let mut ids = BTreeSet::new();
    for bytes in bad_components {
        let error = tracing::dispatcher::with_default(&dispatch, || {
            WasmAgent::load(&bytes, WasmLimits::default(), fixture_host())
        })
        .err()
        .expect("fixture must fail at Wasmtime boundary");
        let diagnostic = error.tool_diagnostic();
        assert!(ids.insert(diagnostic.evidence_id.unwrap()));
        assert_eq!(
            diagnostic.effects,
            microcrab_session::observation::diagnostics::EffectKnowledge::Unknown
        );
        assert!(!diagnostic.result_json().contains("private"));
    }
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 3);
    for (event, phase) in events.iter().zip(["compile", "describe", "instantiate"]) {
        assert_eq!(event.level, tracing::Level::ERROR);
        assert_eq!(event.fields["phase"], format!("{phase:?}"));
        assert!(!format!("{:?}", event.fields).contains("private"));
    }
    assert_eq!(events[0].fields["cause"], "\"wasmtime_error\"");
    assert_eq!(events[1].fields["cause"], "\"unreachable\"");
    assert_eq!(events[2].fields["cause"], "\"unreachable\"");
}


async fn wait_for_observation(predicate: impl Fn() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !predicate() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("process-local observation queue did not drain");
}

#[tokio::test]
async fn guest_observations_survive_trap_without_inventing_lifecycle_identity() {
    use crate::guest_observation_fixture::{NOTE, component_with_reporting_init, instrument};
    use sha2::{Digest, Sha256};
    use tracing::instrument::WithSubscriber;

    let text = instrument(&component_text(), NOTE, NOTE);
    let bytes = component_with_reporting_init(&text);
    let capture = EvidenceCapture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let agent = tracing::dispatcher::with_default(&dispatch, || {
        WasmAgent::load(&bytes, WasmLimits::default(), fixture_host())
    })
    .unwrap();
    let tool = agent
        .tools()
        .into_iter()
        .find(|tool| tool.definition().name == "trap")
        .unwrap();
    let mut state = SessionState::new(Default::default());
    state.epoch_id = 7;
    let mut context = ToolContext::detached(state, CancellationToken::new());
    context.session_id = "private-session".into();
    context.call_id = "private-operation".into();
    let arguments = json!({"password":"private-argument"});
    let context = tool
        .admitted_context(arguments.clone(), context)
        .with_subscriber(capture.clone())
        .await
        .unwrap();
    let execution = context.execution_id.unwrap().0.to_string();
    let failure = tool
        .execute(arguments, context)
        .with_subscriber(capture.clone())
        .await
        .err()
        .unwrap()
        .tool_diagnostic();
    assert_eq!(
        failure.wasm_failure,
        Some(microcrab_session::observation::diagnostics::WasmFailure::Unreachable)
    );
    let is_guest = |ev: &CapturedEvidence| {
        ev.fields
            .get("observation")
            .is_some_and(|v| v == "\"wasm_guest_event\"")
    };
    wait_for_observation(|| {
        capture
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|ev| is_guest(ev))
            .count()
            == 3
    })
    .await;
    let events = capture.0.lock().unwrap();
    let events = events.iter().filter(|ev| is_guest(ev)).collect::<Vec<_>>();
    for (event, stage) in events.into_iter().zip(["describe", "init", "invoke"]) {
        let fields = &event.fields;
        assert_eq!(fields["source"], "\"guest\"");
        assert_eq!(fields["lifecycle_stage"], format!("{stage:?}"));
        assert_eq!(
            fields["component_sha256"],
            format!("{:x}", Sha256::digest(&bytes))
        );
        assert_eq!(fields["guest_phase"], "Phase::Receive");
        assert_eq!(fields["method"], "Method::RuntimeEnable");
        assert_eq!(fields["measured"], "42");
        assert_eq!(fields["limit"], "100");
        assert_eq!(event.level, tracing::Level::WARN);
        if stage == "invoke" {
            assert_eq!(
                fields["operation_sha256"],
                format!("\"{:x}\"", Sha256::digest(b"private-operation"))
            );
            assert_eq!(
                fields["session_sha256"],
                format!("\"{:x}\"", Sha256::digest(b"private-session"))
            );
            assert_eq!(fields["epoch"], "7");
            assert_eq!(fields["execution_id"], execution);
        } else {
            for field in [
                "operation_sha256",
                "session_sha256",
                "epoch",
                "execution_id",
            ] {
                assert!(
                    !fields.contains_key(field),
                    "invented lifecycle field {field}"
                );
            }
        }
        assert!(!format!("{fields:?}").contains("private-"));
    }
}

#[tokio::test]
async fn guest_observation_flood_is_suppressed_without_changing_final_output() {
    use crate::guest_observation_fixture::{NOTE, instrument};
    use tracing::instrument::WithSubscriber;

    let reports = 128;
    let mut flood = format!("i32.const {reports} local.set $remaining (loop $flood {NOTE}");
    flood.push_str(" local.get $remaining i32.const 1 i32.sub local.tee $remaining br_if $flood)");
    let bytes = typed_agent_fixture::component(&instrument(&component_text(), "", &flood));
    let observed = WasmAgent::load(&bytes, WasmLimits::default(), fixture_host())
        .unwrap()
        .tools();
    let unobserved = tools();
    let capture = EvidenceCapture::default();
    for tools in [&observed, &unobserved] {
        let tool = tools
            .iter()
            .find(|tool| tool.definition().name == "count_a")
            .unwrap();
        let result = tool
            .execute_initialized(
                json!({}),
                ToolContext::detached(
                    SessionState::new(Default::default()),
                    CancellationToken::new(),
                ),
            )
            .with_subscriber(capture.clone())
            .await
            .unwrap();
        assert_eq!(final_text(result), "1");
    }
    let is_suppression = |ev: &CapturedEvidence| ev.fields.contains_key("suppressed");
    wait_for_observation(|| capture.0.lock().unwrap().iter().any(is_suppression)).await;
    let events = capture.0.lock().unwrap();
    let suppression = events.iter().find(|ev| is_suppression(ev)).unwrap();
    assert_eq!(suppression.fields["source"], "\"runtime\"");
    let suppressed = suppression.fields["suppressed"].parse::<usize>().unwrap();
    let accepted = events
        .iter()
        .filter(|ev| ev.fields.contains_key("guest_confirmation"))
        .count();
    assert!((1..=32).contains(&accepted));
    assert_eq!(accepted + suppressed, reports);
}
