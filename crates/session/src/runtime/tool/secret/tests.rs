//! Secret substitution tests use the real State, permission and registry owners.
use super::*;
use crate::runtime::tool::{
    AllowTools, SecretDelivery, Tool, ToolOutput, ToolRegistration, ToolRegistry,
};
use crate::{Limits, State};
use async_trait::async_trait;
use microcrab_protocol::ToolDefinition;
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;

async fn state(values: &[(&str, &str)]) -> State {
    let (state, endpoint) = State::new(Limits::default()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    if !values.is_empty() {
        state
            .prepare_secret_write(
                values
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            )
            .await
            .unwrap()
            .commit()
            .unwrap();
    }
    // The live values remain in the State owner; this test needs no subsequent
    // queue write. Keeping a snapshot never exposes them through public views.
    drop(endpoint);
    state
}
fn call(arguments: Value) -> ToolCall {
    ToolCall {
        id: "secret-call".into(),
        name: "secret_echo".into(),
        arguments,
        provider_metadata: None,
    }
}
struct Echo {
    schema: Value,
    delivery: SecretDelivery,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for Echo {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "secret_echo".into(),
            description: "test".into(),
            parameters: self.schema.clone(),
        }
    }
    fn secret_delivery(&self) -> SecretDelivery {
        self.delivery
    }
    async fn execute(&self, args: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        context
            .ensure_admitted_arguments(&args)
            .map_err(PermissionError::into_runtime_error)?;
        assert_eq!(
            context.recorded_arguments().unwrap()["text"],
            "{{secret:token}}"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::text(args["text"].as_str().unwrap()))
    }
}
fn echo(schema: Value, delivery: SecretDelivery) -> Arc<Echo> {
    Arc::new(Echo {
        schema,
        delivery,
        calls: Arc::new(AtomicUsize::new(0)),
    })
}
fn string_schema() -> Value {
    json!({"type":"object","required":["text"],"additionalProperties":false,
        "properties":{"text":{"type":"string","pattern":"^[A-Z]+$","maxLength":12}}})
}
async fn admitted(tool: Arc<Echo>, state: State, call: &ToolCall) -> ToolContext {
    use crate::policy::{Action, ArgumentPredicate, Matcher, Permission, PermissionConfig};
    let runtime = crate::runtime::Runtime::new(
        vec![ToolRegistration::External(tool)],
        Arc::new(AllowTools::new(["secret_echo"])),
    )
    .unwrap();
    let controller = runtime.permission_controller();
    let policy = controller.snapshot().unwrap();
    controller
        .replace(
            policy.revision,
            PermissionConfig {
                overrides: vec![Permission {
                    agent: None,
                    mode: None,
                    tool: Some(Matcher::Exact("secret_echo".into())),
                    // This approval deliberately matches the ORIGINAL placeholder only.
                    args: vec![ArgumentPredicate {
                        pointer: "/text".into(),
                        matcher: Matcher::Exact("{{secret:token}}".into()),
                    }],
                    action: Action::Allow,
                }],
                ..Default::default()
            },
        )
        .unwrap();
    let mut context = ToolContext::new(
        state.clone(),
        state.processes(),
        CancellationToken::new(),
        call.id.clone(),
        call.name.clone(),
        "secret-session".into(),
        "test".into(),
        None,
    );
    let mut descriptor = crate::runtime::execution::ExecutionSnapshot::new(
        context.session_id.clone(),
        call.id.clone(),
        call.name.clone(),
        context.agent.clone(),
        None,
    );
    descriptor.arguments = Some(serde_json::to_string(&call.arguments).unwrap());
    context.execution_id = Some(
        state
            .executions()
            .admit_detached(descriptor, context.cancellation.clone())
            .unwrap(),
    );
    let issuer = crate::context::Content {
        epoch: 0,
        subagent: None,
        system: "test".into(),
        items: vec![],
        tools: runtime.definitions(),
        max_output_tokens: 128,
        max_bytes: 65536,
        turn_id: None,
        round: None,
    };
    runtime
        .admit_invocation(call, &mut context, &issuer)
        .await
        .unwrap();
    context
}

#[tokio::test]
async fn nested_string_leaves_only_are_resolved_once_and_nonsecret_keys_are_untouched() {
    let state = state(&[("token", "{{secret:other}}"), ("other", "PRIVATE")]).await;
    let original = json!({"{{secret:token}}": ["prefix {{secret:token}} suffix", 7, null],
        "literal": "{{secret:bad key}}"});
    let resolved = resolve_value(
        &original,
        &state.secret_snapshot(),
        &super::super::injection::LiteralMask::default(),
        4096,
    )
    .unwrap();
    assert_eq!(
        resolved,
        json!({"{{secret:token}}": ["prefix {{secret:other}} suffix",7,null],
        "literal":"{{secret:bad key}}"})
    );
    assert_eq!(
        original["{{secret:token}}"][0],
        "prefix {{secret:token}} suffix"
    );
}
#[tokio::test]
async fn unknown_secret_and_expansion_failures_have_safe_normal_tool_errors() {
    let state = state(&[(
        "token",
        "PRIVATE_VALUE_PRIVATE_VALUE_PRIVATE_VALUE_PRIVATE_VALUE",
    )])
    .await;
    for (value, bound) in [
        (json!({"v":"{{secret:missing}}"}), 4096),
        (json!({"v":"{{secret:token}}{{secret:token}}"}), 64),
    ] {
        let error = resolve_value(
            &value,
            &state.secret_snapshot(),
            &super::super::injection::LiteralMask::default(),
            bound,
        )
        .unwrap_err();
        assert!(!matches!(error, Error::Limit(_)));
        let text = error.to_string();
        assert!(!text.contains("PRIVATE_VALUE"));
    }
}
#[tokio::test]
async fn serialization_escape_cost_is_bounded_before_effects() {
    let state = state(&[("token", "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")]).await;
    assert!(
        resolve_value(
            &json!({"v":"{{secret:token}}"}),
            &state.secret_snapshot(),
            &super::super::injection::LiteralMask::default(),
            40
        )
        .is_err()
    );
}
#[tokio::test]
async fn inserted_visible_text_is_not_rescanned_as_a_secret() {
    let (state, _endpoint) = State::new(Limits::default()).bind_writer(
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    state
        .prepare_secret_write(std::collections::BTreeMap::from([(
            "token".into(),
            "PRIVATE".into(),
        )]))
        .await
        .unwrap()
        .commit()
        .unwrap();
    state
        .inject("name", json!("{{secret:token}}"))
        .await
        .unwrap();
    let (expanded, mask) = super::super::injection::expand_visible(
        &json!({"text":"pre {{var:string:name}} {{secret:token}}"}),
        &state.injection_view(),
        4096,
    )
    .unwrap();
    let resolved = resolve_value(&expanded, &state.secret_snapshot(), &mask, 4096).unwrap();
    assert_eq!(resolved["text"], "pre {{secret:token}} PRIVATE");
    assert!(!resolved.to_string().contains("PRIVATE_VALUE"));
}
#[tokio::test]
async fn visible_substitution_boundaries_cannot_manufacture_secret_references() {
    let (state, _endpoint) = State::new(Limits::default()).bind_writer(
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    state
        .prepare_secret_write(std::collections::BTreeMap::from([(
            "token".into(),
            "PRIVATE".into(),
        )]))
        .await
        .unwrap()
        .commit()
        .unwrap();
    for (fragment, authored) in [
        ("token", "{{secret:{{var:string:part}}}}"),
        ("token}}", "{{secret:{{var:string:part}}"),
        ("cret", "{{se{{var:string:part}}:token}}"),
        ("", "{{secret:tok{{var:string:part}}en}}"),
        ("", "{{se{{var:string:part}}cret:token}}"),
        ("}", "{{secret:token}{{var:string:part}}"),
    ] {
        state.inject("part", json!(fragment)).await.unwrap();
        let (expanded, mask) = super::super::injection::expand_visible(
            &json!({"text":authored, "real":"{{secret:token}}"}),
            &state.injection_view(),
            4096,
        )
        .unwrap();
        let resolved = resolve_value(&expanded, &state.secret_snapshot(), &mask, 4096).unwrap();
        assert_eq!(resolved["text"], "{{secret:token}}");
        assert_eq!(resolved["real"], "PRIVATE");
    }
}

#[tokio::test]
async fn many_inserted_secret_literals_remain_literal_during_dispatch_and_proof_validation() {
    let (state, _endpoint) = State::new(Limits::default()).bind_writer(
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    state
        .inject("x", json!("{{secret:missing}}"))
        .await
        .unwrap();
    state
        .prepare_secret_write(std::collections::BTreeMap::from([(
            "token".into(),
            "PRIVATE".into(),
        )]))
        .await
        .unwrap()
        .commit()
        .unwrap();
    let (arguments, mask) = super::super::injection::expand_visible(
        &json!({"text":"{{secret:token}}","items":vec!["{{var:x}}"; 20_000]}),
        &state.injection_view(),
        1024 * 1024,
    )
    .unwrap();
    let tool = echo(
        json!({"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"]}),
        SecretDelivery::Resolve,
    );
    let registry = ToolRegistry::new(vec![ToolRegistration::External(tool.clone())]).unwrap();
    let original = call(arguments.clone());
    let mut context = admitted(tool, state, &original).await;
    context.literal_mask = mask;
    let resolved = registry.resolve_call(&original, &mut context).unwrap();
    assert_eq!(resolved.arguments["items"], arguments["items"]);
    assert_eq!(resolved.arguments["text"], "PRIVATE");
    context
        .ensure_admitted_arguments(&resolved.arguments)
        .unwrap();
    assert!(
        resolved.arguments["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item == "{{secret:missing}}")
    );
}

#[tokio::test]
async fn ordinary_visible_variables_cannot_be_resolved_as_secrets() {
    let mut snapshot = crate::SessionState::new(Limits::default());
    snapshot.variables =
        crate::state::variables::Variables::new(json!({"token":"VISIBLE"})).unwrap();
    let state = State::from_snapshot(snapshot);
    assert!(
        resolve_value(
            &json!({"text":"{{secret:token}}"}),
            &state.secret_snapshot(),
            &super::super::injection::LiteralMask::default(),
            4096
        )
        .is_err()
    );
}

#[test]
fn placeholder_scanner_rejects_invalid_keys_and_requires_exact_delimiters() {
    for text in [
        "{{secret:}}",
        "{{secret:2bad}}",
        "{{secret:a b}}",
        "{{secret:token}",
        "{{ secret:token }}",
    ] {
        assert!(!contains_placeholder(text), "{text}");
    }
    assert!(contains_placeholder("pre {{secret:A_b.c-9}} post"));
    assert!(contains_placeholder(&format!(
        "{{{{secret:{}}}}}",
        "A".repeat(128)
    )));
    assert!(!contains_placeholder(&format!(
        "{{{{secret:{}}}}}",
        "A".repeat(129)
    )));
    assert!(!contains_placeholder(&format!(
        "{{{{secret:{}}}}}",
        "密".repeat(128)
    )));
}
#[tokio::test]
async fn constrained_placeholder_is_checked_in_full_after_original_permission_without_reask() {
    let tool = echo(string_schema(), SecretDelivery::Resolve);
    let registry = ToolRegistry::new(vec![ToolRegistration::External(tool.clone())]).unwrap();
    let original = call(json!({"text":"{{secret:token}}"}));
    registry.validate(&original).unwrap();
    let state = state(&[("token", "PRIVATE")]).await;
    let mut context = admitted(tool.clone(), state, &original).await;
    let without_witness = context.clone();
    let resolved = registry.resolve_call(&original, &mut context).unwrap();
    assert_eq!(resolved.arguments["text"], "PRIVATE");
    assert_eq!(context.recorded_arguments().unwrap(), &original.arguments);
    context
        .ensure_admitted_arguments(&resolved.arguments)
        .unwrap();
    assert!(
        context
            .ensure_admitted_arguments(&json!({"text":"CHANGED"}))
            .is_err()
    );
    assert!(
        without_witness
            .ensure_admitted_arguments(&original.arguments)
            .is_err()
    );
    assert!(without_witness.begin_dispatch(None).is_err());
    registry.execute(&resolved, context.clone()).await.unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert!(context.begin_dispatch(None).is_err());
    assert!(registry.resolve_call(&original, &mut context).is_err());
    assert_eq!(
        context.redaction_handle().redact_text("PRIVATE"),
        "{{secret:token}}"
    );
}
#[tokio::test]
async fn fully_resolved_pattern_failure_and_missing_key_do_not_dispatch() {
    for secret in [Some("not uppercase"), None] {
        let tool = echo(string_schema(), SecretDelivery::Resolve);
        let registry = ToolRegistry::new(vec![ToolRegistration::External(tool.clone())]).unwrap();
        let original = call(json!({"text":"{{secret:token}}"}));
        let state = state(&secret.map(|s| vec![("token", s)]).unwrap_or_default()).await;
        let mut context = admitted(tool.clone(), state, &original).await;
        let error = match registry.resolve_call(&original, &mut context) {
            Ok(_) => panic!("invalid resolution accepted"),
            Err(error) => error,
        };
        assert!(!matches!(error, Error::Limit(_)));
        assert!(!error.to_string().contains("not uppercase"));
        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
        assert_eq!(context.recorded_arguments().unwrap(), &original.arguments);
    }
}
#[test]
fn structural_and_branch_errors_are_not_deferred_by_placeholder_string() {
    let registry = ToolRegistry::new(vec![ToolRegistration::External(echo(
        string_schema(),
        SecretDelivery::Resolve,
    ))])
    .unwrap();
    for arguments in [
        json!({"text":7}),
        json!({"text":"{{secret:token}}","unknown":1}),
        json!({}),
    ] {
        assert!(registry.validate(&call(arguments)).is_err());
    }
    let branched = echo(
        json!({"type":"object","oneOf":[
            {"properties":{"text":{"type":"string","pattern":"^A$"}},"required":["text"]},
            {"properties":{"other":{"type":"integer"}},"required":["other"]}
        ]}),
        SecretDelivery::Resolve,
    );
    let registry = ToolRegistry::new(vec![ToolRegistration::External(branched)]).unwrap();
    assert!(
        registry
            .validate(&call(json!({"text":"{{secret:token}}"})))
            .is_err()
    );
}
#[tokio::test]
async fn only_registered_human_delivery_forwards_placeholders_without_lookup() {
    let tool = echo(
        json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
        SecretDelivery::ForwardPlaceholders,
    );
    let registry = ToolRegistry::new(vec![ToolRegistration::External(tool.clone())]).unwrap();
    let original = call(json!({"text":"{{secret:token}}"}));
    let mut context = admitted(tool.clone(), state(&[]).await, &original).await;
    let forwarded = registry.resolve_call(&original, &mut context).unwrap();
    assert_eq!(forwarded.arguments, original.arguments);
    registry.execute(&forwarded, context).await.unwrap();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn cloned_binding_cannot_change_operation_or_component_and_cancellation_revokes_it() {
    let tool = echo(string_schema(), SecretDelivery::Resolve);
    let registry = ToolRegistry::new(vec![ToolRegistration::External(tool.clone())]).unwrap();
    let original = call(json!({"text":"{{secret:token}}"}));
    let mut context = admitted(tool, state(&[("token", "PRIVATE")]).await, &original).await;
    let resolved = registry.resolve_call(&original, &mut context).unwrap();
    let mut changed = context.clone();
    changed.call_id = "another-call".into();
    assert!(
        changed
            .ensure_admitted_arguments(&resolved.arguments)
            .is_err()
    );
    assert!(
        context
            .ensure_admitted(Some("different-component"))
            .is_err()
    );
    context.cancellation.cancel();
    assert!(
        context
            .ensure_admitted_arguments(&resolved.arguments)
            .is_err()
    );
    assert!(context.begin_dispatch(None).is_err());
}

#[tokio::test]
async fn actual_job_write_merges_recipient_before_stdin_echo_and_keeps_rotated_values_masked() {
    use crate::policy::{Action, ArgumentPredicate, Matcher, Permission, PermissionConfig};
    use crate::runtime::execution::{
        ExecutionCapabilities, ExecutionController, ExecutionRegistry, ExecutionSnapshot,
        OutputOptions, OutputWriter,
    };
    use crate::runtime::tool::BuiltinTool;
    use base64::Engine as _;
    use std::sync::{Mutex, Weak};

    fn decoded_ring(registry: &ExecutionRegistry) -> String {
        let checkpoint = registry.recovery_snapshot();
        let row = checkpoint
            .iter()
            .find(|row| row.call_id == "echo-source")
            .unwrap();
        let ring: Value = serde_json::from_str(row.result.as_ref().unwrap()).unwrap();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(ring["content_base64"].as_str().unwrap())
            .unwrap();
        String::from_utf8(bytes).unwrap()
    }
    struct EchoInput {
        owner: Weak<ExecutionRegistry>,
        writer: Mutex<Option<OutputWriter>>,
        first_input: Mutex<Option<String>>,
        effects: AtomicUsize,
    }
    #[async_trait]
    impl ExecutionController for EchoInput {
        fn capabilities(&self) -> ExecutionCapabilities {
            ExecutionCapabilities { input: true }
        }
        async fn write(
            &self,
            data: String,
            _eof: bool,
            _cancel: CancellationToken,
        ) -> Result<(), Error> {
            // This is the actual stdin effect. No model, buffer reader or handler
            // gets a chance to retroactively repair what enters the ring here.
            assert!(!data.contains("{{secret:"));
            self.effects.fetch_add(1, Ordering::SeqCst);
            let writer = self.writer.lock().unwrap().as_ref().unwrap().clone();
            let registry = self.owner.upgrade().unwrap();
            let old = self.first_input.lock().unwrap().clone();
            if let Some(old) = &old {
                writer.append(Some("stdout"), old)?;
                assert!(!decoded_ring(&registry).contains(old));
            }
            let split = 6;
            writer.append(Some("stdout"), &data[..split])?;
            // Split literals must be withheld before checkpoint serialization;
            // installing a witness only after write() returns would fail here.
            assert!(!decoded_ring(&registry).contains(&data[..split]));
            writer.append(Some("stdout"), &data[split..])?;
            let checkpoint = decoded_ring(&registry);
            assert!(!checkpoint.contains(&data));
            assert!(checkpoint.contains("{{secret:token}}"));
            if let Some(old) = old {
                assert!(!checkpoint.contains(&old));
            } else {
                *self.first_input.lock().unwrap() = Some(data);
            }
            Ok(())
        }
        async fn cancel(&self, _cancel: CancellationToken) -> Result<(), Error> {
            Ok(())
        }
        async fn release(&self, _cancel: CancellationToken) -> Result<(), Error> {
            Ok(())
        }
    }

    let (state, _endpoint) = State::new(Limits::default()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let executions = state.executions();
    let echo = Arc::new(EchoInput {
        owner: Arc::downgrade(&executions),
        writer: Mutex::new(None),
        first_input: Mutex::new(None),
        effects: AtomicUsize::new(0),
    });
    let producer_owner = executions.clone();
    let producer_controller = echo.clone();
    // The recipient predates both Secret values and has no initial redaction.
    let mut producer = executions
        .start(
            ExecutionSnapshot::new(
                "secret-session".into(),
                "echo-source".into(),
                "echo-producer".into(),
                "test".into(),
                None,
            ),
            CancellationToken::new(),
            move |progress| async move {
                let (writer, output) = producer_owner.open_output(
                    progress.execution_id(),
                    OutputOptions::default(),
                    producer_controller.clone(),
                )?;
                *producer_controller.writer.lock().unwrap() = Some(writer);
                Ok(ToolOutput::Buffered(output))
            },
        )
        .unwrap();
    let recipient = producer.id;
    producer.activate().unwrap();
    producer.wait().await.unwrap();
    producer.detach();

    let runtime = crate::runtime::Runtime::new(
        vec![ToolRegistration::Builtin(BuiltinTool::Job)],
        Arc::new(AllowTools::new(["job:write"])),
    )
    .unwrap();
    let permission = runtime.permission_controller();
    let policy = permission.snapshot().unwrap();
    permission
        .replace(
            policy.revision,
            PermissionConfig {
                overrides: vec![Permission {
                    agent: None,
                    mode: None,
                    tool: Some(Matcher::Exact("job".into())),
                    args: vec![
                        ArgumentPredicate {
                            pointer: "/action".into(),
                            matcher: Matcher::Exact("write".into()),
                        },
                        ArgumentPredicate {
                            pointer: "/args/data".into(),
                            matcher: Matcher::Exact("{{secret:token}}".into()),
                        },
                    ],
                    action: Action::Allow,
                }],
                ..Default::default()
            },
        )
        .unwrap();
    let tools = ToolRegistry::new(vec![ToolRegistration::Builtin(BuiltinTool::Job)]).unwrap();
    let issuer = crate::context::Content {
        epoch: 0,
        subagent: None,
        system: "test".into(),
        items: vec![],
        tools: runtime.definitions(),
        max_output_tokens: 128,
        max_bytes: 65536,
        turn_id: None,
        round: None,
    };
    for (round, value) in ["OLD_PRIVATE_STDIN_VALUE", "NEW_PRIVATE_STDIN_VALUE"]
        .into_iter()
        .enumerate()
    {
        state
            .prepare_secret_write(std::collections::BTreeMap::from([(
                "token".into(),
                value.into(),
            )]))
            .await
            .unwrap()
            .commit()
            .unwrap();
        let original = ToolCall {
            id: format!("job-write-{round}"),
            name: "job".into(),
            provider_metadata: None,
            arguments: json!({"id":format!("secret-session/{}", recipient.0),
                "action":"write", "args":{"data":"{{secret:token}}"}}),
        };
        let mut context = ToolContext::new(
            state.clone(),
            state.processes(),
            CancellationToken::new(),
            original.id.clone(),
            "job".into(),
            "secret-session".into(),
            "test".into(),
            None,
        );
        let mut descriptor = ExecutionSnapshot::new(
            context.session_id.clone(),
            original.id.clone(),
            "job".into(),
            "test".into(),
            None,
        );
        descriptor.arguments = Some(serde_json::to_string(&original.arguments).unwrap());
        let invocation_id = executions
            .admit_detached(descriptor, context.cancellation.clone())
            .unwrap();
        context.execution_id = Some(invocation_id);
        runtime
            .admit_invocation(&original, &mut context, &issuer)
            .await
            .unwrap();
        let resolved = tools.resolve_call(&original, &mut context).unwrap();
        assert_eq!(resolved.arguments["args"]["data"], value);
        assert_eq!(context.recorded_arguments().unwrap(), &original.arguments);
        executions
            .install_redaction(invocation_id, context.redaction_handle())
            .unwrap();
        let output = tools.execute(&resolved, context).await.unwrap();
        match output {
            ToolOutput::Final(output) => executions
                .complete_detached(invocation_id, output)
                .await
                .unwrap(),
            ToolOutput::Buffered(_) => panic!("job.write must not create another output producer"),
        }
        let snapshots = serde_json::to_string(&executions.recovery_snapshot()).unwrap();
        assert!(!snapshots.contains("OLD_PRIVATE_STDIN_VALUE"));
        assert!(!snapshots.contains("NEW_PRIVATE_STDIN_VALUE"));
    }
    assert_eq!(echo.effects.load(Ordering::SeqCst), 2);
    let writer = echo.writer.lock().unwrap().as_ref().unwrap().clone();
    writer.finish(Ok(())).unwrap();
    let ring = decoded_ring(&executions);
    assert!(!ring.contains("OLD_PRIVATE_STDIN_VALUE"));
    assert!(!ring.contains("NEW_PRIVATE_STDIN_VALUE"));
    assert_eq!(ring.matches("{{secret:token}}").count(), 3);
    let first = serde_json::to_value(executions.read(recipient, 0, 16384).unwrap()).unwrap();
    let again = serde_json::to_value(executions.read(recipient, 0, 16384).unwrap()).unwrap();
    assert_eq!(
        first, again,
        "reading must not consume or repair retained output"
    );
    executions
        .release(recipient, CancellationToken::new())
        .await
        .unwrap();
    executions.close().await.unwrap();
}
