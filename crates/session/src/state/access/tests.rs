//! Projection refresh and hook commits are atomic; previews never call user code.
use super::*;
use crate::state::reference::{
    DynReference, Reference, ReferenceCapabilities, ReferenceContent, ReferenceEntry, ReferenceError,
    ReferenceProjection,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;

struct Projection {
    kind: &'static str,
    value: Arc<Mutex<Option<Value>>>,
    fail: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Reference for Projection {
    fn kind(&self) -> &str {
        self.kind
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities::read_only()
    }

    async fn load(&self, _cancel: CancellationToken) -> Result<(), ReferenceError> {
        Ok(())
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        Ok(vec![])
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(ReferenceError::failed("projection failed"));
        }
        Ok(ReferenceProjection {
            exact: self.value.lock().unwrap().clone(),
            short_model: None,
        })
    }

    async fn expand(
        &self,
        _: &str,
        _cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        Ok(None)
    }
}
fn projection(kind: &'static str, value: Value) -> Projection {
    Projection {
        kind,
        value: Arc::new(Mutex::new(Some(value))),
        fail: Arc::new(AtomicBool::new(false)),
        calls: Arc::new(AtomicUsize::new(0)),
    }
}

#[test]
fn failed_refresh_preserves_complete_projection_and_builtins() {
    let first = projection("skills", json!(["one"]));
    let first_value = first.value.clone();
    let second = projection("memory", json!(["old"]));
    let second_value = second.value.clone();
    let second_fail = second.fail.clone();
    let state = State::new(Limits::default());
    state
        .install_references(
            References::new(vec![
                DynReference::new(first).unwrap(),
                DynReference::new(second).unwrap(),
            ])
            .unwrap(),
        )
        .unwrap();
    state.refresh(Boundary::SessionCreated).unwrap();
    let committed = state.model_variables().unwrap();
    *first_value.lock().unwrap() = Some(json!(["two"]));
    second_fail.store(true, Ordering::SeqCst);
    assert!(matches!(
        state.refresh(Boundary::EpochStarted),
        Err(Error::Reference(_))
    ));
    assert_eq!(state.model_variables().unwrap(), committed);
    second_fail.store(false, Ordering::SeqCst);
    *second_value.lock().unwrap() = None;
    state.refresh(Boundary::EpochStarted).unwrap();
    let refreshed = state.model_variables().unwrap();
    assert_eq!(refreshed["skills"], json!(["two"]));
    assert!(refreshed.get("memory").is_none());
    assert_eq!(state.variables(), json!({}));
}

#[test]
fn preview_reuses_committed_projection_without_calling_kinds() {
    let reference = projection("skills", json!(["committed"]));
    let value = reference.value.clone();
    let fail = reference.fail.clone();
    let calls = reference.calls.clone();
    let state = State::new(Limits::default());
    state
        .install_references(References::new(vec![DynReference::new(reference).unwrap()]).unwrap())
        .unwrap();
    state.refresh(Boundary::SessionCreated).unwrap();
    let preview = state.preview();
    *value.lock().unwrap() = Some(json!(["not committed"]));
    fail.store(true, Ordering::SeqCst);
    preview.update(|snapshot| snapshot.turn_id = Some(1));
    preview.refresh_preview(Boundary::TurnStarted).unwrap();
    preview.refresh_preview(Boundary::RoundStarted).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        preview.model_variables().unwrap()["skills"],
        json!(["committed"])
    );
    assert_eq!(preview.model_variables().unwrap()["session"]["turn"], 1);
    assert_eq!(state.snapshot().turn_id, None);
    assert_eq!(
        state.model_variables().unwrap()["skills"],
        json!(["committed"])
    );
}

#[test]
fn empty_install_is_still_one_time_and_survives_snapshot_replacement() {
    let state = State::new(Limits::default());
    state.install_references(References::default()).unwrap();
    assert!(matches!(
        state.install_references(References::default()),
        Err(Error::Config(_))
    ));
    state.replace(SessionState::new(Limits::default()));
    assert!(matches!(
        state.install_references(References::default()),
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn auditor_view_uses_live_admission_builtins_and_custom_values() {
    use microcrab_auditor::VariableView;
    let closed = Arc::new(AtomicBool::new(false));
    let faulted = Arc::new(AtomicBool::new(false));
    let (state, endpoint) =
        State::new(Limits::default()).bind_writer(closed.clone(), faulted.clone());
    state.inject("null_value", Value::Null).await.unwrap();
    state.refresh(Boundary::RoundStarted).unwrap();
    assert_eq!(VariableView::get(&state, "null_value"), Some(Value::Null));
    assert_eq!(VariableView::get(&state, "missing"), None);
    assert_eq!(
        VariableView::get(&state, "session").unwrap()["boundary"],
        "round_started"
    );
    assert!(VariableView::get(&state, "time").is_some());
    assert!(state.extract("session").is_none());
    assert!(VariableView::get(&state.preview(), "null_value").is_none());
    faulted.store(true, Ordering::SeqCst);
    assert!(VariableView::get(&state, "null_value").is_none());
    faulted.store(false, Ordering::SeqCst);
    closed.store(true, Ordering::SeqCst);
    assert!(VariableView::get(&state, "session").is_none());
    closed.store(false, Ordering::SeqCst);
    drop(endpoint);
    assert!(VariableView::get(&state, "null_value").is_none());
}

#[tokio::test]
async fn immediate_hook_commits_preserve_owned_keys_and_terminal_policy_is_external() {
    let reference = projection("skills", json!(["owned"]));
    let closed = Arc::new(AtomicBool::new(false));
    let (state, _endpoint) =
        State::new(Limits::default()).bind_writer(closed.clone(), Arc::new(AtomicBool::new(false)));
    state
        .install_references(References::new(vec![DynReference::new(reference).unwrap()]).unwrap())
        .unwrap();
    state
        .apply_change(VariablesChange {
            scoped_set: vec![],
            set: Map::from_iter([("keep".into(), json!(1))]),
            remove: vec![],
        })
        .unwrap();
    for key in [
        "skills",
        "session",
        "time",
        "host",
        "agent",
        "mode",
        "mode_prompt",
        "delegatable_agents",
        "subagents",
    ] {
        assert!(
            state
                .apply_change(VariablesChange {
                    scoped_set: vec![],
                    set: Map::from_iter([("partial".into(), json!(true)), (key.into(), json!(2))]),
                    remove: vec!["keep".into()],
                })
                .is_err()
        );
        assert_eq!(state.variables(), json!({"keep":1}));
    }
    assert!(
        state
            .apply_change(VariablesChange {
                scoped_set: vec![],
                set: Map::from_iter([("keep".into(), json!(2))]),
                remove: vec!["keep".into()],
            })
            .is_err()
    );
    assert_eq!(state.variables(), json!({"keep":1}));
    // The Session wrapper admits successful terminal injections synchronously;
    // ordinary external writes remain closed and cannot acquire this authority.
    closed.store(true, Ordering::SeqCst);
    assert!(matches!(
        state.inject("external", json!(true)).await,
        Err(Error::Closed)
    ));
    state
        .apply_change(VariablesChange {
            scoped_set: vec![],
            set: Map::from_iter([("terminal".into(), json!(true))]),
            remove: vec!["keep".into()],
        })
        .unwrap();
    assert_eq!(state.variables(), json!({"terminal":true}));
}

#[test]
fn bounded_custom_candidate_rejection_preserves_committed_variables() {
    let state = State::new(Limits::default());
    state
        .apply_change(VariablesChange {
            scoped_set: vec![],
            set: Map::from_iter([("keep".into(), json!("committed"))]),
            remove: vec![],
        })
        .unwrap();
    let committed = state.variables();
    state.update(|snapshot| snapshot.limits.max_context_bytes = 64);
    let error = state
        .apply_change(VariablesChange {
            scoped_set: vec![],
            set: Map::from_iter([("large".into(), json!("x".repeat(256)))]),
            remove: vec!["keep".into()],
        })
        .unwrap_err();
    let Error::Limit(limit) = error else {
        panic!("expected custom byte limit")
    };
    assert_eq!(limit.kind, crate::LimitKind::CustomVariableBytes);
    assert_eq!(limit.budget_bytes, Some(64));
    assert!(!limit.is_pre_effect());
    assert_eq!(state.variables(), committed);
}

#[test]
fn current_projection_and_model_capture_share_facts_without_reference_reads() {
    let reference = projection("skills", json!(["committed"]));
    let calls = reference.calls.clone();
    let state = State::new(Limits::default());
    state
        .install_references(References::new(vec![DynReference::new(reference).unwrap()]).unwrap())
        .unwrap();
    state.refresh(Boundary::RoundStarted).unwrap();
    state
        .apply_change(VariablesChange {
            scoped_set: vec![],
            set: Map::from_iter([("custom".into(), json!(7))]),
            remove: vec![],
        })
        .unwrap();
    assert_eq!(state.view().unwrap(), state.model_variables().unwrap());
    assert_eq!(state.view().unwrap()["session"]["variables_revision"], 1);
    assert_eq!(state.view().unwrap()["skills"], json!(["committed"]));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(state.view().unwrap().get("subagents").is_none());
}

#[test]
fn controlled_mode_name_and_literal_prompt_are_derived_together() {
    let state = State::new(Limits::default());
    let agent = serde_json::from_value(json!({"name":"fixture","modes":[
        {"name":"plan","role":"both","description":"Planning","prompt":"Read {{ literal }} only","permissions":[]},
        {"name":"build","role":"both","description":"Building","prompt":"Apply changes","permissions":[]}
    ],"tools":[],"variables":{}})).unwrap();
    state.install_agent_metadata(Some(agent));
    state.update(|snapshot| snapshot.selected_mode = Some("plan".into()));
    assert_eq!(state.view().unwrap()["mode"], "plan");
    assert_eq!(
        state.view().unwrap()["mode_prompt"],
        "Read {{ literal }} only"
    );
    state.update(|snapshot| snapshot.selected_mode = Some("build".into()));
    assert_eq!(
        state.model_variables().unwrap()["mode_prompt"],
        "Apply changes"
    );
    state.update(|snapshot| snapshot.selected_mode = None);
    assert!(state.view().unwrap().get("mode_prompt").is_none());
}

#[tokio::test]
async fn committed_references_are_readable_by_auditors_but_never_custom_extraction() {
    use microcrab_auditor::VariableView;
    let (state, _endpoint) = State::new(Limits::default()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    state
        .install_references(
            References::new(vec![
                DynReference::new(projection("skills", json!(["visible"]))).unwrap(),
            ])
            .unwrap(),
        )
        .unwrap();
    state.refresh(Boundary::SessionCreated).unwrap();
    assert_eq!(
        VariableView::get(&state, "skills"),
        Some(json!(["visible"]))
    );
    assert!(state.extract("skills").is_none());
    assert!(state.inject("skills", json!("forged")).await.is_err());
}

#[test]
fn preloaded_reference_replacement_is_atomic_bounded_and_preserves_other_kinds() {
    let state = State::new(Limits::default());
    state
        .install_references(
            References::new(vec![
                DynReference::new(projection("skills", json!(["old"]))).unwrap(),
                DynReference::new(projection("memory", json!(["keep"]))).unwrap(),
            ])
            .unwrap(),
        )
        .unwrap();
    state.refresh(Boundary::SessionCreated).unwrap();
    let failing = projection("skills", json!(["bad"]));
    failing.fail.store(true, Ordering::SeqCst);
    let before = state.view().unwrap();
    assert!(
        state
            .replace_reference(DynReference::new(failing).unwrap())
            .is_err()
    );
    assert_eq!(state.view().unwrap(), before);
    state
        .replace_reference(DynReference::new(projection("skills", json!(["new"]))).unwrap())
        .unwrap();
    assert_eq!(state.view().unwrap()["skills"], json!(["new"]));
    assert_eq!(state.view().unwrap()["memory"], json!(["keep"]));
    state
        .apply_change(VariablesChange {
            scoped_set: vec![],
            set: Map::from_iter([("collision".into(), json!(7))]),
            remove: vec![],
        })
        .unwrap();
    assert!(
        state
            .replace_reference(DynReference::new(projection("collision", json!("owned"))).unwrap())
            .is_err()
    );
    assert_eq!(state.extract("collision"), Some(json!(7)));
    let before = state.view().unwrap();
    state.update(|snapshot| snapshot.limits.max_context_bytes = 256);
    assert!(
        state
            .replace_reference(
                DynReference::new(projection("skills", json!("x".repeat(1024)))).unwrap()
            )
            .is_err()
    );
    assert_eq!(state.view().unwrap(), before);
    state.update(|snapshot| snapshot.limits = Limits::default());
    let empty = projection("skills", Value::Null);
    *empty.value.lock().unwrap() = None;
    state
        .replace_reference(DynReference::new(empty).unwrap())
        .unwrap();
    assert!(state.view().unwrap().get("skills").is_none());
    assert!(state.snapshot().references.owns("skills"));
    assert_eq!(state.view().unwrap()["memory"], json!(["keep"]));
    assert!(DynReference::new(projection("delegatable_agents", json!([]))).is_err());
}

fn policy_observed_state() -> (State, Arc<crate::policy::PermissionController>) {
    use crate::policy::PermissionController;
    let state = State::new(Limits::default());
    let controller = Arc::new(PermissionController::new(Arc::new(
        crate::runtime::tool::DenyAll,
    )));
    let tools = vec![("work".to_owned(), crate::runtime::tool::ToolOrigin::Wasm)];
    controller.configure_tools(tools.clone(), vec![]).unwrap();
    state.install_permission(controller.observe(tools));
    let agent: microcrab_agent_types::AgentInfo = serde_json::from_value(json!({
        "name":"fixture","modes":[
            {"name":"plan","role":"both","prompt":"Inspect only","permissions":[]},
            {"name":"build","role":"both","prompt":"Implement changes","permissions":[]}
        ],"tools":[],"variables":{}
    }))
    .unwrap();
    state.install_agent_metadata(Some(agent));
    controller.configure_mode("fixture", "plan", &[]).unwrap();
    state.update(|snapshot| snapshot.selected_mode = Some("plan".into()));
    (state, controller)
}

#[test]
fn mode_transition_after_state_capture_retries_instead_of_mixing_old_prompt_and_new_policy() {
    let (state, controller) = policy_observed_state();
    let mut captures = 0;
    let value = current_view(true, || {
        captures += 1;
        let captured = state.inner.lock().unwrap().clone();
        if captures == 1 {
            // Deterministically complete a mode switch after cloning State but
            // before the reader captures its policy witness.
            controller.configure_mode("fixture", "build", &[]).unwrap();
            let policy = controller.snapshot().unwrap();
            let mut config = policy.config;
            config.overrides = serde_json::from_value(
                json!([{"tool":{"kind":"exact","value":"work"},"action":"allow"}]),
            )
            .unwrap();
            controller.replace_user(policy.revision, config).unwrap();
            state.update(|snapshot| snapshot.selected_mode = Some("build".into()));
        }
        captured
    })
    .unwrap();
    assert_eq!(captures, 2);
    assert_eq!(value["mode"], "build");
    assert_eq!(value["mode_prompt"], "Implement changes");
    assert_eq!(value["permission"][0]["action"], "allow");
    assert!(state.view().unwrap().get("permission").is_none());
    assert!(state.view_all().unwrap().get("permission").is_none());
    let inspection = state.inspection().unwrap();
    assert!(inspection.values.get("permission").is_none());
    assert!(!inspection.descriptors.contains_key("permission"));
    assert!(crate::admission::variables::is_reserved_key("permission"));
}

#[test]
fn incomplete_mode_transition_exhausts_only_bounded_observations_with_explicit_conflict() {
    let (state, controller) = policy_observed_state();
    controller.configure_mode("fixture", "build", &[]).unwrap();
    let mut captures = 0;
    let error = current_view(false, || {
        captures += 1;
        state.inner.lock().unwrap().clone()
    })
    .unwrap_err();
    assert_eq!(captures, 3);
    assert!(matches!(error, Error::StateObservationConflict));
    assert_eq!(state.snapshot().selected_mode.as_deref(), Some("plan"));
    let diagnostic = error.tool_diagnostic();
    assert_eq!(
        diagnostic.code,
        crate::observation::diagnostics::DiagnosticCode::Conflict
    );
    assert_eq!(diagnostic.retryable, Some(true));
    state.update(|snapshot| snapshot.selected_mode = Some("build".into()));
    assert_eq!(state.view().unwrap()["mode_prompt"], "Implement changes");
}

#[test]
fn unavailable_policy_owner_is_not_retried_or_masked_as_transient_churn() {
    let (state, controller) = policy_observed_state();
    drop(controller);
    let mut captures = 0;
    let error = current_view(false, || {
        captures += 1;
        state.inner.lock().unwrap().clone()
    })
    .unwrap_err();
    assert_eq!(captures, 1);
    assert!(matches!(error, Error::Config(_)));
    assert_eq!(
        error.public_reason(),
        Some("permission interaction is unavailable")
    );
}

#[test]
fn staged_input_cannot_overwrite_a_mode_change_after_its_projection() {
    let (state, controller) = policy_observed_state();
    let original = state.preview();
    let candidate = original.preview();
    candidate
        .apply_change(VariablesChange {
            scoped_set: vec![],
            set: Map::from_iter([("input_only".into(), json!(7))]),
            remove: vec![],
        })
        .unwrap();
    candidate.update(|snapshot| {
        snapshot.turn_id = Some(1);
        snapshot.last_turn_id = 1;
    });
    candidate.model_variables().unwrap();
    controller.configure_mode("fixture", "build", &[]).unwrap();
    state.update(|snapshot| snapshot.selected_mode = Some("build".into()));
    assert!(!state.commit_input(&original, &candidate));
    assert!(state.extract("input_only").is_none());
    assert_eq!(state.snapshot().last_turn_id(), 0);
    assert_eq!(state.view().unwrap()["mode"], "build");
}

fn scoped(
    name: &str,
    value: Value,
    persistence: microcrab_protocol::VariablePersistence,
) -> VariablesChange {
    VariablesChange {
        scoped_set: vec![microcrab_protocol::ScopedVariableSet {
            name: name.into(),
            editors: None,
            value,
            visibility: microcrab_protocol::VariableVisibility::Invisible,
            persistence,
        }],
        ..Default::default()
    }
}

#[test]
fn scopes_are_one_name_with_pure_values_and_separate_metadata() {
    use microcrab_protocol::VariablePersistence::{Ephemeral, Persisted};
    let state = State::new(Limits::default());
    state
        .apply_change(scoped(
            "hidden",
            json!({"token":"persisted-fixture"}),
            Persisted,
        ))
        .unwrap();
    state
        .apply_change(scoped("temporary", json!(null), Ephemeral))
        .unwrap();
    assert!(state.view().unwrap().get("hidden").is_none());
    assert!(state.model_variables().unwrap().get("temporary").is_none());
    assert!(state.extract("hidden").is_none());
    assert_eq!(
        state.view_all().unwrap()["hidden"],
        json!({"token":"persisted-fixture"})
    );
    assert_eq!(state.view_all().unwrap()["temporary"], Value::Null);
    assert_eq!(
        state.variable_metadata().unwrap(),
        json!({"hidden":{"visibility":"invisible","persistence":"persisted","owner":"client","editors":["client"]},"temporary":{"visibility":"invisible","persistence":"ephemeral","owner":"client","editors":["client"]},"agent_init":{"visibility":"invisible","persistence":"persisted","owner":"runtime","editors":["runtime"]},"agent_initialization_pending":{"visibility":"invisible","persistence":"persisted","owner":"runtime","editors":["runtime"]}})
    );
    let revision = state.view().unwrap()["session"]["variables_revision"]
        .as_u64()
        .unwrap();
    state
        .apply_change(scoped(
            "hidden",
            json!({"token":"replacement-fixture"}),
            Ephemeral,
        ))
        .unwrap();
    assert_eq!(
        state.view().unwrap()["session"]["variables_revision"],
        revision + 1
    );
    state
        .apply_change(VariablesChange {
            set: Map::from_iter([("hidden".into(), json!(7))]),
            ..Default::default()
        })
        .unwrap();
    assert!(state.view().unwrap().get("hidden").is_none());
    assert_eq!(state.view_all().unwrap()["hidden"], 7);
    assert_eq!(
        state.variable_metadata().unwrap()["hidden"],
        json!({"visibility":"invisible","persistence":"ephemeral","owner":"client","editors":["client"]})
    );
    state
        .apply_change(VariablesChange {
            remove: vec!["temporary".into()],
            ..Default::default()
        })
        .unwrap();
    assert!(state.view_all().unwrap().get("temporary").is_none());
    assert!(
        state
            .variable_metadata()
            .unwrap()
            .get("temporary")
            .is_none()
    );
}

#[test]
fn invisible_writes_reject_reserved_reference_duplicate_invalid_scope_and_budget_atomically() {
    use microcrab_protocol::VariablePersistence::{Ephemeral, Persisted};
    let state = State::new(Limits::default());
    state
        .apply_change(scoped("retained", json!(1), Ephemeral))
        .unwrap();
    state
        .install_references(
            References::new(vec![
                DynReference::new(projection("skills", json!([]))).unwrap(),
            ])
            .unwrap(),
        )
        .unwrap();
    for persistence in [Persisted, Ephemeral] {
        for name in [
            "session",
            "time",
            "host",
            "agent",
            "executions",
            "skills",
            "execution_capacity",
        ] {
            let mut change = scoped(name, json!("forged"), persistence);
            change.remove.push("retained".into());
            assert!(state.apply_change(change).is_err());
            assert_eq!(state.view_all().unwrap()["retained"], 1);
        }
    }
    let mut duplicate = scoped("retained", json!(2), Persisted);
    duplicate.set.insert("retained".into(), json!(3));
    assert!(state.apply_change(duplicate).is_err());
    let mut invalid = scoped("retained", json!(2), Ephemeral);
    invalid.scoped_set[0].visibility = microcrab_protocol::VariableVisibility::Visible;
    assert!(state.apply_change(invalid).is_err());
    let mut oversized = scoped("large", json!("x".repeat(2 * 1024 * 1024)), Ephemeral);
    oversized.remove.push("retained".into());
    assert!(state.apply_change(oversized).is_err());
    assert_eq!(state.view_all().unwrap()["retained"], 1);
    assert_eq!(state.view().unwrap()["session"]["variables_revision"], 1);
}

#[test]
fn historical_execution_capacity_variable_decodes_without_overriding_live_capacity() {
    let state = State::new(Limits::default());
    assert!(DynReference::new(projection("execution_capacity", json!({}))).is_err());
    for (visibility, field) in [
        ("visible", "variables"),
        ("invisible", "invisible_variables"),
    ] {
        let mut historical = serde_json::to_value(state.recovery_snapshot()).unwrap();
        historical["variables"] = json!({});
        historical
            .as_object_mut()
            .unwrap()
            .remove("invisible_variables");
        historical[field] = json!({"execution_capacity": "old user value"});
        historical["variable_metadata"] = json!({"execution_capacity": {
            "visibility": visibility, "persistence": "persisted", "owner": "client", "editors": ["client"]
        }});
        let historical: SessionState = serde_json::from_value(historical).unwrap();
        let encoded = serde_json::to_vec(&historical).unwrap();
        let decoded: SessionState = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), encoded);
        state.replace(decoded);
        let inspection = state.inspection().unwrap();
        assert_eq!(
            inspection.values["execution_capacity"]["buffers"]["used"],
            0
        );
        assert_eq!(
            inspection.descriptors["execution_capacity"].kind,
            "framework"
        );
        assert!(!inspection.descriptors["execution_capacity"].editable);
        assert!(inspection.metadata.get("execution_capacity").is_none());
        assert!(
            state
                .variable_metadata()
                .unwrap()
                .get("execution_capacity")
                .is_none()
        );
        assert!(state.variables().get("execution_capacity").is_none());
        assert!(state.extract("execution_capacity").is_none());
        assert!(matches!(
            state.injection_view().lookup("execution_capacity"),
            crate::state::variables::InjectionKey::Missing
        ));
        assert_eq!(
            serde_json::to_value(state.recovery_snapshot()).unwrap()[field]["execution_capacity"],
            "old user value"
        );
    }
}

#[test]
fn persisted_scopes_roundtrip_without_ephemeral_and_legacy_bytes_stay_frozen() {
    use microcrab_protocol::VariablePersistence::{Ephemeral, Persisted};
    let state = State::new(Limits::default());
    state
        .apply_change(VariablesChange {
            set: Map::from_iter([("z_last".into(), json!(1)), ("a_first".into(), json!(2))]),
            ..Default::default()
        })
        .unwrap();
    let legacy = serde_json::to_vec(&state.snapshot()).unwrap();
    assert!(String::from_utf8_lossy(&legacy).contains("variable_metadata"));
    assert_eq!(
        serde_json::to_vec(&serde_json::from_slice::<SessionState>(&legacy).unwrap()).unwrap(),
        legacy
    );
    state
        .apply_change(scoped("hidden", json!("persisted-fixture"), Persisted))
        .unwrap();
    state
        .apply_change(scoped(
            "temporary",
            json!("never-durable-fixture"),
            Ephemeral,
        ))
        .unwrap();
    let bytes = serde_json::to_vec(&state.snapshot()).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("never-durable-fixture"));
    let decoded: SessionState = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        decoded.variables.get_all("hidden"),
        Some(&json!("persisted-fixture"))
    );
    assert!(decoded.variables.get_all("temporary").is_none());
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
    assert!(
        state
            .recovery_snapshot()
            .variables
            .get_all("temporary")
            .is_none()
    );
    assert_eq!(
        state.view_all().unwrap()["temporary"],
        "never-durable-fixture"
    );
    let selected = state.snapshot();
    state.replace(selected);
    assert!(state.view_all().unwrap().get("temporary").is_none());
    assert_eq!(state.view_all().unwrap()["hidden"], "persisted-fixture");
}

#[test]
fn initialization_evidence_is_invisible_persisted_and_owned_by_session() {
    use crate::runtime::journal::{AgentInitRecord, AgentInitStatus};
    let state = State::new(Limits::default());
    state.update(|snapshot| {
        snapshot.agent_initialization_pending = true;
        snapshot.agent_init.push(AgentInitRecord {
            version: 1,
            component: "a".repeat(64),
            generation: 1,
            attempt: 1,
            status: AgentInitStatus::Intent,
            cache_points: vec![],
        });
        // Historical custom entries must not shadow authoritative lifecycle facts.
        snapshot.variables = serde_json::from_value(json!({
            "agent_init": "legacy-shadow",
            "agent_initialization_pending": false,
        }))
        .unwrap();
    });
    for key in ["agent_init", "agent_initialization_pending"] {
        assert!(state.model_variables().unwrap().get(key).is_none());
        assert!(state.view().unwrap().get(key).is_none());
        assert_eq!(
            state.variable_metadata().unwrap()[key],
            json!({
                "visibility":"invisible", "persistence":"persisted",
                "owner":"runtime", "editors":["runtime"]
            })
        );
        assert!(
            state
                .apply_change(VariablesChange {
                    set: Map::from_iter([(key.into(), json!("override"))]),
                    ..Default::default()
                })
                .is_err()
        );
    }
    let all = state.view_all().unwrap();
    assert_eq!(all["agent_initialization_pending"], true);
    assert_eq!(all["agent_init"][0]["status"]["status"], "intent");
    let encoded = serde_json::to_value(state.recovery_snapshot()).unwrap();
    assert_eq!(encoded["agent_initialization_pending"], true);
    assert_eq!(encoded["agent_init"], all["agent_init"]);
    state.update(|snapshot| snapshot.agent_initialization_pending = false);
    assert_eq!(
        state.view_all().unwrap()["agent_initialization_pending"],
        false
    );
}

#[test]
fn invisible_only_candidate_revision_prevents_a_stale_input_commit() {
    use microcrab_protocol::VariablePersistence::Ephemeral;
    let state = State::new(Limits::default());
    let original = state.preview();
    let candidate = original.preview();
    candidate
        .apply_change(scoped("input", json!(1), Ephemeral))
        .unwrap();
    state
        .apply_change(scoped("concurrent", json!(2), Ephemeral))
        .unwrap();
    assert!(!state.commit_input(&original, &candidate));
    assert!(state.view_all().unwrap().get("input").is_none());
    assert_eq!(state.view_all().unwrap()["concurrent"], 2);
    assert!(
        State::new(Limits::default())
            .view_all()
            .unwrap()
            .get("concurrent")
            .is_none()
    );
}

#[tokio::test]
async fn governed_editors_are_value_only_and_batches_reject_atomically() {
    use microcrab_protocol::{VariablePersistence, VariableRole};
    use std::sync::atomic::AtomicBool;
    let (client, _endpoint) = State::new(Limits::default()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let agent = client.for_agent();
    let mut create = scoped("private", json!(1), VariablePersistence::Ephemeral);
    create.scoped_set[0].editors = Some(vec![VariableRole::Agent, VariableRole::Agent]);
    client.change(create.clone()).await.unwrap();
    let metadata = client.variable_metadata().unwrap()["private"].clone();
    assert_eq!(metadata["owner"], "client");
    assert_eq!(metadata["editors"], json!(["client", "agent"]));
    agent.inject("private", json!(2)).await.unwrap();
    assert_eq!(client.variable_metadata().unwrap()["private"], metadata);
    let mut same = create.clone();
    same.scoped_set[0].value = json!(3);
    agent.change(same).await.unwrap();
    let before = client.inspection().unwrap();
    let mut denied = scoped("private", json!(4), VariablePersistence::Persisted);
    denied.set.insert("partial".into(), json!(true));
    assert!(matches!(
        agent.change(denied).await,
        Err(Error::VariableForbidden(_))
    ));
    assert_eq!(client.inspection().unwrap().witness, before.witness);
    assert!(client.view_all().unwrap().get("partial").is_none());
    assert_eq!(client.view_all().unwrap()["private"], 3);
    assert!(matches!(
        agent.remove("private").await,
        Err(Error::VariableForbidden(_))
    ));
    let mut revoke = create;
    revoke.scoped_set[0].editors = Some(vec![]);
    client.change(revoke).await.unwrap();
    assert!(matches!(
        agent.inject("private", json!(9)).await,
        Err(Error::VariableForbidden(_))
    ));
    agent.inject("agent_owned", json!(7)).await.unwrap();
    let facts = client.inspection().unwrap();
    assert_eq!(facts.metadata["agent_owned"]["owner"], "agent");
    assert!(!facts.descriptors["agent_owned"].editable);
    assert!(!facts.descriptors["agent_owned"].owner_editable);
    assert!(matches!(
        client.inject("agent_owned", json!(8)).await,
        Err(Error::VariableForbidden(_))
    ));
    // Internal Runtime authority has no custom-variable superuser exemption.
    let mut runtime = client.clone();
    runtime.actor = VariableRole::Runtime;
    assert!(matches!(
        runtime.inject("agent_owned", json!(8)).await,
        Err(Error::VariableForbidden(_))
    ));
}

#[tokio::test]
async fn checked_queue_revalidates_witness_and_history_replacement_fences_aba() {
    use std::sync::atomic::AtomicBool;
    let (state, endpoint) = State::new(Limits::default()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let initial = state.inspection().unwrap();
    let held = endpoint.fence().await.unwrap();
    let first = state.inject("concurrent", json!(1));
    tokio::pin!(first);
    assert!(futures_util::poll!(&mut first).is_pending());
    let checked = state.change_checked(
        VariablesChange {
            set: Map::from_iter([("draft".into(), json!(2))]),
            ..Default::default()
        },
        initial.witness,
    );
    tokio::pin!(checked);
    assert!(futures_util::poll!(&mut checked).is_pending());
    drop(held);
    first.await.unwrap();
    assert!(matches!(checked.await, Err(Error::VariableConflict)));
    assert!(state.extract("draft").is_none());
    let before = state.inspection().unwrap().witness;
    let commit = state
        .change_checked(VariablesChange::default(), before.clone())
        .await
        .unwrap();
    assert!(!commit.changed);
    assert_eq!(commit.before, before);
    assert_eq!(commit.before, commit.after);
    let frozen = state.snapshot();
    state.replace(frozen);
    let after = state.inspection().unwrap().witness;
    assert_ne!(before.runtime_instance_id, after.runtime_instance_id);
    assert!(
        after.variables_revision.parse::<u64>().unwrap()
            > before.variables_revision.parse::<u64>().unwrap()
    );
    assert!(matches!(
        state
            .change_checked(VariablesChange::default(), before)
            .await,
        Err(Error::VariableConflict)
    ));
    assert_ne!(
        State::new(Limits::default())
            .inspection()
            .unwrap()
            .witness
            .runtime_instance_id,
        after.runtime_instance_id
    );
}

#[test]
fn persisted_metadata_rejects_partial_governance_in_both_value_scopes() {
    for visibility in ["visible", "invisible"] {
        let mut wire = serde_json::to_value(SessionState::new(Limits::default())).unwrap();
        if visibility == "visible" {
            wire["variables"] = json!({"governed":1});
        } else {
            wire["invisible_variables"] = json!({"governed":1});
        }
        for partial in [
            json!({"visibility":visibility,"persistence":"persisted","owner":"agent"}),
            json!({"visibility":visibility,"persistence":"persisted","editors":["agent"]}),
        ] {
            wire["variable_metadata"] = json!({"governed":partial});
            let error = serde_json::from_value::<SessionState>(wire.clone()).unwrap_err();
            assert!(error.to_string().contains("both owner and editors"));
        }
        wire["variable_metadata"] =
            json!({"governed":{"visibility":visibility,"persistence":"persisted"}});
        let legacy: SessionState = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            legacy.variables.metadata()["governed"].owner,
            VariableRole::Client
        );
        assert_eq!(
            legacy.variables.metadata()["governed"].editors,
            vec![VariableRole::Client, VariableRole::Agent]
        );
        wire["variable_metadata"] = json!({"governed":{"visibility":visibility,"persistence":"persisted","owner":"agent","editors":[]}});
        let owner_only: SessionState = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            owner_only.variables.metadata()["governed"].editors,
            vec![VariableRole::Agent]
        );
        let encoded = json!({"visibility":visibility,"persistence":"persisted","owner":"agent","editors":["client","client"]});
        wire["variable_metadata"] = json!({"governed":encoded});
        let frozen: SessionState = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            frozen.variables.metadata()["governed"].owner,
            VariableRole::Agent
        );
        assert_eq!(
            frozen.variables.metadata()["governed"].editors,
            vec![VariableRole::Client, VariableRole::Agent]
        );
        // Canonical authority does not rewrite the deliberately unnormalized bytes.
        assert_eq!(
            serde_json::to_value(&frozen).unwrap()["variable_metadata"]["governed"],
            encoded
        );
        let exact = serde_json::to_vec(&frozen).unwrap();
        let decoded: SessionState = serde_json::from_slice(&exact).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), exact);
    }
}

#[test]
fn frozen_legacy_metadata_roundtrips_before_governance_activation() {
    let baseline = SessionState::new(Limits::default());
    let mut wire = serde_json::to_value(baseline).unwrap();
    wire["variables"] = json!({"z_last":1,"a_first":2});
    wire["invisible_variables"] = json!({"hidden":"frozen-fixture"});
    wire["variable_metadata"] =
        json!({"hidden":{"visibility":"invisible","persistence":"persisted"}});
    let frozen: SessionState = serde_json::from_value(wire).unwrap();
    let exact = serde_json::to_vec(&frozen).unwrap();
    let decoded: SessionState = serde_json::from_slice(&exact).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), exact);
    assert!(!String::from_utf8_lossy(&exact).contains("\"owner\""));
    assert_eq!(
        decoded.variables.metadata()["z_last"].editors,
        vec![VariableRole::Client, VariableRole::Agent]
    );
    let state = State::new(Limits::default());
    state.replace(decoded);
    assert_eq!(
        state.variable_metadata().unwrap()["hidden"]["editors"],
        json!(["client", "agent"])
    );
    let active = serde_json::to_vec(&state.recovery_snapshot()).unwrap();
    assert!(String::from_utf8_lossy(&active).contains("\"owner\""));
    assert_eq!(serde_json::to_vec(&frozen).unwrap(), exact);
    let restored: SessionState = serde_json::from_slice(&active).unwrap();
    assert_eq!(
        restored.variables.metadata(),
        state.snapshot().variables.metadata()
    );
}

#[tokio::test]
async fn native_tool_constructor_brands_public_state_as_agent() {
    let (client, _endpoint) = State::new(Limits::default()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    client.inject("client_owned", json!(1)).await.unwrap();
    let context = crate::runtime::tool::ToolContext::new(
        client.clone(),
        client.processes(),
        tokio_util::sync::CancellationToken::new(),
        "call".into(),
        "native".into(),
        "session".into(),
        "agent".into(),
        None,
    );
    assert!(matches!(
        context.state.inject("client_owned", json!(2)).await,
        Err(Error::VariableForbidden(_))
    ));
    context.state.inject("tool_owned", json!(3)).await.unwrap();
    assert_eq!(
        client.variable_metadata().unwrap()["tool_owned"]["owner"],
        "agent"
    );
    for forged in [
        json!({"set":{"x":1},"role":"runtime"}),
        json!({"scoped_set":[{"name":"x","value":1,"visibility":"visible","persistence":"persisted","owner":"runtime"}]}),
    ] {
        assert!(serde_json::from_value::<VariablesChange>(forged).is_err());
    }
    assert!(
        serde_json::from_value::<VariableWitness>(
            json!({"runtime_instance_id":"a","variables_revision":"0","role":"runtime"})
        )
        .is_err()
    );
}

#[test]
fn effective_ephemeral_plain_set_receipts_keep_exact_original_authority_bytes() {
    use microcrab_protocol::VariablePersistence::Ephemeral;
    let state = State::new(Limits::default());
    state
        .apply_change(scoped("private", json!("initial"), Ephemeral))
        .unwrap();
    let mut input = crate::SessionInput::text("opaque input");
    input
        .variables
        .set
        .insert("private".into(), json!("private-plain-set-fixture"));
    input.variables.set.insert("durable".into(), json!(42));
    let exact = serde_json::to_vec(&input).unwrap();
    let receipt = state.receipt_change(&input.variables);
    assert!(
        !serde_json::to_string(&receipt)
            .unwrap()
            .contains("private-plain-set-fixture")
    );
    assert_eq!(receipt.set["durable"], 42);
    let mut hook = VariablesChange::default();
    hook.set
        .insert("private".into(), json!("hook-private-fixture"));
    let mut merged = input.variables.clone();
    merged.overlay(&hook);
    state.apply_change(merged.clone()).unwrap();
    assert_eq!(state.view_all().unwrap()["private"], "hook-private-fixture");
    assert!(state.view().unwrap().get("private").is_none());
    assert!(
        !serde_json::to_string(&state.receipt_change(&merged))
            .unwrap()
            .contains("hook-private-fixture")
    );
    assert!(
        !serde_json::to_string(&state.recovery_snapshot())
            .unwrap()
            .contains("hook-private-fixture")
    );
    assert_eq!(serde_json::to_vec(&input).unwrap(), exact);
}
