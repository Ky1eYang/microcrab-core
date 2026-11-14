//! Validate static backends and bind a concrete State command endpoint.
use super::*;
use crate::runtime::execution::process::ProcessControl;

impl SessionInner {
    pub(crate) fn new(parts: SessionParts) -> Result<Arc<Self>, Error> {
        parts.limits.validate()?;
        if let Some((policy, _)) = &parts.auto_compaction {
            policy.validate()?;
        }
        let system = crate::context::system::render(&parts.system, parts.agent.as_ref())?;
        let context = Context::new(
            system,
            parts.runtime.definitions(),
            parts.limits.max_context_bytes,
        )?;
        let closed = Arc::new(AtomicBool::new(false));
        let faulted = Arc::new(AtomicBool::new(false));
        if parts.session_id.is_empty() || parts.session_id.contains('/') {
            return Err(Error::config("session id must be nonempty without slash"));
        }
        let process_parent_session_id = parts
            .processes_parent
            .as_ref()
            .map(|parent| parent.session_id().to_owned());
        let (process_registry, process_control) = if let Some(parent) = parts.processes_parent {
            let agent_name = parts
                .agent
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_else(|| "subagent".into());
            let registry = parent.registry();
            registry.register_session(&parts.session_id, &parts.limits)?;
            let scoped = parent.scoped(parts.session_id.clone(), agent_name, parts.is_main);
            (registry, scoped)
        } else {
            let registry = crate::runtime::execution::process::ProcessRegistry::new(
                parts.session_id.clone(),
                parts.host.clone(),
                parts.limits.clone(),
            );
            let agent_name = parts
                .agent
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_else(|| "main".into());
            let scoped = registry.scoped(parts.session_id.clone(), agent_name, parts.is_main);
            (registry, scoped)
        };
        let configured_limits = parts.limits.clone();
        let (state, endpoint) =
            State::new(parts.limits).bind_writer(closed.clone(), faulted.clone());
        #[cfg(feature = "wasm")]
        state.update(|snapshot| {
            snapshot.agent_initialization_pending = !parts.runtime.wasm_lifecycles().is_empty();
        });
        state.install_processes(process_control);
        state.install_catalog(parts.catalog.clone(), parts.is_main);
        state.install_agent_metadata(parts.agent.clone());
        state.update(|snapshot| snapshot.session_kind = Some(parts.kind));
        state.install_delegate_availability(parts.runtime.delegate_available());
        state.install_references(crate::state::reference::References::new(parts.references)?)?;
        let auditor = Arc::new(crate::observation::hooks::BoundAuditor {
            inner: parts.auditor,
            state: state.clone(),
            completed: Mutex::new(None),
        });
        let controller = parts.runtime.permission_controller();
        controller
            .bind_closed(closed.clone())
            .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
        controller
            .bind_session_id(&parts.session_id)
            .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
        state.install_permission(
            controller.observe(
                parts
                    .runtime
                    .definitions()
                    .into_iter()
                    .filter_map(|tool| {
                        parts
                            .runtime
                            .origin(&tool.name)
                            .map(|origin| (tool.name, origin))
                    })
                    .collect(),
            ),
        );
        controller
            .bind(parts.client.clone(), auditor.clone())
            .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
        controller
            .bind_one_shot_auditor(auditor.inner.clone())
            .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
        if let Some(agent) = &parts.agent {
            let snapshot = controller
                .snapshot()
                .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
            let selected = if snapshot.config.mode.is_empty() {
                agent.modes.iter().find(|mode| {
                    !matches!(
                        (parts.is_main, mode.role),
                        (true, microcrab_agent_types::AgentRole::SubAgent)
                            | (false, microcrab_agent_types::AgentRole::MainAgent)
                    )
                })
            } else {
                agent.modes.iter().find(|m| m.name == snapshot.config.mode)
            };
            if let Some(mode) = selected {
                if matches!(
                    (parts.is_main, mode.role),
                    (true, microcrab_agent_types::AgentRole::SubAgent)
                        | (false, microcrab_agent_types::AgentRole::MainAgent)
                ) {
                    return Err(Error::config(
                        "initial mode is ineligible for the Session Agent role",
                    ));
                }
                controller
                    .configure_mode(&agent.name, &mode.name, &mode.permissions)
                    .map_err(|error| {
                        Error::config_with_private(error.public_reason(), error.to_string())
                    })?;
                state.update(|snapshot| snapshot.selected_mode = Some(mode.name.clone()));
            } else if !agent.modes.is_empty() {
                return Err(Error::config("unknown selected agent mode"));
            }
        }
        if let Some(parent) = parts.permission_parent {
            controller.set_parent(parent).map_err(|error| {
                Error::config_with_private(error.public_reason(), error.to_string())
            })?;
        }
        if let Some(user) = parts.permissions {
            let current = controller.snapshot().map_err(|error| {
                Error::config_with_private(error.public_reason(), error.to_string())
            })?;
            let mut config = current.config;
            config.overrides = user.overrides;
            config.initialization = user.initialization;
            config.review = user.review;
            config.review_model = user.review_model;
            controller
                .replace_user_with_review(current.revision, config, parts.review_client)
                .map_err(|error| {
                    Error::config_with_private(error.public_reason(), error.to_string())
                })?;
        }
        let one_shots = Arc::new(functional::one_shot::SessionOneShot::default());
        controller
            .bind_one_shot_executor(one_shots.clone())
            .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
        state.refresh_preview(Boundary::SessionCreated)?;
        let inbox = Arc::new(crate::admission::inbox::Inbox::default());
        state.install_inbox(Arc::downgrade(&inbox));
        let notice_sink: Arc<dyn crate::runtime::execution::ExecutionNoticeSink> =
            Arc::new(super::insertion::SessionNoticeSink {
                inbox: inbox.clone(),
                state: state.clone(),
            });
        state
            .executions()
            .bind_notice_sink(Arc::downgrade(&notice_sink));
        let task_persistence_cancel = CancellationToken::new();
        controller
            .bind_lifetime(task_persistence_cancel.clone())
            .map_err(|e| Error::config_with_private(e.public_reason(), e.to_string()))?;
        Ok(Arc::new_cyclic(|weak: &std::sync::Weak<Self>| {
            let control: std::sync::Weak<dyn crate::runtime::tool::switch_mode::SessionControl> =
                weak.clone();
            parts.runtime.bind_controls(control);
            let parent: std::sync::Weak<dyn crate::runtime::tool::delegate::DelegateParent> =
                weak.clone();
            parts.runtime.bind_delegate_parent(parent);
            Self {
                kind: parts.kind,
                one_shot_claimed: AtomicBool::new(false),
                delegation_completion: Mutex::new(None),
                _notice_sink: notice_sink,
                observation: crate::observation::activity::ActivityPublisher::default(),
                inbox,
                inbox_worker_started: AtomicBool::new(false),
                explicit_activation: AtomicBool::new(false),
                inbox_validation_failed: AtomicBool::new(false),
                model_retry_ready: AtomicBool::new(false),
                replacing: AtomicBool::new(false),
                replacement: Arc::new(tokio::sync::Mutex::new(())),
                pending_tools: Mutex::new(None),
                configured_mode: Mutex::new((0, None)),
                pending_mode: Mutex::new(None),
                delegated_children: Mutex::new(std::collections::BTreeMap::new()),
                closed_children: Mutex::new(std::collections::VecDeque::new()),
                parent_relationship: Mutex::new(None),
                applied_tools: std::sync::atomic::AtomicU64::new(0),
                route: parts.route,
                client: parts.client,
                auto_compaction: Mutex::new(parts.auto_compaction),
                auto_compaction_interrupted: AtomicBool::new(false),
                round_limit_callback: parts.round_limit_callback,
                attachment_resolver: parts.attachment_resolver,
                processes: process_registry,
                is_main: parts.is_main,
                process_parent_session_id,
                state: state.clone(),
                configured_limits: Mutex::new(configured_limits),
                context: Mutex::new(context),
                live_context_measurement: Mutex::new(None),
                context_measurement_changes: tokio::sync::watch::channel(()).0,
                runtime: parts.runtime,
                auditor,
                rename: Mutex::new(None),
                one_shots,
                journal: parts.journal,
                active: Mutex::new(None),
                #[cfg(test)]
                command_registration_gate: Mutex::new(None),
                operation: Arc::new(tokio::sync::Mutex::new(())),
                foreground: Arc::new(tokio::sync::Mutex::new(())),
                task_persisted_generation: std::sync::atomic::AtomicU64::new(0),
                selected_checkpoint: Mutex::new(None),
                navigation_epoch: Mutex::new(None),
                started: AtomicBool::new(false),
                closed,
                faulted,
                host: parts.host,
                endpoint,
                agent: parts.agent,
                catalog: parts.catalog,
                continuation: Mutex::new(None),
                cache_report: Mutex::new(None),
                restored: AtomicBool::new(false),
                self_weak: weak.clone(),
                task_persistence_started: AtomicBool::new(false),
                task_persistence_cancel,
                close_result: Mutex::new(None),
            }
        }))
    }
}
