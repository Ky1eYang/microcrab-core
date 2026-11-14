//! Runtime-owned policy publication and object-safe policy/interaction ports.
mod admission;
mod grants;
mod interaction;
pub use admission::AdmittedInvocation;
pub use interaction::{
    AskAttribution, FutureGrantAdoption, FutureGrantPersistence, FutureGrantProposal,
    PermissionInteraction,
};
mod profiles;
mod projection;
#[cfg(test)]
mod tests;
use async_trait::async_trait;
pub use microcrab_permission::*;
pub(crate) use projection::PermissionObservation;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, thiserror::Error)]
pub enum PermissionError {
    #[error("invalid permission configuration: {0}")]
    InvalidConfiguration(String),
    #[error("permission interaction is unavailable")]
    Unavailable,
    #[error("permission review failed")]
    Review,
    #[error("permission operation cancelled")]
    Cancelled,
    #[error("permission deadline exceeded")]
    Deadline,
    #[error("permission policy changed")]
    StalePolicy,
    #[error("permission denied by controller {controller} revision {revision}: {reason}")]
    PolicyDenied {
        controller: u64,
        revision: u64,
        reason: String,
    },
    #[error("admitted invocation has already entered tool execution")]
    AlreadyDispatched,
    #[error("invocation admission proof mismatch: {0}")]
    ProofMismatch(&'static str),
    #[error("permission policy persistence outcome is uncertain: {0}")]
    PersistenceUncertain(String),
    #[error("permission denied")]
    Denied,
}

impl PermissionError {
    pub(crate) fn into_runtime_error(self) -> crate::Error {
        use crate::observation::diagnostics::{
            DiagnosticCode, DiagnosticLayer, DiagnosticPhase, SafeDiagnosticReason, ToolDiagnostic,
        };
        match self {
            Self::Denied
            | Self::PolicyDenied { .. }
            | Self::AlreadyDispatched
            | Self::ProofMismatch(_) => {
                let mut diagnostic = ToolDiagnostic::new(
                    DiagnosticCode::PermissionDenied,
                    DiagnosticLayer::Runtime,
                    DiagnosticPhase::Authorization,
                );
                diagnostic.reason = Some(SafeDiagnosticReason::new(&self.to_string()));
                crate::Error::Diagnostic(diagnostic)
            }
            Self::Cancelled => crate::Error::Cancelled,
            other => crate::Error::config_with_private(other.public_reason(), other.to_string()),
        }
    }
    pub fn public_reason(&self) -> &'static str {
        match self {
            Self::InvalidConfiguration(_) => "permission configuration is invalid",
            Self::Unavailable => "permission interaction is unavailable",
            Self::PersistenceUncertain(_) => {
                "permission policy persistence outcome is uncertain; Session requires recovery"
            }
            Self::Review => "permission review failed",
            Self::Cancelled => "permission operation cancelled",
            Self::Deadline => "permission deadline exceeded",
            Self::AlreadyDispatched => "admitted invocation has already entered tool execution",
            Self::StalePolicy => "permission policy changed",
            Self::ProofMismatch(reason) => reason,
            Self::Denied | Self::PolicyDenied { .. } => "permission denied by Session policy",
        }
    }
}

/// Compatibility interaction port. Runtime resolves shared declarative rules;
/// custom evaluate implementations cannot grant unknown tools or override layers.
#[async_trait]
pub trait SessionPermission: Send + Sync {
    /// One-time native assembly compatibility. These are explicit user grants,
    /// not asynchronous evaluation fallback and never guest-owned configuration.
    fn initial_overrides(&self) -> Vec<Permission> {
        Vec::new()
    }
    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError>;
    /// Retained for source compatibility; all model hints now use Runtime's
    /// declarative resolver without calling this adapter or interaction methods.
    fn declarative_policy(&self) -> bool {
        false
    }
    fn features(&self) -> PermissionFeatures {
        PermissionFeatures::default()
    }
    async fn open_interaction(
        &self,
        _: &PermissionRequest,
        _: &AskAttribution,
    ) -> Result<Option<Arc<dyn PermissionInteraction>>, PermissionError> {
        Ok(None)
    }
    /// Called only after the invocation's dispatch acknowledgement. Transport
    /// starts its independent deadline; adoption remains controller-owned.
    async fn propose_future_grant(
        &self,
        _: FutureGrantProposal,
        _: Arc<dyn FutureGrantAdoption>,
    ) -> Result<(), PermissionError> {
        Err(PermissionError::Unavailable)
    }
    async fn ask(&self, _: &PermissionRequest) -> Result<AskReply, PermissionError> {
        Err(PermissionError::Unavailable)
    }
}

#[derive(Clone)]
struct ReviewServices {
    client: Arc<dyn crate::Client>,
    review_client: Option<Arc<dyn crate::Client>>,
    one_shot_auditor: Arc<dyn crate::Auditor>,
    one_shot_executor: Option<Arc<dyn crate::runtime::subagents::OneShotExecutor>>,
}

/// One controller belongs to one Runtime/Session. Children retain a live parent ceiling.
pub struct PermissionController {
    identity: u64,
    policy: RwLock<PolicySnapshot>,
    tools: RwLock<Vec<(String, crate::runtime::tool::ToolOrigin)>>,
    provider_defaults: RwLock<Vec<Permission>>,
    parent: RwLock<Option<Arc<PermissionController>>>,
    adapter: Arc<dyn SessionPermission>,
    services: RwLock<Option<ReviewServices>>,
    session_id: RwLock<Option<String>>,
    session_closed: RwLock<Option<Arc<std::sync::atomic::AtomicBool>>>,
    grant_persistence: RwLock<Option<Arc<dyn FutureGrantPersistence>>>,
    grant_reserved: std::sync::atomic::AtomicBool,
    grant_faulted: std::sync::atomic::AtomicBool,
    changed: tokio::sync::watch::Sender<()>,
    lifetime: RwLock<CancellationToken>,
}
impl PermissionController {
    pub(crate) fn compiled_inventory(
        &self,
        snapshot: &PolicySnapshot,
        tools: &[(String, crate::runtime::tool::ToolOrigin)],
    ) -> Result<serde_json::Value, PermissionError> {
        projection::project_tools(self, snapshot, tools)
    }
    pub(crate) fn observe(
        self: &Arc<Self>,
        tools: Vec<(String, crate::runtime::tool::ToolOrigin)>,
    ) -> PermissionObservation {
        PermissionObservation::new(self, tools)
    }

    pub fn new(adapter: Arc<dyn SessionPermission>) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self {
            identity: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            policy: RwLock::new(PolicySnapshot {
                revision: 1,
                config: PermissionConfig::default(),
            }),
            tools: RwLock::new(Vec::new()),
            provider_defaults: RwLock::new(Vec::new()),
            parent: RwLock::new(None),
            adapter,
            services: RwLock::new(None),
            session_id: RwLock::new(None),
            session_closed: RwLock::new(None),
            grant_persistence: RwLock::new(None),
            grant_reserved: std::sync::atomic::AtomicBool::new(false),
            grant_faulted: std::sync::atomic::AtomicBool::new(false),
            changed: tokio::sync::watch::channel(()).0,
            lifetime: RwLock::new(CancellationToken::new()),
        }
    }
    pub(crate) fn tool_config(
        &self,
        tools: &[(String, crate::runtime::tool::ToolOrigin)],
        provider: &[Permission],
        guest: &[Permission],
    ) -> Result<PolicySnapshot, PermissionError> {
        let current = self.snapshot()?;
        self.tool_config_for_mode(
            tools,
            provider,
            guest,
            &current.config.agent,
            &current.config.mode,
        )
    }
    pub(crate) fn inventory_mode(
        &self,
        tools: &[(String, crate::runtime::tool::ToolOrigin)],
        guest: &[Permission],
        agent: &str,
        mode: &str,
    ) -> Result<PolicySnapshot, PermissionError> {
        let provider = self
            .provider_defaults
            .read()
            .map_err(|_| PermissionError::Denied)?
            .clone();
        self.tool_config_for_mode(tools, &provider, guest, agent, mode)
    }
    pub(crate) fn tool_config_for_mode(
        &self,
        tools: &[(String, crate::runtime::tool::ToolOrigin)],
        provider: &[Permission],
        guest: &[Permission],
        agent: &str,
        mode: &str,
    ) -> Result<PolicySnapshot, PermissionError> {
        let mut snapshot = self.snapshot()?;
        snapshot.config.agent = agent.into();
        snapshot.config.mode = mode.into();
        snapshot.config.provider_defaults = provider.to_vec();
        snapshot
            .config
            .provider_defaults
            .extend(profiles::guest(tools, guest));
        snapshot.config.defaults =
            profiles::native(tools, &snapshot.config.agent, &snapshot.config.mode);
        validate(&snapshot.config)
            .map_err(|error| PermissionError::InvalidConfiguration(error.to_string()))?;
        Ok(snapshot)
    }
    pub(crate) fn publish_tools(
        &self,
        tools: Vec<(String, crate::runtime::tool::ToolOrigin)>,
        provider: Vec<Permission>,
        snapshot: PolicySnapshot,
    ) -> Result<(), PermissionError> {
        validate(&snapshot.config)
            .map_err(|error| PermissionError::InvalidConfiguration(error.to_string()))?;
        let mut policy = self.policy.write().map_err(|_| PermissionError::Denied)?;
        self.ensure_not_reserved()?;
        let mut registered = self.tools.write().map_err(|_| PermissionError::Denied)?;
        let mut defaults = self
            .provider_defaults
            .write()
            .map_err(|_| PermissionError::Denied)?;
        if policy.revision != snapshot.revision {
            return Err(PermissionError::StalePolicy);
        }
        let revision = policy
            .revision
            .checked_add(1)
            .ok_or_else(|| PermissionError::InvalidConfiguration("revision exhausted".into()))?;
        *policy = PolicySnapshot {
            revision,
            config: snapshot.config,
        };
        *registered = tools;
        *defaults = provider;
        self.changed.send_replace(());
        Ok(())
    }
    pub(crate) fn configure_tools(
        &self,
        tools: Vec<(String, crate::runtime::tool::ToolOrigin)>,
        provider: Vec<Permission>,
    ) -> Result<(), PermissionError> {
        let snapshot = self.tool_config(&tools, &provider, &[])?;
        self.publish_tools(tools, provider, snapshot)
    }
    pub(crate) fn configure_mode(
        &self,
        agent: &str,
        mode: &str,
        guest: &[Permission],
    ) -> Result<(), PermissionError> {
        self.configure_mode_at(self.snapshot()?.revision, agent, mode, guest)
    }
    pub(crate) fn configure_mode_at(
        &self,
        expected_revision: u64,
        agent: &str,
        mode: &str,
        guest: &[Permission],
    ) -> Result<(), PermissionError> {
        let tools = self
            .tools
            .read()
            .map_err(|_| PermissionError::Denied)?
            .clone();
        let mut snapshot = self.snapshot()?;
        if snapshot.revision != expected_revision {
            return Err(PermissionError::StalePolicy);
        }
        snapshot.config.agent = agent.into();
        snapshot.config.mode = mode.into();
        snapshot.config.provider_defaults = self
            .provider_defaults
            .read()
            .map_err(|_| PermissionError::Denied)?
            .clone();
        snapshot
            .config
            .provider_defaults
            .extend(profiles::guest(&tools, guest));
        snapshot.config.defaults = profiles::native(&tools, agent, mode);
        self.replace(snapshot.revision, snapshot.config)?;
        Ok(())
    }
    pub(crate) fn replace_user(
        &self,
        expected_revision: u64,
        config: PermissionConfig,
    ) -> Result<PolicySnapshot, PermissionError> {
        let current = self.snapshot()?;
        if config.agent != current.config.agent
            || config.mode != current.config.mode
            || config.defaults != current.config.defaults
            || config.provider_defaults != current.config.provider_defaults
        {
            return Err(PermissionError::InvalidConfiguration(
                "native and provider policy is read-only; use Session mode selection".into(),
            ));
        }
        self.replace(expected_revision, config)
    }
    /// Publish the target and policy under the policy lock; readers pin both to
    /// one revision before awaiting. Client construction remains Creator-owned.
    pub(crate) fn replace_user_with_review(
        &self,
        expected_revision: u64,
        config: PermissionConfig,
        review_client: Option<Arc<dyn crate::Client>>,
    ) -> Result<PolicySnapshot, PermissionError> {
        validate(&config)
            .map_err(|error| PermissionError::InvalidConfiguration(error.to_string()))?;
        let mut policy = self.policy.write().map_err(|_| PermissionError::Denied)?;
        self.ensure_not_reserved()?;
        self.ensure_not_reserved()?;
        if policy.revision != expected_revision {
            return Err(PermissionError::StalePolicy);
        }
        if config.agent != policy.config.agent
            || config.mode != policy.config.mode
            || config.defaults != policy.config.defaults
            || config.provider_defaults != policy.config.provider_defaults
            || config.review_model.is_some() != review_client.is_some()
        {
            return Err(PermissionError::InvalidConfiguration(
                "review target must be assembled and native policy is read-only".into(),
            ));
        }
        let revision = policy
            .revision
            .checked_add(1)
            .ok_or_else(|| PermissionError::InvalidConfiguration("revision exhausted".into()))?;
        let mut services = self.services.write().map_err(|_| PermissionError::Review)?;
        let current = services.as_mut().ok_or(PermissionError::Review)?;
        current.review_client = review_client;
        *policy = PolicySnapshot { revision, config };
        self.changed.send_replace(());
        Ok(policy.clone())
    }
    /// A coherent assembly witness used by inherited child policy and rollback.
    pub fn review_binding(
        &self,
    ) -> Result<(PolicySnapshot, Option<Arc<dyn crate::Client>>), PermissionError> {
        let policy = self.policy.read().map_err(|_| PermissionError::Denied)?;
        let services = self.services.read().map_err(|_| PermissionError::Review)?;
        Ok((
            policy.clone(),
            services
                .as_ref()
                .and_then(|services| services.review_client.clone()),
        ))
    }
    pub(crate) fn bind(
        &self,
        client: Arc<dyn crate::Client>,
        auditor: Arc<dyn crate::Auditor>,
    ) -> Result<(), PermissionError> {
        *self.services.write().map_err(|_| PermissionError::Review)? = Some(ReviewServices {
            client,
            review_client: None,
            one_shot_auditor: auditor,
            one_shot_executor: None,
        });
        Ok(())
    }
    pub(crate) fn bind_one_shot_executor(
        &self,
        executor: Arc<dyn crate::runtime::subagents::OneShotExecutor>,
    ) -> Result<(), PermissionError> {
        let mut services = self.services.write().map_err(|_| PermissionError::Review)?;
        services
            .as_mut()
            .ok_or(PermissionError::Review)?
            .one_shot_executor = Some(executor);
        Ok(())
    }
    pub(crate) fn bind_one_shot_auditor(
        &self,
        auditor: Arc<dyn crate::Auditor>,
    ) -> Result<(), PermissionError> {
        let mut services = self.services.write().map_err(|_| PermissionError::Review)?;
        services
            .as_mut()
            .ok_or(PermissionError::Review)?
            .one_shot_auditor = auditor;
        Ok(())
    }
    pub fn snapshot(&self) -> Result<PolicySnapshot, PermissionError> {
        self.policy
            .read()
            .map(|p| p.clone())
            .map_err(|_| PermissionError::InvalidConfiguration("policy lock unavailable".into()))
    }
    pub fn features(&self) -> PermissionFeatures {
        self.adapter.features()
    }
    pub(crate) fn replace(
        &self,
        expected_revision: u64,
        config: PermissionConfig,
    ) -> Result<PolicySnapshot, PermissionError> {
        validate(&config).map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?;
        let mut policy = self
            .policy
            .write()
            .map_err(|_| PermissionError::InvalidConfiguration("policy lock unavailable".into()))?;
        self.ensure_not_reserved()?;
        if policy.revision != expected_revision {
            return Err(PermissionError::StalePolicy);
        }
        if config.review_model != policy.config.review_model {
            return Err(PermissionError::InvalidConfiguration(
                "review model changes require an assembled client".into(),
            ));
        }
        let revision = policy
            .revision
            .checked_add(1)
            .ok_or_else(|| PermissionError::InvalidConfiguration("revision exhausted".into()))?;
        *policy = PolicySnapshot { revision, config };
        self.changed.send_replace(());
        Ok(policy.clone())
    }
    pub(crate) fn ensure_parent(
        &self,
        parent: Arc<PermissionController>,
    ) -> Result<(), PermissionError> {
        let existing = self
            .parent
            .read()
            .map_err(|_| PermissionError::Denied)?
            .clone();
        match existing {
            Some(current) if Arc::ptr_eq(&current, &parent) => Ok(()),
            Some(_) => Err(PermissionError::InvalidConfiguration(
                "prepared child has a different permission parent".into(),
            )),
            None => self.set_parent(parent),
        }
    }
    /// Bind once, before publishing a child. A ceiling is never replaced by a copied allowlist.
    pub(crate) fn set_parent(
        &self,
        parent: Arc<PermissionController>,
    ) -> Result<(), PermissionError> {
        let _policy = self.policy.read().map_err(|_| PermissionError::Denied)?;
        self.ensure_not_reserved()?;
        let mut slot = self.parent.write().map_err(|_| {
            PermissionError::InvalidConfiguration("ceiling lock unavailable".into())
        })?;
        if slot.is_some() || std::ptr::eq(self, Arc::as_ptr(&parent)) {
            return Err(PermissionError::InvalidConfiguration(
                "invalid delegation ceiling".into(),
            ));
        }
        // Reject cycles and bound ancestor evaluation work.
        let mut cursor = Some(parent.clone());
        let mut depth = 0;
        while let Some(current) = cursor {
            depth += 1;
            if depth > 16 || std::ptr::eq(self, Arc::as_ptr(&current)) {
                return Err(PermissionError::InvalidConfiguration(
                    "delegation ceiling cycle or depth".into(),
                ));
            }
            cursor = current
                .parent
                .try_read()
                .map_err(|_| {
                    PermissionError::InvalidConfiguration("ceiling busy or unavailable".into())
                })?
                .clone();
        }
        *slot = Some(parent);
        self.changed.send_replace(());
        Ok(())
    }
    pub(crate) fn lineage(
        &self,
    ) -> Result<Vec<(Arc<PermissionController>, PolicySnapshot)>, PermissionError> {
        let mut result = vec![];
        let mut cursor = self
            .parent
            .read()
            .map_err(|_| PermissionError::Denied)?
            .clone();
        while let Some(parent) = cursor {
            if result.len() >= 16 {
                return Err(PermissionError::InvalidConfiguration(
                    "controller ancestry depth budget (16 ancestors) exceeded".into(),
                ));
            }
            let snapshot = parent.snapshot()?;
            cursor = parent
                .parent
                .read()
                .map_err(|_| PermissionError::Denied)?
                .clone();
            result.push((parent, snapshot));
        }
        Ok(result)
    }
    /// Admission has evaluated this exact owner's Ask under the full chain
    /// budget. Resolve the interaction without compiling or copying it again.
    async fn resolve_ask(
        &self,
        snapshot: &PolicySnapshot,
        request: &PermissionRequest,
        state: &crate::State,
        issuer: &crate::context::Content,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            Action,
            Option<FutureGrant>,
            Option<Arc<dyn PermissionInteraction>>,
        ),
        PermissionError,
    > {
        let services = {
            let current = self.policy.read().map_err(|_| PermissionError::Denied)?;
            if current.revision != snapshot.revision {
                return Err(PermissionError::StalePolicy);
            }
            self.services
                .read()
                .map_err(|_| PermissionError::Review)?
                .clone()
        };
        if !self.adapter.features().interactive_ask {
            return Ok((Action::Deny, None, None));
        }
        let attribution = AskAttribution {
            controller_id: self.identity,
            owning_session_id: self
                .owning_session_id()
                .unwrap_or_else(|_| request.session_id.clone()),
        };
        // Opening the lease-backed interaction precedes review. No executor can
        // therefore be bypassed by even a valid automatic reviewer Allow.
        let interaction = match self.adapter.open_interaction(request, &attribution).await? {
            Some(port) => port,
            None => Arc::new(interaction::CallbackInteraction {
                adapter: self.adapter.clone(),
                request: request.clone(),
                deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(120),
            }),
        };
        let review_cancellation = cancellation.child_token();
        let _cancel_review = crate::utils::cancel::CancelOnDrop(review_cancellation.clone());
        let review_enabled = snapshot.config.review
            && services
                .as_ref()
                .is_some_and(|services| services.one_shot_executor.is_some());
        let review = async {
            if snapshot.config.review
                && let Some(ReviewServices {
                    client,
                    review_client,
                    one_shot_auditor,
                    one_shot_executor: Some(one_shot_executor),
                }) = services
            {
                crate::runtime::subagents::action_review::run(
                    one_shot_executor.as_ref(),
                    review_client.unwrap_or(client),
                    one_shot_auditor,
                    state,
                    issuer,
                    request,
                    &review_cancellation,
                )
                .await
            } else if snapshot.config.review {
                Err(PermissionError::Unavailable)
            } else {
                std::future::pending().await
            }
        };
        let user = interaction.user_reply();
        tokio::pin!(user, review);
        let initial_deadline = interaction.deadline();
        let mut review_done = false;
        let mut suggestion: Option<ReviewResult> = None;
        let mut deadline = initial_deadline;
        loop {
            tokio::select! { biased;
                _ = cancellation.cancelled() => return Err(PermissionError::Cancelled),
                answer = &mut user => {
                    review_cancellation.cancel();
                    if !review_done && review_enabled { let _ = (&mut review).await; }
                    let answer = answer?;
                    if answer.action == Action::Ask || answer.reason.len() > 512 { return Ok((Action::Deny, None, None)); }
                    return Ok((answer.action, None, Some(interaction.clone())));
                },
                result = &mut review, if !review_done => {
                    review_done = true;
                    let received_at=tokio::time::Instant::now();
                    if let Ok(result) = result && result.action != Action::Ask && received_at < initial_deadline {
                        validate_review_result(&result, request).map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?;
                        // Exactly one full window, even across the original expiry.
                        deadline = interaction.review(&result, received_at).await?;
                        suggestion = Some(result);
                    }
                },
                _ = tokio::time::sleep_until(deadline) => {
                    review_cancellation.cancel();
                    let (decision, grant) = match suggestion.take() {
                        Some(result) => (Decision::new(result.action, result.reason, "review"), result.future_grant),
                        None => (Decision::new(Action::Deny, "permission deadline exceeded", "timeout"), None),
                    };
                    let answer = interaction.settle(&decision).await?;
                    // A transport accepted user answer at the timer boundary wins;
                    // retaining a losing review proposal would manufacture authority.
                    let selected_review = answer.reason == "review_auto";
                    return Ok((answer.action, if selected_review && answer.action == Action::Allow { grant } else { None }, Some(interaction.clone())));
                }
            }
        }
    }
}
