//! External dependency assembly for Session-owned delegation.
#[cfg(test)]
mod tests;
use crate::{Children, DeclarativePermission, UsageAuditor, UsageLedger};
use async_trait::async_trait;
use microcrab_agent_types::AgentInfo;
use microcrab_auditor::{Auditor, NoopAuditor};
use microcrab_host_api::Host;
pub use microcrab_session::{ChildFactory, ChildSession, OwnedDelegateExecutor};
use microcrab_session::{
    Client, Error, Limits, Session,
    execution::process::ProcessControl,
    policy::PermissionController,
    runtime::{BuiltinTool, Runtime, ToolRegistration, tool::Tool},
};
use microcrab_storage::CreatorJournal;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Native audit policy is injected; Python factories instead fork their callbacks.
/// Like ChildFactory::create, this future must release unpublished resources on
/// drop. It runs before the child Session lifecycle starts.
#[async_trait]
pub trait ChildAuditorFactory: Send + Sync {
    /// Native embeddings may retain the same typed owner for display attribution.
    /// Returning Some bypasses create, so the auditor is never opened twice.
    async fn create_native(
        &self,
        _directory: &Path,
        _journal: &CreatorJournal,
    ) -> Result<Option<Arc<crate::audit::NativeAuditor>>, Error> {
        Ok(None)
    }

    async fn create(
        &self,
        directory: &Path,
        journal: &CreatorJournal,
    ) -> Result<Arc<dyn Auditor>, Error>;
}

/// Server-selected persistence is injected so SQLite children never silently fall
/// back to file history. Directory allocation still owns audit/artifact paths.
/// Opening must be cancellation safe: retain ownership of partial resources until
/// they are returned or dropped, including results produced by blocking workers.
#[async_trait]
pub trait ChildJournalFactory: Send + Sync {
    async fn open(&self, directory: &Path, session_id: &str) -> Result<CreatorJournal, Error>;
    /// A selected backend may supply durable consumption alongside its Journal.
    /// None preserves the embedding's explicit file/memory policy.
    async fn usage(&self, _session_id: &str) -> Result<Option<UsageLedger>, Error> {
        Ok(None)
    }
}

/// A display-enabled child client is assembled only after its exact child
/// identity and Journal exist. The factory owns cancellation-safe unpublished
/// resources and must not route child observations into the parent archive.
#[async_trait]
pub trait ChildDisplayFactory: Send + Sync {
    /// Return a lightweight issuing-route view sharing the factory's resource owner.
    fn for_epoch(&self, _epoch: u64) -> Option<Arc<dyn ChildDisplayFactory>> {
        None
    }
    /// Observe the actual child's acknowledged close. Factory-owned exact Journal,
    /// auditor and recorder references captured during create supply provenance.
    /// A failed close must never be promoted to a successful checkpoint witness.
    async fn finalize(
        &self,
        _session_id: &str,
        _session: &Session,
        _close_succeeded: bool,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn create(
        &self,
        directory: &Path,
        session_id: &str,
        journal: &CreatorJournal,
        auditor: Option<Arc<crate::audit::NativeAuditor>>,
    ) -> Result<Arc<dyn Client>, Error>;
}

/// Depth remains one: child registrations deliberately exclude Delegate.
#[derive(Clone)]
pub struct NativeDelegateConfig {
    pub children: Children,
    pub aggregate: UsageLedger,
    pub client: Arc<dyn Client>,
    pub host: Arc<dyn Host>,
    pub wasm: Vec<Arc<dyn Tool>>,
    pub external: Vec<Arc<dyn Tool>>,
    pub catalog: Vec<AgentInfo>,
    pub permissions: Arc<PermissionController>,
    pub limits: Limits,
    pub directory: PathBuf,
    pub disk: bool,
    pub system: Option<String>,
    pub auditors: Option<Arc<dyn ChildAuditorFactory>>,
    pub journals: Option<Arc<dyn ChildJournalFactory>>,
}

/// Read current admitted external definitions/executors at child assembly. The
/// source must not expose an unadmitted reconnect catalog or replay old calls.
#[async_trait]
pub trait ExternalToolSource: Send + Sync {
    async fn tools(&self) -> Result<Vec<Arc<dyn Tool>>, Error>;
}

/// Restore owner-scoped child settings and bind external persistence before any
/// child lifecycle or model effects. Implementations must be cancellation safe.
#[async_trait]
pub trait ChildConfiguration: Send + Sync {
    async fn apply(&self, child: &Session) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct NativeChildFactory {
    config: NativeDelegateConfig,
    external_source: Option<Arc<dyn ExternalToolSource>>,
    display_factory: Option<Arc<dyn ChildDisplayFactory>>,
    child_configuration: Option<Arc<dyn ChildConfiguration>>,
    permission_adapter: Arc<dyn microcrab_session::policy::SessionPermission>,
    round_limit_callback: Option<Arc<dyn microcrab_session::budget::RoundLimitCallback>>,
    attachment_resolver: Option<Arc<dyn microcrab_session::admission::image::AttachmentResolver>>,
}

impl NativeChildFactory {
    pub fn new(config: NativeDelegateConfig) -> Self {
        Self {
            config,
            external_source: None,
            display_factory: None,
            child_configuration: None,
            permission_adapter: Arc::new(DeclarativePermission),
            round_limit_callback: None,
            attachment_resolver: None,
        }
    }

    pub fn with_child_configuration(mut self, configuration: Arc<dyn ChildConfiguration>) -> Self {
        self.child_configuration = Some(configuration);
        self
    }
    pub fn with_display_factory(mut self, factory: Arc<dyn ChildDisplayFactory>) -> Self {
        self.display_factory = Some(factory);
        self
    }

    pub fn with_round_limit_callback(
        mut self,
        callback: Arc<dyn microcrab_session::budget::RoundLimitCallback>,
    ) -> Self {
        self.round_limit_callback = Some(callback);
        self
    }

    pub fn with_attachment_resolver(
        mut self,
        resolver: Arc<dyn microcrab_session::admission::image::AttachmentResolver>,
    ) -> Self {
        self.attachment_resolver = Some(resolver);
        self
    }

    pub fn with_external_source(mut self, source: Arc<dyn ExternalToolSource>) -> Self {
        self.external_source = Some(source);
        self
    }

    pub fn with_permission_adapter(
        mut self,
        adapter: Arc<dyn microcrab_session::policy::SessionPermission>,
    ) -> Self {
        self.permission_adapter = adapter;
        self
    }

    pub fn executor_with_external_source(
        config: NativeDelegateConfig,
        source: Arc<dyn ExternalToolSource>,
    ) -> OwnedDelegateExecutor {
        let children = config.children.clone();
        OwnedDelegateExecutor::new(
            children,
            Arc::new(Self {
                config,
                external_source: Some(source),
                display_factory: None,
                child_configuration: None,
                permission_adapter: Arc::new(DeclarativePermission),
                round_limit_callback: None,
                attachment_resolver: None,
            }),
        )
    }

    pub fn executor(config: NativeDelegateConfig) -> OwnedDelegateExecutor {
        OwnedDelegateExecutor::new(config.children.clone(), Arc::new(Self::new(config)))
    }
}

#[async_trait]
impl ChildFactory for NativeChildFactory {
    async fn configure(&self, child: &ChildSession) -> Result<(), Error> {
        if let Some(configuration) = &self.child_configuration {
            configuration.apply(&child.session).await?;
        }
        Ok(())
    }
    async fn create_for_epoch(
        &self,
        caller_mode: Option<&str>,
        target: &str,
        requested_mode: Option<&str>,
        context_mode: &str,
        parent_processes: Arc<dyn ProcessControl>,
        issuer_epoch: u64,
    ) -> Result<ChildSession, Error> {
        let mut captured = self.clone();
        if let Some(factory) = &self.display_factory {
            if let Some(scoped) = factory.for_epoch(issuer_epoch) {
                captured.display_factory = Some(scoped);
            }
        }
        captured
            .create(
                caller_mode,
                target,
                requested_mode,
                context_mode,
                parent_processes,
            )
            .await
    }
    async fn finalize(&self, child: &ChildSession, close_succeeded: bool) -> Result<(), Error> {
        if let Some(factory) = &self.display_factory {
            factory
                .finalize(&child.id, &child.session, close_succeeded)
                .await?;
        }
        Ok(())
    }

    async fn create(
        &self,
        caller_mode: Option<&str>,
        target: &str,
        requested_mode: Option<&str>,
        _context_mode: &str,
        parent_processes: Arc<dyn ProcessControl>,
    ) -> Result<ChildSession, Error> {
        let config = &self.config;
        let (agent, mode) = select_child(&config.catalog, caller_mode, target, requested_mode)?;
        let parent = config.directory.clone();
        let (id, directory) =
            tokio::task::spawn_blocking(move || crate::paths::reserve_child(&parent))
                .await
                .map_err(|_| Error::config("child directory worker failed"))??;
        let mut registrations = config
            .wasm
            .iter()
            .cloned()
            .map(ToolRegistration::Wasm)
            .collect::<Vec<_>>();
        let external = match &self.external_source {
            Some(source) => source.tools().await?,
            None => config.external.clone(),
        };
        registrations.extend(external.into_iter().map(ToolRegistration::External));
        registrations.extend([
            ToolRegistration::Builtin(BuiltinTool::Expand),
            ToolRegistration::Builtin(BuiltinTool::Job),
            ToolRegistration::Builtin(BuiltinTool::Sleep),
            ToolRegistration::Builtin(BuiltinTool::SwitchMode),
        ]);
        let runtime = Runtime::new(registrations, self.permission_adapter.clone())?;
        let journal = match &config.journals {
            Some(factory) => factory.open(&directory, &id).await?,
            None => {
                let directory_text = directory
                    .to_str()
                    .ok_or_else(|| Error::config("child directory must be UTF-8"))?;
                CreatorJournal::open(directory_text, id.clone(), config.disk, false)
                    .await?
                    .0
            }
        };
        let ledger_path = directory.clone();
        let disk = config.disk;
        let selected_ledger = match &config.journals {
            Some(factory) => factory.usage(&id).await?,
            None => None,
        };
        let ledger = if let Some(ledger) = selected_ledger {
            ledger
        } else {
            tokio::task::spawn_blocking(move || {
                UsageLedger::open(if disk {
                    Some(ledger_path.as_path())
                } else {
                    None
                })
            })
            .await
            .map_err(|_| Error::config("child usage worker failed"))??
        };
        let native_auditor = match &config.auditors {
            Some(factory) => factory.create_native(&directory, &journal).await?,
            None => None,
        };
        let sink: Arc<dyn Auditor> = match &native_auditor {
            Some(auditor) => auditor.clone(),
            None => match &config.auditors {
                Some(factory) => factory.create(&directory, &journal).await?,
                None => Arc::new(NoopAuditor),
            },
        };
        let auditor: Arc<dyn Auditor> = Arc::new(UsageAuditor::new(
            ledger.clone(),
            config.aggregate.clone(),
            sink,
        ));
        let client = match &self.display_factory {
            Some(factory) => {
                factory
                    .create(&directory, &id, &journal, native_auditor)
                    .await?
            }
            None => config.client.clone(),
        };
        let mut builder =
            inherit_policy(Session::builder_arc(client, runtime), &config.permissions)?
                .session_id(id.clone())
                .processes_parent(parent_processes)
                .catalog(config.catalog.clone())
                .is_main(false)
                .host(config.host.clone())
                .limits(config.limits.clone())
                .agent(agent)
                .auditor(auditor.clone())
                .journal(journal);
        if let Some(callback) = &self.round_limit_callback {
            builder = builder.round_limit_callback(callback.clone());
        }
        if let Some(resolver) = &self.attachment_resolver {
            builder = builder.attachment_resolver(resolver.clone());
        }
        if let Some(system) = &config.system {
            builder = builder.system(system.clone());
        }
        let session = builder.build()?;
        Ok(ChildSession {
            id,
            session,
            mode,
            ledger: Arc::new(ledger),
            auditor,
        })
    }
}

/// Preserve explicit overrides and a live parent ceiling, then install the child's
/// own agent/mode defaults through normal Session initialization.
pub fn inherit_policy(
    builder: microcrab_session::SessionBuilder,
    parent: &Arc<PermissionController>,
) -> Result<microcrab_session::SessionBuilder, Error> {
    let (policy, review_client) = parent
        .review_binding()
        .map_err(|error| Error::config_with_private(error.public_reason(), error.to_string()))?;
    let builder = if let Some(client) = review_client {
        builder.review_client(client)
    } else {
        builder
    };
    Ok(builder
        .permissions(policy.config)
        .permission_parent(parent.clone()))
}

pub use microcrab_session::runtime::tool::delegate::select_child;
