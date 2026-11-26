//! Restore selected child evidence using native dependency owners, without replay.
use super::*;
#[cfg(test)]
mod tests;
impl NativeChildFactory {
    /// Restore one selected persistent child and explicitly attach a new live
    /// endpoint to its recorded parent. The supplied Journal already belongs to
    /// the embedding's verified child directory and selected storage backend.
    /// No directory allocation, original delegate Job, or exchange is restored.
    pub async fn restore_child(
        &self,
        parent: &Session,
        directory: &Path,
        journal: CreatorJournal,
    ) -> Result<ChildSession, Error> {
        let factory = self.clone();
        let parent = parent.clone();
        let directory = directory.to_owned();
        tokio::spawn(async move {
            factory
                .restore_child_owned(parent, directory, journal)
                .await
        })
        .await
        .map_err(|_| Error::tool("recovered child assembly worker stopped unexpectedly"))?
    }
    async fn restore_child_owned(
        &self,
        parent: Session,
        directory: PathBuf,
        journal: CreatorJournal,
    ) -> Result<ChildSession, Error> {
        let cursor = journal
            .cursor()
            .await
            .ok_or_else(|| Error::config("child Journal has no selected identity"))?;
        let id = cursor.session_id.clone();
        if id.len() != 16
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::config(
                "native recovered child session id is not canonical",
            ));
        }
        let recover = journal
            .head_recover()
            .await?
            .ok_or_else(|| Error::config("child Journal has no selected Recover"))?;
        recover.validate()?;
        let identity = recover.state.communication.as_ref().ok_or_else(|| {
            Error::denied("legacy child has no communication reattachment identity")
        })?;
        let reservation = parent.reserve_recovered_child(&id, &identity.identity.agent_id)?;
        let parent_identity = parent
            .communication_identity()
            .ok_or_else(|| Error::denied("legacy parent cannot reattach communication children"))?;
        if identity.identity.depth != 1
            || identity.identity.interaction
                != microcrab_session::state::communication::SessionInteraction::Persistent
            || identity.identity.parent_session_id.as_deref()
                != Some(parent.process_control().session_id())
            || identity.identity.parent_agent_id.as_ref() != Some(&parent_identity.agent_id)
        {
            return Err(Error::denied(
                "selected child Recover names another parent or interaction",
            ));
        }
        if !Arc::ptr_eq(&self.config.permissions, &parent.permission_controller()) {
            return Err(Error::denied(
                "child factory permission owner differs from this live parent",
            ));
        }
        let agent = recover
            .agent
            .as_ref()
            .ok_or_else(|| Error::config("selected child Recover has no Agent metadata"))?
            .current()?;
        if self
            .config
            .catalog
            .iter()
            .find(|entry| entry.name == agent.name)
            != Some(&agent)
        {
            return Err(Error::denied(
                "selected child Agent differs from current admitted factory metadata",
            ));
        }
        let mode = recover
            .state
            .selected_mode
            .clone()
            .ok_or_else(|| Error::config("selected child has no current Agent mode"))?;
        let limits = recover.state.limits.clone();
        limits.ensure_within_parent(&parent.state().limits)?;
        let root = self.config.directory.clone();
        let child_directory = directory.clone();
        let child_id = id.clone();
        tokio::task::spawn_blocking(move || {
            let owner = root.canonicalize()?;
            let root = owner.join("subsessions").canonicalize()?;
            if root.parent() != Some(owner.as_path()) {
                return Err(Error::denied(
                    "recovered children root escapes its current parent directory",
                ));
            }
            let child = child_directory.canonicalize()?;
            if child != root.join(&child_id) || child.parent() != Some(root.as_path()) {
                return Err(Error::denied(
                    "recovered child directory escapes the current parent ownership root",
                ));
            }
            use std::io::Read;
            let path = child.join("creator.json");
            let metadata = std::fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(Error::denied(
                    "recovered child metadata is not an ordinary file",
                ));
            }
            let metadata = std::fs::File::open(path)?;
            let mut bytes = Vec::new();
            metadata.take(16385).read_to_end(&mut bytes)?;
            if bytes.len() > 16384 {
                return Err(Error::config(
                    "recovered child metadata byte budget exceeded",
                ));
            }
            let metadata: serde_json::Value = serde_json::from_slice(&bytes)?;
            if metadata
                .get("session_id")
                .and_then(serde_json::Value::as_str)
                != Some(child_id.as_str())
            {
                return Err(Error::denied(
                    "recovered child directory metadata identity mismatch",
                ));
            }
            Ok::<(), Error>(())
        })
        .await
        .map_err(|_| Error::tool("recovered child directory verification worker failed"))??;
        let mut registrations = self
            .config
            .wasm
            .iter()
            .cloned()
            .map(ToolRegistration::Wasm)
            .collect::<Vec<_>>();
        let external = match &self.external_source {
            Some(source) => source.tools().await?,
            None => self.config.external.clone(),
        };
        registrations.extend(external.into_iter().map(ToolRegistration::External));
        registrations.extend([
            ToolRegistration::Builtin(BuiltinTool::Reference),
            ToolRegistration::Builtin(BuiltinTool::Job),
            ToolRegistration::Builtin(BuiltinTool::Sleep),
            ToolRegistration::Builtin(BuiltinTool::SwitchMode),
        ]);
        let runtime = Runtime::new(registrations, self.permission_adapter.clone())?;
        let selected_ledger = match &self.config.journals {
            Some(factory) => factory.usage(&id).await?,
            None => None,
        };
        let ledger = if let Some(ledger) = selected_ledger {
            ledger
        } else {
            let path = directory.clone();
            let disk = self.config.disk;
            tokio::task::spawn_blocking(move || {
                UsageLedger::open(if disk { Some(path.as_path()) } else { None })
            })
            .await
            .map_err(|_| Error::tool("recovered child usage worker failed"))??
        };
        let native = match &self.config.auditors {
            Some(factory) => factory.create_native(&directory, &journal).await?,
            None => None,
        };
        let sink: Arc<dyn Auditor> = match &native {
            Some(auditor) => auditor.clone(),
            None => match &self.config.auditors {
                Some(factory) => factory.create(&directory, &journal).await?,
                None => Arc::new(NoopAuditor),
            },
        };
        let auditor: Arc<dyn Auditor> = Arc::new(UsageAuditor::new(
            ledger.clone(),
            self.config.aggregate.clone(),
            sink,
        ));
        let client = match &self.display_factory {
            Some(factory) => factory.create(&directory, &id, &journal, native).await?,
            None => self.config.client.clone(),
        };
        let mut builder = inherit_policy(
            Session::builder_arc(client, runtime),
            &self.config.permissions,
        )?
        .session_id(id.clone())
        .processes_parent(parent.process_control())
        .catalog(Vec::new())
        .is_main(false)
        .host(self.config.host.clone())
        .limits(limits)
        .agent(agent)
        .auditor(auditor.clone())
        .journal(journal.clone());
        if let Some(callback) = &self.round_limit_callback {
            builder = builder.round_limit_callback(callback.clone());
        }
        if let Some(resolver) = &self.attachment_resolver {
            builder = builder.attachment_resolver(resolver.clone());
        }
        let session = match Session::restore(builder, recover).await {
            Ok(session) => session,
            Err(error) => {
                microcrab_session::Journal::close(&journal).await?;
                return Err(error);
            }
        };
        let configured = async {
            if let Some(configuration) = &self.child_configuration {
                configuration.apply(&session).await?;
            }
            if journal.cursor().await.as_ref() != Some(&cursor) {
                return Err(Error::StateObservationConflict);
            }
            parent.attach_reserved_child(&session, &reservation).await
        }
        .await;
        if let Err(error) = configured {
            // No lifecycle/model was entered, so release native owners without
            // publishing a terminal checkpoint over the selected frozen evidence.
            session.executions().close().await?;
            microcrab_session::Journal::close(&journal).await?;
            return Err(error);
        }
        Ok(ChildSession {
            id,
            session,
            mode,
            ledger: Arc::new(ledger),
            auditor,
        })
    }
}
