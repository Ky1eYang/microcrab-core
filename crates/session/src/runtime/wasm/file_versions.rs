//! Typed pre-admission binding and shared invocation-side file version owner.
use super::{ToolContext, operations};
use crate::{
    Error, Journal,
    runtime::journal::{FileVersions, REREAD, exact_file_version},
};
use microcrab_host_api::{Host, OpContext, tool_api::ResourceApi};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, MutexGuard};

pub(super) const INTERFACE: &str = "microcrab:session/file-versions@0.2.0";
pub(super) const CAPABILITY: &str = "file_versions_v1";

pub(crate) struct FileVersionOwner {
    fence: Mutex<()>,
    journal: Arc<dyn Journal>,
    host: Arc<dyn Host>,
    session: String,
}
impl FileVersionOwner {
    pub(crate) fn new(journal: Arc<dyn Journal>, host: Arc<dyn Host>, session: String) -> Self {
        Self {
            fence: Mutex::new(()),
            journal,
            host,
            session,
        }
    }

    pub(super) async fn ledger<'a>(
        &'a self,
        host: &dyn Host,
        invocation: &ToolContext,
    ) -> Result<Ledger<'a>, Error> {
        let gate = self.fence.lock().await;
        let authority = host
            .file_version_authority()
            .filter(|scope| Some(scope) == self.host.file_version_authority().as_ref())
            .ok_or_else(|| Error::tool("Host has no authenticated file version authority"))?;
        if invocation.session_id != self.session
            || !invocation.subject.is_tool()
            || !self.journal.supports_file_versions()
        {
            return Err(Error::tool("Session has no durable file version owner"));
        }
        let epoch = invocation.state.observation_position().epoch;
        let stored = self.journal.load_file_versions(&self.session).await?;
        let previous = stored.unwrap_or_else(|| FileVersions::new(&self.session));
        let next = previous.clone();
        Ok(Ledger {
            _gate: gate,
            owner: self,
            authority,
            epoch,
            previous,
            next,
        })
    }
}

pub(super) struct Ledger<'a> {
    _gate: MutexGuard<'a, ()>,
    owner: &'a FileVersionOwner,
    authority: String,
    epoch: u64,
    previous: FileVersions,
    next: FileVersions,
}
impl Ledger<'_> {
    fn resolve(&self, path: &str, alias: &str) -> Result<String, Error> {
        if !self.next.matches(&self.authority, self.epoch) {
            return Err(Error::tool(REREAD));
        }
        self.next.resolve(path, alias)
    }
    pub(super) fn issue(
        &mut self,
        path: &str,
        version: &str,
        observed: bool,
    ) -> Result<String, Error> {
        self.next.bind(&self.authority, self.epoch)?;
        self.next.issue(path, version, observed)
    }
    pub(super) fn confirm(&mut self, path: &str, version: &str, alias: &str) -> Result<(), Error> {
        self.next.confirm(path, version, alias)
    }
    pub(super) async fn commit(&mut self) -> Result<(), Error> {
        if self.previous != self.next {
            self.next.advance()?;
            self.owner
                .journal
                .commit_file_versions(&self.owner.session, self.next.clone())
                .await?;
            self.previous = self.next.clone();
        }
        Ok(())
    }
}

pub(super) fn owner(invocation: &ToolContext) -> Result<Arc<FileVersionOwner>, Error> {
    invocation
        .state
        .file_versions()
        .ok_or_else(|| Error::tool("Session has no durable file version owner"))
}

pub(super) async fn path(
    host: &dyn Host,
    op: &OpContext,
    name: &str,
) -> Result<String, microcrab_host_api::ToolError> {
    if name.is_empty() || name.len() > 4096 || name.contains('\0') {
        return Err(microcrab_host_api::ToolError::invalid_request(
            "invalid file version path",
        ));
    }
    let normalized = ResourceApi::normalize_resource(host, op, PathBuf::from(name)).await?;
    if normalized.is_empty() || normalized.len() > 4096 || normalized.contains('\0') {
        return Err(microcrab_host_api::ToolError::invalid_request(
            "invalid normalized file version path",
        ));
    }
    Ok(normalized)
}

/// Only the exact registered component with the recognized negotiated field paths
/// invokes this adapter. Neither arbitrary WASM output nor tool name is provenance.
pub(crate) async fn bind_arguments(
    host: &dyn Host,
    invocation: &ToolContext,
    authored: &Value,
) -> Result<Option<Value>, Error> {
    let paths: Vec<(String, String, String)> = match invocation.tool.as_str() {
        "edit" | "patch" | "write" => authored
            .get("expected_version")
            .and_then(Value::as_str)
            .map(|value| {
                vec![(
                    "/expected_version".into(),
                    authored["path"].as_str().unwrap_or_default().into(),
                    value.into(),
                )]
            })
            .unwrap_or_default(),
        "apply_patch" => authored["preconditions"]
            .as_array()
            .into_iter()
            .flatten()
            .take(64)
            .enumerate()
            .filter_map(|(index, item)| {
                Some((
                    format!("/preconditions/{index}/expected/version"),
                    item["path"].as_str()?.into(),
                    item["expected"]["version"].as_str()?.into(),
                ))
            })
            .collect(),
        _ => vec![],
    };
    if paths.is_empty() {
        return Ok(None);
    }
    let owner = owner(invocation)?;
    let ledger = owner.ledger(host, invocation).await?;
    let mut bound = authored.clone();
    let op = OpContext {
        trace_id: invocation.call_id.clone(),
        principal: invocation.session_id.clone(),
        cancel: invocation.cancellation.clone(),
        deadline: None,
    };
    for (pointer, name, version) in paths {
        let canonical = path(host, &op, &name)
            .await
            .map_err(Error::from_host_error)?;
        let exact = version.to_ascii_lowercase();
        let value = if exact_file_version(&exact) {
            // Preserve explicit compatibility inputs for existing case-sensitive
            // policy predicates. Coding normalizes the exact digest at its CAS.
            version
        } else {
            ledger.resolve(&canonical, &version)?
        };
        *bound
            .pointer_mut(&pointer)
            .ok_or_else(|| Error::tool(REREAD))? = Value::String(value);
    }
    Ok(Some(bound))
}

pub(super) async fn dispatch_arguments(
    host: &dyn Host,
    invocation: &ToolContext,
    resolved: &Value,
    policy: &Value,
) -> Result<Value, Error> {
    let next = bind_arguments(host, invocation, resolved)
        .await?
        .ok_or_else(|| Error::tool(REREAD))?;
    let differs = match invocation.tool.as_str() {
        "apply_patch" => next.get("preconditions") != policy.get("preconditions"),
        _ => {
            next.get("path") != policy.get("path")
                || next.get("expected_version") != policy.get("expected_version")
        }
    };
    if differs {
        return Err(Error::tool(REREAD));
    }
    Ok(next)
}

pub(super) fn link(
    linker: &mut wasmtime::component::Linker<super::RuntimeState>,
) -> wasmtime::Result<()> {
    let mut interface = linker.instance(INTERFACE)?;
    interface.func_wrap("read", operations::versioned::read)?;
    interface.func_wrap("write", operations::versioned::write)?;
    interface.func_wrap("preflight", operations::mutations::preflight_versioned)?;
    interface.func_wrap("mutate", operations::mutations::mutate_versioned)?;
    Ok(())
}
