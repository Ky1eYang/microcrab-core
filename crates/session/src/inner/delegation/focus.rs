//! Capture selected material under the issuing parent's actual read authority.
use super::*;
use crate::runtime::tool::ToolContext;
use crate::state::reference::ReferenceInfo;
use crate::state::reference::focus::{
    FocusSelection, FocusSnapshot, MAX_FOCUS_BYTES, MAX_FOCUS_REFERENCES,
};
use sha2::{Digest, Sha256};

impl crate::inner::SessionInner {
    pub(super) async fn capture_delegation_focus(
        &self,
        selections: Vec<FocusSelection>,
        invocation: &ToolContext,
    ) -> Result<Vec<FocusSnapshot>, Error> {
        self.ensure_open()?;
        self.require_derivation()?;
        invocation
            .ensure_admitted(None)
            .map_err(|error| error.into_runtime_error())?;
        if invocation.session_id != self.state.processes().session_id() {
            return Err(Error::denied("focus capture belongs to another Session"));
        }
        crate::budget::check(
            crate::LimitKind::ReferenceCount,
            selections.len(),
            MAX_FOCUS_REFERENCES,
        )?;
        let listed = self.state.list_references()?;
        let disclosed = self.state.view()?;
        let limit = MAX_FOCUS_BYTES.min(self.state().limits.max_context_bytes);
        let mut snapshots = Vec::new();
        let mut bytes = 0usize;
        let mut selected = std::collections::BTreeSet::new();
        for selection in selections {
            if invocation.cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let snapshot = match selection {
                FocusSelection::Section { section_id } => {
                    if !selected.insert(format!("section:{section_id}")) {
                        return Err(Error::config("duplicate focus selection"));
                    }
                    let row = listed
                        .iter()
                        .find(|row| row.info.id == section_id)
                        .ok_or_else(|| {
                            Error::denied("selected reference is absent from the parent catalog")
                        })?;
                    let projection = disclosed
                        .get(&row.kind)
                        .ok_or_else(|| Error::denied("selected reference kind is undisclosed"))?;
                    if !contains_id(projection, &section_id) {
                        return Err(Error::denied(
                            "selected reference id is absent from effective parent State",
                        ));
                    }
                    let content = self.state.read_reference(&section_id)?;
                    FocusSnapshot::capture(row.info.clone(), content.body)
                }
                FocusSelection::File { path } => {
                    if path.is_empty() || path.len() > 4096 || path.contains('\0') {
                        return Err(Error::config("invalid selected file path"));
                    }
                    let host = self
                        .host
                        .as_ref()
                        .ok_or_else(|| Error::config("selected files require the parent Host"))?;
                    let normalized = host
                        .normalize_resource(path.into(), invocation.cancellation.clone())
                        .await
                        .map_err(Error::from_host_error)?;
                    if !selected.insert(format!("path:{normalized}")) {
                        return Err(Error::config("duplicate focus selection"));
                    }
                    let available = limit.saturating_sub(bytes);
                    crate::budget::check(
                        crate::LimitKind::ReferenceBytes,
                        bytes.saturating_add(1),
                        limit,
                    )?;
                    let arguments = json!({"path":normalized,"offset_line":1,"max_lines":usize::MAX,"max_bytes":available});
                    let controller = self.runtime.permission_controller();
                    let current = controller
                        .snapshot()
                        .map_err(|error| error.into_runtime_error())?;
                    let lineage = controller
                        .lineage()
                        .map_err(|error| error.into_runtime_error())?;
                    let (policy, _) = controller
                        .compiled_chain(&current, "read")
                        .map_err(|error| error.into_runtime_error())?;
                    if crate::policy::evaluate_compiled(&policy, &arguments).map_err(|error| {
                        Error::config_with_private(
                            "read policy evaluation failed",
                            error.to_string(),
                        )
                    })? != crate::policy::Action::Allow
                    {
                        return Err(Error::denied(
                            "selected file is not already readable under the parent's read policy",
                        ));
                    }
                    if self.runtime.origin("read").is_none() {
                        return Err(Error::denied("parent has no admitted read tool"));
                    }
                    let output = host
                        .read_text(
                            microcrab_host_api::ReadRequest {
                                path: normalized.clone().into(),
                                offset_line: 1,
                                max_lines: usize::MAX,
                                max_bytes: available,
                            },
                            invocation.cancellation.clone(),
                        )
                        .await
                        .map_err(Error::from_host_error)?;
                    if output.truncated || output.next_line.is_some() {
                        return Err(Error::config(
                            "selected file exceeds the bounded focus snapshot capacity",
                        ));
                    }
                    controller
                        .ensure_witness(current.revision, &lineage)
                        .map_err(|error| error.into_runtime_error())?;
                    let digest = format!("{:x}", Sha256::digest(normalized.as_bytes()));
                    let id = format!("focus_{}", &digest[..16]);
                    FocusSnapshot::capture(
                        ReferenceInfo {
                            id,
                            title: normalized.clone(),
                            location: normalized,
                            description: "Immutable selected file snapshot".into(),
                        },
                        output.content,
                    )
                }
            };
            bytes = bytes.checked_add(snapshot.body.len()).ok_or_else(|| {
                crate::budget::reject(crate::LimitKind::Arithmetic, "focus byte overflow")
            })?;
            crate::budget::check(crate::LimitKind::ReferenceBytes, bytes, limit)?;
            snapshots.push(snapshot);
        }
        crate::state::reference::focus::validate_snapshots(&snapshots)?;
        Ok(snapshots)
    }
}
fn contains_id(value: &serde_json::Value, id: &str) -> bool {
    let mut pending = vec![value];
    let mut remaining = 4096usize;
    while let Some(value) = pending.pop() {
        if remaining == 0 {
            return false;
        }
        remaining -= 1;
        match value {
            serde_json::Value::Object(object) => {
                if object.get("id").and_then(serde_json::Value::as_str) == Some(id) {
                    return true;
                }
                pending.extend(object.values());
            }
            serde_json::Value::Array(values) => pending.extend(values),
            _ => {}
        }
    }
    false
}
