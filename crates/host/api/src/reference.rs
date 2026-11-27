//! Unified asynchronous Reference trait and type-erased container shared across Host, Session, and Server.
use async_trait::async_trait;
use microcrab_protocol::{
    ReferenceCapabilities, ReferenceContent, ReferenceEntry, ReferenceError, ReferenceNodeKind,
    ReferencePatch, ReferenceProjection, ReferenceQuota, ReferenceWrite, validate_reference_kind,
};
use serde_json::Value;
use std::any::Any;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

/// Unified reference kind contract implemented closest to each content origin
/// (Core Host, Server Personal Library, Remote Git Skills, etc.) and assembled by Creator.
#[async_trait]
pub trait Reference: Send + Sync + 'static {
    /// Stable projection key in `SessionState`, such as `references`, `skills`, or `media`.
    fn kind(&self) -> &str;

    /// Capability/permission flags advertised by this implementation.
    fn capabilities(&self) -> ReferenceCapabilities;

    /// Initialize or reload catalog state without holding `SessionState` locks.
    async fn load(&self, cancel: CancellationToken) -> Result<(), ReferenceError>;

    /// Pure repeatable read of current catalog entries.
    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError>;

    /// Pure repeatable read of the current State projection (`exact` and optional `short_model`).
    fn projections(&self) -> Result<ReferenceProjection, ReferenceError>;

    /// Return whether this implementation owns `id`. Default scans `list()`.
    async fn owns(&self, id: &str, _cancel: CancellationToken) -> Result<bool, ReferenceError> {
        Ok(self.list()?.iter().any(|entry| entry.id == id))
    }

    /// Expand one owned id (`Directory`, `File`, or `Section`). `Ok(None)` means not owned here.
    async fn expand(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError>;

    /// Explicitly refresh external backing state. Defaults to `load()`.
    async fn refresh(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        self.load(cancel).await
    }

    /// Create a new entry when `capabilities().add == true`.
    async fn add(
        &self,
        _input: ReferenceWrite,
        _cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        Err(ReferenceError::denied(format!(
            "reference kind `{}` does not support add",
            self.kind()
        )))
    }

    /// Update an existing owned entry when `capabilities().update == true`.
    async fn update(
        &self,
        _id: &str,
        _patch: ReferencePatch,
        _cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        Err(ReferenceError::denied(format!(
            "reference kind `{}` does not support update",
            self.kind()
        )))
    }

    /// Remove or unmount an owned entry when `capabilities().remove == true`.
    async fn remove(
        &self,
        _id: &str,
        _cancel: CancellationToken,
    ) -> Result<bool, ReferenceError> {
        Err(ReferenceError::denied(format!(
            "reference kind `{}` does not support remove",
            self.kind()
        )))
    }

    /// Query storage quota when `capabilities().quota == true`.
    async fn quota(
        &self,
        _cancel: CancellationToken,
    ) -> Result<Option<ReferenceQuota>, ReferenceError> {
        Ok(None)
    }

    /// Optional concrete downcast hook for built-in composite containers.
    fn as_any(&self) -> Option<&dyn Any> {
        None
    }
}

/// Type-erased installed `Reference` kind.
#[derive(Clone)]
pub struct DynReference {
    kind: String,
    capabilities: ReferenceCapabilities,
    inner: Arc<dyn Reference>,
}

impl std::fmt::Debug for DynReference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DynReference")
            .field("kind", &self.kind)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl DynReference {
    pub fn new<R: Reference>(reference: R) -> Result<Self, ReferenceError> {
        let kind = reference.kind().to_owned();
        validate_reference_kind(&kind)?;
        let capabilities = reference.capabilities();
        Ok(Self {
            kind,
            capabilities,
            inner: Arc::new(reference),
        })
    }

    pub fn from_arc(reference: Arc<dyn Reference>) -> Result<Self, ReferenceError> {
        let kind = reference.kind().to_owned();
        validate_reference_kind(&kind)?;
        let capabilities = reference.capabilities();
        Ok(Self {
            kind,
            capabilities,
            inner: reference,
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn capabilities(&self) -> ReferenceCapabilities {
        self.capabilities
    }

    pub fn as_composite(&self) -> Option<&CompositeReference> {
        self.inner.as_any()?.downcast_ref::<CompositeReference>()
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub async fn load(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        self.inner.load(cancel).await
    }

    pub fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        self.inner.list()
    }

    pub fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        self.inner.projections()
    }

    pub async fn owns(&self, id: &str, cancel: CancellationToken) -> Result<bool, ReferenceError> {
        self.inner.owns(id, cancel).await
    }

    pub async fn expand(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        self.inner.expand(id, cancel).await
    }

    pub async fn refresh(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        self.inner.refresh(cancel).await
    }

    pub async fn add(
        &self,
        input: ReferenceWrite,
        cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        if !self.capabilities.add {
            return Err(ReferenceError::denied(format!(
                "reference kind `{}` does not allow add",
                self.kind
            )));
        }
        self.inner.add(input, cancel).await
    }

    pub async fn update(
        &self,
        id: &str,
        patch: ReferencePatch,
        cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        if !self.capabilities.update {
            return Err(ReferenceError::denied(format!(
                "reference kind `{}` does not allow update",
                self.kind
            )));
        }
        self.inner.update(id, patch, cancel).await
    }

    pub async fn remove(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<bool, ReferenceError> {
        if !self.capabilities.remove {
            return Err(ReferenceError::denied(format!(
                "reference kind `{}` does not allow remove",
                self.kind
            )));
        }
        self.inner.remove(id, cancel).await
    }

    pub async fn quota(
        &self,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceQuota>, ReferenceError> {
        if !self.capabilities.quota {
            return Ok(None);
        }
        self.inner.quota(cancel).await
    }
}

/// Aggregates multiple `DynReference` sources (for example, Server `references` + Host `AGENTS.md` +
/// Session inline references, or local Host `skills` + remote `GitSkillsReference`) under one
/// unique `SessionState` kind with deduplicated `list()` / `projections()`.
pub struct CompositeReference {
    kind: String,
    members: RwLock<Vec<DynReference>>,
    capabilities: ReferenceCapabilities,
}

impl CompositeReference {
    pub fn new(kind: impl Into<String>, members: Vec<DynReference>) -> Self {
        let mut caps = ReferenceCapabilities::default();
        for member in &members {
            let mc = member.capabilities();
            caps.list |= mc.list;
            caps.expand |= mc.expand;
            caps.add |= mc.add;
            caps.update |= mc.update;
            caps.remove |= mc.remove;
            caps.refresh |= mc.refresh;
            caps.quota |= mc.quota;
        }
        Self {
            kind: kind.into(),
            members: RwLock::new(members),
            capabilities: caps,
        }
    }

    /// Append additional read-only (or compatible) members after construction without
    /// duplicating identical `DynReference` instances.
    pub fn append_members(&self, extra: Vec<DynReference>) {
        let mut guard = self.members.write().unwrap_or_else(|e| e.into_inner());
        for member in extra {
            if !guard.iter().any(|existing| existing.ptr_eq(&member)) {
                guard.push(member);
            }
        }
    }

    pub fn members(&self) -> Vec<DynReference> {
        self.members
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Refresh only members with write/mutation capabilities, skipping read-only
    /// members so turn/round admission never rescans read-only Host or Git sources.
    pub async fn refresh_writable(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        for member in self.members() {
            let caps = member.capabilities();
            if caps.refresh && (caps.add || caps.update || caps.remove) {
                if let Some(nested) = member.as_composite() {
                    Box::pin(nested.refresh_writable(cancel.clone())).await?;
                } else {
                    member.refresh(cancel.clone()).await?;
                }
            }
        }
        Ok(())
    }
}

fn projection_dedup_key(member_writable: bool, item: &Value) -> Option<String> {
    let obj = item.as_object()?;
    if !member_writable && let Some(path) = obj.get("path").and_then(Value::as_str) {
        return Some(format!("path:{path}"));
    }
    obj.get("id")
        .and_then(Value::as_str)
        .map(|id| format!("id:{id}"))
}

fn push_projection_items(
    target: &mut Vec<Value>,
    seen: &mut HashSet<String>,
    member_writable: bool,
    value: Option<Value>,
) {
    let mut push_one = |item: Value| {
        if let Some(key) = projection_dedup_key(member_writable, &item)
            && !seen.insert(key)
        {
            return;
        }
        target.push(item);
    };
    match value {
        Some(Value::Array(items)) => {
            for item in items {
                push_one(item);
            }
        }
        Some(other) => push_one(other),
        None => {}
    }
}

#[async_trait]
impl Reference for CompositeReference {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        self.capabilities
    }

    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }

    async fn load(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        for member in self.members() {
            if let Err(error) = member.load(cancel.clone()).await {
                if member.capabilities().add {
                    return Err(error);
                }
                tracing::warn!(
                    kind = %self.kind,
                    member = %member.kind(),
                    reason = %error,
                    "Optional read-only reference member failed initial load; hiding it"
                );
            }
        }
        Ok(())
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        let mut out = Vec::new();
        let mut seen_ids = HashSet::new();
        let mut seen_ro_locations = HashSet::new();
        let mut skipped_file_ids = HashSet::new();
        for member in self.members() {
            let Ok(entries) = member.list() else {
                continue;
            };
            for entry in entries {
                if !entry.writable
                    && entry.node_kind == ReferenceNodeKind::File
                    && let Some(loc) = &entry.location
                    && !seen_ro_locations.insert(loc.clone())
                {
                    skipped_file_ids.insert(entry.id.clone());
                    continue;
                }
                if entry
                    .parent_id
                    .as_ref()
                    .is_some_and(|parent| skipped_file_ids.contains(parent))
                {
                    continue;
                }
                if seen_ids.insert(entry.id.clone()) {
                    out.push(entry);
                }
            }
        }
        Ok(out)
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        let mut exact_items = Vec::new();
        let mut short_items = Vec::new();
        let mut seen_exact = HashSet::new();
        let mut seen_short = HashSet::new();
        for member in self.members() {
            let member_writable = member.capabilities().add;
            let proj = member.projections()?;
            push_projection_items(&mut exact_items, &mut seen_exact, member_writable, proj.exact);
            push_projection_items(
                &mut short_items,
                &mut seen_short,
                member_writable,
                proj.short_model,
            );
        }
        if exact_items.is_empty() && short_items.is_empty() {
            return Ok(ReferenceProjection::empty());
        }
        Ok(ReferenceProjection {
            exact: (!exact_items.is_empty()).then_some(Value::Array(exact_items)),
            short_model: (!short_items.is_empty()).then_some(Value::Array(short_items)),
        })
    }

    async fn owns(&self, id: &str, cancel: CancellationToken) -> Result<bool, ReferenceError> {
        for member in self.members() {
            if member.owns(id, cancel.clone()).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn expand(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        for member in self.members() {
            if let Some(content) = member.expand(id, cancel.clone()).await? {
                return Ok(Some(content));
            }
        }
        Ok(None)
    }

    async fn refresh(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        for member in self.members() {
            if member.capabilities().refresh
                && let Err(error) = member.refresh(cancel.clone()).await
            {
                if member.capabilities().add {
                    return Err(error);
                }
                tracing::warn!(
                    kind = %self.kind,
                    member = %member.kind(),
                    reason = %error,
                    "Optional read-only reference member failed refresh; retaining previous view"
                );
            }
        }
        Ok(())
    }

    async fn add(
        &self,
        input: ReferenceWrite,
        cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        for member in self.members() {
            if member.capabilities().add {
                return member.add(input, cancel).await;
            }
        }
        Err(ReferenceError::denied(format!(
            "reference kind `{}` has no writable member",
            self.kind
        )))
    }

    async fn update(
        &self,
        id: &str,
        patch: ReferencePatch,
        cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        for member in self.members() {
            if member.owns(id, cancel.clone()).await? {
                return member.update(id, patch, cancel).await;
            }
        }
        Err(ReferenceError::not_found("requested reference was not found"))
    }

    async fn remove(&self, id: &str, cancel: CancellationToken) -> Result<bool, ReferenceError> {
        for member in self.members() {
            if member.owns(id, cancel.clone()).await? {
                return member.remove(id, cancel).await;
            }
        }
        Ok(false)
    }

    async fn quota(
        &self,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceQuota>, ReferenceError> {
        for member in self.members() {
            if member.capabilities().quota
                && let Some(q) = member.quota(cancel.clone()).await?
            {
                return Ok(Some(q));
            }
        }
        Ok(None)
    }
}
