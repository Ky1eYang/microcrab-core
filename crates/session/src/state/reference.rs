//! Session-owned reference wiring over the unified `Reference` trait.
//!
//! Concrete implementations live closest to their content origins (Core Host
//! and Server) and are assembled uniformly by Creator. Session runtime only
//! inspects `DynReference` capabilities, projects per-kind arrays into `State`,
//! and routes `expand` / `add` / `update` / `remove` / `refresh` by `id` or
//! designated default writable kind.
pub mod focus;
#[cfg(test)]
pub(crate) mod test_memory;

pub use super::session::DynamicReferenceMount;
pub use microcrab_host_api::{
    CompositeReference, DynReference, HostMarkdownReference, HostReferenceSpec, Reference,
    ReferenceCapabilities, ReferenceContent, ReferenceEntry, ReferenceError, ReferenceErrorKind,
    ReferenceInfo, ReferenceLimitDetails, ReferenceNodeKind, ReferencePatch, ReferencePayload,
    ReferenceProjection, ReferenceQuota, ReferenceWrite,
};
pub use microcrab_protocol::{canonical_mime, detect_mime_from_bytes, valid_library_id};

use crate::Error;
use crate::admission::image::{image_type, signature_type, validate_image};
use tokio_util::sync::CancellationToken;

/// Literal redirect destination bound prior to permission/execution.
#[derive(Clone, Debug)]
pub struct ReferenceRedirectDestination {
    pub expected_mime: String,
    pub title: String,
}

pub fn inspect_binary_for_storage(
    declared_mime: &str,
    data: &[u8],
) -> Result<(String, Option<u32>, Option<u32>), Error> {
    let mime = canonical_mime(declared_mime)?;
    if data.is_empty() {
        return Err(Error::config("reference content must be nonempty"));
    }
    if image_type(&mime).is_some() {
        let detected = signature_type(data)?;
        if detected != mime {
            return Err(Error::config(
                "image data signature does not match declared MIME type",
            ));
        }
        let info = validate_image(&mime, data)?;
        return Ok((mime, Some(info.width), Some(info.height)));
    }
    Ok((mime, None, None))
}

pub fn content_from_bytes(
    id: impl Into<String>,
    declared_or_path: Option<&str>,
    data: Vec<u8>,
) -> Result<ReferenceContent, Error> {
    let mime = detect_mime_from_bytes(declared_or_path, &data);
    if image_type(&mime).is_some() {
        let detected = signature_type(&data)?;
        let info = validate_image(detected, &data)?;
        return Ok(ReferenceContent::binary(
            id,
            detected,
            data,
            Some(info.width),
            Some(info.height),
        ));
    }
    if (mime.starts_with("text/") || mime == "application/json")
        && let Ok(text) = String::from_utf8(data.clone())
    {
        return Ok(ReferenceContent {
            id: id.into(),
            media_type: mime,
            payload: ReferencePayload::Text(text),
        });
    }
    Ok(ReferenceContent::binary(id, mime, data, None, None))
}

/// Session-owned reference list. Cloning shares the kinds; it does not copy catalogs.
#[derive(Clone, Default)]
pub struct References {
    inner: Vec<DynReference>,
}

impl std::fmt::Debug for References {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("References")
            .field("kinds", &self.kinds())
            .finish()
    }
}

impl References {
    pub(crate) fn new(references: Vec<DynReference>) -> Result<Self, Error> {
        let installed = Self { inner: references };
        installed.validate()?;
        Ok(installed)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &DynReference> {
        self.inner.iter()
    }

    pub(crate) fn get(&self, kind: &str) -> Option<&DynReference> {
        self.inner.iter().find(|reference| reference.kind() == kind)
    }

    pub(crate) fn kinds(&self) -> Vec<String> {
        self.inner
            .iter()
            .map(|reference| reference.kind().to_owned())
            .collect()
    }

    pub(crate) fn owns(&self, key: &str) -> bool {
        self.inner.iter().any(|reference| reference.kind() == key)
    }

    pub(crate) fn upsert(&mut self, reference: DynReference) {
        if let Some(slot) = self
            .inner
            .iter_mut()
            .find(|existing| existing.kind() == reference.kind())
        {
            *slot = reference;
        } else {
            self.inner.push(reference);
        }
    }

    /// Validate restored custom variables before recovery performs side effects.
    pub(crate) fn validate_variables(&self, variables: &super::Variables) -> Result<(), Error> {
        for reference in &self.inner {
            if variables.get_all(reference.kind()).is_some() {
                return Err(Error::config_with_private(
                    "custom variable key collides with an installed reference kind",
                    format!(
                        "custom variable key `{}` collides with an installed reference kind",
                        reference.kind()
                    ),
                ));
            }
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), Error> {
        let mut seen = std::collections::HashSet::new();
        for reference in &self.inner {
            microcrab_protocol::validate_reference_kind(reference.kind())?;
            if !seen.insert(reference.kind().to_owned()) {
                return Err(Error::config_with_private(
                    "duplicate reference kind",
                    format!("duplicate reference kind `{}`", reference.kind()),
                ));
            }
        }
        Ok(())
    }
}

/// One catalog row plus the kind that owns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedReference {
    pub kind: String,
    pub entry: ReferenceEntry,
}

pub(crate) fn list_all(references: &References) -> Result<Vec<ListedReference>, Error> {
    let mut listed = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for reference in references.iter() {
        for entry in reference.list()? {
            validate_id(&entry.id)?;
            if !seen.insert(entry.id.clone()) {
                return Err(Error::reference_with_private(
                    "duplicate listed reference id; ids must be globally unique",
                    format!(
                        "duplicate listed reference id `{}`; ids must be globally unique",
                        entry.id
                    ),
                ));
            }
            listed.push(ListedReference {
                kind: reference.kind().to_owned(),
                entry,
            });
        }
    }
    Ok(listed)
}

pub(crate) fn validate_id(id: &str) -> Result<(), Error> {
    if id.is_empty() {
        return Err(Error::reference("reference id is empty"));
    }
    if id.contains('\0') {
        return Err(Error::reference("reference id contains NUL"));
    }
    Ok(())
}

pub(crate) async fn owner_of_id(
    references: &References,
    id: &str,
    cancel: CancellationToken,
) -> Result<Option<DynReference>, Error> {
    validate_id(id)?;
    let mut owner = None;
    for reference in references.iter() {
        if reference.owns(id, cancel.clone()).await? {
            if owner.is_some() {
                return Err(Error::reference_with_private(
                    "reference id is owned by more than one kind",
                    format!("reference id `{id}` is owned by more than one kind"),
                ));
            }
            owner = Some(reference.clone());
        }
    }
    Ok(owner)
}

pub(crate) async fn expand_id(
    references: &References,
    id: &str,
    cancel: CancellationToken,
) -> Result<ReferenceContent, Error> {
    validate_id(id)?;
    let mut found = None;
    for reference in references.iter() {
        if let Some(content) = reference.expand(id, cancel.clone()).await? {
            if content.id != id {
                return Err(Error::reference_with_private(
                    "reference body id does not match the requested id",
                    format!(
                        "reference kind `{}` returned a body whose id does not match requested id `{id}`",
                        reference.kind()
                    ),
                ));
            }
            if found.is_some() {
                return Err(Error::reference_with_private(
                    "reference id is owned by more than one kind",
                    format!("reference id `{id}` is owned by more than one kind"),
                ));
            }
            found = Some(content);
        }
    }
    found.ok_or_else(|| {
        Error::reference_with_private(
            "requested reference was not found",
            format!("reference `{id}` was not found"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boundary::Boundary;
    use crate::{Limits, State, VariablesChange};
    use async_trait::async_trait;
    use serde_json::{Map, Value, json};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct Sample {
        kind: &'static str,
        loads: Arc<AtomicUsize>,
        changes: Arc<AtomicUsize>,
        projection: Mutex<Option<Value>>,
    }

    #[async_trait]
    impl Reference for Sample {
        fn kind(&self) -> &str {
            self.kind
        }

        fn capabilities(&self) -> ReferenceCapabilities {
            ReferenceCapabilities::read_only()
        }

        async fn load(&self, _cancel: CancellationToken) -> Result<(), ReferenceError> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
            Ok(vec![ReferenceEntry {
                id: "skill#demo".into(),
                node_kind: ReferenceNodeKind::Section,
                parent_id: None,
                title: "Demo".into(),
                location: Some(".crab/skills/demo".into()),
                description: Some("example".into()),
                media_type: Some("text/markdown".into()),
                revision: "rev1".into(),
                writable: false,
                metadata: None,
            }])
        }

        fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
            self.changes.fetch_add(1, Ordering::SeqCst);
            let value = self
                .projection
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            Ok(ReferenceProjection {
                exact: value,
                short_model: None,
            })
        }

        async fn expand(
            &self,
            id: &str,
            _cancel: CancellationToken,
        ) -> Result<Option<ReferenceContent>, ReferenceError> {
            if id == "skill#demo" {
                Ok(Some(ReferenceContent::text(id, "skill body")))
            } else {
                Ok(None)
            }
        }
    }

    fn sample(
        kind: &'static str,
        projection: Option<Value>,
    ) -> (DynReference, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let loads = Arc::new(AtomicUsize::new(0));
        let changes = Arc::new(AtomicUsize::new(0));
        let reference = DynReference::new(Sample {
            kind,
            loads: Arc::clone(&loads),
            changes: Arc::clone(&changes),
            projection: Mutex::new(projection),
        })
        .unwrap();
        (reference, loads, changes)
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Failure {
        None,
        Load,
        List,
        Expand,
        Projection,
    }

    struct Catalog {
        kind: &'static str,
        ids: Vec<&'static str>,
        body_id: Option<&'static str>,
        failure: Failure,
    }

    #[async_trait]
    impl Reference for Catalog {
        fn kind(&self) -> &str {
            self.kind
        }

        fn capabilities(&self) -> ReferenceCapabilities {
            ReferenceCapabilities::read_only()
        }

        async fn load(&self, _cancel: CancellationToken) -> Result<(), ReferenceError> {
            if self.failure == Failure::Load {
                return Err(ReferenceError::failed("sample reference failed"));
            }
            Ok(())
        }

        fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
            if self.failure == Failure::List {
                return Err(ReferenceError::failed("sample reference failed"));
            }
            Ok(self
                .ids
                .iter()
                .map(|id| ReferenceEntry {
                    id: (*id).into(),
                    node_kind: ReferenceNodeKind::File,
                    parent_id: None,
                    title: "entry".into(),
                    location: None,
                    description: None,
                    media_type: Some("text/plain".into()),
                    revision: "rev1".into(),
                    writable: false,
                    metadata: None,
                })
                .collect())
        }

        fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
            if self.failure == Failure::Projection {
                return Err(ReferenceError::failed("sample reference failed"));
            }
            Ok(ReferenceProjection::empty())
        }

        async fn owns(&self, id: &str, _cancel: CancellationToken) -> Result<bool, ReferenceError> {
            Ok(self.ids.contains(&id))
        }

        async fn expand(
            &self,
            id: &str,
            _cancel: CancellationToken,
        ) -> Result<Option<ReferenceContent>, ReferenceError> {
            if self.failure == Failure::Expand {
                return Err(ReferenceError::failed("sample reference failed"));
            }
            Ok(self
                .ids
                .contains(&id)
                .then(|| ReferenceContent::text(self.body_id.unwrap_or(id), "private body")))
        }
    }

    fn catalog(kind: &'static str, ids: Vec<&'static str>) -> Catalog {
        Catalog {
            kind,
            ids,
            body_id: None,
            failure: Failure::None,
        }
    }

    fn catalogs(kinds: Vec<Catalog>) -> References {
        References::new(
            kinds
                .into_iter()
                .map(|kind| DynReference::new(kind).unwrap())
                .collect(),
        )
        .unwrap()
    }

    fn reference_message<T: std::fmt::Debug>(result: Result<T, Error>) -> String {
        match result.unwrap_err() {
            Error::Reference(message) => message.to_string(),
            error => panic!("expected reference error, got {error}"),
        }
    }

    #[test]
    fn rejects_duplicate_listed_ids_within_and_across_kinds() {
        for references in [
            catalogs(vec![catalog("skills", vec!["shared", "shared"])]),
            catalogs(vec![
                catalog("skills", vec!["shared"]),
                catalog("references", vec!["shared"]),
            ]),
        ] {
            assert_eq!(
                reference_message(list_all(&references)),
                "duplicate listed reference id `shared`; ids must be globally unique"
            );
        }
    }

    #[tokio::test]
    async fn list_and_expand_reject_empty_and_nul_ids() {
        let mut failing = catalog("skills", vec![]);
        failing.failure = Failure::Expand;
        let references = catalogs(vec![failing]);
        for (id, message) in [
            ("", "reference id is empty"),
            ("invalid\0id", "reference id contains NUL"),
        ] {
            let listed = catalogs(vec![catalog("skills", vec![id])]);
            assert_eq!(reference_message(list_all(&listed)), message);
            assert_eq!(
                reference_message(expand_id(&references, id, CancellationToken::new()).await),
                message
            );
        }
    }

    #[tokio::test]
    async fn expand_rejects_mismatched_body_without_disclosing_it() {
        let mut inconsistent = catalog("skills", vec!["requested"]);
        inconsistent.body_id = Some("unexpected");
        let references = catalogs(vec![inconsistent]);
        let message = reference_message(
            expand_id(&references, "requested", CancellationToken::new()).await,
        );
        assert!(message.contains("skills"));
        assert!(message.contains("requested"));
        assert!(message.contains("does not match"));
        assert!(!message.contains("unexpected"));
        assert!(!message.contains("private body"));
    }

    #[tokio::test]
    async fn expand_requires_exactly_one_owner_and_accepts_unprefixed_ids() {
        let references = catalogs(vec![
            catalog("skills", vec!["shared"]),
            catalog("references", vec!["shared"]),
        ]);
        assert_eq!(
            reference_message(expand_id(&references, "shared", CancellationToken::new()).await),
            "reference id `shared` is owned by more than one kind"
        );
        assert_eq!(
            reference_message(expand_id(&references, "unknown", CancellationToken::new()).await),
            "reference `unknown` was not found"
        );
        let references = catalogs(vec![
            catalog("skills", vec![]),
            catalog("references", vec!["plain"]),
        ]);
        assert_eq!(
            expand_id(&references, "plain", CancellationToken::new())
                .await
                .unwrap(),
            ReferenceContent::text("plain", "private body")
        );
    }

    #[tokio::test]
    async fn typed_errors_propagate_through_every_erased_method() {
        for failure in [
            Failure::Load,
            Failure::List,
            Failure::Expand,
            Failure::Projection,
        ] {
            let mut kind = catalog("skills", vec!["entry"]);
            kind.failure = failure;
            let reference = DynReference::new(kind).unwrap();
            let message = match failure {
                Failure::Load => reference
                    .load(CancellationToken::new())
                    .await
                    .unwrap_err()
                    .to_string(),
                Failure::List => reference_message(list_all(&References::new(vec![reference]).unwrap())),
                Failure::Expand => reference_message(
                    expand_id(
                        &References::new(vec![reference]).unwrap(),
                        "entry",
                        CancellationToken::new(),
                    )
                    .await,
                ),
                Failure::Projection => reference.projections().unwrap_err().to_string(),
                Failure::None => unreachable!("only failure cases are exercised"),
            };
            assert_eq!(message, "sample reference failed");
        }
    }

    #[tokio::test]
    async fn expand_does_not_enumerate_catalogs() {
        let mut kind = catalog("skills", vec!["entry"]);
        kind.failure = Failure::List;
        let references = catalogs(vec![kind]);
        assert!(list_all(&references).is_err());
        assert_eq!(
            expand_id(&references, "entry", CancellationToken::new())
                .await
                .unwrap()
                .id,
            "entry"
        );
    }

    #[test]
    fn restored_variables_cannot_shadow_installed_kinds_even_without_projection() {
        let references = catalogs(vec![catalog("skills", vec![])]);
        for value in [Value::Null, json!({"secret": "private value"})] {
            let variables = super::super::Variables::new(json!({"skills": value})).unwrap();
            let Error::Config(message) = references.validate_variables(&variables).unwrap_err()
            else {
                panic!("expected a configuration error");
            };
            assert_eq!(
                message.as_str(),
                "custom variable key `skills` collides with an installed reference kind"
            );
        }
        let variables = super::super::Variables::new(json!({"custom": 1})).unwrap();
        references.validate_variables(&variables).unwrap();
        References::default()
            .validate_variables(&variables)
            .unwrap();
    }

    #[tokio::test]
    async fn serde_excludes_live_wiring_while_clones_share_it() {
        let (reference, loads, _) = sample("skills", Some(json!({"catalog": "demo"})));
        let mut snapshot = crate::SessionState::new(Limits::default());
        snapshot.references = References::new(vec![reference]).unwrap();
        let cloned = snapshot.clone();
        for reference in cloned.references.iter() {
            reference.load(CancellationToken::new()).await.unwrap();
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        let encoded = serde_json::to_value(&snapshot).unwrap();
        assert!(encoded.get("references").is_none());
        let decoded: crate::SessionState = serde_json::from_value(encoded).unwrap();
        assert!(decoded.references.iter().next().is_none());
        assert!(list_all(&decoded.references).unwrap().is_empty());
        assert_eq!(
            expand_id(&snapshot.references, "skill#demo", CancellationToken::new())
                .await
                .unwrap()
                .as_text(),
            Some("skill body")
        );
        assert_eq!(
            expand_id(&cloned.references, "skill#demo", CancellationToken::new())
                .await
                .unwrap()
                .as_text(),
            Some("skill body")
        );
    }

    #[test]
    fn rejects_reserved_and_duplicate_kinds() {
        assert!(
            DynReference::new(Sample {
                kind: "host",
                loads: Arc::new(AtomicUsize::new(0)),
                changes: Arc::new(AtomicUsize::new(0)),
                projection: Mutex::new(None),
            })
            .is_err()
        );
        let (first, _, _) = sample("skills", None);
        let (second, _, _) = sample("skills", None);
        assert!(References::new(vec![first, second]).is_err());
    }

    #[tokio::test]
    async fn refresh_calls_projections_once_and_projects_it() {
        let (reference, loads, changes) = sample("skills", Some(json!({"catalog": "demo"})));
        let state = State::new(Limits::default());
        state
            .install_references(References::new(vec![reference]).unwrap())
            .unwrap();
        state.load_references(CancellationToken::new()).await.unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        state.refresh(Boundary::SessionCreated).unwrap();
        assert_eq!(
            state.model_variables().unwrap()["skills"],
            json!({"catalog": "demo"})
        );
        assert_eq!(changes.load(Ordering::SeqCst), 1);
        assert_eq!(
            state
                .read_reference_async("skill#demo", CancellationToken::new())
                .await
                .unwrap()
                .as_text(),
            Some("skill body")
        );
        assert_eq!(state.list_references().unwrap()[0].kind, "skills");
        let mut set = Map::new();
        set.insert("skills".into(), Value::Null);
        assert!(
            state
                .apply_change(VariablesChange {
                    scoped_set: vec![],
                    set,
                    remove: vec![],
                })
                .is_err()
        );
    }

    #[test]
    fn replace_keeps_references_and_drops_projection() {
        let (reference, _, _) = sample("skills", Some(json!("catalog")));
        let state = State::new(Limits::default());
        state
            .install_references(References::new(vec![reference]).unwrap())
            .unwrap();
        state.refresh(Boundary::EpochStarted).unwrap();
        assert!(state.model_variables().unwrap().get("skills").is_some());
        let mut recovered = crate::SessionState::new(Limits::default());
        recovered.epoch_id = 3;
        state.replace(recovered);
        assert_eq!(state.snapshot().epoch_id, 3);
        assert_eq!(
            state.snapshot().references.kinds(),
            vec!["skills".to_owned()]
        );
        assert!(state.model_variables().unwrap().get("skills").is_none());
        let encoded = serde_json::to_value(state.snapshot()).unwrap();
        assert!(encoded.get("references").is_none());
        let decoded: crate::SessionState = serde_json::from_value(encoded).unwrap();
        assert!(decoded.references.iter().next().is_none());
    }

    #[tokio::test]
    async fn refresh_writable_skips_and_isolates_read_only_composite_members() {
        use microcrab_host_api::{CompositeReference, Reference, ReferenceCapabilities};
        use microcrab_protocol::{
            ReferenceContent, ReferenceEntry, ReferenceError, ReferenceProjection, ReferenceWrite,
        };
        struct FailingReadOnly(Arc<AtomicUsize>);
        #[async_trait::async_trait]
        impl Reference for FailingReadOnly {
            fn kind(&self) -> &str {
                "ro"
            }
            fn capabilities(&self) -> ReferenceCapabilities {
                ReferenceCapabilities::read_only()
            }
            async fn load(&self, _: CancellationToken) -> Result<(), ReferenceError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(ReferenceError::failed("ro load failed"))
            }
            fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
                Ok(vec![])
            }
            fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
                Ok(ReferenceProjection::empty())
            }
            async fn expand(
                &self,
                _: &str,
                _: CancellationToken,
            ) -> Result<Option<ReferenceContent>, ReferenceError> {
                Ok(None)
            }
        }
        struct Writable(Arc<AtomicUsize>);
        #[async_trait::async_trait]
        impl Reference for Writable {
            fn kind(&self) -> &str {
                "rw"
            }
            fn capabilities(&self) -> ReferenceCapabilities {
                ReferenceCapabilities::read_write()
            }
            async fn load(&self, _: CancellationToken) -> Result<(), ReferenceError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
                Ok(vec![])
            }
            fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
                Ok(ReferenceProjection::empty())
            }
            async fn expand(
                &self,
                _: &str,
                _: CancellationToken,
            ) -> Result<Option<ReferenceContent>, ReferenceError> {
                Ok(None)
            }
            async fn add(
                &self,
                _: ReferenceWrite,
                _: CancellationToken,
            ) -> Result<ReferenceEntry, ReferenceError> {
                Err(ReferenceError::denied("unused"))
            }
        }
        let ro_loads = Arc::new(AtomicUsize::new(0));
        let rw_loads = Arc::new(AtomicUsize::new(0));
        let composite = DynReference::new(CompositeReference::new(
            "references",
            vec![
                DynReference::new(Writable(rw_loads.clone())).unwrap(),
                DynReference::new(FailingReadOnly(ro_loads.clone())).unwrap(),
            ],
        ))
        .unwrap();
        let state = State::new(Limits::default());
        state
            .install_references(References::new(vec![composite.clone()]).unwrap())
            .unwrap();
        state.load_references(CancellationToken::new()).await.unwrap();
        assert_eq!(rw_loads.load(Ordering::SeqCst), 1);
        assert_eq!(ro_loads.load(Ordering::SeqCst), 1);
        // Ordinary turn refresh skips read-only members completely.
        state
            .refresh_writable_references(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(rw_loads.load(Ordering::SeqCst), 2);
        assert_eq!(ro_loads.load(Ordering::SeqCst), 1);
        // Explicit full refresh still attempts read-only members but isolates failures.
        state
            .refresh_references_async(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(rw_loads.load(Ordering::SeqCst), 3);
        assert_eq!(ro_loads.load(Ordering::SeqCst), 2);
    }
}
