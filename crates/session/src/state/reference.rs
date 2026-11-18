//! Reference kinds written into the model-facing state projection.
//!
//! skills, important reference(like **/AGENTS.md), and memory share this trait. The embedder supplies the
//! concrete kinds at session construction; this module does not discover files.
//!
//! The list is runtime wiring. It is shared by `Arc` across state clones, skipped
//! by recovery serialization, and preserved when a recovered snapshot replaces
//! counters and variables. Each kind synchronizes its own catalog. `load` and
//! `change` run without the state lock so a kind cannot deadlock on `State`.

pub mod focus;
pub mod markdown;
pub use markdown::{MarkdownReferences, ReferenceLimits, ReferenceSource};

use crate::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;

/// One listed entry. `id` is the key later passed to [`Reference::read`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceInfo {
    pub id: String,
    pub title: String,
    pub location: String,
    pub description: String,
}

/// Body returned for one owned id. Absence is `Ok(None)`, not an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceContent {
    pub id: String,
    pub body: String,
}

/// A reference kind installed into [`super::SessionState`].
///
/// `Error` belongs to the kind. The session erases it when the kind is placed
/// in the dynamic list. `load` runs before the first lifecycle boundary and is
/// repeated after workspace reconciliation on initial or live recovery. It must
/// tolerate retries. `change` returns the full current projection for this kind;
/// `None` removes its key. It must be a pure, repeatable read, never consume a
/// pending delta: state clones share the same kind. Admission previews use the
/// frozen projection without calling `change` again.
/// Returning unchanged JSON produces no model diff. A projection error fails
/// the boundary while preserving the previous projection.
///
/// Entry ids are globally unique, non-empty, and contain no NUL. A kind may
/// prefix its ids but the runtime does not require a namespace. Implementors
/// keep `list` and `read` consistent: each listed id belongs to that kind, and
/// each returned body has the requested id. Catalog freshness and synchronization
/// belong to the kind; reads do not enumerate all catalogs to prove membership.
pub trait Reference: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Stable projection key, such as `skills`, `agents`, or `memory`.
    fn kind(&self) -> &str;

    fn load(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn list(&self) -> Result<Vec<ReferenceInfo>, Self::Error>;

    /// `Ok(None)` means this kind does not own `id`.
    fn read(&self, id: &str) -> Result<Option<ReferenceContent>, Self::Error>;

    fn change(&self) -> Result<Option<Value>, Self::Error>;
}

#[async_trait]
trait ErasedReference: Send + Sync {
    async fn load(&self) -> Result<(), Error>;
    fn list(&self) -> Result<Vec<ReferenceInfo>, Error>;
    fn read(&self, id: &str) -> Result<Option<ReferenceContent>, Error>;
    fn change(&self) -> Result<Option<Value>, Error>;
}

struct Erased<R: Reference>(R);

#[async_trait]
impl<R: Reference> ErasedReference for Erased<R> {
    async fn load(&self) -> Result<(), Error> {
        self.0.load().await.map_err(reference_error)
    }

    fn list(&self) -> Result<Vec<ReferenceInfo>, Error> {
        self.0.list().map_err(reference_error)
    }

    fn read(&self, id: &str) -> Result<Option<ReferenceContent>, Error> {
        self.0.read(id).map_err(reference_error)
    }

    fn change(&self) -> Result<Option<Value>, Error> {
        self.0.change().map_err(reference_error)
    }
}

fn reference_error(error: impl std::error::Error) -> Error {
    Error::Reference(error.to_string().into())
}

/// One installed kind. The projection key is fixed at construction.
#[derive(Clone)]
pub struct DynReference {
    kind: String,
    inner: Arc<dyn ErasedReference>,
}

impl std::fmt::Debug for DynReference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DynReference")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl DynReference {
    pub fn new<R: Reference>(reference: R) -> Result<Self, Error> {
        let kind = reference.kind().to_owned();
        super::access::validate_custom_key(&kind)?;
        Ok(Self {
            kind,
            inner: Arc::new(Erased(reference)),
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub(crate) async fn load(&self) -> Result<(), Error> {
        self.inner.load().await
    }

    pub(crate) fn list(&self) -> Result<Vec<ReferenceInfo>, Error> {
        self.inner.list()
    }

    pub(crate) fn read(&self, id: &str) -> Result<Option<ReferenceContent>, Error> {
        self.inner.read(id)
    }

    pub(crate) fn change(&self) -> Result<Option<Value>, Error> {
        self.inner.change()
    }
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

    pub(crate) fn kinds(&self) -> Vec<String> {
        self.inner
            .iter()
            .map(|reference| reference.kind().to_owned())
            .collect()
    }

    pub(crate) fn owns(&self, key: &str) -> bool {
        self.inner.iter().any(|reference| reference.kind() == key)
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
            super::access::validate_custom_key(reference.kind())?;
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
    pub info: ReferenceInfo,
}

pub(crate) fn list_all(references: &References) -> Result<Vec<ListedReference>, Error> {
    let mut listed = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for reference in references.iter() {
        for info in reference.list()? {
            validate_id(&info.id)?;
            if !seen.insert(info.id.clone()) {
                return Err(Error::reference_with_private(
                    "duplicate listed reference id; ids must be globally unique",
                    format!(
                        "duplicate listed reference id `{}`; ids must be globally unique",
                        info.id
                    ),
                ));
            }
            listed.push(ListedReference {
                kind: reference.kind().to_owned(),
                info,
            });
        }
    }
    Ok(listed)
}

fn validate_id(id: &str) -> Result<(), Error> {
    if id.is_empty() {
        return Err(Error::reference("reference id is empty"));
    }
    if id.contains('\0') {
        return Err(Error::reference("reference id contains NUL"));
    }
    Ok(())
}

pub(crate) fn read_id(references: &References, id: &str) -> Result<ReferenceContent, Error> {
    validate_id(id)?;
    let mut found = None;
    for reference in references.iter() {
        if let Some(content) = reference.read(id)? {
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
    use serde_json::{Map, Value, json};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, thiserror::Error)]
    #[error("sample reference failed")]
    struct SampleError;

    struct Sample {
        kind: &'static str,
        loads: Arc<AtomicUsize>,
        changes: Arc<AtomicUsize>,
        projection: Mutex<Option<Value>>,
    }

    impl Reference for Sample {
        type Error = SampleError;

        fn kind(&self) -> &str {
            self.kind
        }

        fn load(&self) -> impl Future<Output = Result<(), Self::Error>> + Send {
            self.loads.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }

        fn list(&self) -> Result<Vec<ReferenceInfo>, Self::Error> {
            Ok(vec![ReferenceInfo {
                id: "skill#demo".into(),
                title: "Demo".into(),
                location: ".crab/skills/demo".into(),
                description: "example".into(),
            }])
        }

        fn read(&self, id: &str) -> Result<Option<ReferenceContent>, Self::Error> {
            if id == "skill#demo" {
                Ok(Some(ReferenceContent {
                    id: id.into(),
                    body: "skill body".into(),
                }))
            } else {
                Ok(None)
            }
        }

        fn change(&self) -> Result<Option<Value>, Self::Error> {
            self.changes.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .projection
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone())
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
        Read,
        Change,
    }

    struct Catalog {
        kind: &'static str,
        ids: Vec<&'static str>,
        body_id: Option<&'static str>,
        failure: Failure,
    }

    impl Reference for Catalog {
        type Error = SampleError;

        fn kind(&self) -> &str {
            self.kind
        }

        async fn load(&self) -> Result<(), Self::Error> {
            if self.failure == Failure::Load {
                return Err(SampleError);
            }
            Ok(())
        }

        fn list(&self) -> Result<Vec<ReferenceInfo>, Self::Error> {
            if self.failure == Failure::List {
                return Err(SampleError);
            }
            Ok(self
                .ids
                .iter()
                .map(|id| ReferenceInfo {
                    id: (*id).into(),
                    title: "entry".into(),
                    location: String::new(),
                    description: String::new(),
                })
                .collect())
        }

        fn read(&self, id: &str) -> Result<Option<ReferenceContent>, Self::Error> {
            if self.failure == Failure::Read {
                return Err(SampleError);
            }
            Ok(self.ids.contains(&id).then(|| ReferenceContent {
                id: self.body_id.unwrap_or(id).into(),
                body: "private body".into(),
            }))
        }

        fn change(&self) -> Result<Option<Value>, Self::Error> {
            if self.failure == Failure::Change {
                return Err(SampleError);
            }
            Ok(None)
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
                catalog("memory", vec!["shared"]),
            ]),
        ] {
            assert_eq!(
                reference_message(list_all(&references)),
                "duplicate listed reference id `shared`; ids must be globally unique"
            );
        }
    }

    #[test]
    fn list_and_read_reject_empty_and_nul_ids() {
        // An invalid request must fail validation before reaching a kind callback.
        let mut failing = catalog("skills", vec![]);
        failing.failure = Failure::Read;
        let references = catalogs(vec![failing]);
        for (id, message) in [
            ("", "reference id is empty"),
            ("invalid\0id", "reference id contains NUL"),
        ] {
            let listed = catalogs(vec![catalog("skills", vec![id])]);
            assert_eq!(reference_message(list_all(&listed)), message);
            assert_eq!(reference_message(read_id(&references, id)), message);
        }
    }

    #[test]
    fn read_rejects_mismatched_body_without_disclosing_it() {
        let mut inconsistent = catalog("skills", vec!["requested"]);
        inconsistent.body_id = Some("unexpected");
        let references = catalogs(vec![inconsistent]);
        let message = reference_message(read_id(&references, "requested"));
        assert!(message.contains("skills"));
        assert!(message.contains("requested"));
        assert!(message.contains("does not match"));
        assert!(!message.contains("unexpected"));
        assert!(!message.contains("private body"));
    }

    #[test]
    fn read_requires_exactly_one_owner_and_accepts_unprefixed_ids() {
        let references = catalogs(vec![
            catalog("skills", vec!["shared"]),
            catalog("memory", vec!["shared"]),
        ]);
        assert_eq!(
            reference_message(read_id(&references, "shared")),
            "reference id `shared` is owned by more than one kind"
        );
        assert_eq!(
            reference_message(read_id(&references, "unknown")),
            "reference `unknown` was not found"
        );
        let references = catalogs(vec![
            catalog("skills", vec![]),
            catalog("memory", vec!["plain"]),
        ]);
        assert_eq!(
            read_id(&references, "plain").unwrap(),
            ReferenceContent {
                id: "plain".into(),
                body: "private body".into(),
            }
        );
    }

    #[tokio::test]
    async fn typed_errors_propagate_through_every_erased_method() {
        for failure in [Failure::Load, Failure::List, Failure::Read, Failure::Change] {
            let mut kind = catalog("skills", vec!["entry"]);
            kind.failure = failure;
            let reference = DynReference::new(kind).unwrap();
            let message = match failure {
                Failure::Load => reference_message(reference.load().await),
                Failure::List => {
                    reference_message(list_all(&References::new(vec![reference]).unwrap()))
                }
                Failure::Read => {
                    reference_message(read_id(&References::new(vec![reference]).unwrap(), "entry"))
                }
                Failure::Change => reference_message(reference.change()),
                Failure::None => unreachable!("only failure cases are exercised"),
            };
            assert_eq!(message, "sample reference failed");
        }
    }

    #[test]
    fn read_does_not_enumerate_catalogs() {
        let mut kind = catalog("skills", vec!["entry"]);
        kind.failure = Failure::List;
        let references = catalogs(vec![kind]);
        assert!(list_all(&references).is_err());
        assert_eq!(read_id(&references, "entry").unwrap().id, "entry");
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
            reference.load().await.unwrap();
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        let encoded = serde_json::to_value(&snapshot).unwrap();
        assert!(encoded.get("references").is_none());
        let decoded: crate::SessionState = serde_json::from_value(encoded).unwrap();
        assert!(decoded.references.iter().next().is_none());
        assert!(list_all(&decoded.references).unwrap().is_empty());
        assert_eq!(
            read_id(&snapshot.references, "skill#demo").unwrap().body,
            "skill body"
        );
        assert_eq!(
            read_id(&cloned.references, "skill#demo").unwrap().body,
            "skill body"
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
    async fn refresh_calls_change_once_and_projects_it() {
        let (reference, loads, changes) = sample("skills", Some(json!({"catalog": "demo"})));
        let state = State::new(Limits::default());
        state
            .install_references(References::new(vec![reference]).unwrap())
            .unwrap();
        state.load_references().await.unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        state.refresh(Boundary::SessionCreated).unwrap();
        assert_eq!(
            state.model_variables().unwrap()["skills"],
            json!({"catalog": "demo"})
        );
        assert_eq!(changes.load(Ordering::SeqCst), 1);
        assert_eq!(
            state.read_reference("skill#demo").unwrap().body,
            "skill body"
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
}
