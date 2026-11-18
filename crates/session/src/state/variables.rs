//! Custom values, governance, and frozen checkpoint encoding provenance.
use crate::Error;
use microcrab_protocol::{
    ScopedVariableSet, VariableMetadata, VariablePersistence, VariableRole, VariableVisibility,
    VariablesChange,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use std::{collections::BTreeMap, fmt};

use std::sync::Arc;

#[derive(Clone, PartialEq)]
struct Variable {
    value: Value,
    public_value: Value,
    metadata: VariableMetadata,
}
impl Variable {
    fn new(name: &str, value: Value, mut metadata: VariableMetadata) -> Self {
        metadata.normalize();
        let public_value = if metadata.visibility == VariableVisibility::Secret {
            Value::String(microcrab_protocol::secret_placeholder(name))
        } else {
            value.clone()
        };
        Self {
            value,
            public_value,
            metadata,
        }
    }
    fn public_value(&self) -> Value {
        self.public_value.clone()
    }
    fn set_value(&mut self, value: Value) {
        self.public_value = if self.metadata.visibility == VariableVisibility::Secret {
            self.public_value.clone()
        } else {
            value.clone()
        };
        self.value = value;
    }
    fn public_value_ref(&self) -> &Value {
        &self.public_value
    }
}

/// Immutable, non-serializable Secret values captured for one authorized operation.
/// Only the dispatch/output owners in this crate can inspect plaintext.
#[derive(Clone)]
pub(crate) struct SecretSnapshot {
    entries: Arc<Vec<(String, String)>>,
}
impl SecretSnapshot {
    pub(crate) fn lookup(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}
impl std::fmt::Debug for SecretSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretSnapshot")
            .field(
                "keys",
                &self.entries.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// One consistent custom-variable view for pre-permission injection.
/// Secret and invisible entries are identities only; their values are not copied.
#[derive(Clone, Debug)]
pub(crate) struct InjectionView {
    visible: BTreeMap<String, Value>,
    invisible: std::collections::BTreeSet<String>,
    secret: std::collections::BTreeSet<String>,
}
impl InjectionView {
    pub(crate) fn lookup(&self, key: &str) -> InjectionKey<'_> {
        if let Some(value) = self.visible.get(key) {
            InjectionKey::Visible(value)
        } else if self.invisible.contains(key) {
            InjectionKey::Invisible
        } else if self.secret.contains(key) {
            InjectionKey::Secret
        } else {
            InjectionKey::Missing
        }
    }
}
pub(crate) enum InjectionKey<'a> {
    Visible(&'a Value),
    Invisible,
    Secret,
    Missing,
}
#[cfg(test)]
impl InjectionView {
    pub(crate) fn fixture(
        visible: BTreeMap<String, Value>,
        invisible: std::collections::BTreeSet<String>,
        secret: std::collections::BTreeSet<String>,
    ) -> Self {
        Self {
            visible,
            invisible,
            secret,
        }
    }
}

#[derive(Clone, Default, PartialEq)]
pub struct Variables {
    // Frozen hashes include insertion order and omitted governance fields.
    entries: Vec<(String, Variable)>,
    historical_metadata: Option<BTreeMap<String, Value>>,
}

impl Variables {
    pub fn new(value: Value) -> Result<Self, Error> {
        let Value::Object(values) = value else {
            return Err(Error::config("runtime state must be a JSON object"));
        };
        let mut variables = Self::default();
        for (key, value) in values {
            super::access::validate_custom_key(&key)?;
            variables.insert(key, value);
        }
        Ok(variables)
    }

    pub fn as_value(&self) -> Value {
        Value::Object(
            self.entries
                .iter()
                .filter(|(_, entry)| {
                    matches!(
                        entry.metadata.visibility,
                        VariableVisibility::Visible | VariableVisibility::Secret
                    )
                })
                .map(|(key, entry)| (key.clone(), entry.public_value()))
                .collect(),
        )
    }
    /// Only a detached projection clone may expose ordinary invisible values;
    /// Secret values remain placeholders even in explicit owner inspection.
    pub(super) fn expose_for_inspection(&mut self) {
        for (_, entry) in &mut self.entries {
            if entry.metadata.visibility == VariableVisibility::Invisible {
                entry.metadata.visibility = VariableVisibility::Visible;
            }
        }
    }
    /// All custom values, including invisible entries, with Secrets always masked.
    pub fn all_values(&self) -> Value {
        Value::Object(
            self.entries
                .iter()
                .map(|(key, entry)| (key.clone(), entry.public_value()))
                .collect(),
        )
    }
    pub(crate) fn injection_view(&self) -> InjectionView {
        let mut visible = BTreeMap::new();
        let mut invisible = std::collections::BTreeSet::new();
        let mut secret = std::collections::BTreeSet::new();
        for (key, entry) in &self.entries {
            match entry.metadata.visibility {
                VariableVisibility::Visible => {
                    visible.insert(key.clone(), entry.value.clone());
                }
                VariableVisibility::Invisible => {
                    invisible.insert(key.clone());
                }
                VariableVisibility::Secret => {
                    secret.insert(key.clone());
                }
            }
        }
        InjectionView {
            visible,
            invisible,
            secret,
        }
    }
    pub(crate) fn secret_snapshot(&self) -> SecretSnapshot {
        SecretSnapshot {
            entries: Arc::new(
                self.entries
                    .iter()
                    .filter(|(_, entry)| entry.metadata.visibility == VariableVisibility::Secret)
                    .filter_map(|(key, entry)| {
                        entry
                            .value
                            .as_str()
                            .map(|value| (key.clone(), value.to_owned()))
                    })
                    .collect(),
            ),
        }
    }
    fn raw_values(&self) -> Value {
        Value::Object(
            self.entries
                .iter()
                .map(|(key, entry)| (key.clone(), entry.value.clone()))
                .collect(),
        )
    }
    pub fn metadata(&self) -> BTreeMap<String, VariableMetadata> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.metadata.clone()))
            .collect()
    }
    pub(crate) fn persisted_invisible(&self) -> Map<String, Value> {
        self.entries
            .iter()
            .filter(|(_, entry)| {
                entry.metadata.visibility == VariableVisibility::Invisible
                    && entry.metadata.persistence == VariablePersistence::Persisted
            })
            .map(|(key, entry)| (key.clone(), entry.value.clone()))
            .collect()
    }
    pub(crate) fn persisted_metadata(&self) -> BTreeMap<String, Value> {
        if let Some(raw) = &self.historical_metadata {
            return raw.clone();
        }
        self.entries
            .iter()
            .filter(|(_, entry)| entry.metadata.persistence == VariablePersistence::Persisted)
            .map(|(key, entry)| (key.clone(), serde_json::json!(entry.metadata)))
            .collect()
    }
    /// Called only after immutable checkpoint validation has succeeded.
    pub(crate) fn activate(&mut self) {
        self.historical_metadata = None;
        for (_, entry) in &mut self.entries {
            entry.metadata.normalize();
        }
    }
    pub(crate) fn restore_metadata(
        &mut self,
        raw: BTreeMap<String, Value>,
        invisible: Map<String, Value>,
    ) -> Result<(), Error> {
        for (name, value) in invisible {
            super::access::validate_custom_key(&name)?;
            if self.get_all(&name).is_some() {
                return Err(Error::config("duplicate persisted variable name"));
            }
            let encoded = raw
                .get(&name)
                .ok_or_else(|| Error::config("missing invisible variable metadata"))?;
            let metadata: VariableMetadata = serde_json::from_value(encoded.clone())?;
            if metadata.visibility != VariableVisibility::Invisible
                || metadata.persistence != VariablePersistence::Persisted
            {
                return Err(Error::config(
                    "invalid persisted invisible variable attributes",
                ));
            }
            let variable = Variable::new(&name, value, metadata);
            self.put(name, variable);
        }
        for (name, encoded) in &raw {
            let metadata: VariableMetadata = serde_json::from_value(encoded.clone())?;
            let Some((_, entry)) = self.entries.iter_mut().find(|(key, _)| key == name) else {
                return Err(Error::config("metadata without a persisted variable"));
            };
            if metadata.persistence != VariablePersistence::Persisted
                || metadata.visibility != entry.metadata.visibility
            {
                return Err(Error::config(
                    "persisted variable metadata disagrees with its value scope",
                ));
            }
            entry.metadata = metadata;
        }
        self.historical_metadata = Some(raw);
        Ok(())
    }
    pub(crate) fn clear_ephemeral(&mut self) {
        self.entries
            .retain(|(_, entry)| entry.metadata.persistence != VariablePersistence::Ephemeral);
    }
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, entry)| entry)
            .filter(|entry| entry.metadata.visibility != VariableVisibility::Invisible)
            .map(|entry| entry.public_value_ref())
    }
    pub fn get_all(&self, key: &str) -> Option<&Value> {
        self.entries
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, entry)| entry)
            .map(|entry| entry.public_value_ref())
    }
    fn insert(&mut self, key: String, value: Value) {
        self.activate();
        if let Some((_, entry)) = self.entries.iter_mut().find(|(name, _)| name == &key) {
            entry.set_value(value);
        } else {
            let variable = Variable::new(
                &key,
                value,
                VariableMetadata {
                    editors: vec![VariableRole::Client],
                    ..Default::default()
                },
            );
            self.put(key, variable);
        }
    }
    pub(crate) fn receipt_change(&self, change: &VariablesChange) -> VariablesChange {
        let mut safe = change.without_ephemeral();
        safe.set.retain(|name, _| {
            !self.entries.iter().any(|(key, entry)| {
                key == name && entry.metadata.persistence == VariablePersistence::Ephemeral
            })
        });
        safe
    }
    /// The caller stages this on a clone and admits its total size before publish.
    pub(crate) fn change(
        &mut self,
        change: &VariablesChange,
        actor: VariableRole,
    ) -> Result<(), Error> {
        for name in &change.remove {
            if let Some((_, entry)) = self.entries.iter().find(|(key, _)| key == name)
                && entry.metadata.owner != actor
            {
                return Err(Error::VariableForbidden(
                    format!(
                        "only owner {:?} may delete variable {name}; caller is {actor:?}",
                        entry.metadata.owner
                    )
                    .into(),
                ));
            }
            self.remove(name);
        }
        for (name, value) in &change.set {
            if let Some((_, entry)) = self.entries.iter_mut().find(|(key, _)| key == name) {
                if !entry.metadata.editors.contains(&actor) && entry.metadata.owner != actor {
                    return Err(Error::VariableForbidden(
                        format!(
                            "caller {actor:?} is not an editor of variable {name}; owner is {:?}",
                            entry.metadata.owner
                        )
                        .into(),
                    ));
                }
                if entry.metadata.visibility == VariableVisibility::Secret {
                    crate::admission::variables::validate_secret(name, value)?;
                }
                entry.set_value(value.clone());
            } else {
                let variable = Variable::new(
                    name,
                    value.clone(),
                    VariableMetadata {
                        owner: actor,
                        editors: vec![actor],
                        ..Default::default()
                    },
                );
                self.put(name.clone(), variable);
            }
        }
        for entry in &change.scoped_set {
            self.change_scoped(entry, actor)?;
        }
        self.activate();
        Ok(())
    }
    fn change_scoped(&mut self, set: &ScopedVariableSet, actor: VariableRole) -> Result<(), Error> {
        let existing = self
            .entries
            .iter()
            .find(|(name, _)| name == &set.name)
            .map(|(_, entry)| entry.metadata.clone());
        if set.visibility == VariableVisibility::Secret
            || existing
                .as_ref()
                .is_some_and(|metadata| metadata.visibility == VariableVisibility::Secret)
        {
            let Some(previous) = existing.as_ref() else {
                return Err(Error::config(
                    "Secret creation requires a Client secret sideband write",
                ));
            };
            if actor != VariableRole::Client
                || previous.visibility != VariableVisibility::Secret
                || set.visibility != VariableVisibility::Secret
                || set.persistence != VariablePersistence::Ephemeral
                || set
                    .editors
                    .as_ref()
                    .is_some_and(|editors| editors.iter().any(|role| *role != VariableRole::Client))
            {
                return Err(Error::config(
                    "Secret scope and Client-only governance cannot be changed",
                ));
            }
            crate::admission::variables::validate_secret(&set.name, &set.value)?;
        }
        let mut metadata = existing.clone().unwrap_or(VariableMetadata {
            owner: actor,
            editors: vec![actor],
            ..Default::default()
        });
        metadata.visibility = set.visibility;
        metadata.persistence = set.persistence;
        if let Some(editors) = &set.editors {
            metadata.editors = editors.clone();
        }
        metadata.normalize();
        if let Some(mut previous) = existing {
            previous.normalize();
            if previous.owner != actor {
                if !previous.editors.contains(&actor) {
                    return Err(Error::VariableForbidden(
                        format!(
                            "caller {actor:?} is not an editor of variable {}; owner is {:?}",
                            set.name, previous.owner
                        )
                        .into(),
                    ));
                }
                if metadata != previous {
                    return Err(Error::VariableForbidden(format!("only owner {:?} may change scope or editors of variable {}; caller is {actor:?}", previous.owner, set.name).into()));
                }
            }
        }
        self.put(
            set.name.clone(),
            Variable::new(&set.name, set.value.clone(), metadata),
        );
        Ok(())
    }
    /// This raw capture is used only for admission size accounting, never disclosure.
    pub(crate) fn admission_value(&self) -> Value {
        serde_json::json!({"values": self.raw_values(), "metadata": self.metadata()})
    }
    pub(super) fn write_secrets(&mut self, values: &BTreeMap<String, String>) -> Result<(), Error> {
        if !(1..=8).contains(&values.len()) {
            return Err(Error::config("Secret writes require 1..8 keys"));
        }
        for (key, value) in values {
            crate::admission::variables::validate_custom_key(key)?;
            crate::admission::variables::validate_secret(key, &Value::String(value.clone()))?;
            if self.entries.iter().any(|(name, entry)| {
                name == key
                    && (entry.metadata.visibility != VariableVisibility::Secret
                        || entry.metadata.owner != VariableRole::Client)
            }) {
                return Err(Error::config("secret_key_conflict"));
            }
        }
        for (key, value) in values {
            self.put(
                key.clone(),
                Variable::new(
                    key,
                    Value::String(value.clone()),
                    VariableMetadata {
                        visibility: VariableVisibility::Secret,
                        persistence: VariablePersistence::Ephemeral,
                        owner: VariableRole::Client,
                        editors: vec![VariableRole::Client],
                    },
                ),
            );
        }
        self.activate();
        Ok(())
    }
    fn put(&mut self, name: String, value: Variable) {
        if let Some((_, entry)) = self.entries.iter_mut().find(|(key, _)| key == &name) {
            *entry = value;
        } else {
            self.entries.push((name, value));
        }
    }
    pub fn remove(&mut self, key: &str) {
        if self.get_all(key).is_some() {
            self.activate();
            self.entries.retain(|(name, _)| name != key);
        }
    }
}
impl Serialize for Variables {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let values: Map<String, Value> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.metadata.visibility == VariableVisibility::Visible)
            .map(|(key, entry)| (key.clone(), entry.value.clone()))
            .collect();
        values.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Variables {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let values = Map::<String, Value>::deserialize(deserializer)?;
        let mut variables = Self::default();
        for (name, value) in values {
            let variable = Variable::new(&name, value, VariableMetadata::default());
            variables.put(name, variable);
        }
        variables.historical_metadata = Some(BTreeMap::new());
        Ok(variables)
    }
}
impl fmt::Debug for Variables {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Variables")
            .field("values", &"[REDACTED]")
            .finish()
    }
}
