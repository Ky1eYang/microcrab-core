//! Custom values, governance, and frozen checkpoint encoding provenance.
use crate::Error;
use microcrab_protocol::{
    ScopedVariableSet, VariableMetadata, VariablePersistence, VariableRole, VariableVisibility,
    VariablesChange,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, PartialEq)]
struct Variable {
    value: Value,
    metadata: VariableMetadata,
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
                .filter(|(_, entry)| entry.metadata.visibility == VariableVisibility::Visible)
                .map(|(key, entry)| (key.clone(), entry.value.clone()))
                .collect(),
        )
    }
    /// Only a detached projection clone may make hidden values visible.
    pub(super) fn expose_for_inspection(&mut self) {
        for (_, entry) in &mut self.entries {
            entry.metadata.visibility = VariableVisibility::Visible;
        }
    }
    pub fn all_values(&self) -> Value {
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
            self.put(name, Variable { value, metadata });
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
            .filter(|entry| entry.metadata.visibility == VariableVisibility::Visible)
            .map(|entry| &entry.value)
    }
    pub fn get_all(&self, key: &str) -> Option<&Value> {
        self.entries
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, entry)| &entry.value)
    }
    pub fn insert(&mut self, key: String, value: Value) {
        self.activate();
        if let Some((_, entry)) = self.entries.iter_mut().find(|(name, _)| name == &key) {
            entry.value = value;
        } else {
            self.put(
                key,
                Variable {
                    value,
                    metadata: VariableMetadata {
                        editors: vec![VariableRole::Client],
                        ..Default::default()
                    },
                },
            );
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
            if let Some((_, entry)) = self.entries.iter().find(|(key, _)| key == name) {
                if entry.metadata.owner != actor {
                    return Err(Error::VariableForbidden(
                        format!(
                            "only owner {:?} may delete variable {name}; caller is {actor:?}",
                            entry.metadata.owner
                        )
                        .into(),
                    ));
                }
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
                entry.value = value.clone();
            } else {
                self.put(
                    name.clone(),
                    Variable {
                        value: value.clone(),
                        metadata: VariableMetadata {
                            owner: actor,
                            editors: vec![actor],
                            ..Default::default()
                        },
                    },
                );
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
            Variable {
                value: set.value.clone(),
                metadata,
            },
        );
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
        self.as_value().serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Variables {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let values = Map::<String, Value>::deserialize(deserializer)?;
        let mut variables = Self::default();
        for (name, value) in values {
            variables.put(
                name,
                Variable {
                    value,
                    metadata: VariableMetadata::default(),
                },
            );
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
