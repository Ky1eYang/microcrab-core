//! Canonical structured user input shared by Session and its consumers.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;

#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VariableVisibility {
    #[default]
    Visible,
    Invisible,
    /// A live write-only value; public projections expose its placeholder.
    Secret,
}

/// Secret keys have an unambiguous, bounded placeholder grammar.
pub fn valid_secret_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

/// Format an already validated key as data, never as a template evaluation.
pub fn secret_placeholder(key: &str) -> String {
    format!("{{{{secret:{key}}}}}")
}

#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VariablePersistence {
    #[default]
    Persisted,
    Ephemeral,
}

#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum VariableRole {
    Runtime,
    #[default]
    Client,
    Agent,
}

fn legacy_editors() -> Vec<VariableRole> {
    vec![VariableRole::Client, VariableRole::Agent]
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VariableMetadata {
    pub visibility: VariableVisibility,
    pub persistence: VariablePersistence,
    pub owner: VariableRole,
    pub editors: Vec<VariableRole>,
}

impl<'de> Deserialize<'de> for VariableMetadata {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            visibility: VariableVisibility,
            persistence: VariablePersistence,
            #[serde(default)]
            owner: VariableRole,
            #[serde(default = "legacy_editors")]
            editors: Vec<VariableRole>,
        }
        let raw = Value::deserialize(deserializer)?;
        if !raw.is_object() {
            return Err(serde::de::Error::custom(
                "variable metadata must be a JSON object",
            ));
        }
        if raw.get("owner").is_some() != raw.get("editors").is_some() {
            return Err(serde::de::Error::custom(
                "variable metadata must include both owner and editors, or omit both for legacy governance",
            ));
        }
        let wire: Wire = serde_json::from_value(raw).map_err(serde::de::Error::custom)?;
        let mut metadata = Self {
            visibility: wire.visibility,
            persistence: wire.persistence,
            owner: wire.owner,
            editors: wire.editors,
        };
        // Raw checkpoint encoding belongs to Variables; semantic authority always
        // retains its owner and has one canonical editor order.
        metadata.normalize();
        Ok(metadata)
    }
}

impl Default for VariableMetadata {
    fn default() -> Self {
        Self {
            visibility: VariableVisibility::Visible,
            persistence: VariablePersistence::Persisted,
            owner: VariableRole::Client,
            editors: legacy_editors(),
        }
    }
}

impl VariableMetadata {
    pub fn normalize(&mut self) {
        self.editors.push(self.owner);
        self.editors.sort();
        self.editors.dedup();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ScopedVariableSet {
    pub name: String,
    pub value: Value,
    pub visibility: VariableVisibility,
    pub persistence: VariablePersistence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub editors: Option<Vec<VariableRole>>,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct VariablesChange {
    #[serde(default)]
    pub set: Map<String, Value>,
    #[serde(default)]
    pub remove: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scoped_set: Vec<ScopedVariableSet>,
}

impl VariablesChange {
    /// Projects explicit ephemeral scoped writes only. Durable live carriers must
    /// additionally classify plain set against State metadata. Hash original input.
    pub fn without_ephemeral(&self) -> Self {
        let mut projected = self.clone();
        projected
            .scoped_set
            .retain(|entry| entry.persistence != VariablePersistence::Ephemeral);
        projected
    }

    /// Apply later operations by logical name, regardless of their former scope.
    /// Removal always removes the name in every scope. Later hook writes win.
    pub fn overlay(&mut self, later: &Self) {
        for name in later
            .set
            .keys()
            .chain(later.remove.iter())
            .chain(later.scoped_set.iter().map(|entry| &entry.name))
        {
            self.set.remove(name);
            self.remove.retain(|key| key != name);
            self.scoped_set.retain(|entry| &entry.name != name);
        }
        self.set.extend(later.set.clone());
        self.remove.extend(later.remove.clone());
        self.scoped_set.extend(later.scoped_set.clone());
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SessionInput {
    pub parts: Vec<InputPart>,
    #[serde(default)]
    pub variables: VariablesChange,
}

impl SessionInput {
    /// Removes explicit ephemeral writes; live carriers also require State metadata.
    pub fn without_ephemeral_variables(&self) -> Self {
        Self {
            parts: self.parts.clone(),
            variables: self.variables.without_ephemeral(),
        }
    }

    pub fn text(text: impl Into<String>) -> Self {
        Self {
            parts: vec![InputPart::Text { text: text.into() }],
            variables: VariablesChange::default(),
        }
    }
}

impl From<String> for SessionInput {
    fn from(text: String) -> Self {
        Self::text(text)
    }
}

impl From<&str> for SessionInput {
    fn from(text: &str) -> Self {
        Self::text(text)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputPart {
    Text { text: String },
    Image { source: ImageSource },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ImageSource {
    /// Resolved by the Session-scoped attachment owner before Context admission.
    Attachment {
        attachment_id: String,
    },
    Url {
        url: String,
    },
    Data {
        media_type: String,
        data: Vec<u8>,
    },
    Path {
        path: PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn secret_keys_and_placeholders_have_one_bounded_wire_grammar() {
        for key in ["token", "_", "Token.key-1", &"k".repeat(128)] {
            assert!(valid_secret_key(key));
            assert_eq!(secret_placeholder(key), format!("{{{{secret:{key}}}}}"));
        }
        for key in [
            "",
            "1token",
            "space key",
            "key:part",
            "key}x",
            "é",
            &"k".repeat(129),
        ] {
            assert!(!valid_secret_key(key));
        }
        assert_eq!(
            serde_json::to_value(VariableVisibility::Secret).unwrap(),
            json!("secret")
        );
        assert_eq!(
            serde_json::from_value::<VariableVisibility>(json!("secret")).unwrap(),
            VariableVisibility::Secret
        );
    }

    #[test]
    fn scope_receipt_projection_preserves_authority_input_and_legacy_empty_shape() {
        assert_eq!(
            serde_json::to_value(VariablesChange::default()).unwrap(),
            json!({"set":{},"remove":[]})
        );
        let mut input = SessionInput::text("message");
        input.variables.scoped_set = vec![
            ScopedVariableSet {
                name: "private".into(),
                value: json!("ephemeral-fixture"),
                visibility: VariableVisibility::Invisible,
                persistence: VariablePersistence::Ephemeral,
                editors: None,
            },
            ScopedVariableSet {
                name: "retained".into(),
                value: json!(7),
                visibility: VariableVisibility::Invisible,
                persistence: VariablePersistence::Persisted,
                editors: None,
            },
        ];
        let original = serde_json::to_vec(&input).unwrap();
        let receipt = input.without_ephemeral_variables();
        assert!(
            !serde_json::to_string(&receipt)
                .unwrap()
                .contains("ephemeral-fixture")
        );
        assert_eq!(receipt.variables.scoped_set.len(), 1);
        assert_eq!(serde_json::to_vec(&input).unwrap(), original);
        assert_ne!(serde_json::to_vec(&receipt).unwrap(), original);
    }

    #[test]
    fn later_hook_scope_or_remove_replaces_the_one_logical_name() {
        let mut client = VariablesChange {
            set: Map::from_iter([("same".into(), json!(1))]),
            ..Default::default()
        };
        let hook = VariablesChange {
            scoped_set: vec![ScopedVariableSet {
                name: "same".into(),
                value: json!(2),
                visibility: VariableVisibility::Invisible,
                persistence: VariablePersistence::Ephemeral,
                editors: None,
            }],
            ..Default::default()
        };
        client.overlay(&hook);
        assert!(client.set.is_empty());
        assert_eq!(client.scoped_set, hook.scoped_set);
        client.overlay(&VariablesChange {
            remove: vec!["same".into()],
            ..Default::default()
        });
        assert!(client.scoped_set.is_empty());
        assert_eq!(client.remove, vec!["same"]);
    }
}
