//! Scope identity is part of every receipt. Native v2 retains its original bytes;
//! browser Conversations carry an explicit discriminator and version instead of
//! inventing a Project. Strict alternatives reject mixed authority documents.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum BindingScope {
    Conversation(ConversationScope),
    Project(ProjectScope),
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectScope {
    pub binding_id: String,
    pub backend_id: String,
    pub project_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConversationScope {
    pub scope_version: ConversationScopeVersion,
    pub scope_kind: ConversationScopeKind,
    pub binding_id: String,
    pub backend_id: String,
    pub conversation_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "u32", into = "u32")]
pub struct ConversationScopeVersion;
impl TryFrom<u32> for ConversationScopeVersion {
    type Error = &'static str;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value == 3 {
            Ok(Self)
        } else {
            Err("unsupported Conversation scope version")
        }
    }
}
impl From<ConversationScopeVersion> for u32 {
    fn from(_: ConversationScopeVersion) -> Self {
        3
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConversationScopeKind {
    Conversation,
}
impl BindingScope {
    pub fn project(backend_id: String, binding_id: String, project_id: String) -> Self {
        Self::Project(ProjectScope {
            backend_id,
            binding_id,
            project_id,
        })
    }
    pub fn conversation(backend_id: String, binding_id: String, conversation_id: String) -> Self {
        Self::Conversation(ConversationScope {
            scope_version: ConversationScopeVersion,
            scope_kind: ConversationScopeKind::Conversation,
            backend_id,
            binding_id,
            conversation_id,
        })
    }
    pub fn project_id(&self) -> Option<&str> {
        match self {
            Self::Project(scope) => Some(&scope.project_id),
            Self::Conversation(_) => None,
        }
    }
    pub fn is_project(&self) -> bool {
        matches!(self, Self::Project(_))
    }
    pub fn valid(&self) -> bool {
        let fields = match self {
            Self::Project(scope) => [&scope.backend_id, &scope.binding_id, &scope.project_id],
            Self::Conversation(scope) => {
                [&scope.backend_id, &scope.binding_id, &scope.conversation_id]
            }
        };
        fields
            .iter()
            .all(|value| !value.trim().is_empty() && value.len() <= 256 && !value.contains('\0'))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn native_bytes_and_explicit_conversation_authority_are_distinct() {
        let native = json!({"backend_id":"b","binding_id":"g","project_id":"p"});
        assert_eq!(
            serde_json::to_value(serde_json::from_value::<BindingScope>(native.clone()).unwrap())
                .unwrap(),
            native
        );
        let scope = BindingScope::conversation("b".into(), "g".into(), "c".into());
        let wire = serde_json::to_value(&scope).unwrap();
        assert_eq!(wire["scope_version"], 3);
        assert_eq!(wire["scope_kind"], "conversation");
        assert!(wire.get("project_id").is_none());
        assert_ne!(
            scope,
            BindingScope::project("b".into(), "g".into(), "c".into())
        );
        for change in [
            json!({"scope_version":2}),
            json!({"project_id":"p"}),
            json!({"scope_kind":"project"}),
        ] {
            let mut bad = wire.clone();
            bad.as_object_mut()
                .unwrap()
                .extend(change.as_object().unwrap().clone());
            assert!(serde_json::from_value::<BindingScope>(bad).is_err());
        }
        assert!(!BindingScope::project("b".into(), "g".into(), "".into()).valid());
    }
}
