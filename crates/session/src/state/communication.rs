//! Durable conversational identity and disclosed correspondents, never credentials.
pub mod inbox;
pub mod reply;
pub mod turn;
use crate::Error;
use serde::{Deserialize, Serialize};

/// Exact communication address, independent of storage/session identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct AgentId(String);
impl<'de> Deserialize<'de> for AgentId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
impl AgentId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub(crate) fn fresh() -> Self {
        Self(format!(
            "agt_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..16]
        ))
    }
}
impl TryFrom<String> for AgentId {
    type Error = Error;
    fn try_from(value: String) -> Result<Self, Error> {
        if value.len() != 20
            || !value.starts_with("agt_")
            || !value.as_bytes()[4..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(Error::config(
                "agent id must be agt_ followed by 16 lowercase hexadecimal digits",
            ));
        }
        Ok(Self(value))
    }
}
impl std::fmt::Display for AgentId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionInteraction {
    #[default]
    Persistent,
    Return,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationIdentity {
    pub agent_id: AgentId,
    pub depth: u8,
    pub interaction: SessionInteraction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<AgentId>,
    /// Runtime lineage ownership token; never projected to the model.
    pub(crate) lineage: String,
}
impl ConversationIdentity {
    pub(crate) fn fresh(depth: u8, interaction: SessionInteraction) -> Self {
        Self {
            agent_id: AgentId::fresh(),
            depth,
            interaction,
            parent_session_id: None,
            parent_agent_id: None,
            lineage: uuid::Uuid::new_v4().to_string(),
        }
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.depth > 1
            || self.lineage.is_empty()
            || self.lineage.len() > 128
            || self.parent_session_id.is_some() != self.parent_agent_id.is_some()
            || (self.depth == 0
                && (self.parent_agent_id.is_some()
                    || self.interaction != SessionInteraction::Persistent))
            || self
                .parent_session_id
                .as_ref()
                .is_some_and(|id| id.is_empty() || id.contains('/'))
            || self.parent_agent_id.as_ref() == Some(&self.agent_id)
        {
            return Err(Error::config("invalid conversational lineage identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorrespondentRelation {
    Parent,
    Child,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Correspondent {
    pub agent_id: AgentId,
    pub agent: String,
    pub relation: CorrespondentRelation,
    pub interaction: SessionInteraction,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalSpeaker {
    pub identity: String,
}
impl ExternalSpeaker {
    pub fn validate(&self) -> Result<(), Error> {
        if self.identity.is_empty()
            || self.identity.len() > 64
            || !self.identity.bytes().enumerate().all(|(index, byte)| {
                byte == b'_'
                    || byte.is_ascii_alphabetic()
                    || (index > 0 && (byte.is_ascii_digit() || byte == b'-'))
            })
        {
            return Err(Error::config(
                "external identity must be a 1..64 byte tag-safe token",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommunicationState {
    pub identity: ConversationIdentity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub correspondents: Vec<Correspondent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_speakers: Vec<ExternalSpeaker>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub focus: Vec<super::reference::focus::FocusSnapshot>,
    #[serde(default)]
    pub inbox: inbox::DurableInbox,
}
impl CommunicationState {
    pub(crate) fn fresh(depth: u8, interaction: SessionInteraction) -> Self {
        Self {
            identity: ConversationIdentity::fresh(depth, interaction),
            correspondents: vec![],
            external_speakers: vec![],
            focus: vec![],
            inbox: inbox::DurableInbox::default(),
        }
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.identity.validate()?;
        self.inbox.validate()?;
        super::reference::focus::validate_snapshots(&self.focus)?;
        let mut agents = std::collections::BTreeSet::new();
        if self.correspondents.len() > 128 || self.external_speakers.len() > 128 {
            return Err(Error::config("communication disclosure capacity exceeded"));
        }
        for correspondent in &self.correspondents {
            if correspondent.agent_id == self.identity.agent_id
                || !agents.insert(&correspondent.agent_id)
                || correspondent.agent.len() > 256
            {
                return Err(Error::config("invalid or duplicate correspondent"));
            }
        }
        let mut speakers = std::collections::BTreeSet::new();
        for speaker in &self.external_speakers {
            speaker.validate()?;
            if !speakers.insert(&speaker.identity) {
                return Err(Error::config("duplicate external identity"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
