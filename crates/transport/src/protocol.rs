//! Supported wire protocols for model providers.
use crate::error::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// OpenAI Chat Completions protocol (`/v1/chat/completions`).
    OpenaiCompatible,
    /// OpenAI Responses API protocol (`/v1/responses`).
    OpenaiResponses,
    /// Anthropic Messages API protocol (`/v1/messages`).
    Anthropic,
    /// Google Gemini REST protocol (`:generateContent` / `:streamGenerateContent?alt=sse`).
    Google,
}

impl Protocol {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::OpenaiCompatible => "openai-compatible",
            Self::OpenaiResponses => "openai",
            Self::Anthropic => "anthropic",
            Self::Google => "google",
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Protocol {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "openai-compatible" => Ok(Self::OpenaiCompatible),
            "openai" | "openai-response" | "openai-responses" => Ok(Self::OpenaiResponses),
            "anthropic" | "claude" => Ok(Self::Anthropic),
            "google" | "gemini" => Ok(Self::Google),
            other => Err(Error::config_with_private(
                "unknown provider protocol; choose openai-compatible, openai, anthropic or google",
                format!("unknown provider protocol: {other}"),
            )),
        }
    }
}

impl Serialize for Protocol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Protocol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::from_str(&raw).map_err(serde::de::Error::custom)
    }
}
