use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

/// Errors intentionally contain no endpoint, credentials, response body or parser detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MetadataError {
    #[error("invalid metadata configuration")]
    InvalidConfiguration,
    #[error("metadata request failed")]
    RequestFailed,
    #[error("metadata request timed out")]
    TimedOut,
    #[error("metadata response status rejected")]
    HttpStatus,
    #[error("metadata response exceeds size limit")]
    BodyTooLarge,
    #[error("invalid metadata response")]
    InvalidResponse,
    #[error("invalid metadata snapshot")]
    InvalidSnapshot,
}

/// Price strings are never converted to floating point. New price keys and
/// conditional overrides remain available without assuming their semantics.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelPricing {
    pub prompt: Option<String>,
    pub completion: Option<String>,
    pub request: Option<String>,
    pub image: Option<String>,
    pub web_search: Option<String>,
    pub internal_reasoning: Option<String>,
    pub input_cache_read: Option<String>,
    pub input_cache_write: Option<String>,
    pub audio: Option<String>,
    pub audio_output: Option<String>,
    pub image_output: Option<String>,
    pub image_token: Option<String>,
    pub input_audio_cache: Option<String>,
    pub input_cache_write_1h: Option<String>,
    pub discount: Option<serde_json::Number>,
    pub overrides: Option<Vec<Value>>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelArchitecture {
    pub modality: Option<String>,
    pub input_modalities: Option<Vec<String>>,
    pub output_modalities: Option<Vec<String>>,
    pub tokenizer: Option<String>,
    pub instruct_type: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// These are catalog ceilings, not a guarantee of output capacity for a request.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelTopProvider {
    pub context_length: Option<u64>,
    pub max_completion_tokens: Option<u64>,
    pub is_moderated: Option<bool>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Default sampling settings remain optional and preserve their JSON numbers.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelDefaultParameters {
    pub temperature: Option<serde_json::Number>,
    pub top_p: Option<serde_json::Number>,
    pub top_k: Option<serde_json::Number>,
    pub frequency_penalty: Option<serde_json::Number>,
    pub presence_penalty: Option<serde_json::Number>,
    pub repetition_penalty: Option<serde_json::Number>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// The OpenAPI schema permits JSON numbers for per-request token limits.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelPerRequestLimits {
    pub prompt_tokens: Option<serde_json::Number>,
    pub completion_tokens: Option<serde_json::Number>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// One catalog row. `provider` and `model_id` are derived from the first slash
/// in `id`, never inferred from display names or normalized to another spelling.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "WireModelMetadata")]
pub struct ModelMetadata {
    pub id: String,
    #[serde(skip)]
    pub provider: String,
    #[serde(skip)]
    pub model_id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created: Option<u64>,
    pub canonical_slug: Option<String>,
    pub context_length: Option<u64>,
    pub pricing: Option<ModelPricing>,
    pub top_provider: Option<ModelTopProvider>,
    pub architecture: Option<ModelArchitecture>,
    pub supported_parameters: Option<Vec<String>>,
    pub per_request_limits: Option<ModelPerRequestLimits>,
    pub limits: Option<BTreeMap<String, Value>>,
    pub default_parameters: Option<ModelDefaultParameters>,
    pub expiration_date: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
struct WireModelMetadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    created: Option<u64>,
    canonical_slug: Option<String>,
    context_length: Option<u64>,
    pricing: Option<ModelPricing>,
    top_provider: Option<ModelTopProvider>,
    architecture: Option<ModelArchitecture>,
    supported_parameters: Option<Vec<String>>,
    per_request_limits: Option<ModelPerRequestLimits>,
    limits: Option<BTreeMap<String, Value>>,
    default_parameters: Option<ModelDefaultParameters>,
    expiration_date: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

fn split_id(id: &str) -> Option<(&str, &str)> {
    let (provider, model_id) = id.split_once('/')?;
    if provider.is_empty()
        || model_id.is_empty()
        || id.chars().any(|c| c.is_whitespace() || c.is_control())
        || model_id.starts_with('/')
        || model_id.ends_with('/')
        || model_id.contains("//")
    {
        return None;
    }
    Some((provider, model_id))
}

impl TryFrom<WireModelMetadata> for ModelMetadata {
    type Error = MetadataError;

    fn try_from(wire: WireModelMetadata) -> Result<Self, Self::Error> {
        let (provider, model_id) = split_id(&wire.id).ok_or(MetadataError::InvalidResponse)?;
        let provider = provider.to_owned();
        let model_id = model_id.to_owned();
        Ok(Self {
            id: wire.id,
            provider,
            model_id,
            name: wire.name,
            description: wire.description,
            created: wire.created,
            canonical_slug: wire.canonical_slug,
            context_length: wire.context_length,
            pricing: wire.pricing,
            top_provider: wire.top_provider,
            architecture: wire.architecture,
            supported_parameters: wire.supported_parameters,
            per_request_limits: wire.per_request_limits,
            limits: wire.limits,
            default_parameters: wire.default_parameters,
            expiration_date: wire.expiration_date,
            extra: wire.extra,
        })
    }
}

/// Owned, serializable cache boundary. `version` identifies this format (1),
/// not a refresh counter. A fresh manager starts with an empty timestamp-0 view;
/// only nonempty validated snapshots can be restored or published by refresh.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    pub version: u64,
    pub source: String,
    pub fetched_at_ms: u64,
    pub models: Vec<ModelMetadata>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl MetadataSnapshot {
    /// Validate cached bytes before using them. The source must be a permitted
    /// unauthenticated HTTPS/explicit loopback URL, and every ID must be unique.
    pub fn validate(&self) -> Result<(), MetadataError> {
        if self.version != 1 || self.fetched_at_ms == 0 || self.models.is_empty() {
            return Err(MetadataError::InvalidSnapshot);
        }
        super::manager::validate_endpoint(&self.source)
            .map_err(|_| MetadataError::InvalidSnapshot)?;
        let mut ids = HashSet::with_capacity(self.models.len());
        for model in &self.models {
            let (provider, model_id) = split_id(&model.id).ok_or(MetadataError::InvalidSnapshot)?;
            if model.provider != provider || model.model_id != model_id || !ids.insert(&model.id) {
                return Err(MetadataError::InvalidSnapshot);
            }
        }
        Ok(())
    }

    /// Compare lowercase keys throughout metadata lookup, without modifying any
    /// stored catalog spelling or inference identity. Provider/model matches
    /// precede unique catalog-ID fallback, then the longest complete model ID
    /// followed by a '-'/'_' variant boundary. Case collisions at any winning
    /// tier remain unknown rather than selecting a row by catalog order.
    pub fn find(&self, provider: &str, model: &str) -> Option<ModelMatch> {
        if provider.is_empty() || model.is_empty() {
            return None;
        }
        let provider = provider.to_lowercase();
        let model = model.to_lowercase();
        let rows = self
            .models
            .iter()
            .map(|row| {
                (
                    row,
                    row.provider.to_lowercase(),
                    row.model_id.to_lowercase(),
                    row.id.to_lowercase(),
                )
            })
            .collect::<Vec<_>>();
        let mut exact = rows
            .iter()
            .filter(|(_, vendor, short, _)| vendor == &provider && short == &model);
        if let Some((row, ..)) = exact.next() {
            if exact.next().is_some() {
                return None;
            }
            return Some(ModelMatch {
                metadata: (*row).clone(),
                kind: ModelMatchKind::Exact,
            });
        }
        let mut matches = rows
            .iter()
            .filter(|(_, _, short, id)| short == &model || id == &model);
        if let Some((row, ..)) = matches.next() {
            if matches.next().is_some() {
                return None;
            }
            return Some(ModelMatch {
                metadata: (*row).clone(),
                kind: ModelMatchKind::ModelIdFallback,
            });
        }
        // A known qualified vendor constrains approximation to that vendor. An
        // unknown prefix may still be part of a nested model_id; keep it intact.
        let qualified = model
            .split_once('/')
            .filter(|(vendor, _)| rows.iter().any(|(_, candidate, _, _)| candidate == vendor));
        let short = qualified.map_or(model.as_str(), |(_, short)| short);
        let mut best: Option<(&ModelMetadata, usize)> = None;
        let mut ambiguous = false;
        for (row, vendor, candidate, _) in &rows {
            if qualified.is_some_and(|(wanted, _)| vendor != wanted) {
                continue;
            }
            let Some(suffix) = short.strip_prefix(candidate.as_str()) else {
                continue;
            };
            if suffix.len() <= 1 || !(suffix.starts_with('-') || suffix.starts_with('_')) {
                continue;
            }
            match best {
                Some((_, length)) if length > candidate.len() => {}
                Some((_, length)) if length == candidate.len() => ambiguous = true,
                _ => {
                    best = Some((row, candidate.len()));
                    ambiguous = false;
                }
            }
        }
        if ambiguous {
            return None;
        }
        best.map(|(row, _)| ModelMatch {
            metadata: row.clone(),
            kind: ModelMatchKind::ModelIdApproximate,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelMatchKind {
    Exact,
    ModelIdFallback,
    ModelIdApproximate,
}

/// Retains every metadata field so consumers need not refetch the catalog row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelMatch {
    pub metadata: ModelMetadata,
    pub kind: ModelMatchKind,
}
