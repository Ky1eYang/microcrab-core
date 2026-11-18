//! Estimated model pressure is separate from the exact Context byte budget.
use super::Content;
use crate::Error;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// An embedding resolves the actual selected route and its advertised capacity.
/// None means measurement/capacity is unavailable, never an empty Context.
/// The route must identify the Client's selected provider/model/tokenizer and
/// remain stable during each request. Runtime checks it before and after await;
/// embeddings serialize route changes with their foreground operation gate.
pub trait TokenEstimator: Send + Sync + 'static {
    fn estimate(&self, content: &Content) -> Result<Option<TokenEstimate>, Error>;
}

#[derive(Clone, Debug, Serialize)]
pub struct TokenEstimate {
    pub route: String,
    pub provenance: String,
    pub input_tokens: u64,
    pub context_window_tokens: u64,
}
impl TokenEstimate {
    /// Conservative text-only fallback: one estimated token per serialized UTF-8
    /// byte, including JSON framing. This deliberately overestimates normal text;
    /// it is neither a tokenizer nor an upper-bound proof for every provider.
    /// Image token cost is provider-specific, so image-bearing input is unavailable.
    pub fn conservative_text(
        content: &Content,
        route: String,
        window: u64,
    ) -> Result<Option<Self>, Error> {
        if window == 0 || content.items.iter().any(|item| matches!(item,
            super::ContentItem::User { parts, .. } | super::ContentItem::Tool { parts, .. } if parts.iter().any(|part| !matches!(part, microcrab_protocol::InputPart::Text { .. }))
        )) {
            return Ok(None);
        }
        let bytes =
            super::measurement::budget_bytes(&content.system, &content.items, &content.tools)?;
        let input_tokens =
            u64::try_from(bytes.len()).map_err(|_| Error::config("token estimate overflow"))?;
        Ok(Some(Self {
            route,
            provenance: "estimated_context_json_utf8_bytes_1_token_per_byte_v1".into(),
            input_tokens,
            context_window_tokens: window,
        }))
    }
    pub fn pressure(&self, output: u32, headroom: u32) -> Option<f64> {
        if self.context_window_tokens == 0 || self.route.is_empty() || self.provenance.is_empty() {
            return None;
        }
        let reserved = u64::from(output).checked_add(u64::from(headroom))?;
        let usable = self.context_window_tokens.saturating_sub(reserved);
        Some(if usable == 0 {
            f64::INFINITY
        } else {
            self.input_tokens as f64 / usable as f64
        })
    }
}

/// One accepted main request's input measurement, bounded independently of bodies.
/// Later assistant, tool and user items are outside this measured request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextUsageEvidence {
    /// Zero identifies the historical insertion-order hash. New evidence always
    /// uses a detached canonical JSON projection and never rewrites frozen input.
    #[serde(default, skip_serializing_if = "is_legacy_fingerprint")]
    fingerprint_version: u8,
    route: String,
    epoch: u64,
    baseline_generation: u64,
    prefix_items: usize,
    fingerprint: String,
    input_tokens: u64,
}

fn is_legacy_fingerprint(version: &u8) -> bool {
    *version == 0
}

impl super::Context {
    fn usage_fingerprint(&self, prefix_items: usize, version: u8) -> Result<Option<String>, Error> {
        use sha2::{Digest, Sha256};
        let Some(prefix) = self.items.get(..prefix_items) else {
            return Ok(None);
        };
        let material = (
            self.epoch,
            self.baseline_generation,
            &self.system,
            &self.tools,
            prefix,
        );
        let bytes = match version {
            0 => serde_json::to_vec(&material)?,
            1 => {
                // Canonicalize only a detached projection. Storage may reorder
                // nested objects, including those inside arrays, without changing
                // the measured material or the live provider cache prefix.
                let mut value = serde_json::to_value(material)?;
                value.sort_all_objects();
                serde_json::to_vec(&value)?
            }
            _ => return Ok(None),
        };
        Ok(Some(format!("{:x}", Sha256::digest(bytes))))
    }

    pub(crate) fn capture_usage_request(
        &self,
        route: Option<String>,
    ) -> Result<Option<ContextUsageEvidence>, Error> {
        let Some(route) =
            route.filter(|route| !route.is_empty() && route.len() <= 4096 && !route.contains('\0'))
        else {
            return Ok(None);
        };
        Ok(Some(ContextUsageEvidence {
            fingerprint_version: 1,
            route,
            epoch: self.epoch,
            baseline_generation: self.baseline_generation,
            prefix_items: self.items.len(),
            fingerprint: self
                .usage_fingerprint(self.items.len(), 1)?
                .ok_or_else(|| Error::protocol("usage request prefix disappeared"))?,
            input_tokens: 0,
        }))
    }

    pub(crate) fn accept_usage_request(
        &mut self,
        evidence: Option<ContextUsageEvidence>,
        route: Option<String>,
        usage: &microcrab_protocol::Usage,
    ) -> Result<(), Error> {
        self.usage_evidence = None;
        let Some(mut evidence) = evidence else {
            return Ok(());
        };
        if usage.samples != 1 || route.as_deref() != Some(evidence.route.as_str()) {
            return Ok(());
        }
        let Some(input_tokens) = usage.input_tokens else {
            return Ok(());
        };
        if self.matches_usage_evidence(&evidence)? {
            evidence.input_tokens = input_tokens;
            self.usage_evidence = Some(evidence);
        }
        Ok(())
    }

    fn matches_usage_evidence(&self, evidence: &ContextUsageEvidence) -> Result<bool, Error> {
        Ok(!self.pending_epoch
            && matches!(
                self.items.get(evidence.prefix_items),
                Some(super::ContentItem::Assistant { .. })
            )
            && evidence.epoch == self.epoch
            && evidence.baseline_generation == self.baseline_generation
            && self
                .usage_fingerprint(evidence.prefix_items, evidence.fingerprint_version)?
                .as_deref()
                == Some(evidence.fingerprint.as_str()))
    }

    pub(crate) fn usage_request_fingerprint(&self) -> Option<&str> {
        self.usage_evidence
            .as_ref()
            .map(|evidence| evidence.fingerprint.as_str())
    }

    fn validate_usage_evidence_identity(&self) -> Result<(), Error> {
        let Some(evidence) = &self.usage_evidence else {
            return Ok(());
        };
        if evidence.fingerprint_version > 1 {
            return Err(Error::protocol(
                "unsupported Context usage fingerprint version",
            ));
        }
        if evidence.route.is_empty() || evidence.route.len() > 4096 || evidence.route.contains('\0')
        {
            return Err(Error::protocol("invalid Context usage evidence route"));
        }
        if evidence.fingerprint.len() != 64
            || !evidence
                .fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Error::protocol(
                "invalid Context usage evidence fingerprint syntax",
            ));
        }
        if self.pending_epoch {
            return Err(Error::protocol_with_private_reason(
                "invalid Context usage evidence",
                "Context usage evidence invalid: pending epoch".into(),
            ));
        }
        if !matches!(
            self.items.get(evidence.prefix_items),
            Some(super::ContentItem::Assistant { .. })
        ) {
            return Err(Error::protocol_with_private_reason(
                "invalid Context usage evidence",
                format!(
                    "Context usage evidence target item at prefix_items {} is not Assistant (total items: {})",
                    evidence.prefix_items,
                    self.items.len()
                ),
            ));
        }
        if evidence.epoch != self.epoch {
            return Err(Error::protocol_with_private_reason(
                "invalid Context usage evidence",
                format!(
                    "Context usage evidence epoch mismatch: evidence {} != context {}",
                    evidence.epoch, self.epoch
                ),
            ));
        }
        if evidence.baseline_generation != self.baseline_generation {
            return Err(Error::protocol_with_private_reason(
                "invalid Context usage evidence",
                format!(
                    "Context usage evidence baseline_generation mismatch: evidence {} != context {}",
                    evidence.baseline_generation, self.baseline_generation
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn validate_usage_evidence(&self) -> Result<(), Error> {
        self.validate_usage_evidence_identity()?;
        let Some(evidence) = &self.usage_evidence else {
            return Ok(());
        };
        let computed =
            self.usage_fingerprint(evidence.prefix_items, evidence.fingerprint_version)?;
        if computed.as_deref() != Some(evidence.fingerprint.as_str()) {
            return Err(Error::protocol(
                "Context usage evidence fingerprint mismatch",
            ));
        }
        Ok(())
    }

    /// Historical storage sorted objects after the legacy hash was captured.
    /// That lost insertion order cannot be reconstructed or trusted as coverage.
    /// Only this restore boundary can retire that optional calibration; malformed
    /// identities and all new-version mismatches still fail validation.
    pub(super) fn restore_usage_evidence(&mut self) -> Result<(), Error> {
        self.validate_usage_evidence_identity()?;
        if let Some(evidence) = &self.usage_evidence
            && evidence.fingerprint_version == 0
            && !self.matches_usage_evidence(evidence)?
        {
            tracing::warn!(
                reason = "legacy_usage_fingerprint_mismatch",
                "restored Context without unverifiable usage calibration"
            );
            self.usage_evidence = None;
        }
        self.validate_usage_evidence()
    }

    /// Return the latest accepted main request's exact input usage and current
    /// route capacity. This is a post-response measurement, not an estimate of
    /// unmeasured assistant/tool/user suffixes; absent evidence stays unavailable.
    pub fn estimate_tokens(
        &self,
        estimator: &dyn TokenEstimator,
        max_output_tokens: u32,
    ) -> Result<Option<TokenEstimate>, Error> {
        let Some(evidence) = self.usage_evidence.as_ref() else {
            return Ok(None);
        };
        if !self.matches_usage_evidence(evidence)? {
            return Ok(None);
        }
        // Resolve route/capacity without asking the text heuristic to price a
        // measured image prefix or any still-unmeasured suffix.
        let mut probe = self.content(max_output_tokens)?;
        probe.items.clear();
        let Some(mut resolved) = estimator.estimate(&probe)? else {
            return Ok(None);
        };
        if resolved.route != evidence.route {
            return Ok(None);
        }
        resolved.input_tokens = evidence.input_tokens;
        resolved.provenance = "verified_accepted_main_request_input_tokens_v1".into();
        Ok(Some(resolved))
    }
}

/// Explicit core/embedding policy. Server supplies enabled defaults; core and
/// Python opt in through their builder/configuration, never through frontend state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AutoCompactionPolicy {
    pub enabled: bool,
    pub turn_threshold: f64,
    pub round_threshold: f64,
    #[serde(default)]
    pub headroom_tokens: u32,
}
impl Default for AutoCompactionPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            turn_threshold: 0.8,
            round_threshold: 0.95,
            headroom_tokens: 0,
        }
    }
}
impl AutoCompactionPolicy {
    pub fn server_default() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        if !(0.0 < self.turn_threshold
            && self.turn_threshold < self.round_threshold
            && self.round_threshold < 1.0)
        {
            return Err(Error::config(
                "auto compaction requires 0 < Turn < Round < 1",
            ));
        }
        Ok(())
    }
}
