//! Expand bodies from installed Session-owned reference catalogs.
use super::{Tool, ToolContext};
use crate::Error;
use async_trait::async_trait;
use microcrab_protocol::ToolDefinition;
use serde_json::{Value, json};

/// Expand one or more disjoint sections from installed reference catalogs.
#[derive(Debug, Default, Clone, Copy)]
pub struct Expand;

impl Expand {
    pub fn definition() -> ToolDefinition {
        ToolDefinition {
            name: "expand".into(),
            description: "Expand exact disjoint Markdown sections from this Session's installed reference catalog, within the tool output budget. Choose ids or reference; overlapping section bodies are not implicitly added.".into(),
            parameters: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "ids": { "type": "array", "minItems": 1, "maxItems": 128, "uniqueItems": true,
                        "description": "Batch selector: 1..=128 unique nonempty disclosed IDs in result order. Returns a JSON array of exact sections with IDs and Unicode character counts. Omission requires reference; null is invalid.",
                        "items": { "type": "string", "minLength": 1, "description": "Exact opaque section ID disclosed in State, not a filesystem path or URL; unknown IDs fail." } },
                    "reference": { "type": "string", "minLength": 1, "description": "Legacy single selector: one exact nonempty opaque ID; returns the section body as text. Omission requires ids; null is invalid." }
                }
            }),
        }
    }
}

/// The public schema stays portable; the builtin owns the selector relation.
/// Catalog admission calls this before authorization/execution, and direct builtin
/// execution repeats it before reading any installed reference.
pub(crate) fn validate_selectors(arguments: &Value) -> Result<(), Error> {
    if arguments.get("reference").is_some() == arguments.get("ids").is_some() {
        use crate::observation::diagnostics::{
            DiagnosticCode, DiagnosticLayer, DiagnosticPhase, EffectKnowledge,
            SafeDiagnosticReason, ToolDiagnostic,
        };
        let mut diagnostic = ToolDiagnostic::new(
            DiagnosticCode::InvalidArgument,
            DiagnosticLayer::Runtime,
            DiagnosticPhase::Validation,
        );
        diagnostic.reason = Some(SafeDiagnosticReason::new(
            "expand requires exactly one of ids or reference",
        ));
        diagnostic.effects = EffectKnowledge::NotStarted;
        return Err(Error::Diagnostic(diagnostic));
    }
    Ok(())
}

#[async_trait]
impl Tool for Expand {
    fn definition(&self) -> ToolDefinition {
        Self::definition()
    }

    async fn execute(
        &self,
        arguments: Value,
        context: ToolContext,
    ) -> Result<super::ToolOutput, Error> {
        if context.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        validate_selectors(&arguments)?;
        let single = arguments.get("reference");
        let batch = arguments.get("ids");
        let ids: Vec<&str> = if let Some(single) = single {
            vec![
                single
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| Error::tool("expand reference must be a nonempty ID"))?,
            ]
        } else {
            let rows = batch
                .and_then(Value::as_array)
                .filter(|rows| !rows.is_empty() && rows.len() <= 128)
                .ok_or_else(|| Error::tool("expand ids must contain 1..128 IDs"))?;
            rows.iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .ok_or_else(|| Error::tool("expand ids must be nonempty strings"))
                })
                .collect::<Result<_, _>>()?
        };
        let mut seen = std::collections::HashSet::new();
        let limit = context.state.snapshot().limits.max_tool_output_bytes;
        let mut output = Vec::new();
        let mut bytes = 0usize;
        for id in ids {
            if context.cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if !seen.insert(id) {
                return Err(Error::tool("expand IDs must be unique"));
            }
            let content = context.state.read_reference(id)?;
            bytes = bytes.checked_add(content.body.len()).ok_or_else(|| {
                crate::budget::reject(
                    crate::LimitKind::Arithmetic,
                    "expanded reference byte overflow",
                )
            })?;
            crate::budget::check(crate::LimitKind::ToolOutput, bytes, limit)?;
            output.push(json!({"id": content.id, "char_length":content.body.chars().count(), "content":content.body}));
        }
        if single.is_some() {
            return Ok(super::ToolOutput::text(
                output[0]["content"]
                    .as_str()
                    .expect("reference content is text")
                    .to_owned(),
            ));
        }
        let output = serde_json::to_string(&output)?;
        crate::budget::check(crate::LimitKind::ToolOutput, output.len(), limit)?;
        Ok(super::ToolOutput::text(output))
    }
}
