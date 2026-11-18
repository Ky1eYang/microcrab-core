//! The only producer of original-to-resolved invocation witnesses. Plaintext
//! arguments exist only for immediate dispatch; proofs retain a dedicated immutable
//! Secret snapshot and the original approval, never another resolved argument DTO.
use super::{SecretDelivery, ToolContext};
use crate::Error;
use crate::policy::{AdmittedInvocation, PermissionError};
use crate::state::variables::SecretSnapshot;
use microcrab_protocol::ToolCall;
use serde_json::Value;
use std::sync::Arc;

const MAX_RESOLVED_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_ARGUMENT_DEPTH: usize = 128;

/// Fields and construction remain private to this canonical resolver. This is
/// deliberately neither serializable nor Debug and exposes no proof mutator.
pub(crate) struct ResolvedArguments {
    proof: Arc<AdmittedInvocation>,
    secrets: SecretSnapshot,
    mask: super::injection::LiteralMask,
    max_bytes: usize,
}
impl ResolvedArguments {
    pub(crate) fn belongs_to(&self, proof: &AdmittedInvocation) -> bool {
        std::ptr::eq(self.proof.as_ref(), proof)
    }
    pub(super) fn validate(
        &self,
        context: &ToolContext,
        arguments: &Value,
    ) -> Result<(), PermissionError> {
        let proof = context.admission.as_ref().ok_or(PermissionError::Denied)?;
        if !Arc::ptr_eq(&self.proof, proof) {
            return Err(PermissionError::ProofMismatch("resolver approval differs"));
        }
        // No plaintext argument copy is stored in the generic admission proof.
        // Re-materialization is bounded and uses this exact immutable snapshot.
        let expected = resolve_value(
            self.proof.original_arguments(),
            &self.secrets,
            &self.mask,
            self.max_bytes,
        )
        .map_err(|_| PermissionError::ProofMismatch("resolved arguments unavailable"))?;
        if &expected != arguments {
            return Err(PermissionError::ProofMismatch("resolved arguments differ"));
        }
        Ok(())
    }
}

/// All public records are already based on `call` at this point. Permission has
/// completed on those original arguments and must not be rerun on the plaintext.
pub(super) fn resolve(
    call: &ToolCall,
    context: &mut ToolContext,
    delivery: SecretDelivery,
    validate: impl FnOnce(&ToolCall) -> Result<(), Error>,
) -> Result<ToolCall, Error> {
    context
        .ensure_admitted_arguments(&call.arguments)
        .map_err(PermissionError::into_runtime_error)?;
    let secrets = context.state.secret_snapshot();
    // Constructors pin both this handle and its process wrapper before admission.
    // Extend the shared witness without replacing either invocation identity.
    context
        .redaction
        .merge(&super::super::execution::InvocationRedaction::from_snapshot(secrets.clone())?)?;
    if delivery == SecretDelivery::ForwardPlaceholders || !has_placeholder(&call.arguments) {
        validate(call)?;
        return Ok(call.clone());
    }
    let max_bytes = context
        .state
        .snapshot()
        .limits
        .max_context_bytes
        .min(MAX_RESOLVED_ARGUMENT_BYTES);
    let arguments = resolve_value(&call.arguments, &secrets, &context.literal_mask, max_bytes)?;
    let resolved = ToolCall {
        arguments,
        ..call.clone()
    };
    // Fully validate the actual input with the unchanged registered schema. Its
    // error must not interpolate plaintext instance values or downstream errors.
    validate(&resolved).map_err(|_| Error::tool(
        "secret substitution does not satisfy the registered tool schema; correct the placeholder or request a suitable secret",
    ))?;
    let proof = context
        .admission
        .as_ref()
        .ok_or_else(|| Error::denied("secret resolution requires an admitted invocation"))?
        .clone();
    let witness = Arc::new(ResolvedArguments {
        proof: proof.clone(),
        secrets,
        mask: context.literal_mask.clone(),
        max_bytes,
    });
    proof
        .certify_resolution(context, &witness)
        .map_err(PermissionError::into_runtime_error)?;
    context.resolved_arguments = Some(witness);
    Ok(resolved)
}

pub(super) fn has_placeholder(value: &Value) -> bool {
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            Value::String(text) if contains_placeholder(text) => return true,
            Value::Array(items) => pending.extend(items),
            Value::Object(fields) => pending.extend(fields.values()),
            _ => {}
        }
    }
    false
}
pub(super) fn contains_placeholder(text: &str) -> bool {
    placeholders(text).next().is_some()
}
fn placeholders(text: &str) -> impl Iterator<Item = (usize, usize, &str)> {
    text.match_indices("{{secret:").filter_map(|(start, _)| {
        let key_start = start + "{{secret:".len();
        let suffix = &text[key_start..];
        // A canonical key is at most 128 ASCII bytes. Never scan an unbounded
        // malformed suffix again for every candidate prefix.
        let end = suffix
            .as_bytes()
            .windows(2)
            .take(129)
            .position(|pair| pair == b"}}")?;
        let key = &suffix[..end];
        microcrab_protocol::valid_secret_key(key).then_some((start, key_start + end + 2, key))
    })
}
fn resolve_value(
    value: &Value,
    secrets: &SecretSnapshot,
    mask: &super::injection::LiteralMask,
    max_bytes: usize,
) -> Result<Value, Error> {
    if serde_json::to_vec(value)?.len() > max_bytes {
        return Err(expansion_error());
    }
    let mut remaining = max_bytes;
    let resolved = visit(value, secrets, mask, &mut remaining, max_bytes, "", 0)?;
    if serde_json::to_vec(&resolved)?.len() > max_bytes {
        return Err(expansion_error());
    }
    Ok(resolved)
}
fn visit(
    value: &Value,
    secrets: &SecretSnapshot,
    mask: &super::injection::LiteralMask,
    remaining: &mut usize,
    max_bytes: usize,
    pointer: &str,
    depth: usize,
) -> Result<Value, Error> {
    if depth > MAX_ARGUMENT_DEPTH {
        return Err(expansion_error());
    }
    if mask.covers(pointer) {
        return Ok(value.clone());
    }
    Ok(match value {
        Value::String(text) => {
            let mut result = String::new();
            let mut consumed = 0;
            for (start, end, key) in placeholders(text) {
                if start < consumed {
                    continue;
                }
                if mask.intersects(pointer, start, end) {
                    append(&mut result, &text[consumed..end], max_bytes)?;
                    consumed = end;
                    continue;
                }
                append(&mut result, &text[consumed..start], max_bytes)?;
                let secret = secrets.lookup(key).ok_or_else(|| Error::tool(
                    "unknown or expired secret placeholder; submit the secret again and retry with the returned placeholder",
                ))?;
                // Insert once. A placeholder inside a secret is literal data.
                append(&mut result, secret, max_bytes)?;
                consumed = end;
            }
            append(&mut result, &text[consumed..], max_bytes)?;
            *remaining = remaining
                .checked_sub(result.len())
                .ok_or_else(expansion_error)?;
            Value::String(result)
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    visit(
                        item,
                        secrets,
                        mask,
                        remaining,
                        max_bytes,
                        &format!("{pointer}/{index}"),
                        depth + 1,
                    )
                })
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, item)| {
                    Ok((
                        key.clone(),
                        visit(
                            item,
                            secrets,
                            mask,
                            remaining,
                            max_bytes,
                            &format!("{pointer}/{}", super::injection::escape_pointer(key)),
                            depth + 1,
                        )?,
                    ))
                })
                .collect::<Result<_, Error>>()?,
        ),
        _ => value.clone(),
    })
}
fn append(result: &mut String, text: &str, max_bytes: usize) -> Result<(), Error> {
    if result
        .len()
        .checked_add(text.len())
        .is_none_or(|bytes| bytes > max_bytes)
    {
        return Err(expansion_error());
    }
    result.push_str(text);
    Ok(())
}
fn expansion_error() -> Error {
    Error::tool("resolved secret arguments exceed the invocation argument byte/depth budget")
}

#[cfg(test)]
mod tests;
