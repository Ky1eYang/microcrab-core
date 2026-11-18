//! Private execution-scoped secret witnesses. Never serialized or printed.
use crate::{Error, state::variables::SecretSnapshot};
use std::sync::{Arc, Mutex};

const MAX_VALUES: usize = 1024;
const MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Default)]
struct Witnesses {
    values: Vec<(String, String)>,
    matcher: Option<aho_corasick::AhoCorasick>,
    max_value_bytes: usize,
}
impl Witnesses {
    fn build(mut values: Vec<(String, String)>) -> Result<Self, Error> {
        values.sort();
        values.dedup_by(|right, left| right.0 == left.0);
        if values.len() > MAX_VALUES
            || values
                .iter()
                .map(|(value, key)| value.len() + key.len())
                .sum::<usize>()
                > MAX_BYTES
        {
            return Err(Error::tool("execution secret witness capacity exceeded"));
        }
        let max_value_bytes = values
            .iter()
            .map(|(value, key)| value.len().max(key.len()))
            .max()
            .unwrap_or(0);
        let matcher = if values.is_empty() {
            None
        } else {
            Some(
                aho_corasick::AhoCorasick::builder()
                    .match_kind(aho_corasick::MatchKind::LeftmostLongest)
                    .kind(Some(aho_corasick::AhoCorasickKind::NoncontiguousNFA))
                    .build(
                        values
                            .iter()
                            .map(|(value, _)| value)
                            .chain(values.iter().map(|(_, key)| key)),
                    )
                    .map_err(|_| {
                        Error::tool("execution secret matcher exceeds construction capacity")
                    })?,
            )
        };
        if matcher
            .as_ref()
            .is_some_and(|matcher| matcher.memory_usage() > 64 * 1024 * 1024)
        {
            return Err(Error::tool(
                "execution secret matcher exceeds retained memory capacity",
            ));
        }
        Ok(Self {
            values,
            matcher,
            max_value_bytes,
        })
    }
}
#[derive(Clone, Default)]
pub struct InvocationRedaction(Arc<Mutex<Witnesses>>);
impl std::fmt::Debug for InvocationRedaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvocationRedaction")
            .finish_non_exhaustive()
    }
}
impl InvocationRedaction {
    pub(crate) fn from_snapshot(snapshot: SecretSnapshot) -> Result<Self, Error> {
        let values = snapshot
            .entries()
            .filter(|(_, value)| !value.is_empty())
            .map(|(key, value)| (value.to_owned(), format!("{{{{secret:{key}}}}}")))
            .collect::<Vec<_>>();
        Ok(Self(Arc::new(Mutex::new(Witnesses::build(values)?))))
    }
    pub(crate) fn merge(&self, other: &Self) -> Result<(), Error> {
        if Arc::ptr_eq(&self.0, &other.0) {
            return Ok(());
        }
        let additions = other
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values
            .clone();
        let mut witnesses = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut values = witnesses.values.clone();
        for (value, key) in additions {
            if !values.iter().any(|(old, _)| old == &value) {
                values.push((value, key));
            }
        }
        *witnesses = Witnesses::build(values)?;
        Ok(())
    }
    pub(crate) fn redact_diagnostic(
        &self,
        mut diagnostic: crate::observation::diagnostics::ToolDiagnostic,
    ) -> crate::observation::diagnostics::ToolDiagnostic {
        diagnostic.reason = diagnostic.reason.as_ref().map(|reason| {
            crate::observation::diagnostics::SafeDiagnosticReason::new(
                &self.redact_text(reason.as_str()),
            )
        });
        diagnostic
    }
    pub(crate) fn redact_error(&self, error: Error) -> Error {
        if self
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values
            .is_empty()
        {
            return error;
        }
        match error {
            Error::Cancelled | Error::ExecutionInterrupted => error,
            Error::Limit(limit) => {
                let label = self.redact_text(&limit.to_string());
                Error::Limit(limit.with_label(label))
            }
            Error::ToolFailure(failure) => match failure.redacted(self) {
                Ok(failure) => Error::ToolFailure(failure),
                Err(error) => error,
            },
            error => Error::Diagnostic(self.redact_diagnostic(error.tool_diagnostic())),
        }
    }
    pub fn stream(&self) -> InvocationRedactionStream {
        InvocationRedactionStream::new(self.clone())
    }
    pub fn redact_text(&self, text: &str) -> String {
        self.project(text, true).0
    }
    pub(crate) fn redact_content(&self, text: &str) -> String {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
            let safe = match self.try_redact_value(&value) {
                Ok(value) => value,
                Err(_) => {
                    return "Tool output withheld because a JSON property name contains a secret."
                        .to_owned();
                }
            };
            if safe != value {
                // Values originate in serde_json and are always serializable.
                return safe.to_string();
            }
        }
        self.redact_text(text)
    }
    pub fn try_redact_value(&self, value: &serde_json::Value) -> Result<serde_json::Value, Error> {
        match value {
            serde_json::Value::Object(values) => {
                let mut safe = serde_json::Map::with_capacity(values.len());
                for (key, value) in values {
                    if self.redact_text(key) != *key {
                        return Err(Error::tool(
                            "tool JSON output withheld because a property name contains a secret",
                        ));
                    }
                    safe.insert(key.clone(), self.try_redact_value(value)?);
                }
                Ok(serde_json::Value::Object(safe))
            }
            serde_json::Value::Array(values) => values
                .iter()
                .map(|value| self.try_redact_value(value))
                .collect::<Result<Vec<_>, _>>()
                .map(serde_json::Value::Array),
            _ => Ok(self.redact_value(value)),
        }
    }
    /// For canonical typed envelopes: field names are protocol identities.
    /// Untrusted business JSON must use try_redact_value to reject secret keys.
    pub fn redact_value(&self, value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::String(text) => serde_json::Value::String(self.redact_text(text)),
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.iter().map(|v| self.redact_value(v)).collect())
            }
            serde_json::Value::Object(values) => serde_json::Value::Object(
                values
                    .iter()
                    .map(|(k, v)| (k.clone(), self.redact_value(v)))
                    .collect(),
            ),
            _ => value.clone(),
        }
    }
    // Leftmost, longest literal matching. Prefixes are delayed until enough data
    // arrives to decide; replacement text never goes through the matcher again.
    fn project(&self, input: &str, final_chunk: bool) -> (String, String) {
        let witnesses = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let Some(matcher) = &witnesses.matcher else {
            return (input.to_owned(), String::new());
        };
        let mut cutoff = if final_chunk {
            input.len()
        } else {
            input
                .len()
                .saturating_sub(witnesses.max_value_bytes.saturating_sub(1))
        };
        while !input.is_char_boundary(cutoff) {
            cutoff -= 1;
        }
        let mut output = String::new();
        let mut offset = 0;
        for matched in matcher.find_iter(input) {
            if matched.start() >= cutoff {
                break;
            }
            output.push_str(&input[offset..matched.start()]);
            let pattern = matched.pattern().as_usize();
            if pattern < witnesses.values.len() {
                output.push_str(&witnesses.values[pattern].1);
            } else {
                output.push_str(&input[matched.start()..matched.end()]);
            }
            offset = matched.end();
        }
        let end = cutoff.max(offset);
        output.push_str(&input[offset..end]);
        (output, input[end..].to_owned())
    }
    #[cfg(test)]
    fn stream_chunk(&self, pending: &mut String, chunk: &str, finish: bool) -> String {
        pending.push_str(chunk);
        let (safe, suffix) = self.project(pending, finish);
        *pending = suffix;
        safe
    }
}

mod ordered;
pub use ordered::InvocationRedactionStream;

#[cfg(test)]
mod tests {
    use super::*;
    fn witness(values: &[(&str, &str)]) -> InvocationRedaction {
        InvocationRedaction(Arc::new(Mutex::new(
            Witnesses::build(
                values
                    .iter()
                    .map(|(value, key)| (value.to_string(), key.to_string()))
                    .collect(),
            )
            .unwrap(),
        )))
    }
    #[test]
    fn no_secret_large_append_bypasses_private_pending_capacity() {
        use crate::runtime::execution::{ExecutionId, OutputOptions, buffer::Buffer};
        let text = "中a".repeat(256 * 1024);
        let mut stream = InvocationRedaction::default().stream();
        assert_eq!(stream.push(Some("stdout"), &text).unwrap(), text);
        assert!(stream.finish_all().unwrap().is_empty());
        let limit = 2 * 1024 * 1024;
        let mut ring = Buffer::new(OutputOptions::default(), limit).unwrap();
        ring.append(Some("stdout"), &text).unwrap();
        assert_eq!(ring.end, text.len() as u64);
        let page = ring.page(ExecutionId(1), 0, limit, 0).unwrap();
        assert_eq!(
            page.chunks
                .iter()
                .map(|chunk| chunk.content.as_str())
                .collect::<String>(),
            text
        );
        assert!(!ring.persisted().contains("withheld_redaction_tail"));
    }
    #[test]
    fn split_overlapping_utf8_and_escaped_json_are_masked() {
        let handle = witness(&[
            ("abc", "SHORT"),
            ("abcdef", "LONG"),
            ("秘密", "MASK"),
            ("quote\"secret", "SAFE"),
        ]);
        let mut pending = String::new();
        let mut output = handle.stream_chunk(&mut pending, "prefix abc", false);
        assert!(!output.contains("abc"));
        output.push_str(&handle.stream_chunk(&mut pending, "def 秘", false));
        output.push_str(&handle.stream_chunk(&mut pending, "密", false));
        output.push_str(&handle.stream_chunk(&mut pending, "", true));
        assert_eq!(output, "prefix LONG MASK");
        let value = handle.redact_value(&serde_json::json!({"error":"quote\"secret"}));
        assert_eq!(value["error"], "SAFE");
        assert!(!serde_json::to_string(&value).unwrap().contains("secret"));
        assert!(!format!("{handle:?}").contains("secret"));
    }
    #[tokio::test]
    async fn canonical_ring_checkpoint_never_retains_raw_and_reads_do_not_consume() {
        use crate::runtime::execution::{
            CancellationController, ExecutionRegistry, ExecutionSnapshot, OutputOptions,
        };
        use base64::Engine as _;
        use tokio_util::sync::CancellationToken;
        let registry = ExecutionRegistry::new(crate::Limits::default());
        let row = ExecutionSnapshot::new(
            "owner".into(),
            "call".into(),
            "stream".into(),
            "agent".into(),
            None,
        );
        let id = registry
            .admit_detached(row, CancellationToken::new())
            .unwrap();
        let handle = witness(&[("old-secret", "OLD"), ("秘密", "MASK")]);
        registry.install_redaction(id, handle.clone()).unwrap();
        let (writer, _output) = registry
            .open_output(
                id,
                OutputOptions::default(),
                Arc::new(CancellationController::new(CancellationToken::new())),
            )
            .unwrap();
        writer.append(Some("stdout"), "old-").unwrap();
        writer.append(Some("stderr"), "秘").unwrap();
        let checkpoint = registry.recovery_snapshot();
        for row in &checkpoint {
            let ring: serde_json::Value =
                serde_json::from_str(row.result.as_ref().unwrap()).unwrap();
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(ring["content_base64"].as_str().unwrap())
                .unwrap();
            assert!(!String::from_utf8(bytes).unwrap().contains("old-"));
        }
        registry
            .merge_redaction(id, &witness(&[("new-secret", "NEW")]))
            .unwrap();
        writer.append(Some("stdout"), "secret new-secret").unwrap();
        writer.append(Some("stderr"), "密").unwrap();
        writer
            .finish(Err(super::super::ToolFailure::failed(
                "failure",
                "old-secret \"秘密\" new-secret",
            )))
            .unwrap();
        let first = registry.read(id, 0, 16384).unwrap();
        let second = registry.read(id, 0, 16384).unwrap();
        assert_eq!(
            serde_json::to_value(&first).unwrap(),
            serde_json::to_value(&second).unwrap()
        );
        let projected = serde_json::to_string(&first).unwrap();
        assert!(!projected.contains("old-secret"));
        assert!(!projected.contains("new-secret"));
        assert!(!projected.contains("秘密"));
        let checkpoint = registry.recovery_snapshot();
        let encoded = checkpoint[0].result.as_ref().unwrap();
        let ring: serde_json::Value = serde_json::from_str(encoded).unwrap();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(ring["content_base64"].as_str().unwrap())
            .unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(!text.contains("old-secret"));
        assert!(!text.contains("new-secret"));
        assert!(!text.contains("秘密"));
        assert!(text.contains("OLD"));
        assert!(text.contains("NEW"));
        assert!(text.contains("MASK"));
        registry
            .release(id, CancellationToken::new())
            .await
            .unwrap();
        assert!(registry.read(id, 0, 16384).unwrap().chunks.is_empty());
    }
    #[tokio::test]
    async fn final_and_typed_failure_are_masked_before_checkpoint_serialization() {
        use crate::runtime::execution::{
            ExecutionRegistry, ExecutionSnapshot, FinalOutput, ToolFailure,
        };
        use tokio_util::sync::CancellationToken;
        let registry = ExecutionRegistry::new(crate::Limits::default());
        let row = ExecutionSnapshot::new(
            "owner".into(),
            "call".into(),
            "tool".into(),
            "agent".into(),
            None,
        );
        let id = registry
            .admit_detached(row, CancellationToken::new())
            .unwrap();
        registry
            .install_redaction(id, witness(&[("quote\"secret\n", "MASK")]))
            .unwrap();
        let content = serde_json::json!({"value":"quote\"secret\n"}).to_string();
        registry
            .complete_detached(
                id,
                FinalOutput {
                    content,
                    failure: Some(ToolFailure::failed("reported", "quote\"secret\n")),
                    attachments: vec![],
                },
            )
            .await
            .unwrap();
        let checkpoint = serde_json::to_value(registry.recovery_snapshot()).unwrap();
        let result = checkpoint[0]["result"].as_str().unwrap();
        let decoded: serde_json::Value = serde_json::from_str(result).unwrap();
        assert_eq!(decoded["value"], "MASK");
        assert_eq!(checkpoint[0]["failure"]["message"], "MASK");
        assert!(!checkpoint.to_string().contains("secret"));
    }
    #[test]
    fn separate_public_stream_channels_flush_without_cross_channel_matching() {
        let handle = witness(&[("secret", "MASK")]);
        let mut stream = handle.stream();
        assert!(stream.push_ordered(Some("out"), "sec").unwrap().is_empty());
        assert!(stream.push_ordered(Some("err"), "B").unwrap().is_empty());
        let mut output = stream.push_ordered(Some("out"), "retC").unwrap();
        output.extend(stream.finish_all().unwrap());
        assert_eq!(
            output,
            vec![
                (Some("out".into()), "MASK".into()),
                (Some("err".into()), "B".into()),
                (Some("out".into()), "C".into())
            ]
        );
        let mut single = handle.stream();
        single.push(Some("out"), "sec").unwrap();
        assert!(single.push(Some("err"), "B").is_err());
        assert_eq!(single.push(Some("out"), "ret").unwrap(), "MASK");
    }
    #[test]
    fn checkpoint_exposes_withheld_tail_loss_without_persisting_candidate_bytes() {
        use crate::runtime::execution::{ExecutionId, OutputOptions, buffer::Buffer};
        let mut ring = Buffer::new(OutputOptions::default(), 4096).unwrap();
        ring.install_redaction(witness(&[("unrelated-long-secret-value", "MASK")]));
        ring.append(None, "ordinary").unwrap();
        assert_eq!(ring.end, 0);
        let encoded = ring.persisted();
        let checkpoint: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(checkpoint["withheld_redaction_tail"], true);
        assert!(!encoded.contains("ordinary"));
        let restored = Buffer::restore(&ring.metadata(), Some(&encoded), 4096).unwrap();
        let page = restored.page(ExecutionId(1), 0, 4096, 0).unwrap();
        assert!(page.chunks.iter().any(|chunk| {
            chunk
                .content
                .contains("[upstream lost: withheld redaction tail]")
        }));
        let empty = Buffer::new(OutputOptions::default(), 4096).unwrap();
        assert!(!empty.persisted().contains("withheld_redaction_tail"));
    }
    #[tokio::test]
    async fn secret_channel_names_are_rejected_before_registry_metadata() {
        use crate::runtime::execution::{
            CancellationController, ExecutionRegistry, ExecutionSnapshot, OutputOptions,
        };
        use tokio_util::sync::CancellationToken;
        let registry = ExecutionRegistry::new(crate::Limits::default());
        let row = ExecutionSnapshot::new(
            "owner".into(),
            "call".into(),
            "stream".into(),
            "agent".into(),
            None,
        );
        let id = registry
            .admit_detached(row, CancellationToken::new())
            .unwrap();
        registry
            .install_redaction(id, witness(&[("sensitive-channel", "MASK")]))
            .unwrap();
        let controller = Arc::new(CancellationController::new(CancellationToken::new()));
        assert!(
            registry
                .open_output(
                    id,
                    OutputOptions {
                        channels: vec!["sensitive-channel".into()]
                    },
                    controller.clone()
                )
                .is_err()
        );
        let (writer, _output) = registry
            .open_output(id, OutputOptions::default(), controller)
            .unwrap();
        assert!(writer.append(Some("sensitive-channel"), "body").is_err());
        assert!(
            !serde_json::to_string(&registry.recovery_snapshot())
                .unwrap()
                .contains("sensitive-channel")
        );
    }
    #[tokio::test]
    async fn invalid_failure_bounds_preserve_concrete_cause_without_raw_message() {
        use crate::runtime::execution::{ExecutionRegistry, ExecutionSnapshot, ToolFailure};
        use tokio_util::sync::CancellationToken;
        let registry = ExecutionRegistry::new(crate::Limits::default());
        let row = ExecutionSnapshot::new(
            "owner".into(),
            "call".into(),
            "tool".into(),
            "agent".into(),
            None,
        );
        let id = registry
            .admit_detached(row, CancellationToken::new())
            .unwrap();
        registry
            .install_redaction(id, witness(&[("private-value", "MASK")]))
            .unwrap();
        registry
            .finish_error(
                id,
                &Error::ToolFailure(ToolFailure::failed("reported", "private-value".repeat(400))),
            )
            .await;
        let snapshot = registry.recovery_snapshot();
        assert!(snapshot[0].failure.is_none());
        assert_eq!(
            snapshot[0].diagnostic.as_ref().unwrap().summary(),
            "invalid tool-reported failure bounds"
        );
        assert!(
            !serde_json::to_string(&snapshot)
                .unwrap()
                .contains("private-value")
        );
    }
    #[test]
    fn short_secret_placeholders_are_idempotent_and_business_keys_fail_closed() {
        let handle = witness(&[("secret", "{{secret:KEY}}"), ("status", "{{secret:STATE}}")]);
        let once = handle.redact_text("a secret b");
        assert_eq!(once, "a {{secret:KEY}} b");
        assert_eq!(handle.redact_text(&once), once);
        let typed = handle.redact_value(&serde_json::json!({"status":"secret"}));
        assert_eq!(typed["status"], "{{secret:KEY}}");
        assert!(
            handle
                .try_redact_value(&serde_json::json!({"secret":"one", "{{secret:KEY}}":"two"}))
                .is_err()
        );
        assert!(
            handle
                .redact_content("{\"secret\":\"value\"}")
                .contains("withheld")
        );
    }
    #[test]
    fn nested_business_json_preserves_structure_and_rejects_sensitive_keys() {
        let handle = witness(&[("private-value", "{{secret:KEY}}")]);
        let original = serde_json::json!({
            "outer": [{"leaf": "private-value", "n": 7, "null": null}, true],
            "tail": "public"
        });
        let safe = handle.try_redact_value(&original).unwrap();
        assert_eq!(safe["outer"][0]["leaf"], "{{secret:KEY}}");
        assert_eq!(safe["outer"][0]["n"], 7);
        assert!(safe["outer"][0]["null"].is_null());
        assert_eq!(safe["outer"][1], true);
        assert_eq!(safe["tail"], "public");
        assert_eq!(original["outer"][0]["leaf"], "private-value");
        assert_eq!(handle.try_redact_value(&safe).unwrap(), safe);
        assert!(
            handle
                .try_redact_value(&serde_json::json!({"outer": [{"private-value": "public"}]}))
                .is_err()
        );
    }

    #[test]
    fn capacity_failure_does_not_change_existing_witnesses() {
        let handle = witness(&[("old", "OLD")]);
        let mut values = (0..MAX_VALUES)
            .map(|index| (format!("value-{index}"), "MASK".to_owned()))
            .collect::<Vec<_>>();
        let other = InvocationRedaction(Arc::new(Mutex::new(
            Witnesses::build(std::mem::take(&mut values)).unwrap(),
        )));
        assert!(handle.merge(&other).is_err());
        assert_eq!(handle.redact_text("old"), "OLD");
        assert_eq!(handle.redact_text("value-0"), "value-0");
    }
    #[test]
    fn merging_witnesses_keeps_old_values_and_partial_tail() {
        let handle = witness(&[("old-token", "OLD")]);
        let rotation = witness(&[("new-token", "NEW")]);
        let mut pending = String::new();
        assert_eq!(handle.stream_chunk(&mut pending, "old-", false), "");
        handle.merge(&rotation).unwrap();
        assert_eq!(
            handle.stream_chunk(&mut pending, "token new-token", true),
            "OLD NEW"
        );
        assert_eq!(handle.redact_text("old-token new-token"), "OLD NEW");
    }
}
