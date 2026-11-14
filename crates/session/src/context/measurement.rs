//! Read-only measurements use precisely the admission budget envelope, including
//! JSON escaping and punctuation. They are valid while tool calls await results.
use super::{Content, ContentItem};
use crate::Error;
use microcrab_protocol::ToolDefinition;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
#[cfg(test)]
mod live_tests;

#[derive(Clone, Debug, Serialize)]
pub struct ContextMeasurement {
    pub scope: &'static str,
    pub provenance: &'static str,
    pub epoch: u64,
    pub baseline_generation: u64,
    pub pending_epoch: bool,
    pub bytes: usize,
    pub limit_bytes: usize,
    pub remaining_bytes: usize,
    pub categories: ContextBytes,
    pub counts: ContextCounts,
    /// Hash of the same budget bytes, never raw model content.
    pub fingerprint: String,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct ContextBytes {
    pub system: usize,
    pub tools: usize,
    pub state: usize,
    pub user: usize,
    pub assistant: usize,
    pub tool_result: usize,
    pub framing: usize,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct ContextCounts {
    pub items: usize,
    pub tool_definitions: usize,
    pub pending_tool_calls: usize,
    pub state_frames: usize,
    pub user_messages: usize,
    pub assistant_messages: usize,
    pub tool_results: usize,
    pub tool_calls: usize,
    /// Distinct recorded epoch/turn identities; unknown legacy positions stay null.
    pub turns: Option<usize>,
    /// Validated assistant response items, independent of page size/retries.
    pub steps: usize,
}
#[derive(Serialize)]
struct Budget<'a> {
    system: &'a str,
    items: &'a [ContentItem],
    tools: &'a [ToolDefinition],
}
pub(super) fn budget_bytes(
    system: &str,
    items: &[ContentItem],
    tools: &[ToolDefinition],
) -> Result<Vec<u8>, Error> {
    Ok(serde_json::to_vec(&Budget {
        system,
        items,
        tools,
    })?)
}

impl ContextMeasurement {
    /// Exact provenance for a provider's issued Content, without requiring a
    /// checkpoint or changing the advertised request/system/tool bytes.
    pub fn for_content(content: &Content) -> Result<Self, Error> {
        Self::measure(
            &content.system,
            &content.items,
            &content.tools,
            content.epoch,
            false,
            content.max_bytes,
            0,
        )
    }
    pub(super) fn measure(
        system: &str,
        items: &[ContentItem],
        tools: &[ToolDefinition],
        epoch: u64,
        pending_epoch: bool,
        limit_bytes: usize,
        pending_tool_calls: usize,
    ) -> Result<Self, Error> {
        let serialized = budget_bytes(system, items, tools)?;
        let bytes = serialized.len();
        let mut categories = ContextBytes {
            system: serde_json::to_vec(system)?.len(),
            tools: serde_json::to_vec(tools)?.len(),
            ..Default::default()
        };
        let mut counts = ContextCounts {
            items: items.len(),
            tool_definitions: tools.len(),
            pending_tool_calls,
            ..Default::default()
        };
        let mut state = serde_json::json!({});
        let mut turns = BTreeSet::new();
        let mut unknown_positions = false;
        let mut awaiting_position = false;
        for item in items {
            let size = serde_json::to_vec(item)?.len();
            match item {
                ContentItem::State(frame) => {
                    categories.state += size;
                    counts.state_frames += 1;
                    microcrab_transport::apply_state_patch(&mut state, frame)
                        .map_err(Error::protocol)?;
                    if let (Some(epoch), Some(turn)) = (
                        state
                            .pointer("/session/epoch")
                            .and_then(serde_json::Value::as_u64),
                        state
                            .pointer("/session/turn")
                            .and_then(serde_json::Value::as_u64),
                    ) {
                        turns.insert((epoch, turn));
                        awaiting_position = false;
                    }
                }
                ContentItem::User { .. } => {
                    categories.user += size;
                    counts.user_messages += 1;
                    if let (Some(epoch), Some(turn)) = (
                        state
                            .pointer("/session/epoch")
                            .and_then(serde_json::Value::as_u64),
                        state
                            .pointer("/session/turn")
                            .and_then(serde_json::Value::as_u64),
                    ) {
                        turns.insert((epoch, turn));
                        awaiting_position = false;
                    } else {
                        awaiting_position = true;
                    }
                }
                ContentItem::Assistant { tool_calls, .. } => {
                    categories.assistant += size;
                    counts.assistant_messages += 1;
                    counts.steps += 1;
                    counts.tool_calls += tool_calls.len();
                    if let (Some(epoch), Some(turn)) = (
                        state
                            .pointer("/session/epoch")
                            .and_then(serde_json::Value::as_u64),
                        state
                            .pointer("/session/turn")
                            .and_then(serde_json::Value::as_u64),
                    ) {
                        turns.insert((epoch, turn));
                        awaiting_position = false;
                    } else {
                        unknown_positions = true;
                    }
                }
                ContentItem::Tool { .. } => {
                    categories.tool_result += size;
                    counts.tool_results += 1;
                }
            }
        }
        counts.turns = (!unknown_positions && !awaiting_position).then_some(turns.len());
        categories.framing = bytes.saturating_sub(
            categories.system
                + categories.tools
                + categories.state
                + categories.user
                + categories.assistant
                + categories.tool_result,
        );
        Ok(Self {
            scope: "current_selected_context",
            provenance: "context_budget_json_v1",
            epoch,
            baseline_generation: 0,
            pending_epoch,
            bytes,
            limit_bytes,
            remaining_bytes: limit_bytes.saturating_sub(bytes),
            categories,
            counts,
            fingerprint: format!("{:x}", Sha256::digest(&serialized)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Context;
    use microcrab_protocol::{Response, ToolCall, Usage};
    use serde_json::json;

    #[test]
    fn exact_budget_measurement_handles_escaped_multibyte_pending_tools() {
        let tool = ToolDefinition {
            name: "read".into(),
            description: "中文\n\"quoted\"".into(),
            parameters: json!({"type":"object"}),
        };
        let mut context = Context::new("系统\n\"quote\"".into(), vec![tool], 65536).unwrap();
        context
            .admit_user(
                "hello 中文",
                &json!({"session":{"epoch":0,"turn":1,"round":1}}),
            )
            .unwrap();
        context
            .append_response(&Response {
                provider_metadata: None,
                content: "".into(),
                tool_calls: vec![ToolCall {
                    provider_metadata: None,
                    id: "call".into(),
                    name: "read".into(),
                    arguments: json!({"path":"中文"}),
                }],
                usage: Usage::default(),
            })
            .unwrap();
        assert!(context.snapshot().is_err());
        let measurement = context.measure().unwrap();
        let expected = budget_bytes(&context.system, &context.items, &context.tools)
            .unwrap()
            .len();
        assert_eq!(measurement.bytes, expected);
        assert_eq!(measurement.counts.pending_tool_calls, 1);
        assert_eq!(measurement.counts.tool_calls, 1);
        assert_eq!(measurement.counts.turns, Some(1));
        let c = &measurement.categories;
        assert_eq!(
            c.system + c.tools + c.state + c.user + c.assistant + c.tool_result + c.framing,
            expected
        );
        context
            .append_tool_result("call".into(), "done".into())
            .unwrap();
        assert_eq!(context.measure().unwrap().counts.pending_tool_calls, 0);
        assert_ne!(
            context.measure().unwrap().fingerprint,
            measurement.fingerprint
        );
    }
    #[test]
    fn selected_history_counts_do_not_depend_on_message_pages_and_survive_restore() {
        let mut context = Context::new("s".into(), vec![], 1024 * 1024).unwrap();
        for turn in 1..=80 {
            context
                .admit_user(
                    "question",
                    &json!({"session":{"epoch":0,"turn":turn,"round":1}}),
                )
                .unwrap();
            context
                .append_response(&Response {
                    provider_metadata: None,
                    content: "answer".into(),
                    tool_calls: vec![],
                    usage: Usage::default(),
                })
                .unwrap();
        }
        let before = context.measure().unwrap();
        assert_eq!(before.counts.turns, Some(80));
        assert_eq!(before.counts.steps, 80);
        assert_eq!(before.counts.user_messages, 80);
        let restored = Context::from_snapshot(context.snapshot().unwrap())
            .unwrap()
            .measure()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&before).unwrap(),
            serde_json::to_value(&restored).unwrap()
        );
    }
    #[test]
    fn shared_budget_rejection_retains_exact_context_and_safe_numeric_facts() {
        let mut context = Context::new("s".into(), vec![], 256).unwrap();
        context.admit_user("accepted", &json!({})).unwrap();
        let before = context.measure().unwrap();
        let error = context
            .admit_user("secret".repeat(20), &json!({}))
            .unwrap_err();
        let Error::Limit(limit) = error else {
            panic!("expected typed Context rejection")
        };
        assert_eq!(limit.kind, crate::budget::LimitKind::ContextBytes);
        assert!(limit.required_bytes.unwrap() > limit.budget_bytes.unwrap());
        assert_eq!(limit.budget_bytes, Some(256));
        // The owner has no retry witness here; a kind alone never proves pre-effect.
        assert!(!limit.is_pre_effect());
        let diagnostic = limit.clone().with_label("secret owner diagnostic");
        assert!(diagnostic.to_string().contains("secret"));
        assert_eq!(diagnostic.sanitized(), limit.sanitized());
        assert!(!diagnostic.sanitized().to_string().contains("secret"));
        assert_eq!(context.measure().unwrap().fingerprint, before.fingerprint);
        assert_eq!(context.items().len(), before.counts.items);

        let boundary = Context::new("s".into(), vec![], 1000)
            .unwrap()
            .measure()
            .unwrap()
            .bytes;
        Context::new("s".into(), vec![], boundary).unwrap();
        let Error::Limit(limit) = Context::new("s".into(), vec![], boundary - 1).unwrap_err()
        else {
            panic!("expected exact-boundary rejection")
        };
        assert_eq!(limit.required_bytes, Some(boundary));
        assert_eq!(limit.budget_bytes, Some(boundary - 1));
    }

    #[test]
    fn admission_limit_uses_identical_serialization_and_read_does_not_change_epoch() {
        let context = Context::new("s".into(), vec![], 1000).unwrap();
        let bytes = context.measure().unwrap().bytes;
        assert!(Context::new("s".into(), vec![], bytes).is_ok());
        assert!(Context::new("s".into(), vec![], bytes - 1).is_err());
        let mut context = Context::new("s".into(), vec![], 1000).unwrap();
        context.compact("summary".into(), &json!({})).unwrap();
        let first = context.measure().unwrap();
        assert!(first.pending_epoch);
        assert_eq!(first.epoch, 0);
        assert_eq!(context.measure().unwrap().fingerprint, first.fingerprint);
        assert!(context.pending_epoch());
    }
}
