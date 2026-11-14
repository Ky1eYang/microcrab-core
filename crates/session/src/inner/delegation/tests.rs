use super::bound_delegate_receipt;
use crate::{
    Error,
    runtime::execution::{ExecutionId, ExecutionSnapshot, ExecutionStatus},
};
use serde_json::{Value, json};

fn receipt(content: &str, count: u64) -> Value {
    let rows: Vec<_> = (1..=count)
        .map(|id| {
            let mut row = ExecutionSnapshot::new(
                "child-identity".into(),
                format!("call-{id}"),
                "external_tool".into(),
                "reviewer".into(),
                Some("review".into()),
            );
            row.execution_id = ExecutionId(id);
            row.status = ExecutionStatus::Succeeded;
            row
        })
        .collect();
    json!({
        "child_id": "child-identity",
        "subagent": "review",
        "outcome": {"content": content, "rounds": 2, "usage": {"input_tokens": 13}},
        "usage": {"input_tokens": 13},
        "executions": rows,
    })
}

#[test]
fn bounds_current_execution_metadata_with_explicit_omission() {
    let original = receipt("", 24);
    let encoded = bound_delegate_receipt(original.clone(), 1024).unwrap();
    assert!(encoded.len() <= 1024);
    let bounded: Value = serde_json::from_str(&encoded).unwrap();
    let retained = bounded["executions"].as_array().unwrap();
    let omitted = bounded["executions_omitted"].as_u64().unwrap();
    assert!(!retained.is_empty());
    assert!(omitted > 0);
    assert_eq!(retained.len() as u64 + omitted, 24);
    assert_eq!(
        retained.as_slice(),
        &original["executions"].as_array().unwrap()[..retained.len()]
    );
    assert_eq!(bounded["child_id"], original["child_id"]);
    assert_eq!(bounded["subagent"], original["subagent"]);
    assert_eq!(bounded["outcome"], original["outcome"]);
    assert_eq!(bounded["usage"], original["usage"]);
    assert!(bounded.get("tool_tasks_omitted").is_none());
}

#[test]
fn bounds_serialized_unicode_outcome_without_losing_metadata() {
    // Quotes, control characters and backslashes consume more JSON bytes than
    // their UTF-8 source; verify the final serialized receipt, not a text estimate.
    let original = receipt(&"界🦀\"\n\\".repeat(400), 1);
    let encoded = bound_delegate_receipt(original.clone(), 1024).unwrap();
    assert!(encoded.len() <= 1024);
    let bounded: Value = serde_json::from_str(&encoded).unwrap();
    let content = bounded["outcome"]["content"].as_str().unwrap();
    assert!(!content.is_empty());
    assert!(
        original["outcome"]["content"]
            .as_str()
            .unwrap()
            .starts_with(content)
    );
    assert_eq!(bounded["outcome"]["content_truncated"], true);
    assert_eq!(bounded["outcome"]["rounds"], original["outcome"]["rounds"]);
    assert_eq!(bounded["outcome"]["usage"], original["outcome"]["usage"]);
    assert_eq!(bounded["usage"], original["usage"]);
    assert_eq!(bounded["executions"], original["executions"]);
}

#[test]
fn only_unavoidable_receipt_metadata_fails_closed() {
    let original = receipt("", 0);
    let exact = original.to_string();
    assert_eq!(
        bound_delegate_receipt(original.clone(), exact.len()).unwrap(),
        exact
    );
    assert!(matches!(
        bound_delegate_receipt(original, 1),
        Err(Error::Tool(message)) if message.as_str() == "delegate result metadata exceeds output budget"
    ));
}
