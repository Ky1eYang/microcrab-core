use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::{ContentItem, Context};
use microcrab_transport::render_ast;
use serde_json::{Value, json};

fn schema() -> Value {
    serde_json::from_str(
        r#"{"type":"object","properties":{"z":{"type":"array","items":{"z":2,"a":1}},"a":{"type":"string"}},"required":["z","a"]}"#,
    )
    .unwrap()
}

fn arguments() -> Value {
    serde_json::from_str(
        r#"{"z":{"z":2,"a":1},"a":[{"z":false,"a":null},[3,2,1]],"text":"  preserve\ntext","number":1.25}"#,
    )
    .unwrap()
}

fn context() -> Context {
    Context::new(
        "Frozen system\n  unchanged".into(),
        vec![ToolDefinition {
            name: "echo".into(),
            description: "Frozen tool".into(),
            parameters: schema(),
        }],
        65_536,
    )
    .unwrap()
}

fn response() -> Response {
    Response {
        provider_metadata: None,
        content: "  preserve assistant\ntext".into(),
        tool_calls: vec![ToolCall {
            provider_metadata: None,
            id: "one".into(),
            name: "echo".into(),
            arguments: arguments(),
        }],
        usage: Usage::default(),
    }
}

// Reproduce storage's recursive object sorting and JSON decode without a storage
// dependency. Arrays and scalar encodings use ordinary serde JSON throughout.
fn canonical_roundtrip(context: &Context) -> Context {
    let mut checkpoint = serde_json::to_value(context.snapshot().unwrap()).unwrap();
    checkpoint.sort_all_objects();
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    Context::from_snapshot(serde_json::from_slice(&bytes).unwrap()).unwrap()
}

fn assert_model_bytes_unchanged(context: &Context) {
    let live = context.content(256).unwrap();
    let restored = canonical_roundtrip(context).content(256).unwrap();
    assert_eq!(
        serde_json::to_vec(&live).unwrap(),
        serde_json::to_vec(&restored).unwrap(),
        "checkpoint restore must preserve serialized model Content"
    );
    assert_eq!(
        serde_json::to_vec(&render_ast(&live).unwrap()).unwrap(),
        serde_json::to_vec(&render_ast(&restored).unwrap()).unwrap(),
        "state JSON text and rendered model prefix must remain identical"
    );
}

#[test]
fn first_admission_freezes_nested_schema_state_and_tool_argument_bytes_across_restore() {
    let mut context = context();
    let initial: Value = serde_json::from_str(
        r#"{"z_remove":2,"a_remove":1,"nested":{"z":2,"a":[{"z":false,"a":null},[3,2,1]]}}"#,
    )
    .unwrap();
    let initial_bytes = serde_json::to_vec(&initial).unwrap();
    context.admit_user("  first\nuser", &initial).unwrap();
    assert_model_bytes_unchanged(&context);
    assert_eq!(serde_json::to_vec(&initial).unwrap(), initial_bytes);
    assert_eq!(
        context.content(256).unwrap().tools[0].parameters["required"],
        json!(["z", "a"])
    );

    let response = response();
    let original_arguments = response.tool_calls[0].arguments.to_string();
    assert!(original_arguments.starts_with("{\"z\":"));
    context.append_response(&response).unwrap();
    let tool_result = "{\"z\":2,\"a\":1}\n  preserve tool bytes";
    context
        .append_tool_result("one".into(), tool_result.into())
        .unwrap();
    assert_eq!(
        response.tool_calls[0].arguments.to_string(),
        original_arguments
    );
    let content = context.content(256).unwrap();
    let ContentItem::Assistant { tool_calls, .. } = &content.items[2] else {
        panic!("assistant tool call");
    };
    assert_eq!(tool_calls[0].arguments, response.tool_calls[0].arguments);
    assert_eq!(tool_calls[0].arguments["a"][1], json!([3, 2, 1]));
    let ContentItem::Tool { content, .. } = &content.items[3] else {
        panic!("tool result");
    };
    assert_eq!(content, tool_result);
    assert_model_bytes_unchanged(&context);

    let changed: Value =
        serde_json::from_str(r#"{"z_new":{"z":2,"a":1},"a_new":[{"z":false,"a":null},[3,2,1]]}"#)
            .unwrap();
    let changed_bytes = serde_json::to_vec(&changed).unwrap();
    context.synchronize_state(&changed).unwrap();
    let ContentItem::State(microcrab_transport::StateFrame::StatePatch { set, remove, .. }) =
        context.items().last().unwrap()
    else {
        panic!("new state patch");
    };
    assert_eq!(
        remove,
        &vec![
            "a_remove".to_owned(),
            "nested".to_owned(),
            "z_remove".to_owned()
        ]
    );
    assert_eq!(set["a_new"][1], json!([3, 2, 1]));
    let before = serde_json::to_vec(&context.content(256).unwrap()).unwrap();
    context.synchronize_state(&changed).unwrap();
    assert_eq!(
        serde_json::to_vec(&context.content(256).unwrap()).unwrap(),
        before
    );
    assert_eq!(serde_json::to_vec(&changed).unwrap(), changed_bytes);
    assert_model_bytes_unchanged(&context);
    let mut restored = canonical_roundtrip(&context);
    restored.synchronize_state(&changed).unwrap();
    assert_eq!(
        serde_json::to_vec(&restored.content(256).unwrap()).unwrap(),
        before
    );
}

#[test]
fn restoring_legacy_context_does_not_normalize_historical_values_or_text() {
    let mut context = context();
    let historical_state: Value = serde_json::from_str(r#"{"z":{"z":2,"a":1},"a":0}"#).unwrap();
    context
        .admit_user("  historical\nuser", &historical_state)
        .unwrap();
    context.append_response(&response()).unwrap();
    context
        .append_tool_result("one".into(), "{\"z\":2,\"a\":1}".into())
        .unwrap();
    let mut legacy = context.snapshot().unwrap();
    legacy.tools[0].parameters = schema();
    let ContentItem::State(microcrab_transport::StateFrame::StateSnapshot { state }) =
        &mut legacy.items[0]
    else {
        panic!("initial state frame");
    };
    *state = historical_state;
    let ContentItem::Assistant { tool_calls, .. } = &mut legacy.items[2] else {
        panic!("historical assistant");
    };
    tool_calls[0].arguments = arguments();
    let historical_bytes = serde_json::to_vec(&legacy).unwrap();
    let restored = Context::from_snapshot(legacy).unwrap();
    assert_eq!(
        serde_json::to_vec(&restored.snapshot().unwrap()).unwrap(),
        historical_bytes
    );
}
