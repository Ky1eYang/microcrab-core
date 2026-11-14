//! Google replay evidence must survive the real Context and snapshot boundary.
use microcrab_protocol::{
    AssistantProviderMetadata, Response, ToolCall, ToolCallProviderMetadata, ToolDefinition, Usage,
};
use microcrab_session::context::{ContentItem, Context};
use microcrab_transport::render_ast;
use microcrab_transport::{Google, GoogleConfig};
use serde_json::{Value, json};

#[test]
fn full_google_model_parts_survive_context_restore_and_keep_wire_id_absence() {
    let mut context = Context::new(
        "Frozen Google system".into(),
        vec![ToolDefinition {
            name: "lookup".into(),
            description: "Frozen lookup tool".into(),
            parameters: json!({"type":"object","properties":{"city":{"type":"string"}}}),
        }],
        65_536,
    )
    .unwrap();
    context.admit_user("Look up Tokyo", &json!({})).unwrap();
    let client = Google::new(GoogleConfig {
        endpoint: "http://127.0.0.1:1/v1beta".into(),
        model: "gemini-test".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 65_536,
    })
    .unwrap();
    let initial = client
        .adapt(&render_ast(&context.content(128).unwrap()).unwrap())
        .unwrap();
    let parts = vec![
        json!({"thought":true,"text":"Synthetic private thought","thoughtSignature":"dGhvdWdodA=="}),
        json!({"text":"Before.","thoughtSignature":"dGV4dA=="}),
        json!({"functionCall":{"name":"lookup","args":{"city":"Tokyo"}},"thoughtSignature":"Y2FsbA=="}),
        json!({"text":"After."}),
        json!({"text":"","thoughtSignature":"dGFpbA=="}),
    ];
    context
        .append_response(&Response {
            content: "Before.After.".into(),
            tool_calls: vec![ToolCall {
                id: "runtime-only-call".into(),
                name: "lookup".into(),
                arguments: json!({"city":"Tokyo"}),
                provider_metadata: Some(ToolCallProviderMetadata::Google {
                    thought_signature: "Y2FsbA==".into(),
                }),
            }],
            provider_metadata: Some(AssistantProviderMetadata::Google {
                parts: parts.clone(),
            }),
            usage: Usage::default(),
        })
        .unwrap();
    context
        .append_tool_result("runtime-only-call".into(), "{\"temperature\":20}".into())
        .unwrap();
    let snapshot = serde_json::to_vec(&context.snapshot().unwrap()).unwrap();
    let restored = Context::from_snapshot(serde_json::from_slice(&snapshot).unwrap()).unwrap();
    let content = restored.content(128).unwrap();
    let assistant = content
        .items
        .iter()
        .find_map(|item| match item {
            ContentItem::Assistant { content, .. } => Some(content),
            _ => None,
        })
        .unwrap();
    assert_eq!(assistant, "Before.After.");
    assert!(!assistant.contains("Synthetic private thought"));
    let replay = client.adapt(&render_ast(&content).unwrap()).unwrap();
    assert_eq!(replay["systemInstruction"], initial["systemInstruction"]);
    assert_eq!(replay["tools"], initial["tools"]);
    assert_eq!(replay["generationConfig"], initial["generationConfig"]);
    assert_eq!(replay["contents"][0], initial["contents"][0]);
    assert_eq!(replay["contents"][1], json!({"role":"model","parts":parts}));
    let result = &replay["contents"][2]["parts"][0]["functionResponse"];
    assert_eq!(result["name"], "lookup");
    assert_eq!(result["response"], json!({"temperature":20}));
    assert!(result.get("id").is_none());
    assert_eq!(
        replay["contents"][1]["parts"][2]["functionCall"].get("id"),
        None::<&Value>
    );
    assert_eq!(
        client
            .adapt(&render_ast(&context.content(128).unwrap()).unwrap())
            .unwrap(),
        replay,
        "snapshot restore must preserve the complete next provider request"
    );
}
