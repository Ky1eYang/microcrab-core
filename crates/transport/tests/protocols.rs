use microcrab_protocol::{InputPart, Message, Request, ToolCall, ToolDefinition};
use microcrab_transport::{
    Anthropic, AnthropicConfig, OpenAiResponse, OpenAiResponseConfig, Protocol, TransportClient,
    TransportConfig, create_client,
};
use serde_json::json;
use std::str::FromStr;

fn sample_request() -> Request {
    Request {
        epoch: 1,
        messages: vec![
            Message::System {
                content: "You are a helpful assistant.".into(),
            },
            Message::User {
                parts: vec![InputPart::Text {
                    text: "Search for weather in Tokyo".into(),
                }],
            },
            Message::Assistant {
                provider_metadata: None,
                content: "Calling tool".into(),
                tool_calls: vec![ToolCall {
                    provider_metadata: None,
                    id: "call_123".into(),
                    name: "get_weather".into(),
                    arguments: json!({"city": "Tokyo"}),
                }],
            },
            Message::Tool {
                call_id: "call_123".into(),
                content: json!({"temperature": 22}).to_string(),
            },
        ],
        tools: vec![ToolDefinition {
            name: "get_weather".into(),
            description: "Get current weather".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "city": { "type": "string" }
                },
                "required": ["city"]
            }),
        }],
        max_output_tokens: 1024,
    }
}

#[test]
fn test_protocol_parsing_and_dispatch() {
    for (raw, expected) in [
        ("openai-compatible", Protocol::OpenaiCompatible),
        ("openai", Protocol::OpenaiResponses),
        ("openai-response", Protocol::OpenaiResponses),
        ("openai-responses", Protocol::OpenaiResponses),
        ("anthropic", Protocol::Anthropic),
        ("claude", Protocol::Anthropic),
        ("google", Protocol::Google),
        ("gemini", Protocol::Google),
    ] {
        let parsed = Protocol::from_str(raw).unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(
            serde_json::from_value::<Protocol>(json!(raw)).unwrap(),
            expected
        );
        assert_eq!(
            serde_json::to_value(parsed).unwrap(),
            json!(parsed.as_str())
        );

        let config = TransportConfig {
            auth: Default::default(),
            protocol: parsed,
            endpoint: "http://127.0.0.1:8080".into(),
            model: "test-model".into(),
            api_key: Some("secret".into()),
            timeout_ms: 5000,
            max_response_bytes: 65536,
        };

        let _client = create_client(config.clone()).unwrap();
        // Client builds successfully
        let tc = TransportClient::new(config).unwrap();
        match parsed {
            Protocol::OpenaiCompatible => {
                assert!(matches!(tc, TransportClient::OpenAiCompatible(_)))
            }
            Protocol::OpenaiResponses => assert!(matches!(tc, TransportClient::OpenAiResponses(_))),
            Protocol::Anthropic => assert!(matches!(tc, TransportClient::Anthropic(_))),
            Protocol::Google => assert!(matches!(tc, TransportClient::Google(_))),
        }
    }
    assert!(Protocol::from_str("unknown").is_err());
    assert!(serde_json::from_value::<Protocol>(json!("unknown")).is_err());
}

#[test]
fn test_anthropic_adapt() {
    let client = Anthropic::new(AnthropicConfig {
        endpoint: "https://api.anthropic.com/v1".into(),
        model: "claude-3-5-sonnet".into(),
        api_key: Some("sk-ant-test".into()),
        timeout_ms: 10000,
        max_response_bytes: 65536,
    })
    .unwrap();

    let request = sample_request();
    let adapted = client.adapt(&request).unwrap();

    // Top-level system
    assert_eq!(
        adapted["system"].as_str(),
        Some("You are a helpful assistant.")
    );
    assert_eq!(adapted["model"].as_str(), Some("claude-3-5-sonnet"));
    assert_eq!(adapted["max_tokens"].as_u64(), Some(1024));

    // Tools
    let tools = adapted["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"].as_str(), Some("get_weather"));
    assert!(tools[0].get("input_schema").is_some());

    // Messages
    let messages = adapted["messages"].as_array().unwrap();
    // Alternating user and assistant:
    // 1. user ("Search for weather in Tokyo")
    // 2. assistant ("Calling tool" + tool_use)
    // 3. user (tool_result)
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0]["role"], "user");
    assert_eq!(messages[1]["role"], "assistant");
    assert_eq!(messages[2]["role"], "user");

    let tool_use = &messages[1]["content"][1];
    assert_eq!(tool_use["type"], "tool_use");
    assert_eq!(tool_use["id"], "call_123");
    assert_eq!(tool_use["name"], "get_weather");

    let tool_result = &messages[2]["content"][0];
    assert_eq!(tool_result["type"], "tool_result");
    assert_eq!(tool_result["tool_use_id"], "call_123");
}

#[test]
fn test_openai_responses_adapt() {
    let client = OpenAiResponse::new(OpenAiResponseConfig {
        endpoint: "https://api.openai.com/v1".into(),
        model: "gpt-4.5-preview".into(),
        api_key: Some("sk-test".into()),
        timeout_ms: 10000,
        max_response_bytes: 65536,
    })
    .unwrap();

    let request = sample_request();
    let adapted = client.adapt(&request).unwrap();

    assert_eq!(
        adapted["instructions"].as_str(),
        Some("You are a helpful assistant.")
    );
    assert_eq!(adapted["model"].as_str(), Some("gpt-4.5-preview"));
    assert_eq!(adapted["max_output_tokens"].as_u64(), Some(1024));

    let tools = adapted["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["type"], "function");
    assert_eq!(tools[0]["name"], "get_weather");

    let input = adapted["input"].as_array().unwrap();
    // input items:
    // 1. user message
    // 2. assistant message ("Calling tool")
    // 3. function_call
    // 4. function_call_output
    assert_eq!(input.len(), 4);
    assert_eq!(input[0]["role"], "user");
    assert_eq!(input[1]["role"], "assistant");
    assert_eq!(input[2]["type"], "function_call");
    assert_eq!(input[2]["call_id"], "call_123");
    assert_eq!(input[2]["name"], "get_weather");
    assert_eq!(input[3]["type"], "function_call_output");
    assert_eq!(input[3]["call_id"], "call_123");
}
