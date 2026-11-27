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
                parts: vec![],
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
            headers: Default::default(),
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

    assert!(adapted.get("instructions").is_none());
    assert!(
        adapted["prompt_cache_key"]
            .as_str()
            .is_some_and(|key| key.starts_with("mc_v1_") && key.len() == 38)
    );
    assert_eq!(adapted["model"].as_str(), Some("gpt-4.5-preview"));
    assert_eq!(adapted["max_output_tokens"].as_u64(), Some(1024));

    let tools = adapted["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["type"], "function");
    assert_eq!(tools[0]["name"], "get_weather");

    let input = adapted["input"].as_array().unwrap();
    // input items:
    // 1. developer message (system prompt)
    // 2. user message
    // 3. assistant message ("Calling tool")
    // 4. function_call
    // 5. function_call_output
    assert_eq!(input.len(), 5);
    assert_eq!(input[0]["role"], "developer");
    assert_eq!(input[0]["content"], "You are a helpful assistant.");
    assert_eq!(input[1]["role"], "user");
    assert_eq!(input[2]["role"], "assistant");
    assert_eq!(input[3]["type"], "function_call");
    assert_eq!(input[3]["call_id"], "call_123");
    assert_eq!(input[3]["name"], "get_weather");
    assert_eq!(input[4]["type"], "function_call_output");
    assert_eq!(input[4]["call_id"], "call_123");
}

#[tokio::test]
async fn custom_headers_are_sent_across_all_four_protocol_adapters() {
    use microcrab_auditor::{EmptyVariableView, NoopAuditor};
    use microcrab_transport::{Client, Content, ContentItem};
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    let mut headers = BTreeMap::new();
    headers.insert("User-Agent".into(), "pi".into());
    headers.insert("X-Client-Profile".into(), "test-header".into());

    for (protocol, body) in [
        (
            Protocol::OpenaiCompatible,
            r#"{"id":"1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#,
        ),
        (
            Protocol::OpenaiResponses,
            r#"{"id":"1","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}"#,
        ),
        (
            Protocol::Anthropic,
            r#"{"id":"1","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn"}"#,
        ),
        (
            Protocol::Google,
            r#"{"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"},"finishReason":"STOP"}]}"#,
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let body = body.to_owned();
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = [0u8; 8192];
            let mut raw = Vec::new();
            loop {
                let n = socket.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
                if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let head = String::from_utf8_lossy(&raw).to_ascii_lowercase();
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            head
        });
        let client = TransportClient::new(TransportConfig {
            auth: Default::default(),
            protocol,
            endpoint,
            model: "fixture".into(),
            api_key: Some("secret".into()),
            timeout_ms: 5000,
            max_response_bytes: 65536,
            headers: headers.clone(),
        })
        .unwrap();
        let res = client
            .complete(
                &Content {
                    epoch: 0,
                    subagent: None,
                    system: "sys".into(),
                    items: vec![ContentItem::User {
                        envelope: None,
                        parts: vec![InputPart::Text { text: "hi".into() }],
                        source: None,
                    }],
                    tools: vec![],
                    max_output_tokens: 64,
                    max_bytes: 16384,
                    turn_id: None,
                    round: None,
                },
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res.content, "ok");
        let head = worker.join().unwrap();
        assert!(head.contains("user-agent: pi\r\n"), "{protocol:?}: {head}");
        assert!(
            head.contains("x-client-profile: test-header\r\n"),
            "{protocol:?}: {head}"
        );
    }
}
