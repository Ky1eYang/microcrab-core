use super::*;
use crate::{
    Anthropic, AnthropicConfig, Google, GoogleConfig, OpenAiCompatible, OpenAiCompatibleConfig,
    OpenAiResponse, OpenAiResponseConfig,
};
use microcrab_protocol::{Message, Request, ToolCall, ToolDefinition};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};

fn image() -> InputPart {
    InputPart::Image {
        source: ImageSource::Data {
            media_type: "image/png".into(),
            data: vec![1, 2, 3],
        },
    }
}
fn request() -> Request {
    Request {
        epoch: 1,
        messages: vec![
            Message::System {
                content: "frozen".into(),
            },
            Message::Assistant {
                content: String::new(),
                tool_calls: ["a", "b"]
                    .into_iter()
                    .map(|id| ToolCall {
                        id: id.into(),
                        name: "read".into(),
                        arguments: json!({}),
                        provider_metadata: None,
                    })
                    .collect(),
                provider_metadata: None,
            },
            Message::Tool {
                call_id: "a".into(),
                content: "first".into(),
                parts: vec![
                    InputPart::Text {
                        text: "caption".into(),
                    },
                    image(),
                ],
            },
            Message::Tool {
                call_id: "b".into(),
                content: "second".into(),
                parts: vec![image()],
            },
            Message::User {
                parts: vec![InputPart::Text {
                    text: "continue".into(),
                }],
            },
        ],
        tools: vec![ToolDefinition {
            name: "read".into(),
            description: "read".into(),
            parameters: json!({"type":"object"}),
        }],
        max_output_tokens: 32,
    }
}
fn bodies(request: &Request) -> Vec<Value> {
    let anthropic = Anthropic::new(AnthropicConfig {
        endpoint: "https://example.invalid/v1".into(),
        model: "fixture".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 4096,
    })
    .unwrap();
    let responses = OpenAiResponse::new(OpenAiResponseConfig {
        endpoint: "https://example.invalid/v1".into(),
        model: "fixture".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 4096,
    })
    .unwrap();
    let compatible = OpenAiCompatible::new(OpenAiCompatibleConfig {
        endpoint: "https://example.invalid/chat/completions".into(),
        model: "fixture".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 4096,
    })
    .unwrap();
    let google = google();
    vec![
        anthropic.adapt(request).unwrap(),
        responses.adapt(request).unwrap(),
        compatible.adapt(request).unwrap(),
        google
            .with_function_response_media(true)
            .adapt(request)
            .unwrap(),
    ]
}
fn google() -> Google {
    Google::new(GoogleConfig {
        endpoint: "https://example.invalid/v1beta".into(),
        model: "fixture".into(),
        api_key: None,
        timeout_ms: 1000,
        max_response_bytes: 4096,
    })
    .unwrap()
}
#[test]
fn native_tool_image_shapes_and_complete_batch_order_are_preserved() {
    let bodies = bodies(&request());
    let a = &bodies[0]["messages"][1]["content"];
    assert_eq!(a[0]["tool_use_id"], "a");
    assert_eq!(a[1]["tool_use_id"], "b");
    assert_eq!(a[0]["content"][0], json!({"type":"text","text":"first"}));
    assert_eq!(a[0]["content"][1], json!({"type":"text","text":"caption"}));
    assert_eq!(
        a[0]["content"][2]["source"],
        json!({"type":"base64","media_type":"image/png","data":"AQID"})
    );
    let r = &bodies[1]["input"];
    assert_eq!(r[2]["call_id"], "a");
    assert_eq!(
        r[2]["output"][2],
        json!({"type":"input_image","image_url":"data:image/png;base64,AQID"})
    );
    let c = bodies[2]["messages"].as_array().unwrap();
    assert_eq!(
        c.iter()
            .map(|m| m["role"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "system",
            "assistant",
            "tool",
            "tool",
            "user",
            "user",
            "user"
        ]
    );
    assert_eq!(c[2]["content"], "first\ncaption");
    assert_eq!(c[4]["content"][0]["text"], "[Image output for tool call a]");
    assert_eq!(c[5]["content"][0]["text"], "[Image output for tool call b]");
    let g = &bodies[3]["contents"][1]["parts"];
    assert_eq!(
        g[0]["functionResponse"]["parts"][0],
        json!({"inlineData":{"mimeType":"image/png","data":"AQID"}})
    );
    assert_eq!(g[1]["functionResponse"]["name"], "read");
    for body in bodies {
        let authored = body.to_string();
        crate::validate_request_rewrite(&authored, &authored, 65536).unwrap();
        let mut changed = body.clone();
        if changed.get("system").is_some() {
            changed["system"] = json!("changed");
        } else if changed.get("instructions").is_some() {
            changed["instructions"] = json!("changed");
        } else if changed.get("systemInstruction").is_some() {
            changed["systemInstruction"]["parts"][0]["text"] = json!("changed");
        } else {
            changed["messages"][0]["content"] = json!("changed");
        }
        assert!(crate::validate_request_rewrite(&authored, &changed.to_string(), 65536).is_err());
    }
}
#[test]
fn empty_tool_parts_retain_legacy_wire_shapes() {
    let mut request = request();
    for message in &mut request.messages {
        if let Message::Tool { parts, .. } = message {
            parts.clear();
        }
    }
    let bodies = bodies(&request);
    assert_eq!(
        bodies[0]["messages"][1]["content"][0],
        json!({"type":"tool_result","tool_use_id":"a","content":"first"})
    );
    assert_eq!(
        bodies[1]["input"][2],
        json!({"type":"function_call_output","call_id":"a","output":"first"})
    );
    assert_eq!(bodies[2]["messages"].as_array().unwrap().len(), 5);
    assert!(
        bodies[3]["contents"][1]["parts"][0]["functionResponse"]
            .get("parts")
            .is_none()
    );
}
#[test]
fn google_capability_default_and_unwitnessed_url_fail_explicitly() {
    let mut request = request();
    assert!(
        google()
            .adapt(&request)
            .unwrap_err()
            .to_string()
            .contains("route/model")
    );
    if let Message::Tool { parts, .. } = &mut request.messages[2] {
        *parts = vec![InputPart::Image {
            source: ImageSource::Url {
                url: "https://example.invalid/image.png?private".into(),
            },
        }];
    }
    let error = google()
        .with_function_response_media(true)
        .adapt(&request)
        .unwrap_err();
    assert!(error.to_string().contains("MIME witness"));
    assert!(!error.to_string().contains("private"));
}
#[test]
fn typed_fetch_evidence_is_exact_and_sanitized() {
    for (status, code, expected) in [
        (400, "failed_to_download_image", true),
        (422, "failed_to_download_image", true),
        (403, "failed_to_download_image", false),
        (400, "invalid_image_url", false),
        (400, "invalid_request_error", false),
        (500, "failed_to_download_image", false),
    ] {
        let body = json!({"error":{"code":code,"message":"https://private.invalid/?token=secret"}})
            .to_string();
        let error = remote_image_fetch_rejection(
            reqwest::StatusCode::from_u16(status).unwrap(),
            body.as_bytes(),
        );
        assert_eq!(error.is_some(), expected);
        if let Some(error) = error {
            assert!(!error.diagnostic_reason().contains("secret"));
        }
    }
}
struct Resolver {
    calls: AtomicUsize,
    cancel: bool,
}
#[async_trait::async_trait]
impl ImageUrlResolver for Resolver {
    async fn upload(
        &self,
        _: &str,
        _: &[u8],
        _: CancellationToken,
    ) -> Result<Option<String>, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.cancel {
            Err(Error::Cancelled)
        } else {
            Ok(Some("https://example.invalid/image?private=token".into()))
        }
    }
}
fn content() -> Content {
    Content {
        epoch: 1,
        subagent: None,
        system: "frozen".into(),
        items: vec![
            ContentItem::User {
                parts: vec![image()],
                source: None,
                envelope: None,
            },
            ContentItem::Tool {
                call_id: "a".into(),
                content: "result".into(),
                parts: vec![image(), image()],
            },
        ],
        tools: vec![],
        max_output_tokens: 32,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(1),
    }
}
#[tokio::test]
async fn projection_defaults_and_cache_preserve_canonical_data_and_user_images() {
    let resolver = Arc::new(Resolver {
        calls: AtomicUsize::new(0),
        cancel: false,
    });
    let disabled = MediaProjection::new(resolver.clone(), false);
    let canonical = content();
    assert!(
        disabled
            .project_tool_images(&canonical, CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    let projection = MediaProjection::new(resolver.clone(), true);
    for _ in 0..2 {
        let projected = projection
            .project_tool_images(&canonical, CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projected.items[0], canonical.items[0]);
        assert_eq!(projected.system, canonical.system);
        assert!(
            matches!(&projected.items[1], ContentItem::Tool { parts, .. } if matches!(&parts[0], InputPart::Image { source: ImageSource::Url { .. } }))
        );
    }
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert!(
        matches!(&canonical.items[1], ContentItem::Tool { parts, .. } if matches!(&parts[0], InputPart::Image { source: ImageSource::Data { .. } }))
    );
}
#[tokio::test]
async fn callback_cancellation_is_never_swallowed_or_cached() {
    let resolver = Arc::new(Resolver {
        calls: AtomicUsize::new(0),
        cancel: true,
    });
    let projection = MediaProjection::new(resolver.clone(), true);
    for _ in 0..2 {
        assert!(matches!(
            projection
                .project_tool_images(&content(), CancellationToken::new())
                .await,
            Err(Error::Cancelled)
        ));
    }
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn hint_admission_requires_bounded_https_without_inline_credentials() {
    for url in [
        "http://example.invalid/a",
        "https://user:secret@example.invalid/a",
        "https://example.invalid/a#private",
        "data:image/png;base64,AQID",
        "not a URL",
    ] {
        assert!(!valid_hint(url));
    }
    assert!(!valid_hint(&format!(
        "https://example.invalid/{}",
        "x".repeat(8192)
    )));
    assert!(valid_hint("https://example.invalid/a?signature=private"));
}
#[tokio::test]
async fn full_hint_cache_uses_data_without_evicting_stable_prefix() {
    let resolver = Arc::new(Resolver {
        calls: AtomicUsize::new(0),
        cancel: false,
    });
    let projection = MediaProjection::new(resolver.clone(), true);
    for n in 0u16..129 {
        let mut canonical = content();
        if let ContentItem::Tool { parts, .. } = &mut canonical.items[1] {
            *parts = vec![InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/png".into(),
                    data: n.to_be_bytes().to_vec(),
                },
            }];
        }
        let projected = projection
            .project_tool_images(&canonical, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(projected.is_some(), n < 128);
    }
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 128);
    assert_eq!(projection.cache.lock().await.len(), 128);
}
#[test]
fn request_rewrite_rejects_malformed_tool_media_unions_and_retains_tools() {
    for body in bodies(&request()) {
        let authored = body.to_string();
        let mut changed = body.clone();
        changed["tools"] = json!([]);
        assert!(crate::validate_request_rewrite(&authored, &changed.to_string(), 65536).is_err());
        let mut changed = body.clone();
        if changed.get("input").is_some() {
            changed["input"][2]["output"][2] = json!({"type":"input_image"});
        } else if changed.get("contents").is_some() {
            changed["contents"][1]["parts"][0]["functionResponse"]["parts"][0] =
                json!({"text":"wrong union"});
        } else if changed.get("system").is_some() {
            changed["messages"][1]["content"][0]["content"][2] =
                json!({"type":"image","source":{"type":"base64"}});
        } else {
            changed["messages"][4]["content"][1] = json!({"type":"image_url"});
        }
        assert!(crate::validate_request_rewrite(&authored, &changed.to_string(), 65536).is_err());
    }
}
