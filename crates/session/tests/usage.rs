//! Usage fixtures exercise the public transport and aggregation contract.
use microcrab_auditor::ChannelAuditor;
use microcrab_protocol::Usage;
use microcrab_session::context::Context;
use microcrab_transport::Client;
use microcrab_transport::Error;
use microcrab_transport::{OpenAiCompatible, OpenAiCompatibleConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

async fn parse_usage(usage: Option<Value>) -> Result<Usage, Error> {
    let mut body =
        json!({"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}]});
    if let Some(usage) = usage {
        body["usage"] = usage;
    }
    let body = body.to_string();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 2048];
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buffer[..read]);
            if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|length| length.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if request.len() >= end + 4 + length {
                    break;
                }
            }
        }
        let wire = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(wire.as_bytes()).await.unwrap();
    });
    let client = OpenAiCompatible::new(OpenAiCompatibleConfig {
        endpoint,
        model: "usage-fixture".into(),
        api_key: None,
        timeout_ms: 2000,
        max_response_bytes: 16_384,
    })
    .unwrap();
    let mut context = Context::new("system".into(), vec![], 16_384).unwrap();
    context.admit_user("task", &json!({})).unwrap();
    let result = client
        .complete(
            &context.content(100).unwrap(),
            &ChannelAuditor::disconnected(),
            &microcrab_auditor::EmptyVariableView,
            CancellationToken::new(),
        )
        .await;
    server.await.unwrap();
    result.map(|response| response.usage)
}

#[tokio::test]
async fn absent_null_and_zero_are_distinct_and_raw_unknown_fields_survive() {
    for wire in [None, Some(Value::Null)] {
        let usage = parse_usage(wire).await.unwrap();
        assert_eq!(
            usage,
            Usage {
                samples: 1,
                ..Usage::default()
            }
        );
    }
    let raw = json!({"prompt_tokens":0,"completion_tokens":0,"vendor":{"future_counter":23}});
    let usage = parse_usage(Some(raw.clone())).await.unwrap();
    assert_eq!(usage.input_tokens, Some(0));
    assert_eq!(usage.output_tokens, Some(0));
    assert_eq!(usage.total_tokens, Some(0));
    assert_eq!(usage.cache_read_tokens, None);
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.uncached_input_tokens, None);
    assert_eq!(usage.reasoning_tokens, None);
    assert_eq!(usage.non_reasoning_output_tokens, None);
    assert_eq!(usage.provider_details, vec![raw]);
    assert_eq!(usage.samples, 1);
    let raw = json!({"prompt_tokens":null,"completion_tokens":0});
    let usage = parse_usage(Some(raw.clone())).await.unwrap();
    assert_eq!(usage.input_tokens, None);
    assert_eq!(usage.total_tokens, None);
    assert_eq!(usage.provider_details, vec![raw]);
}

#[tokio::test]
async fn exclusive_reasoning_requires_an_exact_reported_total_and_preserves_raw_usage() {
    let raw = json!({"prompt_tokens":2722,"completion_tokens":6,"total_tokens":3369,
        "prompt_tokens_details":{"cached_tokens":192},
        "completion_tokens_details":{"reasoning_tokens":641}});
    let usage = parse_usage(Some(raw.clone())).await.unwrap();
    assert_eq!(usage.input_tokens, Some(2722));
    assert_eq!(usage.output_tokens, Some(647));
    assert_eq!(usage.reasoning_tokens, Some(641));
    assert_eq!(usage.non_reasoning_output_tokens, Some(6));
    assert_eq!(usage.total_tokens, Some(3369));
    assert_eq!(usage.provider_details, vec![raw.clone()]);
    for total in [Value::Null, json!(3368)] {
        let mut invalid = raw.clone();
        invalid["total_tokens"] = total;
        assert!(parse_usage(Some(invalid)).await.is_err());
    }
}

#[tokio::test]
async fn inclusive_cache_reasoning_and_explicit_total_keep_provider_semantics() {
    let raw = json!({
        "prompt_tokens":100,"completion_tokens":30,"total_tokens":137,
        "prompt_tokens_details":{"cached_tokens":40,"cache_write_tokens":10,"audio_tokens":2},
        "completion_tokens_details":{"reasoning_tokens":20,"accepted_prediction_tokens":3}
    });
    let usage = parse_usage(Some(raw.clone())).await.unwrap();
    assert_eq!(usage.input_tokens, Some(100));
    assert_eq!(usage.output_tokens, Some(30));
    assert_eq!(usage.cache_read_tokens, Some(40));
    assert_eq!(usage.cache_write_tokens, Some(10));
    assert_eq!(usage.uncached_input_tokens, Some(50));
    assert_eq!(usage.reasoning_tokens, Some(20));
    assert_eq!(usage.non_reasoning_output_tokens, Some(10));
    assert_eq!(usage.total_tokens, Some(137));
    assert_eq!(usage.provider_details, vec![raw]);
    let usage = parse_usage(Some(json!({
        "prompt_tokens":100,"completion_tokens":30,
        "prompt_tokens_details":{"cached_tokens":40},
        "completion_tokens_details":{"reasoning_tokens":0}
    })))
    .await
    .unwrap();
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.uncached_input_tokens, None);
    assert_eq!(usage.non_reasoning_output_tokens, Some(30));
    assert_eq!(usage.total_tokens, Some(130));
}

#[tokio::test]
async fn aliases_and_explicit_exclusive_cache_schema_are_normalized() {
    let usage = parse_usage(Some(json!({
        "input_tokens":100,"output_tokens":20,
        "input_tokens_details":{"cached_tokens":30,"cache_creation_tokens":10},
        "output_tokens_details":{"reasoning_tokens":5}
    })))
    .await
    .unwrap();
    assert_eq!(usage.input_tokens, Some(100));
    assert_eq!(usage.uncached_input_tokens, Some(60));
    assert_eq!(usage.non_reasoning_output_tokens, Some(15));
    let usage = parse_usage(Some(json!({
        "prompt_tokens":100,"completion_tokens":20,
        "prompt_cache_hit_tokens":30,"prompt_cache_miss_tokens":70
    })))
    .await
    .unwrap();
    assert_eq!(usage.cache_read_tokens, Some(30));
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.uncached_input_tokens, Some(70));
    let usage = parse_usage(Some(json!({
        "input_tokens":50,"output_tokens":20,
        "cache_read_input_tokens":30,"cache_creation_input_tokens":10,
        "reasoning_tokens":5
    })))
    .await
    .unwrap();
    assert_eq!(usage.input_tokens, Some(90));
    assert_eq!(usage.uncached_input_tokens, Some(50));
    assert_eq!(usage.cache_read_tokens, Some(30));
    assert_eq!(usage.cache_write_tokens, Some(10));
    assert_eq!(usage.total_tokens, Some(110));
    let usage = parse_usage(Some(json!({
        "prompt_tokens":90,"input_tokens":50,"output_tokens":20,
        "cache_read_input_tokens":30,"cache_creation_input_tokens":10
    })))
    .await
    .unwrap();
    assert_eq!(usage.input_tokens, Some(90));
    assert_eq!(usage.uncached_input_tokens, Some(50));
    let usage = parse_usage(Some(json!({
        "input_tokens":50,"output_tokens":20,"cache_read_input_tokens":30
    })))
    .await
    .unwrap();
    assert_eq!(usage.input_tokens, None);
    assert_eq!(usage.uncached_input_tokens, Some(50));
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.total_tokens, None);
    let usage = parse_usage(Some(json!({
        "prompt_tokens":100,"input_tokens":100,"completion_tokens":20,"output_tokens":20,
        "cache_read_tokens":0,"cache_write_tokens":0,"reasoning_tokens":0
    })))
    .await
    .unwrap();
    assert_eq!(usage.uncached_input_tokens, Some(100));
    assert_eq!(usage.non_reasoning_output_tokens, Some(20));
}

#[tokio::test]
async fn explicit_openai_semantics_ignore_foreign_counts_and_preserve_gateway_details() {
    for (input, output) in [(96, 0), (341, 96), (13309, 96), (12274, 341)] {
        for foreign in [
            json!(0),
            json!(999),
            json!(-1),
            json!(1.5),
            json!("invalid"),
        ] {
            let raw = json!({
                "usage_semantic":"openai","usage_source":"anthropic",
                "prompt_tokens":input,"completion_tokens":output,
                "prompt_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},
                "completion_tokens_details":{"reasoning_tokens":0},
                "input_tokens":foreign,"output_tokens":foreign,
                "input_tokens_details":{"cached_tokens":foreign,"cache_creation_tokens":foreign},
                "output_tokens_details":{"reasoning_tokens":foreign},
                "cache_read_input_tokens":foreign,"cache_creation_input_tokens":foreign,
                "billing_usage":{"usage_source":"anthropic","input_tokens":96,"output_tokens":0}
            });
            let usage = parse_usage(Some(raw.clone())).await.unwrap();
            assert_eq!(usage.input_tokens, Some(input));
            assert_eq!(usage.output_tokens, Some(output));
            assert_eq!(usage.total_tokens, Some(input + output));
            assert_eq!(usage.cache_read_tokens, Some(0));
            assert_eq!(usage.cache_write_tokens, Some(0));
            assert_eq!(usage.uncached_input_tokens, Some(input));
            assert_eq!(usage.reasoning_tokens, Some(0));
            assert_eq!(usage.non_reasoning_output_tokens, Some(output));
            assert_eq!(usage.provider_details, vec![raw]);
        }
    }
}

#[tokio::test]
async fn explicit_openai_zero_and_missing_dimensions_never_fall_back_to_foreign_aliases() {
    for raw in [
        json!({"usage_semantic":"openai","prompt_tokens":0,"completion_tokens":0,
            "input_tokens":341,"output_tokens":96}),
        json!({"usage_semantic":"openai","prompt_tokens":0,"output_tokens":96}),
        json!({"usage_semantic":"openai","completion_tokens":0,"input_tokens":341}),
        json!({"usage_semantic":"openai","input_tokens":341,"output_tokens":96,
            "input_tokens_details":"invalid","output_tokens_details":"invalid"}),
    ] {
        let usage = parse_usage(Some(raw.clone())).await.unwrap();
        let input = raw.get("prompt_tokens").map(|_| 0);
        let output = raw.get("completion_tokens").map(|_| 0);
        assert_eq!(usage.input_tokens, input);
        assert_eq!(usage.output_tokens, output);
        assert_eq!(
            usage.total_tokens,
            input.zip(output).map(|(input, output)| input + output)
        );
        assert_eq!(usage.provider_details, vec![raw]);
    }
}

#[tokio::test]
async fn canonical_openai_counts_win_without_a_semantic_marker_or_from_source_annotations() {
    for annotation in [
        json!({}),
        json!({"usage_source":"anthropic"}),
        json!({"billing_usage":{"usage_semantic":"anthropic","input_tokens":50}}),
    ] {
        for raw in [
            json!({"prompt_tokens":2,"input_tokens":3}),
            json!({"completion_tokens":2,"output_tokens":3}),
            json!({"prompt_tokens":341,"completion_tokens":96,"input_tokens":0,"output_tokens":0,
                "cache_read_input_tokens":0,"cache_creation_input_tokens":0}),
        ] {
            let mut raw = raw;
            raw.as_object_mut()
                .unwrap()
                .extend(annotation.as_object().unwrap().clone());
            let usage = parse_usage(Some(raw.clone())).await.unwrap();
            assert_eq!(
                usage.input_tokens,
                raw.get("prompt_tokens").and_then(Value::as_u64)
            );
            assert_eq!(
                usage.output_tokens,
                raw.get("completion_tokens").and_then(Value::as_u64)
            );
            assert_eq!(
                usage.total_tokens,
                usage
                    .input_tokens
                    .zip(usage.output_tokens)
                    .map(|(input, output)| input + output)
            );
            assert_eq!(usage.provider_details, vec![raw]);
        }
    }
}

#[tokio::test]
async fn empty_foreign_detail_wrappers_do_not_override_unmarked_base_usage() {
    for field in ["prompt_tokens_details", "completion_tokens_details"] {
        let mut raw = json!({"input_tokens":100,"output_tokens":20});
        raw[field] = json!({});
        let usage = parse_usage(Some(raw.clone())).await.unwrap();
        assert_eq!(
            (usage.input_tokens, usage.output_tokens, usage.total_tokens),
            (Some(100), Some(20), Some(120))
        );
        assert_eq!(usage.provider_details, vec![raw.clone()]);
        raw["input_tokens"] = json!(-1);
        assert!(matches!(
            parse_usage(Some(raw)).await,
            Err(Error::Provider(_))
        ));
        let mut raw = json!({"input_tokens":50,"output_tokens":20,
            "cache_read_input_tokens":30,"cache_creation_input_tokens":10});
        raw[field] = json!({});
        let usage = parse_usage(Some(raw)).await.unwrap();
        assert_eq!(
            (usage.input_tokens, usage.output_tokens, usage.total_tokens),
            (Some(90), Some(20), Some(110))
        );
    }
}

#[tokio::test]
async fn explicit_anthropic_semantics_ignore_openai_counts_and_detail_wrappers() {
    for foreign in [
        json!(0),
        json!(999),
        json!(-1),
        json!(1.5),
        json!("invalid"),
    ] {
        let raw = json!({
            "usage_semantic":"anthropic","usage_source":"openai",
            "input_tokens":50,"output_tokens":20,
            "cache_read_input_tokens":30,"cache_creation_input_tokens":10,
            "prompt_tokens":foreign,"completion_tokens":foreign,
            "prompt_tokens_details":foreign,"completion_tokens_details":foreign
        });
        let usage = parse_usage(Some(raw.clone())).await.unwrap();
        assert_eq!(usage.input_tokens, Some(90));
        assert_eq!(usage.output_tokens, Some(20));
        assert_eq!(usage.cache_read_tokens, Some(30));
        assert_eq!(usage.cache_write_tokens, Some(10));
        assert_eq!(usage.uncached_input_tokens, Some(50));
        assert_eq!(usage.total_tokens, Some(110));
        assert_eq!(usage.provider_details, vec![raw]);
    }
}

#[tokio::test]
async fn explicit_semantics_still_reject_malformed_selected_counts_and_conflicting_cache_aliases() {
    for bad in [json!(-1), json!(1.5), json!("2")] {
        for raw in [
            json!({"usage_semantic":"openai","prompt_tokens":bad,"input_tokens":96}),
            json!({"usage_semantic":"openai","completion_tokens":bad,"output_tokens":96}),
            json!({"usage_semantic":"openai","prompt_tokens":96,
                "prompt_tokens_details":{"cached_tokens":bad},"input_tokens_details":{"cached_tokens":0}}),
            json!({"usage_semantic":"openai","completion_tokens":96,
                "completion_tokens_details":{"reasoning_tokens":bad},"output_tokens_details":{"reasoning_tokens":0}}),
            json!({"usage_semantic":"anthropic","input_tokens":bad,"prompt_tokens":96}),
            json!({"usage_semantic":"anthropic","output_tokens":bad,"completion_tokens":96}),
            json!({"usage_semantic":"anthropic","input_tokens":50,"cache_read_input_tokens":bad}),
            json!({"usage_semantic":"anthropic","input_tokens":50,"cache_creation_input_tokens":bad}),
        ] {
            assert!(matches!(
                parse_usage(Some(raw)).await,
                Err(Error::Provider(_))
            ));
        }
    }
    let raw = json!({"usage_semantic":"openai","prompt_tokens":96,
        "prompt_tokens_details":{"cache_write_tokens":2,"cache_creation_tokens":3}});
    assert!(matches!(
        parse_usage(Some(raw)).await,
        Err(Error::Provider(_))
    ));
}

#[tokio::test]
async fn malformed_selected_counts_cache_conflicts_underflow_and_overflow_are_errors() {
    let oversized: Value = serde_json::from_str("18446744073709551616").unwrap();
    for bad in [json!(-1), json!(1.5), json!("2"), json!(true), oversized] {
        for path in [
            vec!["prompt_tokens"],
            vec!["completion_tokens"],
            vec!["total_tokens"],
            vec!["input_tokens"],
            vec!["output_tokens"],
            vec!["cache_read_input_tokens"],
            vec!["cache_creation_input_tokens"],
            vec!["reasoning_tokens"],
            vec!["prompt_tokens_details", "cached_tokens"],
            vec!["completion_tokens_details", "reasoning_tokens"],
        ] {
            let raw = if path.len() == 1 {
                let mut raw = json!({});
                raw[path[0]] = bad.clone();
                raw
            } else {
                let mut raw = json!({});
                raw[path[0]] = json!({});
                raw[path[0]][path[1]] = bad.clone();
                raw
            };
            assert!(
                matches!(parse_usage(Some(raw)).await, Err(Error::Provider(_))),
                "{path:?}"
            );
        }
    }
    for raw in [
        json!([]),
        json!(17),
        json!({"prompt_tokens_details":{"cache_write_tokens":2,"cache_creation_tokens":3}}),
        json!({"prompt_tokens_details":"invalid"}),
        json!({"prompt_tokens":2,"prompt_tokens_details":{"cached_tokens":3}}),
        json!({"completion_tokens":2,"reasoning_tokens":3}),
        json!({"prompt_tokens":u64::MAX,"completion_tokens":1}),
        json!({"input_tokens":u64::MAX,"cache_read_input_tokens":1,"cache_creation_input_tokens":0}),
    ] {
        assert!(matches!(
            parse_usage(Some(raw)).await,
            Err(Error::Provider(_))
        ));
    }
}

fn known_usage(value: u64) -> Usage {
    Usage {
        input_tokens: Some(value),
        output_tokens: Some(value),
        cache_read_tokens: Some(value),
        cache_write_tokens: Some(value),
        reasoning_tokens: Some(value),
        uncached_input_tokens: Some(value),
        non_reasoning_output_tokens: Some(value),
        total_tokens: Some(value),
        provider_details: vec![json!({"reported":value})],
        samples: 1,
    }
}

#[test]
fn aggregation_identity_known_zero_unknown_attempts_and_raw_order() {
    let mut total = Usage::default();
    total.accumulate(&Usage::default());
    assert_eq!(total, Usage::default());
    let known = known_usage(3);
    total.accumulate(&known);
    assert_eq!(total, known);
    total.accumulate(&known_usage(0));
    assert_eq!(total.input_tokens, Some(3));
    total.accumulate(&known_usage(2));
    assert_eq!(total.input_tokens, Some(5));
    assert_eq!(total.output_tokens, Some(5));
    assert_eq!(total.cache_read_tokens, Some(5));
    assert_eq!(total.cache_write_tokens, Some(5));
    assert_eq!(total.reasoning_tokens, Some(5));
    assert_eq!(total.uncached_input_tokens, Some(5));
    assert_eq!(total.non_reasoning_output_tokens, Some(5));
    assert_eq!(total.total_tokens, Some(5));
    assert_eq!(total.samples, 3);
    assert_eq!(
        total.provider_details,
        vec![
            json!({"reported":3}),
            json!({"reported":0}),
            json!({"reported":2})
        ]
    );
    let unknown = Usage {
        samples: 1,
        ..Usage::default()
    };
    total.accumulate(&unknown);
    let mut expected = unknown.clone();
    expected.samples = 4;
    expected.provider_details = total.provider_details.clone();
    assert_eq!(total, expected);
    // An unavailable first attempt must also poison later reported counts.
    let mut total = unknown;
    total.accumulate(&known);
    assert_eq!(total.input_tokens, None);
    assert_eq!(total.output_tokens, None);
    assert_eq!(total.samples, 2);
    let before = total.clone();
    total.accumulate(&Usage::default());
    assert_eq!(total, before);
}

#[test]
fn aggregate_preserves_independently_known_dimensions_and_handles_overflow() {
    let mut total = known_usage(2);
    let partial = Usage {
        output_tokens: Some(4),
        total_tokens: Some(8),
        samples: 1,
        ..Usage::default()
    };
    total.accumulate(&partial);
    assert_eq!(total.input_tokens, None);
    assert_eq!(total.output_tokens, Some(6));
    assert_eq!(total.total_tokens, Some(10));
    assert_eq!(total.cache_write_tokens, None);
    let mut total = known_usage(u64::MAX);
    total.accumulate(&known_usage(1));
    assert_eq!(total.input_tokens, None);
    assert_eq!(total.output_tokens, None);
    assert_eq!(total.total_tokens, None);
    assert_eq!(total.samples, 2);
    let serialized = serde_json::to_value(&total).unwrap();
    assert_eq!(serialized["input_tokens"], Value::Null);
    assert_eq!(serde_json::from_value::<Usage>(serialized).unwrap(), total);
}
