use super::*;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
    task::JoinHandle,
};

fn row(id: &str) -> ModelMetadata {
    serde_json::from_value(json!({ "id": id })).expect("valid model fixture")
}

fn snapshot(ids: &[&str]) -> MetadataSnapshot {
    MetadataSnapshot {
        version: 1,
        source: OPENROUTER_MODELS_ENDPOINT.into(),
        fetched_at_ms: 1,
        models: ids.iter().map(|id| row(id)).collect(),
        extra: BTreeMap::new(),
    }
}

#[test]
fn optional_public_catalog_fixture_validates_and_retains_every_field() {
    let Some(path) = std::env::var_os("MICROCRAB_PUBLIC_MODELS_FIXTURE") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    assert!(
        path.is_absolute(),
        "public fixture path must be resolved by the caller"
    );
    let bytes = std::fs::read(path).expect("read authorized public catalog fixture");
    let original: Value = serde_json::from_slice(&bytes).expect("public fixture JSON");
    // Exercise exactly the response type used by the production fetch path.
    let envelope: super::manager::Envelope =
        serde_json::from_slice(&bytes).expect("typed public catalog envelope");
    let snapshot = MetadataSnapshot {
        version: 1,
        source: OPENROUTER_MODELS_ENDPOINT.into(),
        fetched_at_ms: 1,
        models: envelope.data,
        extra: envelope.extra,
    };
    snapshot
        .validate()
        .expect("validated public catalog snapshot");
    let original_rows = original["data"].as_array().expect("catalog data array");
    assert!(!original_rows.is_empty());
    assert_eq!(snapshot.models.len(), original_rows.len());
    if let Some(total) = original.get("total_count") {
        assert_eq!(total.as_u64(), Some(snapshot.models.len() as u64));
    }
    assert!(
        original
            .get("links")
            .and_then(|links| links.get("next"))
            .is_none_or(Value::is_null)
    );
    let cache = serde_json::to_vec(&snapshot).expect("serialize full public snapshot");
    let restored: MetadataSnapshot =
        serde_json::from_slice(&cache).expect("restore full public snapshot");
    restored
        .validate()
        .expect("validate restored public snapshot");
    assert_eq!(snapshot, restored);
    for (input, model) in original_rows.iter().zip(&restored.models) {
        let output = serde_json::to_value(model).expect("serialize retained public model");
        assert_original_values_retained(input, &output);
        // String equality includes trailing zeros and exponent spelling.
        let prices = input["pricing"].as_object().expect("public pricing object");
        for (key, price) in prices {
            if price.is_string() {
                assert_eq!(price, &output["pricing"][key]);
            }
        }
        let (provider, model_id) = model.id.split_once('/').expect("qualified public ID");
        assert_eq!(model.provider, provider);
        assert_eq!(model.model_id, model_id);
    }
    for (key, value) in original
        .as_object()
        .expect("public envelope")
        .iter()
        .filter(|(key, _)| key.as_str() != "data")
    {
        assert_original_values_retained(
            value,
            restored.extra.get(key).expect("retained envelope extra"),
        );
    }
}

// Known optional fields may add nulls when absent. Every original value must
// nevertheless survive recursively, including new keys inside typed objects.
fn assert_original_values_retained(original: &Value, retained: &Value) {
    match original {
        Value::Object(fields) => {
            let retained = retained.as_object().expect("retained object");
            for (key, value) in fields {
                assert_original_values_retained(
                    value,
                    retained.get(key).expect("retained original key"),
                );
            }
        }
        Value::Array(values) => {
            let retained = retained.as_array().expect("retained array");
            assert_eq!(values.len(), retained.len());
            for (value, retained) in values.iter().zip(retained) {
                assert_original_values_retained(value, retained);
            }
        }
        _ => assert_eq!(original, retained),
    }
}

#[test]
fn exact_wins_aliases_fallback_and_ambiguity_is_unknown() {
    let catalog = snapshot(&[
        "google/gemini-4-argon",
        "other/gemini-4-argon",
        "openai/unique",
    ]);
    let exact = catalog
        .find("google", "gemini-4-argon")
        .expect("exact match");
    assert_eq!(exact.metadata.id, "google/gemini-4-argon");
    assert_eq!(exact.kind, ModelMatchKind::Exact);
    assert!(catalog.find("gateway", "gemini-4-argon").is_none());
    let alias = catalog
        .find("gateway", "unique")
        .expect("unambiguous fallback");
    assert_eq!(alias.metadata.id, "openai/unique");
    assert_eq!(alias.kind, ModelMatchKind::ModelIdFallback);
    let qualified = catalog
        .find("gateway", "google/gemini-4-argon")
        .expect("qualified gateway model");
    assert_eq!(qualified.metadata.id, "google/gemini-4-argon");
    assert!(catalog.find("gateway", "unknown/gemini-4-argon").is_none());
    assert_eq!(
        catalog.find("GOOGLE", "Gemini-4-argon").unwrap().kind,
        ModelMatchKind::Exact
    );
    assert!(catalog.find("", "unique").is_none());
    assert!(catalog.find("google", "").is_none());
}

#[test]
fn lowercase_metadata_matching_preserves_spelling_and_rejects_case_collisions() {
    let mut catalog = snapshot(&["DeepSeek/DeepSeek-V4-Flash-0731", "Other/model-lite"]);
    catalog.models[0].context_length = Some(1_048_576);
    let before = serde_json::to_vec(&catalog).unwrap();
    for (provider, model, kind) in [
        ("DEEPSEEK", "deepseek-v4-flash-0731", ModelMatchKind::Exact),
        (
            "a6api",
            "DeepSeek-V4-Flash-0731",
            ModelMatchKind::ModelIdFallback,
        ),
        (
            "a6api",
            "DEEPSEEK/DEEPSEEK-V4-FLASH-0731",
            ModelMatchKind::ModelIdFallback,
        ),
        (
            "a6api",
            "DEEPSEEK/DEEPSEEK-V4-FLASH-0731-HIGH",
            ModelMatchKind::ModelIdApproximate,
        ),
    ] {
        let found = catalog.find(provider, model).unwrap();
        assert_eq!(found.kind, kind);
        assert_eq!(found.metadata.id, "DeepSeek/DeepSeek-V4-Flash-0731");
        assert_eq!(found.metadata.context_length, Some(1_048_576));
    }
    assert_eq!(serde_json::to_vec(&catalog).unwrap(), before);
    for (ids, target) in [
        (vec!["Vendor/Model", "vendor/model"], "model"),
        (vec!["Vendor/Model", "Other/model"], "model"),
        (
            vec!["Vendor/family/model", "Other/VENDOR/family/MODEL"],
            "vendor/family/model",
        ),
    ] {
        let collision = snapshot(&ids);
        assert!(collision.find("gateway", target).is_none());
    }
    let duplicate = snapshot(&["Vendor/Model", "vendor/model"]);
    assert!(duplicate.find("vendor", "Model").is_none());
    assert!(duplicate.find("gateway", "VENDOR/MODEL").is_none());
    assert!(duplicate.find("gateway", "VENDOR/MODEL-HIGH").is_none());
    let constrained = snapshot(&["Vendor/Model", "Other/model"]);
    assert_eq!(
        constrained.find("VENDOR", "MODEL").unwrap().kind,
        ModelMatchKind::Exact
    );
    assert_eq!(
        constrained
            .find("gateway", "VENDOR/MODEL-HIGH")
            .unwrap()
            .metadata
            .id,
        "Vendor/Model"
    );
    let longest = snapshot(&["Vendor/Model", "Other/MODEL", "Third/Model-Lite"]);
    assert_eq!(
        longest
            .find("gateway", "MODEL-LITE-HIGH")
            .unwrap()
            .metadata
            .id,
        "Third/Model-Lite"
    );
}

#[test]
fn variant_metadata_uses_the_longest_complete_model_id_without_crossing_versions() {
    let catalog = snapshot(&[
        "google/gemini-3.7-flash",
        "google/gemini-3.8-flash",
        "google/gemini-3.8-flash-lite",
    ]);
    for (model, expected) in [
        ("gemini-3.8-flash-high", "google/gemini-3.8-flash"),
        ("gemini-3.8-flash_medium", "google/gemini-3.8-flash"),
        ("gemini-3.8-flash-lite-high", "google/gemini-3.8-flash-lite"),
        ("google/gemini-3.8-flash-high", "google/gemini-3.8-flash"),
    ] {
        let found = catalog.find("gateway", model).unwrap();
        assert_eq!(found.metadata.id, expected);
        assert_eq!(found.kind, ModelMatchKind::ModelIdApproximate);
        assert_eq!(
            serde_json::to_value(found.kind).unwrap(),
            "model_id_approximate"
        );
    }
    for model in [
        "gemini-3.9-flash-high",
        "gemini-3.8-flashhigh",
        "gemini-3.8-flash-",
        "unknown/gemini-3.8-flash-high",
    ] {
        assert!(catalog.find("gateway", model).is_none(), "{model}");
    }
    assert_eq!(
        catalog.find("gateway", "gemini-3.8-flash").unwrap().kind,
        ModelMatchKind::ModelIdFallback
    );
    let exact = snapshot(&["google/gemini-3.8-flash", "google/gemini-3.8-flash-high"]);
    assert_eq!(
        exact.find("google", "gemini-3.8-flash-high").unwrap().kind,
        ModelMatchKind::Exact
    );
    assert_eq!(
        exact
            .find("gateway", "gemini-3.8-flash-high")
            .unwrap()
            .metadata
            .id,
        "google/gemini-3.8-flash-high"
    );
}

#[test]
fn approximate_equal_best_is_unknown_unless_a_qualified_vendor_disambiguates() {
    let catalog = snapshot(&["google/gemini-3.8-flash", "other/gemini-3.8-flash"]);
    assert!(catalog.find("gateway", "gemini-3.8-flash-high").is_none());
    assert_eq!(
        catalog
            .find("gateway", "google/gemini-3.8-flash-high")
            .unwrap()
            .metadata
            .id,
        "google/gemini-3.8-flash"
    );
    assert_eq!(
        catalog.find("other", "gemini-3.8-flash").unwrap().kind,
        ModelMatchKind::Exact
    );
    // A qualified-ID/short-ID collision remains ambiguous before approximation.
    let collision = snapshot(&["provider/family/model", "other/provider/family/model"]);
    assert!(collision.find("gateway", "provider/family/model").is_none());
    // A longer candidate must clear ambiguity among shorter candidates.
    let specific = snapshot(&["google/model", "other/model", "third/model-lite"]);
    assert_eq!(
        specific
            .find("gateway", "model-lite-high")
            .unwrap()
            .metadata
            .id,
        "third/model-lite"
    );
}

#[test]
fn first_slash_preserves_nested_model_ids() {
    let catalog = snapshot(&["provider/family/model"]);
    assert_eq!(catalog.models[0].provider, "provider");
    assert_eq!(catalog.models[0].model_id, "family/model");
    assert_eq!(
        catalog
            .find("provider", "family/model")
            .expect("exact nested ID")
            .kind,
        ModelMatchKind::Exact
    );
    assert_eq!(
        catalog
            .find("gateway", "provider/family/model")
            .expect("qualified nested ID")
            .metadata
            .id,
        "provider/family/model"
    );
    assert_eq!(
        catalog
            .find("gateway", "family/model")
            .expect("nested model ID fallback")
            .metadata
            .id,
        "provider/family/model"
    );
    let collision = snapshot(&["provider/family/model", "other/provider/family/model"]);
    assert!(collision.find("gateway", "provider/family/model").is_none());
}

#[test]
fn missing_and_nullable_fields_do_not_invent_limits() {
    let missing = row("google/gemini-4-argon");
    assert!(missing.context_length.is_none());
    assert!(missing.top_provider.is_none());
    let nullable: ModelMetadata = serde_json::from_value(json!({
        "id": "google/gemini-4-argon", "context_length": null,
        "pricing": {"prompt": null},
        "top_provider": {"context_length": null, "max_completion_tokens": null, "is_moderated": false},
        "architecture": {"instruct_type": null, "tokenizer": null},
        "supported_parameters": null, "per_request_limits": null,
        "limits": null, "default_parameters": null, "expiration_date": null
    })).expect("nullable schema");
    assert!(nullable.context_length.is_none());
    let top = nullable.top_provider.expect("top provider");
    assert_eq!(top.context_length, None);
    assert_eq!(top.max_completion_tokens, None);
    assert_eq!(top.is_moderated, Some(false));
}

#[test]
fn pricing_precision_and_unknown_fields_survive_snapshot_roundtrip() {
    let input = json!({
        "id": "google/gemini-4-argon", "name": "Argon", "description": "Description",
        "created": 1700000000, "canonical_slug": "google/permanent", "context_length": 1000000,
        "pricing": {
            "prompt": "0.000000000000000000123456789012345678900",
            "completion": "1e-25", "input_cache_read": "0", "future_price": "000.00100",
            "overrides": [{"min_prompt_tokens": 200000, "prompt": "0.000005", "future_condition": {"mode": "x"}}],
            "future_pricing": {"array": [true, null, {"value": "retained"}]}
        },
        "top_provider": {"context_length": 999999, "max_completion_tokens": 16384, "is_moderated": true, "future_limit": [1,2]},
        "architecture": {"modality": "text+image->text", "input_modalities": ["text","image"], "output_modalities": ["text"], "tokenizer": "Other", "instruct_type": null, "future_arch": {"x": true}},
        "supported_parameters": ["tools", "reasoning"],
        "per_request_limits": {"requests": 10}, "limits": {"future": "limit"},
        "default_parameters": {"temperature": 0.7, "future_parameter": {"a": [1]}},
        "expiration_date": "2030-01-01", "future_metadata": {"raw": [1, "two", false, null]}
    });
    let model: ModelMetadata = serde_json::from_value(input.clone()).expect("full metadata");
    let mut catalog = snapshot(&["placeholder/model"]);
    catalog.models = vec![model];
    catalog
        .extra
        .insert("future_envelope".into(), json!({"nested": "retained"}));
    let encoded = serde_json::to_vec(&catalog).expect("serialize cache");
    let restored: MetadataSnapshot = serde_json::from_slice(&encoded).expect("restore cache");
    restored.validate().expect("valid restored snapshot");
    assert_eq!(restored, catalog);
    let output = serde_json::to_value(&restored.models[0]).expect("serialize model");
    assert_eq!(output["pricing"]["prompt"], input["pricing"]["prompt"]);
    assert_eq!(
        output["pricing"]["future_price"],
        input["pricing"]["future_price"]
    );
    assert_eq!(
        output["pricing"]["overrides"],
        input["pricing"]["overrides"]
    );
    assert_eq!(
        output["pricing"]["future_pricing"],
        input["pricing"]["future_pricing"]
    );
    assert_eq!(
        output["architecture"]["future_arch"],
        input["architecture"]["future_arch"]
    );
    assert_eq!(
        output["top_provider"]["future_limit"],
        input["top_provider"]["future_limit"]
    );
    assert_eq!(output["future_metadata"], input["future_metadata"]);
    assert_eq!(restored.models[0].provider, "google");
    assert_eq!(restored.models[0].model_id, "gemini-4-argon");
}

#[test]
fn invalid_empty_duplicate_and_inconsistent_snapshots_are_rejected() {
    for id in [
        "",
        "no-slash",
        "/model",
        "provider/",
        "provider//model",
        "provider/model/",
        "provider/model name",
        "provider/model\n",
    ] {
        assert!(
            serde_json::from_value::<ModelMetadata>(json!({"id":id})).is_err(),
            "{id:?}"
        );
    }
    assert!(snapshot(&[]).validate().is_err());
    assert!(snapshot(&["p/m", "p/m"]).validate().is_err());
    let mut inconsistent = snapshot(&["p/m"]);
    inconsistent.models[0].provider = "other".into();
    assert!(inconsistent.validate().is_err());
    let mut wrong_version = snapshot(&["p/m"]);
    wrong_version.version = 2;
    assert!(wrong_version.validate().is_err());
    let mut unstamped = snapshot(&["p/m"]);
    unstamped.fetched_at_ms = 0;
    assert!(unstamped.validate().is_err());
}

#[test]
fn config_rejects_auth_non_loopback_http_and_unbounded_values() {
    for endpoint in [
        "http://example.com/models",
        "ftp://127.0.0.1/models",
        "not a URL",
        "https://secret@example.com/models",
        "https://example.com/models#secret",
        "https://example.com/models?api_key=secret",
        "http://localhost:1234/models",
    ] {
        assert!(
            OpenRouterModelManager::with_endpoint(endpoint, Duration::from_secs(1), 1024).is_err()
        );
    }
    assert!(
        OpenRouterModelManager::with_endpoint("http://127.0.0.1/models", Duration::ZERO, 1024)
            .is_err()
    );
    assert!(
        OpenRouterModelManager::with_endpoint(
            "http://127.0.0.1/models",
            Duration::from_secs(121),
            1024
        )
        .is_err()
    );
    assert!(
        OpenRouterModelManager::with_endpoint("http://127.0.0.1/models", Duration::from_secs(1), 0)
            .is_err()
    );
    assert!(
        OpenRouterModelManager::with_endpoint(
            "http://127.0.0.1/models",
            Duration::from_secs(1),
            64 * 1024 * 1024 + 1
        )
        .is_err()
    );
    let restored: Arc<dyn ModelMetadataManager> = Arc::new(
        OpenRouterModelManager::with_cached_snapshot(snapshot(&["p/m"]))
            .expect("validated restore"),
    );
    assert_eq!(
        restored.find("p", "m").expect("restored lookup").kind,
        ModelMatchKind::Exact
    );
    let mut wrong_source = snapshot(&["p/m"]);
    wrong_source.source = "https://example.com/models".into();
    assert!(OpenRouterModelManager::with_cached_snapshot(wrong_source).is_err());
    assert_eq!(
        MetadataError::InvalidResponse.to_string(),
        "invalid metadata response"
    );
}

struct Reply {
    wire: String,
    delay: Duration,
}

impl Reply {
    fn json(body: Value) -> Self {
        let body = body.to_string();
        Self {
            wire: format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ),
            delay: Duration::ZERO,
        }
    }

    fn raw(wire: String) -> Self {
        Self {
            wire,
            delay: Duration::ZERO,
        }
    }
}

struct Loopback {
    endpoint: String,
    requests: mpsc::UnboundedReceiver<String>,
    task: JoinHandle<()>,
}

impl Drop for Loopback {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(replies: Vec<Reply>) -> Loopback {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let endpoint = format!(
        "http://{}/models",
        listener.local_addr().expect("local address")
    );
    let (tx, requests) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        for reply in replies {
            let (mut socket, _) = listener.accept().await.expect("accept loopback");
            let mut request = Vec::new();
            loop {
                let mut buffer = [0_u8; 1024];
                let count = socket.read(&mut buffer).await.expect("read request");
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
                assert!(request.len() < 8192, "bounded test request");
            }
            tx.send(String::from_utf8(request).expect("HTTP headers"))
                .expect("request receiver");
            if !reply.delay.is_zero() {
                tokio::time::sleep(reply.delay).await;
            }
            // Timeout/cancellation tests intentionally close the client first.
            let _ = socket.write_all(reply.wire.as_bytes()).await;
        }
    });
    Loopback {
        endpoint,
        requests,
        task,
    }
}

fn manager(server: &Loopback) -> OpenRouterModelManager {
    OpenRouterModelManager::with_endpoint(&server.endpoint, Duration::from_secs(2), 1024 * 1024)
        .expect("loopback manager")
}

#[tokio::test]
async fn refresh_gets_public_metadata_without_auth_and_preserves_envelope() {
    let mut server = serve(vec![Reply::json(json!({
        "data": [{"id": "google/gemini-4-argon", "context_length": 1000000}],
        "total_count": 1, "links": {"next": null}, "future_envelope": {"x": true}
    }))])
    .await;
    let manager: Arc<dyn ModelMetadataManager> = Arc::new(manager(&server));
    let old = manager.snapshot();
    assert!(old.models.is_empty());
    let fetched = manager.refresh().await.expect("refresh");
    assert_eq!(fetched.models[0].provider, "google");
    assert_eq!(fetched.models[0].model_id, "gemini-4-argon");
    assert!(fetched.fetched_at_ms > 0);
    assert_eq!(fetched.extra["future_envelope"], json!({"x":true}));
    assert!(Arc::ptr_eq(&fetched, &manager.snapshot()));
    assert!(
        old.models.is_empty(),
        "old immutable readers remain unchanged"
    );
    let request = server
        .requests
        .recv()
        .await
        .expect("captured request")
        .to_ascii_lowercase();
    assert!(request.starts_with("get /models http/1.1\r\n"));
    assert!(request.contains("accept: application/json\r\n"));
    assert!(!request.contains("authorization:"));
    assert!(!request.contains("cookie:"));
    assert!(!request.contains("api-key:"));
}

#[tokio::test]
async fn failed_refresh_keeps_last_good_snapshot() {
    let server = serve(vec![
        Reply::json(json!({"data": [{"id":"p/m"}]})),
        Reply::raw(
            "HTTP/1.1 500 Failed\r\nContent-Length: 6\r\nConnection: close\r\n\r\nsecret".into(),
        ),
        Reply::json(json!({"data": [{"id":"p/m"},{"id":"p/m"}]})),
        Reply::json(json!({"data": []})),
        Reply::json(json!({"data": [{"id":"invalid"}]})),
        Reply::raw("HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n{".into()),
        Reply::json(json!({"data": [{"id":"p/m"}], "links":{"next":"/next"}})),
        Reply::json(json!({"data": [{"id":"p/m"}], "total_count":2})),
    ])
    .await;
    let manager = manager(&server);
    let good = manager.refresh().await.expect("initial snapshot");
    assert_eq!(
        manager.refresh().await.err(),
        Some(MetadataError::HttpStatus)
    );
    assert!(Arc::ptr_eq(&good, &manager.snapshot()));
    for _ in 0..6 {
        assert_eq!(
            manager.refresh().await.err(),
            Some(MetadataError::InvalidResponse)
        );
        assert!(Arc::ptr_eq(&good, &manager.snapshot()));
    }
}

#[tokio::test]
async fn concurrent_refreshes_share_success_and_failure() {
    for success in [true, false] {
        let mut reply = if success {
            Reply::json(json!({"data":[{"id":"p/m"}]}))
        } else {
            Reply::raw("HTTP/1.1 503 Busy\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into())
        };
        reply.delay = Duration::from_millis(30);
        let mut server = serve(vec![reply]).await;
        let manager = manager(&server);
        let (a, b, c) = tokio::join!(manager.refresh(), manager.refresh(), manager.refresh());
        if success {
            let a = a.expect("first refresh");
            assert!(Arc::ptr_eq(&a, &b.expect("second refresh")));
            assert!(Arc::ptr_eq(&a, &c.expect("third refresh")));
        } else {
            assert_eq!(a.err(), Some(MetadataError::HttpStatus));
            assert_eq!(b.err(), Some(MetadataError::HttpStatus));
            assert_eq!(c.err(), Some(MetadataError::HttpStatus));
        }
        assert!(server.requests.recv().await.is_some());
        assert!(
            server.requests.try_recv().is_err(),
            "one fetch for overlapping refreshes"
        );
    }
}

#[tokio::test]
async fn body_caps_cover_content_length_and_chunked_streaming() {
    let over = "x".repeat(256);
    for reply in [
        Reply::raw(format!(
            "HTTP/1.1 200 OK\r\nContent-Length: 256\r\nConnection: close\r\n\r\n{over}"
        )),
        Reply::raw(format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n100\r\n{over}\r\n0\r\n\r\n"
        )),
    ] {
        let server = serve(vec![reply]).await;
        let manager =
            OpenRouterModelManager::with_endpoint(&server.endpoint, Duration::from_secs(2), 128)
                .expect("small cap");
        assert_eq!(
            manager.refresh().await.err(),
            Some(MetadataError::BodyTooLarge)
        );
        assert!(manager.snapshot().models.is_empty());
    }
}

#[tokio::test]
async fn request_timeout_is_bounded_and_does_not_publish() {
    let mut reply = Reply::json(json!({"data":[{"id":"p/m"}]}));
    reply.delay = Duration::from_secs(1);
    let server = serve(vec![reply]).await;
    let manager =
        OpenRouterModelManager::with_endpoint(&server.endpoint, Duration::from_millis(30), 1024)
            .expect("short timeout");
    let result = tokio::time::timeout(Duration::from_secs(2), manager.refresh())
        .await
        .expect("bounded refresh");
    assert_eq!(result.err(), Some(MetadataError::TimedOut));
    assert!(manager.snapshot().models.is_empty());
}

#[tokio::test]
async fn stalled_response_body_obeys_total_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let endpoint = format!(
        "http://{}/models",
        listener.local_addr().expect("local address")
    );
    let (headers_sent, received_headers) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept request");
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.expect("read request");
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{")
            .await
            .expect("send partial body");
        let _ = headers_sent.send(());
        std::future::pending::<()>().await;
    });
    let manager =
        OpenRouterModelManager::with_endpoint(&endpoint, Duration::from_millis(100), 1024)
            .expect("bounded manager");
    let result = manager.refresh().await;
    received_headers.await.expect("response body started");
    assert_eq!(result.err(), Some(MetadataError::TimedOut));
    assert!(manager.snapshot().models.is_empty());
    server.abort();
    assert!(
        server
            .await
            .expect_err("cancel stalled test server")
            .is_cancelled()
    );
}

#[tokio::test]
async fn cancelled_refresh_releases_gate_and_next_call_can_retry() {
    let mut first = Reply::json(json!({"data":[{"id":"p/cancelled"}]}));
    first.delay = Duration::from_millis(30);
    let mut server = serve(vec![
        first,
        Reply::json(json!({"data":[{"id":"p/retried"}]})),
    ])
    .await;
    let manager = Arc::new(manager(&server));
    let leader_manager = manager.clone();
    let leader = tokio::spawn(async move { leader_manager.refresh().await });
    server
        .requests
        .recv()
        .await
        .expect("leader request in progress");
    leader.abort();
    assert!(leader.await.expect_err("cancelled leader").is_cancelled());
    assert!(manager.snapshot().models.is_empty());
    let retried = manager.refresh().await.expect("retry after cancellation");
    assert_eq!(retried.models[0].id, "p/retried");
    assert!(Arc::ptr_eq(&retried, &manager.snapshot()));
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let mut destination = serve(vec![Reply::json(json!({"data":[{"id":"p/m"}]}))]).await;
    let server = serve(vec![Reply::raw(format!(
        "HTTP/1.1 302 Found\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        destination.endpoint
    ))])
    .await;
    let manager = manager(&server);
    assert_eq!(
        manager.refresh().await.err(),
        Some(MetadataError::HttpStatus)
    );
    assert!(destination.requests.try_recv().is_err());
    assert!(manager.snapshot().models.is_empty());
}
