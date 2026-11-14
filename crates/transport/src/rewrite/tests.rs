use super::*;
use serde_json::json;

fn chat() -> Value {
    json!({"model":"m","messages":[{"role":"system","content":"frozen\n  exact"},{"role":"user","content":"hello"}],"tools":[],"stream":true,"max_tokens":128})
}

fn responses() -> Value {
    json!({"model":"m","instructions":"frozen\n  exact","input":[
        {"role":"user","content":[{"type":"input_text","text":"hello"},{"type":"input_image","image_url":"data:image/png;base64,aA=="}]},
        {"type":"reasoning","summary":[{"type":"summary_text","text":"consider"}],"encrypted_content":"opaque"},
        {"type":"function_call","call_id":"c","name":"echo","arguments":"{}"},
        {"type":"function_call_output","call_id":"c","output":"done"},
        {"role":"assistant","content":[{"type":"output_text","text":"answer"}]}
    ],"tools":[{"type":"function","name":"echo","parameters":{"type":"object"},"strict":false}],"stream":true,"max_output_tokens":128})
}

fn anthropic() -> Value {
    json!({"model":"m","system":[{"type":"text","text":"frozen\n  exact","cache_control":{"type":"ephemeral"}}],"messages":[
        {"role":"user","content":[{"type":"text","text":"hello"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aA=="}}]},
        {"role":"assistant","content":[{"type":"thinking","thinking":"consider","signature":"signed"},{"type":"redacted_thinking","data":"opaque"},{"type":"tool_use","id":"c","name":"echo","input":{}}]},
        {"role":"user","content":[{"type":"tool_result","tool_use_id":"c","content":"done"}]}
    ],"tools":[{"name":"echo","input_schema":{"type":"object"}}],"stream":true,"max_tokens":128})
}

fn validate(old: &Value, new: &Value) -> Result<(), Error> {
    validate_request_rewrite(&old.to_string(), &new.to_string(), 65_536)
}

#[test]
fn unchanged_native_bodies_and_reasoning_are_accepted() {
    for body in [chat(), responses(), anthropic()] {
        validate(&body, &body).unwrap();
    }
    let mut body = responses();
    body["input"][1] = json!({"type":"reasoning","summary":[],"encrypted_content":"opaque"});
    validate(&body, &body).unwrap();
    body["input"][1] = json!({"type":"reasoning","summary":[],"id":"native-reasoning"});
    validate(&body, &body).unwrap();
    body["input"] = json!("native string input");
    validate(&body, &body).unwrap();
    let mut body = anthropic();
    body["messages"][2]["content"][0]["content"] = json!([
        {"type":"text","text":"tool image"},
        {"type":"image","source":{"type":"url","url":"https://example.test/image"}}
    ]);
    validate(&body, &body).unwrap();
    let mut body = chat();
    body.as_object_mut().unwrap().remove("max_tokens");
    body["max_completion_tokens"] = json!(128);
    validate(&body, &body).unwrap();
}

#[test]
fn protocol_selection_does_not_require_system_or_tools() {
    let mut native = anthropic();
    native.as_object_mut().unwrap().remove("system");
    native.as_object_mut().unwrap().remove("tools");
    validate(&native, &native).unwrap();
    let shared = json!({"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"stream":true,"max_tokens":128});
    validate(&shared, &shared).unwrap();
    let mut guessed = shared.clone();
    guessed["messages"][0]["content"] =
        json!([{"type":"image","source":{"type":"url","url":"https://example.test/image"}}]);
    assert!(validate(&shared, &guessed).is_err());
    let mut chat = chat();
    chat["system"] = json!("mixed protocol");
    assert!(validate(&chat, &chat).is_err());
}

#[test]
fn explicit_content_sampling_and_reduced_native_caps_are_allowed() {
    for (old, key, pointer) in [
        (chat(), "max_tokens", "/messages/1/content"),
        (responses(), "max_output_tokens", "/input/0/content/0/text"),
        (anthropic(), "max_tokens", "/messages/0/content/0/text"),
    ] {
        let mut new = old.clone();
        *new.pointer_mut(pointer).unwrap() = json!("rewritten user");
        new["temperature"] = json!(0.1);
        new[key] = json!(64);
        validate(&old, &new).unwrap();
    }
}

#[test]
fn native_frozen_values_and_stream_mode_are_exact() {
    for (old, pointer) in [
        (chat(), "/messages/0/content"),
        (responses(), "/instructions"),
        (anthropic(), "/system/0/text"),
        (anthropic(), "/system/0/cache_control/type"),
    ] {
        let mut new = old.clone();
        *new.pointer_mut(pointer).unwrap() = json!("tampered");
        assert!(validate(&old, &new).is_err(), "{pointer}");
    }
    for old in [chat(), responses(), anthropic()] {
        for key in ["tools", "stream"] {
            let mut new = old.clone();
            new.as_object_mut().unwrap().remove(key);
            assert!(validate(&old, &new).is_err(), "removed {key}");
        }
        let mut new = old.clone();
        new["stream"] = json!(false);
        assert!(validate(&old, &new).is_err());
        let mut new = old.clone();
        new["tools"] = json!([]);
        if old["tools"] != json!([]) {
            assert!(validate(&old, &new).is_err());
        }
    }
    let old = responses();
    let mut new = old.clone();
    new["tools"][0]["strict"] = json!(true);
    assert!(validate(&old, &new).is_err());
    let mut new = old.clone();
    new["input"]
        .as_array_mut()
        .unwrap()
        .push(json!({"role":"developer","content":"injected"}));
    assert!(validate(&old, &new).is_err());
    let old = anthropic();
    let mut new = old.clone();
    new["messages"]
        .as_array_mut()
        .unwrap()
        .push(json!({"role":"system","content":"injected"}));
    assert!(validate(&old, &new).is_err());
}

#[test]
fn output_caps_cannot_disappear_increase_or_change_native_field() {
    for (old, key, wrong_key) in [
        (chat(), "max_tokens", "max_completion_tokens"),
        (responses(), "max_output_tokens", "max_tokens"),
        (anthropic(), "max_tokens", "max_output_tokens"),
    ] {
        for cap in [
            Value::Null,
            json!(0),
            json!(129),
            json!(-1),
            json!(1.5),
            json!("64"),
        ] {
            let mut new = old.clone();
            new[key] = cap;
            assert!(validate(&old, &new).is_err());
        }
        let mut missing = old.clone();
        missing.as_object_mut().unwrap().remove(key);
        assert!(validate(&old, &missing).is_err());
        missing[wrong_key] = json!(64);
        assert!(validate(&old, &missing).is_err());
        let mut alias = old.clone();
        alias[wrong_key] = json!(64);
        assert!(validate(&old, &alias).is_err());
        let mut invalid_authored = old.clone();
        invalid_authored[key] = json!(0);
        assert!(validate(&invalid_authored, &old).is_err());
    }
}

#[test]
fn multimodal_blocks_and_native_calls_require_their_payloads() {
    for (old, pointer, malformed) in [
        (responses(), "/input/0/content/1/image_url", json!(5)),
        (responses(), "/input/2/arguments", json!({})),
        (responses(), "/input/3/call_id", json!("")),
        (
            anthropic(),
            "/messages/0/content/1/source/data",
            json!(null),
        ),
        (anthropic(), "/messages/1/content/2/input", json!("{}")),
        (anthropic(), "/messages/2/content/0/tool_use_id", json!("")),
    ] {
        let mut new = old.clone();
        *new.pointer_mut(pointer).unwrap() = malformed;
        assert!(validate(&old, &new).is_err(), "{pointer}");
    }
    let mut body = chat();
    body["messages"][1]["content"] = json!([{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"https://example.test/image"}}]);
    body["messages"].as_array_mut().unwrap().push(json!({"role":"assistant","content":null,"tool_calls":[{"id":"c","type":"function","function":{"name":"echo","arguments":"{}"}}]}));
    body["messages"]
        .as_array_mut()
        .unwrap()
        .push(json!({"role":"tool","tool_call_id":"c","content":"done"}));
    validate(&body, &body).unwrap();
    let mut invalid = body.clone();
    invalid["messages"][1]["content"][1]["image_url"]["url"] = json!(null);
    assert!(validate(&body, &invalid).is_err());
    let mut invalid = body.clone();
    invalid["messages"][2]["tool_calls"][0]["function"]["arguments"] = json!({});
    assert!(validate(&body, &invalid).is_err());
}

#[test]
fn malformed_roots_roles_and_cross_protocol_envelopes_are_rejected() {
    for old in [chat(), responses(), anthropic()] {
        for malformed in [json!(null), json!([]), json!({}), json!({"model":"m"})] {
            assert!(validate(&old, &malformed).is_err());
        }
        let mut invalid = old.clone();
        invalid["stream"] = json!("true");
        assert!(validate(&old, &invalid).is_err());
        let mut invalid = old.clone();
        invalid["model"] = json!("");
        assert!(validate(&old, &invalid).is_err());
        assert!(validate_request_rewrite(&old.to_string(), "{", 65_536).is_err());
    }
    for old in [chat(), anthropic()] {
        let mut invalid = old.clone();
        invalid["messages"] = json!([]);
        assert!(validate(&old, &invalid).is_err());
        let mut invalid = old.clone();
        invalid["messages"][0]["role"] = json!("unknown");
        assert!(validate(&old, &invalid).is_err());
    }
    let old = responses();
    for input in [
        json!([]),
        json!({}),
        json!([{"type":"unknown"}]),
        json!([{"role":"user","content":[{}]}]),
    ] {
        let mut invalid = old.clone();
        invalid["input"] = input;
        assert!(validate(&old, &invalid).is_err());
    }
    assert!(validate(&chat(), &responses()).is_err());
    assert!(validate(&responses(), &anthropic()).is_err());
}

#[test]
fn conversation_capacity_does_not_expand_plugin_rewrite_budget() {
    let old = chat();
    let mut large = old.clone();
    large["messages"][1]["content"] = json!("x".repeat(MAX_REQUEST_REWRITE_BYTES));
    assert!(matches!(
        validate_request_rewrite(&old.to_string(), &large.to_string(), 64 * 1024 * 1024),
        Err(Error::Limit(_))
    ));
    validate_request_rewrite(&old.to_string(), &old.to_string(), 64 * 1024 * 1024).unwrap();
    assert!(validate_request_rewrite(&old.to_string(), &old.to_string(), 1).is_err());
}
