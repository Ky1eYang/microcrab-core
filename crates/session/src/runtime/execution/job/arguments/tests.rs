//! Handle syntax and current encoded batch inputs never bypass ownership checks.
use super::*;

#[test]
fn encoded_batch_strings_match_native_arrays_for_every_job_action() {
    let ids: Vec<_> = (1..=8).map(|id| format!("owner/{id}")).collect();
    let encoded = serde_json::to_string(&ids).unwrap();
    for action in ["status", "read", "wait", "write", "cancel", "release"] {
        let args = if action == "write" {
            json!({"data":"input"})
        } else {
            json!({})
        };
        for id in [json!(ids), json!(encoded), json!(format!(" \n{encoded}\t"))] {
            let value = json!({"id":id,"action":action,"args":args});
            let parsed = validate(&value).unwrap();
            assert!(parsed.batch);
            assert_eq!(
                parsed.ids,
                ids.iter().map(String::as_str).collect::<Vec<_>>()
            );
            assert_eq!(parsed.action, action);
            assert_eq!(parsed.args, args);
        }
    }
    let one = json!({"id":"[\"owner/1\"]","action":"status"});
    assert!(
        validate(&one).unwrap().batch,
        "one-member encoded arrays retain batch shape"
    );
}

#[test]
fn malformed_scalar_and_batch_handles_never_become_permission_errors() {
    for id in [
        "owner",
        "/1",
        "owner/",
        "owner/0",
        "owner/-1",
        "owner/+1",
        "owner/ 1",
        "owner/1\n",
        "owner/١",
        "owner/18446744073709551616",
        "foreign/not-a-number",
        "foreign/1/2",
        "\"owner/1\"",
        "[\"owner/0\"]",
        "[\"owner/1\",null]",
        "[\"owner/1\",]",
    ] {
        for id in [json!(id), json!(["owner/1", id])] {
            let arguments = json!({"id":id,"action":"read"});
            assert!(
                matches!(validate(&arguments), Err(Error::Diagnostic(diagnostic))
                if diagnostic.code == crate::observation::diagnostics::DiagnosticCode::InvalidArgument)
            );
        }
    }
}

#[test]
fn encoded_arrays_enforce_count_types_and_one_decoding_layer() {
    let sixteen: Vec<_> = (1..=16).map(|id| format!("owner/{id}")).collect();
    let value = json!({"id":serde_json::to_string(&sixteen).unwrap(),"action":"status"});
    assert_eq!(validate(&value).unwrap().ids.len(), 16);
    let seventeen: Vec<_> = (1..=17).map(|id| format!("owner/{id}")).collect();
    for encoded in [
        "[]".to_owned(),
        "[null]".into(),
        "[1]".into(),
        "[[\"owner/1\"]]".into(),
        "[\"\"]".into(),
        "[\"[\\\"owner/1\\\"]\"]".into(),
        serde_json::to_string(&seventeen).unwrap(),
        serde_json::to_string(&serde_json::to_string(&sixteen).unwrap()).unwrap(),
        "[\"owner/1\"] trailing".into(),
    ] {
        let arguments = json!({"id":encoded,"action":"release"});
        let error = match validate(&arguments) {
            Err(error) => error,
            Ok(_) => panic!("invalid encoded array accepted"),
        };
        let diagnostic = error.tool_diagnostic();
        assert_eq!(
            diagnostic.code,
            crate::observation::diagnostics::DiagnosticCode::InvalidArgument
        );
        assert_eq!(
            diagnostic.phase,
            crate::observation::diagnostics::DiagnosticPhase::Validation
        );
        assert_eq!(
            diagnostic.effects,
            crate::observation::diagnostics::EffectKnowledge::NotStarted
        );
        assert!(diagnostic.summary().contains("JSON-encoded array string"));
    }
}

#[test]
fn valid_opaque_namespaces_and_integer_boundaries_keep_existing_semantics() {
    for (id, namespace, number) in [
        ("owner/1", "owner", 1),
        ("foreign/1", "foreign", 1),
        ("带 空格的会话/1", "带 空格的会话", 1),
        (" /1", " ", 1),
        ("[namespace/1", "[namespace", 1),
        ("owner/01", "owner", 1),
        ("owner/18446744073709551615", "owner", u64::MAX),
    ] {
        assert_eq!(parse_handle(id).unwrap(), (namespace, number));
        let arguments = json!({"id":id,"action":"status"});
        let parsed = validate(&arguments).unwrap();
        assert!(!parsed.batch);
        assert_eq!(parsed.ids, [id]);
    }
}

#[test]
fn ownership_is_checked_only_after_the_complete_handle_is_valid() {
    for id in ["foreign/1", "带 空格的会话/1"] {
        assert!(matches!(
            super::super::resolve(id, "owner"),
            Err(Error::Denied(_))
        ));
    }
    for id in [
        "foreign/not-a-number",
        "foreign/0",
        "foreign/1/2",
        "[\"foreign/1\"]",
    ] {
        assert!(
            matches!(super::super::resolve(id, "owner"), Err(Error::Diagnostic(diagnostic))
            if diagnostic.code == crate::observation::diagnostics::DiagnosticCode::InvalidArgument)
        );
    }
    assert_eq!(super::super::resolve("owner/1", "owner").unwrap().0, 1);
    assert_eq!(
        super::super::resolve("带 空格的会话/1", "带 空格的会话")
            .unwrap()
            .0,
        1
    );
}
