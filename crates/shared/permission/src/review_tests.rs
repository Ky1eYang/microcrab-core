//! Regex and review-output behavioral contracts, authored without execution.
use super::*;
use serde_json::json;
fn conditional(matcher: Matcher) -> CompiledToolPolicy {
    CompiledToolPolicy::Conditional {
        branches: vec![CompiledBranch {
            args: vec![ArgumentPredicate {
                pointer: "/nested/path".into(),
                matcher,
            }],
            action: Action::Allow,
            source: None,
        }],
        default_action: Action::Ask,
    }
}
#[test]
fn regex_is_full_string_on_selected_strings_only() {
    let policy = conditional(Matcher::Regex(r"src/[a-z]+\.rs".into()));
    for value in [
        json!({"nested":{"path":"src/main.rs"}}),
        json!({"nested":{"path":"src/lib.rs"}}),
    ] {
        assert_eq!(evaluate_compiled(&policy, &value).unwrap(), Action::Allow);
    }
    for value in [
        json!({"nested":{"path":"prefix/src/main.rs"}}),
        json!({"nested":{"path":"src/main.rs.bak"}}),
        json!({"nested":{"path":1}}),
        json!({}),
        json!("src/main.rs"),
    ] {
        assert_eq!(evaluate_compiled(&policy, &value).unwrap(), Action::Ask);
    }
}
#[test]
fn regex_rejects_invalid_pattern_budgets_and_tool_selectors() {
    for pattern in [
        "(".into(),
        "a".repeat(MAX_REGEX_BYTES + 1),
        r"(a)\1".into(),
        "a{1000000}".into(),
    ] {
        assert!(evaluate_compiled(&conditional(Matcher::Regex(pattern)), &json!({})).is_err());
    }
    let mut config = PermissionConfig::default();
    config.overrides.push(Permission {
        agent: None,
        mode: None,
        tool: Some(Matcher::Regex("read".into())),
        args: vec![],
        action: Action::Allow,
    });
    assert!(validate(&config).unwrap_err().0.contains("JSON-pointer"));
    assert!(
        evaluate_compiled(
            &conditional(Matcher::Regex(".*".into())),
            &json!({"nested":{"path":"x".repeat(MAX_TEXT+1)}})
        )
        .is_err()
    );
}
#[test]
fn regex_work_budget_is_shared_across_ancestor_members() {
    let member = conditional(Matcher::Regex("x".into()));
    let policy = CompiledToolPolicy::AllOf {
        policies: vec![member; 5],
    };
    assert!(
        evaluate_compiled(&policy, &json!({"nested":{"path":"x"}}))
            .unwrap_err()
            .0
            .contains("work budget")
    );
}
#[test]
fn reviewer_proposal_requires_allow_tool_and_original_arguments_match() {
    let request = PermissionRequest {
        subject: InvocationSubject::Tool,
        session_id: "owner".into(),
        call_id: "call".into(),
        agent: "coding".into(),
        mode: "build".into(),
        tool: "read".into(),
        arguments: json!({"path":"src/main.rs"}),
        revision: 1,
    };
    let grant = FutureGrant {
        args: vec![ArgumentPredicate {
            pointer: "/path".into(),
            matcher: Matcher::Regex(r"src/[a-z]+\.rs".into()),
        }],
    };
    let result = ReviewResult {
        action: Action::Allow,
        reason: "Read source files".into(),
        future_grant: Some(grant),
    };
    validate_review_result(&result, &request).unwrap();
    for action in [Action::Ask, Action::Deny] {
        let mut invalid = result.clone();
        invalid.action = action;
        assert!(validate_review_result(&invalid, &request).is_err());
    }
    let mut missing = request.clone();
    missing.arguments = json!({"path":"private/key"});
    assert!(validate_review_result(&result, &missing).is_err());
    let mut init = request;
    init.subject = InvocationSubject::AgentInit {
        component: "fixture".into(),
        generation: 1,
        attempt: 1,
    };
    assert!(validate_review_result(&result, &init).is_err());
    assert!(
        serde_json::from_value::<ReviewResult>(json!({"action":"allow","reason":"ok","policy":{}}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<ReviewResult>(
            json!({"action":"allow","reason":"ok","future_grant":{"args":[],"tool":"*"}})
        )
        .is_err()
    );
}
