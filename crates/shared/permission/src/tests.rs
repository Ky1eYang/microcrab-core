use super::*;
use serde_json::json;

fn rule(action: Action) -> Permission {
    Permission {
        agent: None,
        mode: None,
        tool: Some(Matcher::Exact("read".into())),
        args: vec![],
        action,
    }
}
fn guarded(action: Action, pointer: &str, text: &str) -> Permission {
    Permission {
        args: vec![ArgumentPredicate {
            pointer: pointer.into(),
            matcher: Matcher::Glob(text.into()),
        }],
        ..rule(action)
    }
}
fn snapshot() -> PolicySnapshot {
    PolicySnapshot {
        revision: 1,
        config: PermissionConfig {
            agent: "coding".into(),
            mode: "build".into(),
            ..Default::default()
        },
    }
}
fn request(arguments: Value) -> PermissionRequest {
    PermissionRequest {
        subject: InvocationSubject::Tool,
        session_id: "s".into(),
        call_id: "c".into(),
        agent: "coding".into(),
        mode: "build".into(),
        tool: "read".into(),
        arguments,
        revision: 1,
    }
}
#[test]
fn ordered_layers_and_conjunctive_arguments() {
    let mut s = snapshot();
    s.config.provider_defaults = vec![rule(Action::Allow)];
    s.config.defaults = vec![rule(Action::Ask)];
    s.config.overrides = vec![
        rule(Action::Allow),
        guarded(Action::Deny, "/path", "safe/*"),
    ];
    for (args, expected) in [
        (json!({"path":"safe/file"}), Action::Deny),
        (json!({"path":"other"}), Action::Allow),
    ] {
        let p = compile(&s, "read").unwrap();
        assert_eq!(evaluate_compiled(&p, &args).unwrap(), expected);
        assert_eq!(evaluate(&s, &request(args)).unwrap().action, expected);
    }
    s.config.overrides.clear();
    assert_eq!(
        evaluate(&s, &request(json!({}))).unwrap().action,
        Action::Ask
    );
    s.config.defaults.clear();
    assert_eq!(
        evaluate(&s, &request(json!({}))).unwrap().action,
        Action::Allow
    );
    s.config.provider_defaults.clear();
    assert_eq!(
        evaluate(&s, &request(json!({}))).unwrap().action,
        Action::Deny
    );
}
#[test]
fn overlapping_guards_order_and_unconditional_shadowing_are_exact() {
    let mut s = snapshot();
    s.config.overrides = vec![
        guarded(Action::Allow, "/path", "safe/*"),
        guarded(Action::Ask, "/path", "safe/file"),
    ];
    let p = compile(&s, "read").unwrap();
    assert_eq!(
        evaluate_compiled(&p, &json!({"path":"safe/file"})).unwrap(),
        Action::Ask
    );
    assert_eq!(
        evaluate_compiled(&p, &json!({"path":"safe/other"})).unwrap(),
        Action::Allow
    );
    assert_eq!(
        evaluate_compiled(&p, &json!({"path":"else"})).unwrap(),
        Action::Deny
    );
    if let CompiledToolPolicy::Conditional { branches, .. } = p {
        assert_eq!(branches[0].source.as_deref(), Some("override:1"));
    } else {
        panic!("guarded policy must remain conditional");
    }
    s.config.overrides.push(rule(Action::Deny));
    assert_eq!(
        compile(&s, "read").unwrap(),
        CompiledToolPolicy::Static {
            action: Action::Deny
        }
    );
    s.config.overrides = vec![
        rule(Action::Allow),
        guarded(Action::Allow, "/path", "safe/*"),
    ];
    assert_eq!(
        compile(&s, "read").unwrap(),
        CompiledToolPolicy::Static {
            action: Action::Allow
        }
    );
}
#[test]
fn predicates_are_conjunctive_and_never_stringify_or_quantify_batches() {
    let mut s = snapshot();
    let mut r = guarded(Action::Allow, "/a~1b/~0name", "yes");
    r.args.push(ArgumentPredicate {
        pointer: "/action".into(),
        matcher: Matcher::Exact("read".into()),
    });
    s.config.overrides.push(r);
    let p = compile(&s, "read").unwrap();
    assert_eq!(
        evaluate_compiled(&p, &json!({"a/b":{"~name":"yes"},"action":"read"})).unwrap(),
        Action::Allow
    );
    for value in [
        json!(null),
        json!(true),
        json!(1),
        json!(["yes"]),
        json!({}),
        json!("no"),
    ] {
        assert_eq!(
            evaluate_compiled(&p, &json!({"a/b":{"~name":value},"action":"read"})).unwrap(),
            Action::Deny
        );
    }
    assert_eq!(
        evaluate_compiled(&p, &json!({"a/b":{"~name":"yes"}})).unwrap(),
        Action::Deny
    );
    assert_eq!(
        evaluate_compiled(&p, &json!([{"a/b":{"~name":"yes"},"action":"read"}])).unwrap(),
        Action::Deny
    );
}
#[test]
fn oversized_predicate_input_fails_but_large_unmatched_payload_is_allowed() {
    let mut s = snapshot();
    s.config.defaults.push(rule(Action::Allow));
    s.config
        .overrides
        .push(guarded(Action::Deny, "/command", "*"));
    assert!(
        evaluate(&s, &request(json!({"command":"x".repeat(4097)})))
            .unwrap_err()
            .0
            .contains("input byte budget")
    );
    assert_eq!(
        evaluate(&s, &request(json!({"content":"x".repeat(1024*1024)})))
            .unwrap()
            .action,
        Action::Allow
    );
}
#[test]
fn own_agent_mode_selectors_and_known_tool_are_used() {
    let mut s = snapshot();
    s.config.defaults.push(rule(Action::Allow));
    let mut r = rule(Action::Deny);
    r.mode = Some("plan".into());
    s.config.overrides.push(r);
    assert_eq!(
        evaluate_compiled(&compile(&s, "read").unwrap(), &json!({})).unwrap(),
        Action::Allow
    );
    assert_eq!(
        evaluate_compiled(&compile(&s, "write").unwrap(), &json!({})).unwrap(),
        Action::Deny
    );
    s.config.mode = "plan".into();
    assert_eq!(
        evaluate_compiled(&compile(&s, "read").unwrap(), &json!({})).unwrap(),
        Action::Deny
    );
}
#[test]
fn initialization_is_separate_and_stale_requests_fail() {
    let mut s = snapshot();
    s.config.overrides.push(Permission {
        tool: Some(Matcher::Glob("*".into())),
        ..rule(Action::Allow)
    });
    let mut r = request(json!({"component":"hash","imports":["file.write"]}));
    r.subject = InvocationSubject::AgentInit {
        component: "hash".into(),
        generation: 1,
        attempt: 2,
    };
    assert_eq!(evaluate(&s, &r).unwrap().action, Action::Allow);
    for action in [Action::Allow, Action::Ask, Action::Deny] {
        s.config.initialization = action;
        assert_eq!(evaluate(&s, &r).unwrap().action, action);
    }
    r.revision = 2;
    assert!(evaluate(&s, &r).unwrap_err().0.contains("stale"));
    assert_eq!(
        serde_json::from_value::<PermissionConfig>(json!({}))
            .unwrap()
            .initialization,
        Action::Allow
    );
}
#[test]
fn all_of_preserves_members_and_strictest_raw_action() {
    let mut s = snapshot();
    s.config
        .overrides
        .push(guarded(Action::Ask, "/path", "safe/*"));
    let child = compile(&s, "read").unwrap();
    let parent = CompiledToolPolicy::Static {
        action: Action::Allow,
    };
    let p = combine(vec![child.clone(), parent.clone()]).unwrap();
    assert_eq!(
        evaluate_compiled(&p, &json!({"path":"safe/file"})).unwrap(),
        Action::Ask
    );
    assert_eq!(
        evaluate_compiled(&p, &json!({"path":"other"})).unwrap(),
        Action::Deny
    );
    let asks = combine(vec![
        CompiledToolPolicy::Static {
            action: Action::Ask,
        },
        CompiledToolPolicy::Static {
            action: Action::Ask,
        },
    ])
    .unwrap();
    assert_eq!(
        asks,
        CompiledToolPolicy::Static {
            action: Action::Ask
        }
    );
    assert_eq!(evaluate_compiled(&asks, &json!({})).unwrap(), Action::Ask);
    let wire = serde_json::to_value(&p).unwrap();
    assert_eq!(wire["kind"], "all_of");
    assert_eq!(wire["policies"][0]["kind"], "conditional");
    assert_eq!(
        serde_json::from_value::<CompiledToolPolicy>(wire).unwrap(),
        p
    );
    assert!(combine(vec![]).is_err());
}
#[test]
fn member_evaluation_preserves_order_and_shares_work_even_after_deny() {
    let ordered = CompiledToolPolicy::AllOf {
        policies: vec![
            CompiledToolPolicy::Static {
                action: Action::Deny,
            },
            CompiledToolPolicy::Static {
                action: Action::Ask,
            },
            CompiledToolPolicy::AllOf {
                policies: vec![CompiledToolPolicy::Static {
                    action: Action::Allow,
                }],
            },
        ],
    };
    assert_eq!(
        evaluate_compiled_members(&ordered, &json!({})).unwrap(),
        vec![Action::Deny, Action::Ask, Action::Allow]
    );
    let expensive = CompiledToolPolicy::Conditional {
        branches: vec![CompiledBranch {
            args: vec![ArgumentPredicate {
                pointer: "/path".into(),
                matcher: Matcher::Glob("*".repeat(600)),
            }],
            action: Action::Ask,
            source: None,
        }],
        default_action: Action::Deny,
    };
    let arguments = json!({"path":"x".repeat(1000)});
    assert_eq!(
        evaluate_compiled_members(&expensive, &arguments).unwrap(),
        vec![Action::Ask]
    );
    // Each member costs 600 * 1001, so both individually fit but their sum does not.
    let combined = CompiledToolPolicy::AllOf {
        policies: vec![
            CompiledToolPolicy::Static {
                action: Action::Deny,
            },
            expensive.clone(),
            expensive,
        ],
    };
    assert!(
        evaluate_compiled_members(&combined, &arguments)
            .unwrap_err()
            .0
            .contains("matcher work budget")
    );
}

#[test]
fn strict_live_schema_rejects_every_legacy_field_and_action() {
    for field in [
        "capability",
        "resource",
        "arguments",
        "operation_arguments",
        "conditions",
    ] {
        let mut value = json!({"action":"allow"});
        value[field] = json!(null);
        assert!(
            serde_json::from_value::<Permission>(value)
                .unwrap_err()
                .to_string()
                .contains(field)
        );
    }
    for action in ["conditional", "conditional_ask", "unknown"] {
        assert!(serde_json::from_value::<Action>(json!(action)).is_err());
    }
    for field in ["capability", "resources", "operation_arguments"] {
        let mut value = serde_json::to_value(request(json!({}))).unwrap();
        value[field] = json!(null);
        assert!(serde_json::from_value::<PermissionRequest>(value).is_err());
    }
    assert!(
        serde_json::from_value::<CompiledToolPolicy>(
            json!({"kind":"static","action":"allow","owner":"x"})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<Matcher>(json!({"kind":"exact","value":"x","extra":1})).is_err()
    );
}
#[test]
fn compiled_and_request_evaluation_share_the_exact_argument_byte_boundary() {
    let mut s = snapshot();
    let mut r = request(json!({
        "content": "x".repeat(MAX_ARGUMENT_BYTES - r#"{"content":""}"#.len())
    }));
    assert_eq!(
        serde_json::to_vec(&r.arguments).unwrap().len(),
        MAX_ARGUMENT_BYTES
    );
    assert!(serde_json::to_vec(&r).unwrap().len() > MAX_ARGUMENT_BYTES);
    for action in [Action::Allow, Action::Ask, Action::Deny] {
        s.config.overrides = vec![rule(action)];
        let compiled = compile(&s, "read").unwrap();
        assert_eq!(evaluate_compiled(&compiled, &r.arguments).unwrap(), action);
        assert_eq!(
            evaluate_compiled_members(&compiled, &r.arguments).unwrap(),
            vec![action]
        );
        assert_eq!(evaluate(&s, &r).unwrap().action, action);
    }
    let Value::String(content) = &mut r.arguments["content"] else {
        panic!("string payload fixture")
    };
    content.push('x');
    let compiled = compile(&s, "read").unwrap();
    for error in [
        evaluate_compiled(&compiled, &r.arguments).unwrap_err(),
        evaluate_compiled_members(&compiled, &r.arguments).unwrap_err(),
        evaluate(&s, &r).unwrap_err(),
    ] {
        assert!(error.0.contains("argument byte budget"));
    }
}

#[test]
fn glob_pointer_work_and_composition_budgets_fail_concretely() {
    assert!(matches(&Matcher::Glob("src/*.rs".into()), "src/main.rs"));
    assert!(matches(
        &Matcher::Glob("git status*".into()),
        "git status; erase data"
    ));
    assert!(!matches(&Matcher::Exact("x".into()), &"x".repeat(4097)));
    for pointer in ["/~2", "/~", "relative"] {
        let mut s = snapshot();
        s.config
            .overrides
            .push(guarded(Action::Allow, pointer, "x"));
        assert!(validate(&s.config).is_err());
    }
    let mut s = snapshot();
    s.config
        .overrides
        .push(guarded(Action::Allow, "/path", &"?".repeat(4096)));
    assert!(
        evaluate_compiled(
            &compile(&s, "read").unwrap(),
            &json!({"path":"x".repeat(4096)})
        )
        .unwrap_err()
        .0
        .contains("work budget")
    );
    let mut p = CompiledToolPolicy::Static {
        action: Action::Allow,
    };
    for _ in 1..MAX_POLICY_DEPTH {
        p = CompiledToolPolicy::AllOf { policies: vec![p] };
    }
    assert!(combine(vec![p]).unwrap_err().0.contains("depth"));
    assert!(
        combine(vec![
            CompiledToolPolicy::Static {
                action: Action::Allow
            };
            MAX_POLICY_NODES
        ])
        .unwrap_err()
        .0
        .contains("node")
    );
    let branch = CompiledBranch {
        args: vec![ArgumentPredicate {
            pointer: "/p".into(),
            matcher: Matcher::Exact("x".repeat(4096)),
        }],
        action: Action::Ask,
        source: None,
    };
    assert!(
        combine(vec![CompiledToolPolicy::Conditional {
            branches: vec![branch; 256],
            default_action: Action::Deny
        }])
        .unwrap_err()
        .0
        .contains("projection")
    );
}
