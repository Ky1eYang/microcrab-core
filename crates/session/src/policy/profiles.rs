//! Native policy profiles and origin-scoped provider metadata.
use super::{Action, Matcher, Permission};
use crate::runtime::tool::{BuiltinTool, ToolOrigin};

fn rule(tool: &str, action: Action) -> Permission {
    Permission {
        agent: None,
        mode: None,
        tool: Some(Matcher::Exact(tool.into())),
        args: vec![],
        action,
    }
}

/// Mode transitions require Ask; execution/output controls retain native Allow.
pub(super) fn native(tools: &[(String, ToolOrigin)], _agent: &str, _mode: &str) -> Vec<Permission> {
    tools
        .iter()
        .filter(|(name, origin)| {
            *origin == ToolOrigin::Builtin && BuiltinTool::from_name(name).is_some()
        })
        .flat_map(|(name, _)| {
            if name == "send_message" {
                let mut send = rule(name, Action::Ask);
                send.args.push(super::ArgumentPredicate {
                    pointer: "/intent".into(),
                    matcher: Matcher::Exact("send".into()),
                });
                let mut reply = rule(name, Action::Allow);
                reply.args.push(super::ArgumentPredicate {
                    pointer: "/intent".into(),
                    matcher: Matcher::Exact("reply".into()),
                });
                return vec![send, reply];
            }
            if name == "job" {
                let mut redirect = rule(name, Action::Ask);
                redirect.args.push(super::ArgumentPredicate {
                    pointer: "/action".into(),
                    matcher: Matcher::Exact(crate::runtime::execution::job::REDIRECT.into()),
                });
                return vec![rule(name, Action::Allow), redirect];
            }
            if name == crate::runtime::tool::view_image::NAME {
                return ["file:*", "bytes:*", "inline:*", "https:*"]
                    .into_iter()
                    .map(|pattern| {
                        let mut source = rule(name, Action::Allow);
                        source.args.push(super::ArgumentPredicate {
                            pointer: "/image_url".into(),
                            matcher: Matcher::Glob(pattern.into()),
                        });
                        source
                    })
                    .collect();
            }
            vec![rule(
                name,
                if matches!(name.as_str(), "switch_mode" | "escalate" | "send_message") {
                    Action::Ask
                } else {
                    Action::Allow
                },
            )]
        })
        .collect()
}

/// Guest declarations apply only to exact registered WASM tool identities.
pub(super) fn guest(tools: &[(String, ToolOrigin)], rules: &[Permission]) -> Vec<Permission> {
    let mut result = Vec::new();
    for (name, origin) in tools {
        if *origin != ToolOrigin::Wasm {
            continue;
        }
        for declared in rules {
            if declared
                .tool
                .as_ref()
                .is_some_and(|selector| !super::matches(selector, name))
            {
                continue;
            }
            let mut scoped = declared.clone();
            scoped.tool = Some(Matcher::Exact(name.clone()));
            result.push(scoped);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn view_image_defaults_allow_each_supported_source_without_blanket_allow() {
        let rules = native(
            &[("view_image".into(), ToolOrigin::Builtin)],
            "agent",
            "mode",
        );
        assert_eq!(rules.len(), 4);
        assert!(
            rules
                .iter()
                .all(|rule| rule.args[0].pointer == "/image_url")
        );
        assert!(
            rules
                .iter()
                .all(|rule| rule.action == Action::Allow && rule.args.len() == 1)
        );
        let policy = microcrab_permission::PolicySnapshot {
            revision: 0,
            config: microcrab_permission::PermissionConfig {
                defaults: rules,
                ..Default::default()
            },
        };
        let compiled = microcrab_permission::compile(&policy, "view_image").unwrap();
        for source in [
            serde_json::json!({"image_url":"file:digest"}),
            serde_json::json!({"image_url":"bytes:digest"}),
            serde_json::json!({"image_url":"inline:digest"}),
            serde_json::json!({"image_url":"https:digest"}),
        ] {
            assert_eq!(
                microcrab_permission::evaluate_compiled(&compiled, &source).unwrap(),
                Action::Allow
            );
        }
        assert_eq!(
            microcrab_permission::evaluate_compiled(&compiled, &serde_json::json!({})).unwrap(),
            Action::Deny
        );
        let mut policy = policy;
        let mut deny_file = rule("view_image", Action::Deny);
        deny_file.args.push(super::super::ArgumentPredicate {
            pointer: "/image_url".into(),
            matcher: Matcher::Glob("file:*".into()),
        });
        policy.config.overrides.push(deny_file);
        let compiled = microcrab_permission::compile(&policy, "view_image").unwrap();
        assert_eq!(
            microcrab_permission::evaluate_compiled(&compiled, &serde_json::json!({"image_url":"file:digest"})).unwrap(),
            Action::Deny
        );
        assert_eq!(
            microcrab_permission::evaluate_compiled(&compiled, &serde_json::json!({"image_url":"bytes:digest"})).unwrap(),
            Action::Allow
        );
    }

    #[test]
    fn reserved_mode_switch_has_native_ask_while_job_keeps_allow() {
        let rules = native(
            &[
                ("switch_mode".into(), ToolOrigin::Builtin),
                ("job".into(), ToolOrigin::Builtin),
                ("switch_mode".into(), ToolOrigin::External),
            ],
            "agent",
            "mode",
        );
        assert_eq!(rules.len(), 3);
        assert_eq!(rules[0].action, Action::Ask);
        let policy = microcrab_permission::PolicySnapshot {
            revision: 0,
            config: microcrab_permission::PermissionConfig {
                defaults: rules,
                ..Default::default()
            },
        };
        let compiled = microcrab_permission::compile(&policy, "job").unwrap();
        for action in [
            "status", "read", "wait", "write", "cancel", "release", "redirect",
        ] {
            assert_eq!(
                microcrab_permission::evaluate_compiled(
                    &compiled,
                    &serde_json::json!({"action":action})
                )
                .unwrap(),
                if action == "redirect" {
                    Action::Ask
                } else {
                    Action::Allow
                },
            );
        }
    }
}
