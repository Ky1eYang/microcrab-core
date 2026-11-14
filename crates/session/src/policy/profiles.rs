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
        .map(|(name, _)| {
            rule(
                name,
                if name == "switch_mode" {
                    Action::Ask
                } else {
                    Action::Allow
                },
            )
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
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].action, Action::Ask);
        assert_eq!(rules[1].action, Action::Allow);
    }
}
