//! Model-visible compiled tool facts; projection never resolves approvals.
use super::{
    Action, CompiledToolPolicy, PermissionController, PermissionError, PermissionListError,
    PolicySnapshot,
};
use crate::runtime::handler::permissions::AppliedCatalog;
use serde_json::{Value, json};
use std::sync::{Arc, RwLock, Weak};

/// A weak port avoids State -> controller -> auditor -> State ownership cycles.
#[derive(Clone)]
pub(crate) struct PermissionObservation {
    controller: Weak<PermissionController>,
    catalog: ObservationCatalog,
}

#[derive(Clone)]
enum ObservationCatalog {
    Runtime(Weak<RwLock<AppliedCatalog>>),
    #[cfg(test)]
    Fixture(Arc<Vec<(String, ToolOrigin)>>),
}

fn observed_policy_error(error: PermissionListError) -> PermissionError {
    match error {
        PermissionListError::Busy => PermissionError::StalePolicy,
        PermissionListError::Policy(error) => error,
        PermissionListError::Unavailable | PermissionListError::Budget => {
            PermissionError::Unavailable
        }
    }
}
impl PermissionObservation {
    pub(crate) fn runtime(
        controller: &Arc<PermissionController>,
        tools: &Arc<RwLock<AppliedCatalog>>,
    ) -> Self {
        Self {
            controller: Arc::downgrade(controller),
            catalog: ObservationCatalog::Runtime(Arc::downgrade(tools)),
        }
    }

    #[cfg(test)]
    pub(super) fn new(
        controller: &Arc<PermissionController>,
        tools: Vec<(String, ToolOrigin)>,
    ) -> Self {
        Self {
            controller: Arc::downgrade(controller),
            catalog: ObservationCatalog::Fixture(Arc::new(tools)),
        }
    }
    /// A current-State reader can derive permissions and all directory entries
    /// independently, then reject a mixed local/ancestor revision observation.
    pub(crate) fn capture_witness(
        &self,
        selected: Option<(&str, &str)>,
    ) -> Result<impl FnOnce() -> Result<(), PermissionError>, PermissionError> {
        let controller = self
            .controller
            .upgrade()
            .ok_or(PermissionError::Unavailable)?;
        let snapshot = controller.snapshot()?;
        if selected.is_some_and(|(agent, mode)| {
            snapshot.config.agent != agent || snapshot.config.mode != mode
        }) {
            return Err(PermissionError::StalePolicy);
        }
        let lineage = controller.lineage()?;
        Ok(move || controller.ensure_witness(snapshot.revision, &lineage))
    }

    /// Only /target and /mode are known. Other selectors remain residual guards;
    /// their absence from this partial input must never be interpreted as missing.
    pub(crate) fn delegate_policy(
        &self,
        target: &str,
        mode: &str,
    ) -> Result<(Action, Value), PermissionError> {
        let controller = self
            .controller
            .upgrade()
            .ok_or(PermissionError::Unavailable)?;
        let snapshot = controller.snapshot()?;
        let (compiled, _) = controller.compiled_chain(&snapshot, "delegate")?;
        let residual = specialize(&compiled, target, mode)?;
        let actions = action_bounds(&residual);
        let summary = if actions.iter().all(|a| *a == Action::Deny) {
            Action::Deny
        } else if actions.iter().all(|a| *a == Action::Allow) {
            Action::Allow
        } else {
            Action::Ask
        };
        let mut row = serde_json::to_value(residual).map_err(serialization)?;
        strip_model_facts(&mut row);
        row["action_bounds"] = json!(actions);
        row["arguments_unresolved"] = json!(actions.len() > 1);
        Ok((summary, row))
    }
    pub(crate) fn project(
        &self,
        names: Option<&std::collections::BTreeSet<String>>,
    ) -> Result<Value, PermissionError> {
        let controller = self
            .controller
            .upgrade()
            .ok_or(PermissionError::Unavailable)?;
        match &self.catalog {
            ObservationCatalog::Runtime(tools) => {
                let tools = tools.upgrade().ok_or(PermissionError::Unavailable)?;
                let (catalog, policy) =
                    AppliedCatalog::observe(&tools, &controller).map_err(observed_policy_error)?;
                let value = project_model_tools(
                    &controller,
                    &policy,
                    catalog.registry().tool_names(),
                    names,
                )?;
                let (current, current_policy) =
                    AppliedCatalog::observe(&tools, &controller).map_err(observed_policy_error)?;
                if !catalog.matches(&current) || policy.revision != current_policy.revision {
                    return Err(PermissionError::StalePolicy);
                }
                Ok(value)
            }
            #[cfg(test)]
            ObservationCatalog::Fixture(tools) => project_model_tools(
                &controller,
                &controller.snapshot()?,
                tools.iter().map(|(name, _)| name.as_str()),
                names,
            ),
        }
    }
}

fn strip_model_facts(row: &mut Value) {
    if let Some(fields) = row.as_object_mut() {
        fields.remove("owners");
    }
    if let Some(branches) = row.get_mut("branches").and_then(Value::as_array_mut) {
        for branch in branches {
            if let Some(fields) = branch.as_object_mut() {
                fields.remove("source");
            }
        }
    }
    if let Some(policies) = row.get_mut("policies").and_then(Value::as_array_mut) {
        for policy in policies {
            strip_model_facts(policy);
        }
    }
}

#[cfg(test)]
pub(super) fn project_tools(
    controller: &PermissionController,
    snapshot: &PolicySnapshot,
    tools: &[(String, ToolOrigin)],
) -> Result<Value, PermissionError> {
    if tools.len() > 4096 {
        return Err(PermissionError::InvalidConfiguration(
            "tool projection row budget (4096) exceeded".into(),
        ));
    }
    let lineage = controller.lineage()?;
    let mut rows = Vec::with_capacity(tools.len());
    let mut bytes = 2usize;
    for (name, _) in tools {
        let (compiled, owners) = controller.compiled_chain(snapshot, name)?;
        let mut row = serde_json::to_value(compiled).map_err(serialization)?;
        row["tool"] = json!({"kind":"exact","value":name});
        row["owners"] = json!(owners);
        bytes = bytes
            .saturating_add(serde_json::to_vec(&row).map_err(serialization)?.len())
            .saturating_add(1);
        if bytes > 2 * 1024 * 1024 {
            return Err(PermissionError::InvalidConfiguration(
                "tool inventory projection byte budget (2097152) exceeded".into(),
            ));
        }
        rows.push(row);
    }
    controller.ensure_witness(snapshot.revision, &lineage)?;
    Ok(Value::Array(rows))
}

/// Model grouping is presentation only. Management rows keep exact tool and
/// owner witnesses; admission never consumes these grouped objects.
fn project_model_tools<'a>(
    controller: &PermissionController,
    snapshot: &PolicySnapshot,
    tools: impl ExactSizeIterator<Item = &'a str>,
    names: Option<&std::collections::BTreeSet<String>>,
) -> Result<Value, PermissionError> {
    if tools.len() > 4096 {
        return Err(PermissionError::InvalidConfiguration(
            "tool projection input budget (4096) exceeded".into(),
        ));
    }
    let lineage = controller.lineage()?;
    controller.ensure_witness(snapshot.revision, &lineage)?;
    let rows = tools
        .filter(|name| names.is_none_or(|set| set.contains(*name)))
        .map(|name| {
            let compiled = PermissionController::compile_witness(snapshot, &lineage, name)?;
            let mut policy = serde_json::to_value(compiled).map_err(serialization)?;
            strip_model_facts(&mut policy);
            Ok((name.to_owned(), policy))
        });
    let value = group_model_policies(rows)?;
    controller.ensure_witness(snapshot.revision, &lineage)?;
    Ok(value)
}

fn group_model_policies(
    rows: impl IntoIterator<Item = Result<(String, Value), PermissionError>>,
) -> Result<Value, PermissionError> {
    // Canonical serde_json object ordering plus BTreeMap yields stable groups;
    // predicates, branch order and independent AllOf ancestors remain intact.
    let mut groups =
        std::collections::BTreeMap::<String, (Value, std::collections::BTreeSet<String>)>::new();
    // Lower bound on the final grouped encoding. Stream one compiled row at a
    // time: duplicates never accumulate 4096 copies before deduplication. Unique
    // policy/key and tool bytes cannot outgrow the final projection budget.
    let mut retained_bytes = 2usize;
    for row in rows {
        let (tool, mut policy) = row?;
        policy.sort_all_objects();
        let key = serde_json::to_string(&policy).map_err(serialization)?;
        if !groups.contains_key(&key) {
            retained_bytes = retained_bytes.saturating_add(key.len());
        }
        let tools = &mut groups
            .entry(key)
            .or_insert_with(|| (policy, Default::default()))
            .1;
        if !tools.contains(&tool) {
            retained_bytes = retained_bytes
                .saturating_add(serde_json::to_vec(&tool).map_err(serialization)?.len());
            tools.insert(tool);
        }
        if retained_bytes > 2 * 1024 * 1024 {
            return Err(PermissionError::InvalidConfiguration(
                "model permission projection byte budget (2097152) exceeded".into(),
            ));
        }
    }
    let value = Value::Array(
        groups
            .into_values()
            .map(|(mut policy, tools)| {
                policy["tools"] = json!(tools);
                policy
            })
            .collect(),
    );
    if serde_json::to_vec(&value).map_err(serialization)?.len() > 2 * 1024 * 1024 {
        return Err(PermissionError::InvalidConfiguration(
            "model permission projection byte budget (2097152) exceeded".into(),
        ));
    }
    Ok(value)
}

/// Preserve ordered first-match semantics while substituting known selectors.
fn specialize(
    policy: &CompiledToolPolicy,
    target: &str,
    mode: &str,
) -> Result<CompiledToolPolicy, PermissionError> {
    specialize_bounded(policy, target, mode, &mut super::MAX_MATCH_WORK.clone())
}
fn specialize_bounded(
    policy: &CompiledToolPolicy,
    target: &str,
    mode: &str,
    work: &mut usize,
) -> Result<CompiledToolPolicy, PermissionError> {
    Ok(match policy {
        CompiledToolPolicy::Static { action } => CompiledToolPolicy::Static { action: *action },
        CompiledToolPolicy::AllOf { policies } => super::combine(
            policies
                .iter()
                .map(|p| specialize_bounded(p, target, mode, work))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?,
        CompiledToolPolicy::Conditional {
            branches,
            default_action,
        } => {
            let mut residual = Vec::new();
            let mut fallback = *default_action;
            for branch in branches {
                let mut args = Vec::new();
                let mut excluded = false;
                for predicate in &branch.args {
                    let known = match predicate.pointer.as_str() {
                        "/target" => Some(target),
                        "/mode" => Some(mode),
                        _ => None,
                    };
                    if let Some(value) = known {
                        let cost = match &predicate.matcher {
                            super::Matcher::Exact(pattern) => {
                                pattern.len().saturating_add(value.len())
                            }
                            super::Matcher::Glob(pattern) => {
                                pattern.len().saturating_mul(value.len().saturating_add(1))
                            }
                            super::Matcher::Regex(pattern) => {
                                if pattern.len() > super::MAX_REGEX_BYTES {
                                    return Err(PermissionError::InvalidConfiguration(
                                        "delegate partial-evaluation regex pattern byte budget exceeded".into(),
                                    ));
                                }
                                super::MAX_REGEX_COMPILED_BYTES.saturating_add(
                                    pattern.len().saturating_mul(value.len().saturating_add(1)),
                                )
                            }
                        };
                        *work = work.checked_sub(cost).ok_or_else(|| {
                            PermissionError::InvalidConfiguration(
                                "delegate partial-evaluation matcher work budget exceeded".into(),
                            )
                        })?;
                        if !super::matches(&predicate.matcher, value) {
                            excluded = true;
                            break;
                        }
                    } else {
                        args.push(predicate.clone());
                    }
                }
                if excluded {
                    continue;
                }
                if args.is_empty() {
                    fallback = branch.action;
                    break;
                }
                let mut branch = branch.clone();
                branch.args = args;
                residual.push(branch);
            }
            if residual.is_empty() || residual.iter().all(|b| b.action == fallback) {
                CompiledToolPolicy::Static { action: fallback }
            } else {
                CompiledToolPolicy::Conditional {
                    branches: residual,
                    default_action: fallback,
                }
            }
        }
    })
}
/// An upper bound, not a reachability proof: ordered guards and different owners
/// can test the same unknown argument. Mixed bounds stay unresolved until runtime
/// evaluates the whole invocation; inventory must not claim every member is feasible.
fn action_bounds(policy: &CompiledToolPolicy) -> Vec<Action> {
    match policy {
        CompiledToolPolicy::Static { action } => vec![*action],
        CompiledToolPolicy::Conditional {
            branches,
            default_action,
        } => {
            let mut actions = vec![*default_action];
            for branch in branches {
                if !actions.contains(&branch.action) {
                    actions.push(branch.action);
                }
            }
            actions
        }
        CompiledToolPolicy::AllOf { policies } => {
            let mut actions = vec![Action::Allow];
            for policy in policies {
                let next = action_bounds(policy);
                let mut combined = Vec::new();
                for a in &actions {
                    for b in &next {
                        let action = a.strictest(*b);
                        if !combined.contains(&action) {
                            combined.push(action);
                        }
                    }
                }
                actions = combined;
            }
            actions
        }
    }
}

fn serialization(error: serde_json::Error) -> PermissionError {
    PermissionError::InvalidConfiguration(format!("tool policy projection serialization: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use microcrab_permission::{ArgumentPredicate, CompiledBranch, Matcher, evaluate_compiled};

    #[test]
    fn model_groups_equal_residual_policies_and_orders_tools_deterministically() {
        let conditional = json!({"kind":"conditional","branches":[
            {"args":[{"pointer":"/path","matcher":{"kind":"exact","value":"safe"}}],"action":"allow"},
            {"args":[{"pointer":"/path","matcher":{"kind":"glob","value":"*"}}],"action":"deny"}
        ],"default_action":"ask"});
        let mut reordered = conditional.clone();
        reordered["branches"].as_array_mut().unwrap().reverse();
        let all_of = json!({"kind":"all_of","policies":[conditional.clone(),{"kind":"static","action":"ask"}]});
        let mut reversed_owners = all_of.clone();
        reversed_owners["policies"]
            .as_array_mut()
            .unwrap()
            .reverse();
        let rows = vec![
            ("z".into(), json!({"kind":"static","action":"allow"})),
            ("a".into(), json!({"kind":"static","action":"allow"})),
            ("deny".into(), json!({"kind":"static","action":"deny"})),
            ("conditional-b".into(), conditional.clone()),
            ("conditional-a".into(), conditional.clone()),
            ("reordered".into(), reordered),
            ("all-b".into(), all_of.clone()),
            ("all-a".into(), all_of),
            ("reversed-owners".into(), reversed_owners),
        ];
        let grouped = group_model_policies(rows.clone().into_iter().map(Ok)).unwrap();
        let mut reversed = rows;
        reversed.reverse();
        assert_eq!(
            grouped,
            group_model_policies(reversed.into_iter().map(Ok)).unwrap()
        );
        let groups = grouped.as_array().unwrap();
        assert_eq!(groups.len(), 6);
        assert!(
            groups
                .iter()
                .any(|row| row["tools"] == json!(["a", "z"]) && row["action"] == "allow")
        );
        assert!(
            groups
                .iter()
                .any(|row| row["tools"] == json!(["conditional-a", "conditional-b"]))
        );
        assert!(
            groups
                .iter()
                .any(|row| row["tools"] == json!(["all-a", "all-b"]))
        );
        assert!(
            groups
                .iter()
                .all(|row| row.get("tool").is_none() && row.get("owners").is_none())
        );
    }

    #[test]
    fn model_strips_nested_management_facts_without_dropping_predicates_or_order() {
        let mut policy = json!({"kind":"all_of","owners":[{"controller":"private"}],"policies":[
            {"kind":"conditional","branches":[{"source":"private","action":"deny","args":[{"pointer":"/path","matcher":{"kind":"regex","value":"private/.*"}}]}],"default_action":"allow"},
            {"kind":"static","action":"ask"}
        ]});
        strip_model_facts(&mut policy);
        assert!(policy.get("owners").is_none());
        assert!(policy["policies"][0]["branches"][0].get("source").is_none());
        assert_eq!(
            policy["policies"][0]["branches"][0]["args"][0]["matcher"]["kind"],
            "regex"
        );
        assert_eq!(policy["policies"][1]["action"], "ask");
    }

    #[test]
    fn delegate_regex_specializes_known_selectors_and_keeps_unknown_guards() {
        let policy = CompiledToolPolicy::Conditional {
            branches: vec![CompiledBranch {
                args: vec![
                    ArgumentPredicate {
                        pointer: "/target".into(),
                        matcher: Matcher::Regex("work(er)?".into()),
                    },
                    ArgumentPredicate {
                        pointer: "/input".into(),
                        matcher: Matcher::Regex("safe .*".into()),
                    },
                ],
                action: Action::Allow,
                source: None,
            }],
            default_action: Action::Deny,
        };
        let residual = specialize(&policy, "worker", "review").unwrap();
        assert_eq!(action_bounds(&residual), vec![Action::Deny, Action::Allow]);
        assert_eq!(
            evaluate_compiled(&residual, &json!({"input":"safe task"})).unwrap(),
            Action::Allow
        );
        assert_eq!(
            evaluate_compiled(&residual, &json!({"input":null})).unwrap(),
            Action::Deny
        );
        assert_eq!(
            specialize(&policy, "prefix-worker", "review").unwrap(),
            CompiledToolPolicy::Static {
                action: Action::Deny
            }
        );
    }

    #[test]
    fn grouping_stops_lazy_expansion_when_distinct_policies_exceed_the_budget() {
        let generated = std::cell::Cell::new(0);
        let rows = (0..4096).map(|index| {
            generated.set(generated.get() + 1);
            let predicate =
                json!({"pointer":"/path","matcher":{"kind":"exact","value":"x".repeat(4096)}});
            let mut branches = vec![json!({"args":[predicate],"action":"allow"}); 200];
            branches[0]["args"][0]["matcher"]["value"] = json!(format!("unique-{index}"));
            Ok((
                format!("tool-{index}"),
                json!({"kind":"conditional","branches":branches,"default_action":"deny"}),
            ))
        });
        let error = group_model_policies(rows).unwrap_err();
        assert!(
            matches!(error, PermissionError::InvalidConfiguration(reason) if reason.contains("byte budget"))
        );
        assert_eq!(
            generated.get(),
            3,
            "remaining expanded rows must never be retained or consumed"
        );
    }

    #[test]
    fn equivalent_lazy_policies_keep_the_full_catalog_with_one_residual_group() {
        let policy = json!({"kind":"conditional","branches":[{"args":[{"pointer":"/path","matcher":{"kind":"glob","value":"src/*"}}],"action":"allow"}],"default_action":"deny"});
        let grouped = group_model_policies(
            (0..4096).map(|index| Ok((format!("tool-{index:04}"), policy.clone()))),
        )
        .unwrap();
        assert_eq!(grouped.as_array().unwrap().len(), 1);
        assert_eq!(grouped[0]["tools"].as_array().unwrap().len(), 4096);
        assert_eq!(grouped[0]["branches"], policy["branches"]);
        assert_eq!(grouped[0]["tools"][0], "tool-0000");
        assert_eq!(grouped[0]["tools"][4095], "tool-4095");
    }

    #[test]
    fn projection_witness_retains_typed_ancestor_staleness() {
        let parent = Arc::new(PermissionController::new(Arc::new(
            crate::runtime::tool::DenyAll,
        )));
        let child = Arc::new(PermissionController::new(Arc::new(
            crate::runtime::tool::DenyAll,
        )));
        child.set_parent(parent.clone()).unwrap();
        let observation = child.observe(vec![]);
        let witness = observation.capture_witness(None).unwrap();
        let snapshot = parent.snapshot().unwrap();
        let mut config = snapshot.config;
        config.review = !config.review;
        parent.replace_user(snapshot.revision, config).unwrap();
        assert!(matches!(witness(), Err(PermissionError::StalePolicy)));
    }

    #[test]
    fn correlated_owner_guards_keep_bounds_distinct_from_runtime_decisions() {
        let guarded = |action, default_action| CompiledToolPolicy::Conditional {
            branches: vec![CompiledBranch {
                args: vec![ArgumentPredicate {
                    pointer: "/path".into(),
                    matcher: Matcher::Exact("safe".into()),
                }],
                action,
                source: None,
            }],
            default_action,
        };
        let policy = CompiledToolPolicy::AllOf {
            policies: vec![
                guarded(Action::Allow, Action::Deny),
                guarded(Action::Deny, Action::Allow),
            ],
        };
        // Unknown arguments do not prove that every action in the bound is
        // feasible. The complete residual policy still denies every invocation.
        assert!(action_bounds(&policy).contains(&Action::Allow));
        for arguments in [json!({"path":"safe"}), json!({"path":"other"}), json!({})] {
            assert_eq!(
                evaluate_compiled(&policy, &arguments).unwrap(),
                Action::Deny
            );
        }
    }
}
