//! Strict live tool policy contracts and bounded, side-effect-free compilation.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Allow,
    Ask,
    Deny,
}
impl Action {
    /// Combine policy outcomes without resolving any owner's independent Ask.
    pub const fn strictest(self, other: Self) -> Self {
        match (self, other) {
            (Self::Deny, _) | (_, Self::Deny) => Self::Deny,
            (Self::Ask, _) | (_, Self::Ask) => Self::Ask,
            _ => Self::Allow,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Ask => "ask",
            Self::Deny => "deny",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Matcher {
    Exact(String),
    Glob(String),
    Regex(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArgumentPredicate {
    pub pointer: String,
    pub matcher: Matcher,
}
/// Selectors and string argument predicates are conjunctive. Rules only select tools.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permission {
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub tool: Option<Matcher>,
    #[serde(default)]
    pub args: Vec<ArgumentPredicate>,
    pub action: Action,
}
/// Runtime-created attribution; tool names cannot impersonate lifecycle work.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InvocationSubject {
    #[default]
    Tool,
    AgentInit {
        component: String,
        generation: u64,
        attempt: u64,
    },
}
impl InvocationSubject {
    pub fn is_tool(&self) -> bool {
        matches!(self, Self::Tool)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionRequest {
    #[serde(default, skip_serializing_if = "InvocationSubject::is_tool")]
    pub subject: InvocationSubject,
    pub session_id: String,
    pub call_id: String,
    pub agent: String,
    pub mode: String,
    pub tool: String,
    /// The original whole-call arguments, never an internal Host operation payload.
    pub arguments: Value,
    pub revision: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub action: Action,
    pub reason: String,
    pub source: String,
}
impl Decision {
    pub fn new(action: Action, reason: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            action,
            reason: reason.into(),
            source: source.into(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AskReply {
    pub action: Action,
    pub reason: String,
}
/// A review suggestion never directly changes current or future authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewResult {
    pub action: Action,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub future_grant: Option<FutureGrant>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FutureGrant {
    pub args: Vec<ArgumentPredicate>,
}

pub fn validate_review_result(
    result: &ReviewResult,
    request: &PermissionRequest,
) -> Result<(), ValidationError> {
    encoded_budget(result, 16 * 1024, "review reply byte budget")?;
    if result.reason.len() > 512 || result.reason.chars().any(char::is_control) {
        return Err(failure(
            "review reason exceeds byte budget or contains control characters",
        ));
    }
    if let Some(grant) = &result.future_grant {
        if result.action != Action::Allow || !request.subject.is_tool() || grant.args.is_empty() {
            return Err(failure(
                "future grants require Allow for a tool with nonempty argument predicates",
            ));
        }
        validate_args(&grant.args)?;
        if grant.args.iter().all(|p| match &p.matcher {
            Matcher::Glob(v) => v.chars().all(|c| c == '*'),
            Matcher::Regex(v) => [
                ".*",
                ".*?",
                "(?s).*",
                "[\\s\\S]*",
                "(?:.*)",
                "^.*$",
                "\\A.*\\z",
            ]
            .contains(&v.as_str()),
            Matcher::Exact(_) => false,
        }) {
            return Err(failure(
                "future grant predicates must constrain matching arguments",
            ));
        }
        encoded_budget(
            &request.arguments,
            MAX_ARGUMENT_BYTES,
            "argument byte budget",
        )?;
        if !args_match(&grant.args, &request.arguments, &mut MAX_MATCH_WORK.clone())? {
            return Err(failure(
                "future grant does not match original invocation arguments",
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionFeatures {
    pub interactive_ask: bool,
    pub remembered_grants: bool,
    pub live_edit: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewModelSelection {
    pub provider_id: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionConfig {
    #[serde(default)]
    pub agent: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub provider_defaults: Vec<Permission>,
    #[serde(default)]
    pub defaults: Vec<Permission>,
    #[serde(default)]
    pub overrides: Vec<Permission>,
    /// Independent component-level lifecycle gate; tool rules never affect it.
    #[serde(default = "initialization_default")]
    pub initialization: Action,
    #[serde(default = "enabled")]
    pub review: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_model: Option<ReviewModelSelection>,
}
fn enabled() -> bool {
    true
}
fn initialization_default() -> Action {
    Action::Allow
}
impl Default for PermissionConfig {
    fn default() -> Self {
        Self {
            agent: String::new(),
            mode: String::new(),
            provider_defaults: vec![],
            defaults: vec![],
            overrides: vec![],
            initialization: Action::Allow,
            review: true,
            review_model: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySnapshot {
    pub revision: u64,
    pub config: PermissionConfig,
}
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid permission configuration: {0}")]
pub struct ValidationError(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledBranch {
    pub args: Vec<ArgumentPredicate>,
    pub action: Action,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}
/// Controller identities/revisions and Ask ownership belong to Runtime, outside nodes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompiledToolPolicy {
    Static {
        action: Action,
    },
    Conditional {
        branches: Vec<CompiledBranch>,
        default_action: Action,
    },
    AllOf {
        policies: Vec<CompiledToolPolicy>,
    },
}
pub const MAX_TEXT: usize = 4096;
pub const MAX_RULES: usize = 256;
pub const MAX_PREDICATES: usize = 16;
pub const MAX_POLICY_DEPTH: usize = 32;
pub const MAX_POLICY_NODES: usize = 1024;
pub const MAX_POLICY_BRANCHES: usize = 4096;
pub const MAX_PROJECTION_BYTES: usize = 1024 * 1024;
pub const MAX_CONFIG_BYTES: usize = 1024 * 1024;
pub const MAX_ARGUMENT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_MATCH_WORK: usize = 1_000_000;
fn failure(message: &str) -> ValidationError {
    ValidationError(message.into())
}
pub const MAX_REGEX_BYTES: usize = 2048;
pub const MAX_REGEX_COMPILED_BYTES: usize = 256 * 1024;
fn regex(pattern: &str) -> Result<regex::Regex, ValidationError> {
    if pattern.len() > MAX_REGEX_BYTES {
        return Err(failure("regex pattern byte budget (2048) exceeded"));
    }
    regex::RegexBuilder::new(&format!("\\A(?:{pattern})\\z"))
        .size_limit(MAX_REGEX_COMPILED_BYTES)
        .dfa_size_limit(MAX_REGEX_COMPILED_BYTES)
        .nest_limit(32)
        .build()
        .map_err(|error| ValidationError(format!("invalid or over-budget regex: {error}")))
}
fn validate_matcher(m: &Matcher) -> Result<(), ValidationError> {
    let (Matcher::Exact(s) | Matcher::Glob(s) | Matcher::Regex(s)) = m;
    if s.len() > MAX_TEXT {
        return Err(failure("matcher byte budget (4096) exceeded"));
    }
    if let Matcher::Regex(pattern) = m {
        regex(pattern)?;
    }
    Ok(())
}
fn validate_args(args: &[ArgumentPredicate]) -> Result<(), ValidationError> {
    if args.len() > MAX_PREDICATES {
        return Err(failure("predicate count budget (16) exceeded"));
    }
    for arg in args {
        if arg.pointer.len() > 512
            || (!arg.pointer.is_empty() && !arg.pointer.starts_with('/'))
            || arg.pointer.split('/').count() > 33
        {
            return Err(failure(
                "invalid JSON pointer or pointer depth/byte budget exceeded",
            ));
        }
        let bytes = arg.pointer.as_bytes();
        for (i, b) in bytes.iter().enumerate() {
            if *b == b'~' && !matches!(bytes.get(i + 1), Some(b'0' | b'1')) {
                return Err(failure("invalid JSON pointer escape"));
            }
        }
        validate_matcher(&arg.matcher)?;
    }
    Ok(())
}
/// Count encoded bytes without first allocating an unbounded serialization buffer.
fn encoded_budget(
    value: &impl Serialize,
    limit: usize,
    label: &str,
) -> Result<(), ValidationError> {
    struct Counter {
        remaining: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.remaining = self
                .remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("encoded byte budget exceeded"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter { remaining: limit }, value)
        .map_err(|e| ValidationError(format!("{label}: {e}")))
}
pub fn validate(config: &PermissionConfig) -> Result<(), ValidationError> {
    if config.agent.len() > 128
        || config.mode.len() > 128
        || config
            .provider_defaults
            .len()
            .saturating_add(config.defaults.len())
            .saturating_add(config.overrides.len())
            > MAX_RULES
    {
        return Err(failure("policy selector/rule count budget exceeded"));
    }
    if config.review_model.as_ref().is_some_and(|m| {
        m.provider_id.is_empty()
            || m.provider_id.len() > 256
            || m.model.is_empty()
            || m.model.len() > 1024
            || m.provider_id.contains(char::is_control)
            || m.model.contains(char::is_control)
            || m.variant
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 64 || v.contains(char::is_control))
    }) {
        return Err(failure("invalid review model selection"));
    }
    for (layer, rules) in [
        ("provider", &config.provider_defaults),
        ("mode", &config.defaults),
        ("override", &config.overrides),
    ] {
        for (index, rule) in rules.iter().enumerate() {
            let check = || {
                if rule.agent.as_ref().is_some_and(|v| v.len() > 128)
                    || rule.mode.as_ref().is_some_and(|v| v.len() > 128)
                {
                    return Err(failure("agent/mode selector byte budget exceeded"));
                }
                if let Some(m) = &rule.tool {
                    if matches!(m, Matcher::Regex(_)) {
                        return Err(failure(
                            "regex is supported only for JSON-pointer argument strings",
                        ));
                    }
                    validate_matcher(m)?;
                }
                validate_args(&rule.args)
            };
            check().map_err(|e| ValidationError(format!("{layer}:{index}: {}", e.0)))?;
        }
    }
    encoded_budget(config, MAX_CONFIG_BYTES, "configuration byte budget")
}
/// Glob matches bytes literally except '*' and '?'; it makes no shell safety claim.
pub fn matches(matcher: &Matcher, text: &str) -> bool {
    if text.len() > MAX_TEXT {
        return false;
    }
    match matcher {
        Matcher::Exact(expected) => expected.len() <= MAX_TEXT && expected == text,
        Matcher::Regex(pattern) => regex(pattern).is_ok_and(|compiled| compiled.is_match(text)),
        Matcher::Glob(pattern) => {
            if pattern.len() > MAX_TEXT {
                return false;
            }
            let text = text.as_bytes();
            let mut row = vec![false; text.len() + 1];
            row[0] = true;
            for p in pattern.bytes() {
                let mut next = vec![false; text.len() + 1];
                next[0] = p == b'*' && row[0];
                for j in 1..=text.len() {
                    next[j] = if p == b'*' {
                        row[j] || next[j - 1]
                    } else {
                        row[j - 1] && (p == b'?' || p == text[j - 1])
                    };
                }
                row = next;
            }
            row[text.len()]
        }
    }
}
fn bounded_match(m: &Matcher, text: &str, work: &mut usize) -> Result<bool, ValidationError> {
    if text.len() > MAX_TEXT {
        return Err(failure("predicate input byte budget (4096) exceeded"));
    }
    let cost = match m {
        Matcher::Exact(p) => p.len().saturating_add(text.len()),
        Matcher::Glob(p) => p.len().saturating_mul(text.len().saturating_add(1)),
        Matcher::Regex(p) => MAX_REGEX_COMPILED_BYTES
            .saturating_add(p.len().saturating_mul(text.len().saturating_add(1))),
    };
    *work = work
        .checked_sub(cost)
        .ok_or_else(|| failure("matcher work budget (1000000) exceeded"))?;
    Ok(matches(m, text))
}
/// Compile one controller's reverse-priority decision list for its own agent/mode.
pub fn compile(
    snapshot: &PolicySnapshot,
    tool: &str,
) -> Result<CompiledToolPolicy, ValidationError> {
    validate(&snapshot.config)?;
    if tool.len() > MAX_TEXT {
        return Err(failure("tool name byte budget exceeded"));
    }
    let mut work = MAX_MATCH_WORK;
    let mut branches = Vec::new();
    let mut default_action = Action::Deny;
    'layers: for (layer, rules) in [
        ("override", &snapshot.config.overrides),
        ("mode", &snapshot.config.defaults),
        ("provider", &snapshot.config.provider_defaults),
    ] {
        for (index, rule) in rules.iter().enumerate().rev() {
            if rule
                .agent
                .as_ref()
                .is_some_and(|v| v != &snapshot.config.agent)
                || rule
                    .mode
                    .as_ref()
                    .is_some_and(|v| v != &snapshot.config.mode)
            {
                continue;
            }
            if let Some(m) = &rule.tool
                && !bounded_match(m, tool, &mut work)?
            {
                continue;
            }
            if rule.args.is_empty() {
                default_action = rule.action;
                break 'layers;
            }
            branches.push(CompiledBranch {
                args: rule.args.clone(),
                action: rule.action,
                source: Some(format!("{layer}:{index}")),
            });
        }
    }
    let policy = if branches.iter().all(|b| b.action == default_action) {
        CompiledToolPolicy::Static {
            action: default_action,
        }
    } else {
        CompiledToolPolicy::Conditional {
            branches,
            default_action,
        }
    };
    validate_compiled(&policy)?;
    Ok(policy)
}
fn validate_compiled(policy: &CompiledToolPolicy) -> Result<(), ValidationError> {
    let mut stack = vec![(policy, 1usize)];
    let mut nodes = 0usize;
    let mut branches = 0usize;
    while let Some((node, depth)) = stack.pop() {
        nodes += 1;
        if nodes > MAX_POLICY_NODES || depth > MAX_POLICY_DEPTH {
            return Err(failure("compiled policy node/depth budget exceeded"));
        }
        match node {
            CompiledToolPolicy::Static { .. } => {}
            CompiledToolPolicy::Conditional { branches: list, .. } => {
                branches = branches.saturating_add(list.len());
                if branches > MAX_POLICY_BRANCHES {
                    return Err(failure("compiled policy branch budget exceeded"));
                }
                for branch in list {
                    validate_args(&branch.args)?;
                    if branch.source.as_ref().is_some_and(|s| s.len() > 512) {
                        return Err(failure("compiled source byte budget exceeded"));
                    }
                }
            }
            CompiledToolPolicy::AllOf { policies } => {
                if policies.is_empty() {
                    return Err(failure("all_of requires at least one policy"));
                }
                if stack
                    .len()
                    .saturating_add(policies.len())
                    .saturating_add(nodes)
                    > MAX_POLICY_NODES
                {
                    return Err(failure("compiled policy node budget exceeded"));
                }
                stack.extend(policies.iter().map(|p| (p, depth + 1)));
            }
        }
    }
    encoded_budget(
        policy,
        MAX_PROJECTION_BYTES,
        "compiled projection byte budget",
    )
}
fn args_match(
    args: &[ArgumentPredicate],
    value: &Value,
    work: &mut usize,
) -> Result<bool, ValidationError> {
    for p in args {
        let Some(text) = value.pointer(&p.pointer).and_then(Value::as_str) else {
            return Ok(false);
        };
        if !bounded_match(&p.matcher, text, work)? {
            return Ok(false);
        }
    }
    Ok(true)
}
fn evaluate_node(
    policy: &CompiledToolPolicy,
    value: &Value,
    work: &mut usize,
) -> Result<(Action, Option<String>), ValidationError> {
    match policy {
        CompiledToolPolicy::Static { action } => Ok((*action, None)),
        CompiledToolPolicy::Conditional {
            branches,
            default_action,
        } => {
            for branch in branches {
                if args_match(&branch.args, value, work).map_err(|e| {
                    ValidationError(format!(
                        "{}: {}",
                        branch.source.as_deref().unwrap_or("compiled branch"),
                        e.0
                    ))
                })? {
                    return Ok((branch.action, branch.source.clone()));
                }
            }
            Ok((*default_action, None))
        }
        CompiledToolPolicy::AllOf { policies } => {
            let mut action = Action::Allow;
            // Evaluate every member: a Deny must not hide malformed/exhausted ceilings.
            for policy in policies {
                let (next, _) = evaluate_node(policy, value, work)?;
                action = action.strictest(next);
            }
            Ok((action, None))
        }
    }
}
pub fn evaluate_compiled(
    policy: &CompiledToolPolicy,
    arguments: &Value,
) -> Result<Action, ValidationError> {
    validate_compiled(policy)?;
    encoded_budget(arguments, MAX_ARGUMENT_BYTES, "argument byte budget")?;
    let mut work = MAX_MATCH_WORK;
    Ok(evaluate_node(policy, arguments, &mut work)?.0)
}
/// Evaluate immediate owner policies in order under one shared argument/work budget.
/// Nested AllOf nodes remain one member; Deny never skips later member validation.
pub fn evaluate_compiled_members(
    policy: &CompiledToolPolicy,
    arguments: &Value,
) -> Result<Vec<Action>, ValidationError> {
    validate_compiled(policy)?;
    encoded_budget(arguments, MAX_ARGUMENT_BYTES, "argument byte budget")?;
    let members = match policy {
        CompiledToolPolicy::AllOf { policies } => policies.as_slice(),
        _ => std::slice::from_ref(policy),
    };
    let mut work = MAX_MATCH_WORK;
    members
        .iter()
        .map(|member| evaluate_node(member, arguments, &mut work).map(|(action, _)| action))
        .collect()
}

/// Runtime retains controller/Ask ownership separately from this raw summary.
pub fn combine(policies: Vec<CompiledToolPolicy>) -> Result<CompiledToolPolicy, ValidationError> {
    let policy = CompiledToolPolicy::AllOf { policies };
    validate_compiled(&policy)?;
    let CompiledToolPolicy::AllOf { mut policies } = policy else {
        unreachable!("constructed all_of")
    };
    if policies.len() == 1 {
        return Ok(policies.remove(0));
    }
    if policies
        .iter()
        .all(|p| matches!(p, CompiledToolPolicy::Static { .. }))
    {
        let action = policies.iter().fold(Action::Allow, |action, p| {
            let CompiledToolPolicy::Static { action: next } = p else {
                unreachable!("checked static members")
            };
            action.strictest(*next)
        });
        return Ok(CompiledToolPolicy::Static { action });
    }
    Ok(CompiledToolPolicy::AllOf { policies })
}
pub fn evaluate(
    snapshot: &PolicySnapshot,
    request: &PermissionRequest,
) -> Result<Decision, ValidationError> {
    validate(&snapshot.config)?;
    if request.revision != snapshot.revision {
        return Err(failure("stale permission revision"));
    }
    encoded_budget(
        &request.arguments,
        MAX_ARGUMENT_BYTES,
        "argument byte budget",
    )?;
    if !request.subject.is_tool() {
        return Ok(Decision::new(
            snapshot.config.initialization,
            "component initialization lifecycle gate",
            "lifecycle",
        ));
    }
    let policy = compile(snapshot, &request.tool)?;
    let (action, source) = evaluate_node(&policy, &request.arguments, &mut MAX_MATCH_WORK.clone())?;
    Ok(Decision::new(
        action,
        "compiled tool policy",
        source.unwrap_or_else(|| "compiled".into()),
    ))
}

#[cfg(test)]
mod review_tests;
#[cfg(test)]
mod tests;
