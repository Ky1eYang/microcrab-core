//! Internal clean-context review; never allocates a Session command or mutates Context.
use super::{Builtin, ContextMode, Source};
use crate::context::{Content, ContentItem};
use crate::policy::{InvocationSubject, PermissionError, PermissionRequest, ReviewResult};
use crate::{Client, Error, State};
use microcrab_auditor::Auditor;
use microcrab_protocol::InputPart;
use tokio_util::sync::CancellationToken;

pub(super) const BUILTIN: Builtin = Builtin {
    name: "action_review",
    source: Source::System,
    context_mode: ContextMode::Clean,
    prompt: include_str!("../../../templates/subagents/ACTION_REVIEW.tmpl"),
};

pub(crate) const BINDINGS: &[&str] = &[
    "user_input",
    "tool",
    "tool_name",
    "tool_description",
    "tool_args",
    "operation_kind",
    "mode",
    "mode_prompt",
];

// Exact previously shipped default bytes only. Keep compatibility without carrying
// the retired prompt assets or rewriting any saved custom configuration.
const LEGACY_DEFAULT_SHA256: [&str; 3] = [
    "29d89e6b445812fdbbc43ce6a5fecc53238c1d6ff26987108cfab7f5b05a0abc",
    "8d11b97ef87ff2db01f622ba5504b72903be0af97cc909514067c72c5df11cd3",
    "360ab615c5daf31468adb9537151d16c400cb8a7a5b5a285705e698b5466082a",
];

fn is_legacy_default(markdown: &str) -> bool {
    use sha2::{Digest, Sha256};
    LEGACY_DEFAULT_SHA256.contains(&format!("{:x}", Sha256::digest(markdown)).as_str())
}

fn latest_user_text(issuer: &Content) -> String {
    let Some(ContentItem::User { parts, .. }) = crate::context::latest_direct_user(&issuer.items)
    else {
        return String::new();
    };
    parts
        .iter()
        .filter_map(|part| match part {
            InputPart::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn prepare_review(
    issuer: &Content,
    markdown: &str,
    bindings: &[(&str, &str)],
) -> Result<Content, Error> {
    // Saved defaults use the current authored asset; custom templates stay literal.
    if !markdown.is_empty() && markdown != BUILTIN.prompt && !is_legacy_default(markdown) {
        return BUILTIN
            .definition()
            .prepare_markdown(markdown, issuer, bindings);
    }
    let escaped = bindings
        .iter()
        .map(|(name, value)| {
            if matches!(*name, "user_input" | "mode_prompt" | "tool_description") {
                escape_review_data(value, issuer.max_bytes).map(Some)
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let bindings = bindings
        .iter()
        .zip(&escaped)
        .map(|(&(name, value), escaped)| (name, escaped.as_deref().unwrap_or(value)))
        .collect::<Vec<_>>();
    BUILTIN
        .definition()
        .prepare_markdown(BUILTIN.prompt, issuer, &bindings)
}

fn escape_review_data(value: &str, maximum: usize) -> Result<String, Error> {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            _ => escaped.push(ch),
        }
        crate::budget::check(crate::LimitKind::ContextBytes, escaped.len(), maximum)?;
    }
    Ok(escaped)
}

#[derive(serde::Serialize)]
struct InitReviewArguments<'a> {
    component: String,
    imports: &'a serde_json::Value,
    trust: &'a str,
}

fn review_arguments(
    permission: &PermissionRequest,
    preview: Option<&serde_json::Value>,
    short_model: bool,
) -> Result<String, PermissionError> {
    let encoded = match &permission.subject {
        InvocationSubject::AgentInit { component, .. } if short_model => {
            // Read scope from the exact typed initialization request, not the
            // ordinary tool argument preview or an unrelated field named component.
            let arguments = &permission.arguments;
            let imports = arguments
                .get("imports")
                .filter(|value| {
                    value
                        .as_array()
                        .is_some_and(|imports| imports.iter().all(serde_json::Value::is_string))
                })
                .ok_or(PermissionError::Review)?;
            let trust = arguments
                .get("trust")
                .and_then(serde_json::Value::as_str)
                .ok_or(PermissionError::Review)?;
            if arguments
                .get("component")
                .and_then(serde_json::Value::as_str)
                != Some(component)
            {
                return Err(PermissionError::Review);
            }
            let component = if component.len() == 64
                && component
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                format!("{}…", &component[..16])
            } else {
                component.clone()
            };
            serde_json::to_string(&InitReviewArguments {
                component,
                imports,
                trust,
            })
        }
        _ => serde_json::to_string(preview.unwrap_or(&permission.arguments)),
    }
    .map_err(|_| PermissionError::Review)?;
    Ok(encoded.replace('<', "\\u003c").replace('>', "\\u003e"))
}

#[expect(
    clippy::too_many_arguments,
    reason = "Model preview, tool description, and grant-validation evidence are separate"
)]
pub(crate) async fn run(
    executor: &dyn super::OneShotExecutor,
    client: std::sync::Arc<dyn Client>,
    auditor: std::sync::Arc<dyn Auditor>,
    state: &State,
    issuer: &Content,
    permission: &PermissionRequest,
    tool_description: Option<&str>,
    preview: Option<&serde_json::Value>,
    cancellation: &CancellationToken,
) -> Result<ReviewResult, PermissionError> {
    let input = serde_json::to_string(permission).map_err(|_| PermissionError::Review)?;
    if input.len() > 65536 {
        return Err(PermissionError::Review);
    }
    let tool_args = review_arguments(permission, preview, state.short_model_refs())?;
    let user_input = latest_user_text(issuer);
    let (mode, mode_prompt) = state.selected_mode_instructions();
    let operation_kind = if permission.subject.is_tool() {
        "tool"
    } else {
        "agent_init"
    };
    let description = tool_description.unwrap_or(if permission.subject.is_tool() {
        ""
    } else {
        "Initialize the registered WASM agent component within its declared host import scope."
    });
    let request = prepare_review(
        issuer,
        &executor.prompt_config().action_review_template,
        &[
            ("user_input", &user_input),
            ("tool", &permission.tool),
            ("tool_name", &permission.tool),
            ("tool_description", description),
            ("tool_args", &tool_args),
            ("operation_kind", operation_kind),
            ("mode", &mode),
            ("mode_prompt", &mode_prompt),
        ],
    )
    .map_err(|_| PermissionError::Review)?;
    let response = executor
        .execute(
            client,
            auditor.clone(),
            std::sync::Arc::new(state.clone()),
            &request,
            cancellation,
        )
        .await
        .map_err(|error| match error {
            Error::Cancelled => PermissionError::Cancelled,
            _ => PermissionError::Review,
        })?;
    if cancellation.is_cancelled() {
        return Err(PermissionError::Cancelled);
    }
    // The one-shot executor owns RoundCompleted, usage acceptance and hook
    // injection exactly once in its isolated child State. The parent never
    // synthesizes another completion for equal request bytes.
    if !response.tool_calls.is_empty() || response.content.len() > 16 * 1024 {
        return Err(PermissionError::Review);
    }
    let reply: ReviewResult = serde_json::from_str(&response.content).map_err(|error| {
        PermissionError::InvalidConfiguration(format!("invalid action review JSON: {error}"))
    })?;
    crate::policy::validate_review_result(&reply, permission).map_err(|error| {
        PermissionError::InvalidConfiguration(format!("invalid action review result: {error}"))
    })?;
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_review_uses_typed_init_identity_without_rewriting_scope_or_tool_arguments() {
        use serde_json::{Value, json};
        let digest = "a1".repeat(32);
        let exact = json!({"component": digest, "imports": ["write-text", "custom<scope>"], "trust": "exact trust statement"});
        let mut request = PermissionRequest {
            subject: InvocationSubject::AgentInit {
                component: digest.clone(),
                generation: 4,
                attempt: 2,
            },
            session_id: "private session".into(),
            call_id: format!("agent-init:4:2:{digest}"),
            agent: "coding".into(),
            mode: "normal".into(),
            tool: String::new(),
            arguments: exact.clone(),
            revision: 7,
        };
        let encoded = review_arguments(&request, None, true).unwrap();
        assert!(!encoded.contains(&digest));
        assert!(!encoded.contains("<scope>"));
        let short: Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(
            short,
            json!({"component": "a1a1a1a1a1a1a1a1…", "imports": exact["imports"], "trust": exact["trust"]})
        );
        assert_eq!(request.arguments, exact);
        assert_eq!(
            serde_json::from_str::<Value>(&review_arguments(&request, None, false).unwrap())
                .unwrap(),
            exact
        );
        request.subject = InvocationSubject::Tool;
        assert_eq!(
            serde_json::from_str::<Value>(&review_arguments(&request, None, true).unwrap())
                .unwrap(),
            exact
        );
    }

    #[test]
    fn real_user_selection_ignores_framework_carriers_and_does_not_skip_an_empty_latest_input() {
        let mut issuer = crate::context::Context::new("parent".into(), vec![], 65_536)
            .unwrap()
            .content(128)
            .unwrap();
        let input = |source, text: &str| ContentItem::User {
            envelope: None,
            source,
            parts: vec![InputPart::Text { text: text.into() }],
        };
        assert_eq!(latest_user_text(&issuer), "");
        issuer.items = vec![
            input(None, "old"),
            input(None, "latest"),
            input(
                Some(microcrab_transport::UserSource::Framework),
                "framework",
            ),
            ContentItem::Tool {
                call_id: "fixture".into(),
                content: "not intent".into(),
                parts: vec![],
            },
        ];
        assert_eq!(latest_user_text(&issuer), "latest");
        issuer.items.push(input(None, ""));
        assert_eq!(latest_user_text(&issuer), "");
    }

    #[test]
    fn tool_and_initialization_review_exclude_compaction_summary_and_framework_carriers() {
        let mut context = crate::context::Context::new("parent".into(), vec![], 65_536).unwrap();
        let state = serde_json::json!({});
        context.compact("summary".into(), &state).unwrap();
        assert_eq!(latest_user_text(&context.review_issuer(128)), "");
        assert_eq!(
            latest_user_text(&context.initialization_review_issuer(128).unwrap()),
            ""
        );
        context.admit_user("direct intent", &state).unwrap();
        context
            .admit_framework("framework notice".into(), &state)
            .unwrap();
        assert_eq!(
            latest_user_text(&context.review_issuer(128)),
            "direct intent"
        );
        assert_eq!(
            latest_user_text(&context.initialization_review_issuer(128).unwrap()),
            "direct intent"
        );
        assert!(context.initialization_review_issuer(0).is_err());
    }

    #[test]
    fn default_embeds_intent_but_custom_bindings_remain_literal() {
        let issuer = crate::context::Context::new("parent".into(), vec![], 65_536)
            .unwrap()
            .content(128)
            .unwrap();
        let bindings = [
            ("user_input", "intent {{ mode }}"),
            ("tool_name", "write"),
            ("tool_description", "Write <safe> UTF-8 file."),
            ("tool_args", "{}"),
            ("operation_kind", "tool"),
            ("mode", "plan"),
            ("mode_prompt", "Read {{ literally }}"),
        ];
        let current = prepare_review(&issuer, BUILTIN.prompt, &bindings).unwrap();
        assert!(current.system.contains("Read {{ literally }}"));
        assert!(
            current
                .system
                .contains("<untrusted_tool_description>\nWrite &lt;safe&gt; UTF-8 file.\n</untrusted_tool_description>")
        );
        assert!(
            current
                .system
                .contains("<untrusted_user_input>\nintent {{ mode }}\n</untrusted_user_input>")
        );
        assert!(
            matches!(&current.items[..], [ContentItem::User { parts, .. }]
            if matches!(&parts[..], [InputPart::Text { text }] if text == "begin"))
        );
        let custom = prepare_review(
            &issuer,
            "{{ user_input }} | {{ mode }} | {{ mode_prompt }}",
            &bindings,
        )
        .unwrap();
        assert_eq!(
            custom.system,
            "intent {{ mode }} | plan | Read {{ literally }}"
        );
        assert!(prepare_review(&issuer, "{{ permission_request }}", &bindings).is_err());
    }
}
