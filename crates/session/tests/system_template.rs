//! SYSTEM rendering uses agent-owned structured variables and remains frozen per epoch.
use microcrab_agent_types::{AgentInfo, Hint, Rule, Variable};
use microcrab_auditor::Auditor;
use microcrab_protocol::{Response, Usage};
use microcrab_session::context::Content;
use microcrab_session::context::system::{DEFAULT_SYSTEM, render};
use microcrab_session::runtime::{Runtime, tool::DenyAll};
use microcrab_session::{Client, Error, Session};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

fn agent() -> AgentInfo {
    let mut variables = Variable::default();
    variables
        .inject("description", "Agent <description> & details")
        .unwrap();
    variables
        .inject(
            "rules",
            vec![
                Rule {
                    name: "first-rule".into(),
                    description: "First rule text".into(),
                },
                Rule {
                    name: "second-rule".into(),
                    description: "Second rule text".into(),
                },
            ],
        )
        .unwrap();
    variables
        .inject(
            "hints",
            vec![
                Hint {
                    name: "first-hint".into(),
                    description: "First <hint> & details".into(),
                },
                Hint {
                    name: "second-hint".into(),
                    description: "Second hint text".into(),
                },
            ],
        )
        .unwrap();
    AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "metadata-only-name".into(),
        modes: vec![],
        tools: vec![],
        variables,
    }
}

#[test]
fn unresolved_variables_fail_strictly_instead_of_disappearing() {
    for (template, info) in [
        ("{{ missing }}", None),
        ("{{ agent.description }}", None),
        ("{{ agent.missing }}", Some(agent())),
        ("{{ agent.missing | json_encode }}", Some(agent())),
        ("{{ agent.name }}", Some(agent())),
        ("{{ agent.modes }}", Some(agent())),
        ("{{ agent.tools }}", Some(agent())),
        ("{{ workspace }}", Some(agent())),
    ] {
        assert!(
            matches!(render(template, info.as_ref()), Err(Error::Config(_))),
            "{template}"
        );
    }
    assert_eq!(render("STATIC", Some(&agent())).unwrap(), "STATIC");
}

#[test]
fn optional_values_remain_guarded_and_defaults_do_not_hide_required_values() {
    assert_eq!(
        render(
            "{% if agent is defined %}present{% else %}absent{% endif %}|{{ client_prompt | default(value=\"fallback\") }}",
            None,
        )
        .unwrap(),
        "absent|fallback"
    );
    assert_eq!(
        render(
            "{% if agent.missing is defined %}unexpected{% else %}optional absent{% endif %}|{{ agent.missing | default(value=\"fallback\") }}",
            Some(&agent()),
        )
        .unwrap(),
        "optional absent|fallback"
    );
}

#[test]
fn injected_strings_are_literal_and_nonempty_valid_templates_are_required() {
    let mut info = agent();
    info.variables
        .inject(
            "literal",
            "{{ missing }}{% if missing %}<raw> & intact{% endif %}",
        )
        .unwrap();
    assert_eq!(
        render("{{ agent.literal }}", Some(&info)).unwrap(),
        "{{ missing }}{% if missing %}<raw> & intact{% endif %}"
    );
    assert_eq!(
        render("", None).unwrap(),
        render(DEFAULT_SYSTEM, None).unwrap()
    );
    let rendered = render(DEFAULT_SYSTEM, None).unwrap();
    assert_variable_injection_hint_is_rendered(&rendered);
    assert!(!rendered.contains("{{ \""));
    for template in [
        " \n\t",
        "{% if true %}",
        "{{",
        "{% if false %}hidden{% endif %}",
    ] {
        assert!(
            matches!(render(template, None), Err(Error::Config(_))),
            "{template}"
        );
    }
}

#[test]
fn arbitrary_structured_agent_values_render_exactly_without_stringification_or_appending() {
    let mut info = agent();
    let payload = json!({
        "enabled": true,
        "count": 7,
        "ratio": 1.5,
        "nested": {"label": "<typed> & intact"},
        "entries": [{"name":"alpha","value":2}, {"name":"beta","value":3}],
        "optional": null,
    });
    info.variables.inject("payload", &payload).unwrap();
    assert_eq!(info.variables.get("payload"), Some(&payload));
    assert_eq!(render(
        "{{ agent.payload.enabled }}|{{ agent.payload.count }}|{{ agent.payload.ratio }}|{{ agent.payload.nested.label }}|{{ agent.payload.entries[0].name }}={{ agent.payload.entries[0].value }};{{ agent.payload.entries[1].name }}={{ agent.payload.entries[1].value }};|{{ agent.payload.optional | json_encode }}",
        Some(&info),
    ).unwrap(), "true|7|1.5|<typed> & intact|alpha=2;beta=3;|null");
    assert_eq!(
        render("{{ agent.payload | json_encode }}", Some(&info)).unwrap(),
        serde_json::to_string(&payload).unwrap()
    );
    assert_eq!(
        render(
            "{{ agent.payload | json_encode(pretty=true) }}",
            Some(&info)
        )
        .unwrap(),
        serde_json::to_string_pretty(&payload).unwrap()
    );
    assert_eq!(
        render(
            "{{ agent.payload | json_encode(pretty=\"ignored\") }}",
            Some(&info)
        )
        .unwrap(),
        serde_json::to_string(&payload).unwrap()
    );
}

fn assert_variable_injection_hint_is_rendered(rendered: &str) {
    assert_eq!(rendered.matches("**variable-injection**").count(), 1);
    assert_eq!(rendered.matches("**private-bytes**").count(), 1);
    for guidance in [
        "If the producer advertises private byte capture",
        "Never use job read for private bytes",
        "Release the source Job after redirect is confirmed",
        r#"{"image_url":"{{var:base64:preview}}"}"#,
        "Do not repeat the same invalid arguments",
    ] {
        assert!(rendered.contains(guidance), "{guidance}");
    }
    let mut without_grammar = rendered.to_owned();
    for placeholder in [
        "{{var:KEY}}",
        "{{var:KEY:/a/b}}",
        "{{var:string:KEY}}",
        "{{var:number:KEY:/a/b}}",
        "{{var:boolean:KEY}}",
        "{{secret:KEY}}",
        "{{var:base64:KEY}}",
        "{{var:base64:@<selector>}}",
        "{{var:base64:preview}}",
    ] {
        assert_eq!(rendered.matches(placeholder).count(), 1, "{placeholder}");
        without_grammar = without_grammar.replace(placeholder, "");
    }
    assert!(
        !without_grammar.contains("{{"),
        "unexpanded Tera expression in rendered SYSTEM: {without_grammar}"
    );
}

#[test]
fn default_template_renders_description_and_each_rule_and_hint_once_in_order() {
    let rendered = render(DEFAULT_SYSTEM, Some(&agent())).unwrap();
    for text in [
        "Agent <description> & details",
        "* **first-rule**\n   First rule text",
        "* **second-rule**\n   Second rule text",
        "* **first-hint**\n   First <hint> & details",
        "* **second-hint**\n   Second hint text",
    ] {
        assert_eq!(rendered.matches(text).count(), 1, "{text}");
    }
    assert!(rendered.find("first-rule").unwrap() < rendered.find("second-rule").unwrap());
    assert!(rendered.find("first-hint").unwrap() < rendered.find("second-hint").unwrap());
    assert!(!rendered.contains("metadata-only-name"));
    assert_variable_injection_hint_is_rendered(&rendered);
    assert!(!rendered.contains("{%"));
}

#[derive(Default)]
struct Capture(Mutex<Vec<Content>>);
#[async_trait::async_trait]
impl Client for Capture {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.lock().unwrap().push(content.clone());
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage {
                input_tokens: Some(2),
                output_tokens: Some(1),
                samples: 1,
                ..Default::default()
            },
        })
    }
}
#[tokio::test]
async fn public_one_shot_builder_renders_explicit_system() {
    let session = Session::builder_arc(Arc::new(Capture::default()), runtime())
        .kind(microcrab_session::SessionKind::OneShot)
        .system("PUBLIC {{ client_prompt }}")
        .client_prompt("  {{ literal }} ")
        .build()
        .unwrap();
    assert_eq!(
        session.context_snapshot().unwrap().system,
        "PUBLIC   {{ literal }} "
    );
    assert!(
        Session::builder_arc(Arc::new(Capture::default()), runtime())
            .kind(microcrab_session::SessionKind::OneShot)
            .system("{{ missing }}")
            .build()
            .is_err()
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn prompt_snapshot_survives_restore_fork_and_configures_real_helpers() {
    use microcrab_session::context::system::PromptConfig;
    use microcrab_session::runtime::journal::MemoryJournal;
    let config = PromptConfig {
        system_template: "FROZEN {{ client_prompt }}".into(),
        client_prompt: "  {{ authored_literal }}\n".into(),
        rename_template: "CUSTOM RENAME {{ user_input }}".into(),
        compact_template: "CUSTOM COMPACT {{ user_input }}".into(),
        ..Default::default()
    };
    let journal = MemoryJournal::new();
    let client = Arc::new(Capture::default());
    let source = Session::builder_arc(client.clone(), runtime())
        .session_id("prompt-source")
        .prompt_config(config.clone())
        .journal(journal.clone())
        .build()
        .unwrap();
    source.submit("first").await.unwrap();
    let frozen = source.context_snapshot().unwrap().system;
    source.rename_with_model("{{ nested }}").await.unwrap();
    assert_eq!(
        client.0.lock().unwrap().last().unwrap().system,
        "CUSTOM RENAME {{ nested }}"
    );
    source
        .compact_with_model("literal {{ missing }}", json!({}))
        .await
        .unwrap();
    let requests = client.0.lock().unwrap().clone();
    let compact = requests.last().unwrap();
    assert_eq!(compact.system, frozen);
    assert!(
        serde_json::to_string(&compact.items)
            .unwrap()
            .contains("CUSTOM COMPACT literal {{ missing }}")
    );
    let checkpoint = journal.latest().unwrap();
    assert_eq!(checkpoint.prompt_config.as_ref(), Some(&config));
    let before_recover = serde_json::to_value(source.context_snapshot().unwrap()).unwrap();
    let mut incompatible = checkpoint.clone();
    incompatible.prompt_config.as_mut().unwrap().client_prompt = "replacement".into();
    assert!(source.recover(incompatible).await.is_err());
    assert_eq!(source.prompt_config(), &config);
    assert_eq!(
        serde_json::to_value(source.context_snapshot().unwrap()).unwrap(),
        before_recover
    );
    let restored = Session::restore(
        Session::builder_arc(client.clone(), runtime())
            .session_id("prompt-restored")
            .system("{{ invalid_new_default }}"),
        checkpoint.clone(),
    )
    .await
    .unwrap();
    assert_eq!(restored.prompt_config(), &config);
    assert_eq!(restored.context_snapshot().unwrap().system, frozen);
    let fork = restored
        .fork(
            Session::builder_arc(client.clone(), runtime())
                .session_id("prompt-fork")
                .client_prompt("replacement"),
        )
        .await
        .unwrap();
    assert_eq!(fork.prompt_config(), &config);
    assert_eq!(
        serde_json::to_value(fork.context_snapshot().unwrap()).unwrap(),
        serde_json::to_value(restored.context_snapshot().unwrap()).unwrap()
    );
    let mut legacy = checkpoint;
    legacy.prompt_config = None;
    let old = Session::restore(
        Session::builder_arc(client, runtime())
            .session_id("prompt-legacy")
            .client_prompt("new default must not apply"),
        legacy,
    )
    .await
    .unwrap();
    assert_eq!(old.prompt_config(), &PromptConfig::default());
    assert_eq!(old.context_snapshot().unwrap().system, frozen);
    old.close().await.unwrap();
    fork.close().await.unwrap();
    restored.close().await.unwrap();
    source.close().await.unwrap();
}

fn runtime() -> Runtime {
    Runtime::new(vec![], Arc::new(DenyAll)).unwrap()
}

#[tokio::test]
async fn session_default_system_is_rendered_without_an_agent() {
    let client = Arc::new(Capture::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .build()
        .unwrap();
    let outcome = session.submit("inspect").await.unwrap();
    assert_eq!(outcome.usage.input_tokens, Some(2));
    assert_eq!(outcome.usage.output_tokens, Some(1));
    let system = client.0.lock().unwrap()[0].system.clone();
    assert_eq!(system, render(DEFAULT_SYSTEM, None).unwrap());
    assert!(system.contains("# Rule Framework & Execution Hierarchy"));
    assert!(system.contains("clarification-and-decision"));
    assert!(!system.contains("# Agent"));
    assert_variable_injection_hint_is_rendered(&system);
    assert!(!system.contains("{%"));
    session.close().await.unwrap();
}

#[tokio::test]
async fn session_variables_cannot_override_agent_system_and_same_epoch_stays_stable() {
    let mut info = agent();
    info.modes = vec![microcrab_agent_types::AgentMode {
        name: "root".into(),
        description: String::new(),
        prompt: String::new(),
        role: microcrab_agent_types::AgentRole::Both,
        permissions: vec![],
    }];
    let template =
        "SYSTEM {{ agent.description }}: {% for rule in agent.rules %}{{ rule.name }};{% endfor %}";
    let expected = render(template, Some(&info)).unwrap();
    let client = Arc::new(Capture::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .agent(info)
        .system(template)
        .build()
        .unwrap();
    session.select_mode("root").await.unwrap();
    let state = session.state_handle();
    assert!(
        state
            .inject("agent", json!({"description":"forged","rules":[]}))
            .await
            .is_err()
    );
    state
        .inject("description", json!("also forged"))
        .await
        .unwrap();
    session.submit("first").await.unwrap();
    let first_epoch = session.state().epoch_id;
    state
        .inject("description", json!("changed during epoch"))
        .await
        .unwrap();
    session.submit("second").await.unwrap();
    assert_eq!(session.state().epoch_id, first_epoch);
    assert_eq!(
        state.extract("description"),
        Some(json!("changed during epoch"))
    );
    {
        let requests = client.0.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].system, expected);
        assert_eq!(requests[1].system, expected);
        assert_eq!(requests[0].tools, requests[1].tools);
    }
    session
        .compact("summary", json!({"description":"new epoch forgery"}))
        .await
        .unwrap();
    session.submit("third").await.unwrap();
    assert_eq!(session.state().epoch_id, first_epoch + 1);
    assert_eq!(client.0.lock().unwrap()[2].system, expected);
    session.close().await.unwrap();
}
