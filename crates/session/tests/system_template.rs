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
            "{% if agent is defined %}present{% else %}absent{% endif %}|{{ missing | default(value=\"fallback\") }}",
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
    for template in [
        "",
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
        "{{ agent.payload.enabled }}|{{ agent.payload.count }}|{{ agent.payload.ratio }}|{{ agent.payload.nested.label }}|{% for entry in agent.payload.entries %}{{ entry.name }}={{ entry.value }};{% endfor %}|{{ agent.payload.optional | json_encode }}",
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
    assert!(!rendered.contains("{{"));
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
    assert!(!system.contains("{{"));
    assert!(!system.contains("{%"));
    session.close().await.unwrap();
}

#[tokio::test]
async fn session_variables_cannot_override_agent_system_and_same_epoch_stays_stable() {
    let info = agent();
    let template =
        "SYSTEM {{ agent.description }}: {% for rule in agent.rules %}{{ rule.name }};{% endfor %}";
    let expected = render(template, Some(&info)).unwrap();
    let client = Arc::new(Capture::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .agent(info)
        .system(template)
        .build()
        .unwrap();
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
