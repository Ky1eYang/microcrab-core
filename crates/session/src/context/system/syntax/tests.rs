use super::*;
use crate::context::system::{DEFAULT_SYSTEM, render};
use microcrab_agent_types::{AgentInfo, Variable};
use serde_json::json;

fn agent(rules: usize, payload: Value) -> AgentInfo {
    let mut variables = Variable::default();
    variables
        .inject(
            "rules",
            vec![json!({"name":"rule","description":"description"}); rules],
        )
        .unwrap();
    variables.inject("payload", payload).unwrap();
    AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "test".into(),
        modes: vec![],
        tools: vec![],
        variables,
    }
}

#[test]
fn bounded_subset_accepts_embedded_conditions_literal_paths_filters_and_quoted_delimiters() {
    validate(DEFAULT_SYSTEM).unwrap();
    for source in [
        "{# comment {{ ignored }} #}{% if agent is defined %}{{ agent.description | default(value='}}%}') | safe }}{% endif %}",
        "{{ agent.payload['value'] | json_encode(pretty=true) | safe }}",
        "{% if agent.rules is defined and agent.rules | length > 0 %}{% for rule in agent.rules %}{% if rule.name %}{{ rule.name }}{% endif %}{% endfor %}{% endif %}",
        "{% if not (agent is defined) %}none{% elif client_prompt is defined and client_prompt %}{{ client_prompt | trim }}{% else %}empty{% endif %}",
    ] {
        validate(source).unwrap();
    }
    let info = agent(2, json!({"value":"{{ literal }}"}));
    assert_eq!(
        render("{{ agent.payload['value'] | upper }}", Some(&info)).unwrap(),
        "{{ LITERAL }}"
    );
    assert_eq!(
        render(
            "{% for rule in agent.rules %}{{ rule.name }};{% endfor %}",
            Some(&info)
        )
        .unwrap(),
        "rule;rule;"
    );
}

#[test]
fn bounded_subset_rejects_unbounded_or_constructive_tera_before_execution() {
    for source in [
        "{{ range(end=100000) }}",
        "{% for rule in agent.rules %}{% for hint in agent.hints %}{% endfor %}{% endfor %}",
        "{% for item in agent.payload %}{% endfor %}",
        "{% set captured %}body{% endset %}",
        "{% set x = agent.payload %}",
        "{% include 'other' %}",
        "{% extends 'other' %}",
        "{% component Example() %}body{% endcomponent %}",
        "{% macro repeat() %}body{% endmacro %}",
        "{% raw %}raw{% endraw %}",
        "{{ client_prompt ~ client_prompt }}",
        "{{ 5 * 100000000 }}",
        "{{ agent.payload | indent(width=1000000) }}",
        "{{ agent.payload | json_encode | json_encode }}",
        "{% if agent.payload | json_encode %}hidden{% endif %}",
        "{{ agent.payload | default(value=agent.other) }}",
        "{{ agent.payload() }}",
        "{{ loop.index }}",
        "{% if true %}one{% else %}two{% else %}three{% endif %}",
        "{% if true %}{% endfor %}",
        "{{ 'unclosed }}",
        "{# unclosed",
    ] {
        assert!(validate(source).is_err(), "{source}");
    }
    let source = "{{ client_prompt }}".repeat(MAX_TAGS + 1);
    assert!(validate(&source).is_err());
    let source = format!(
        "{}text{}",
        "{% if true %}".repeat(MAX_DEPTH + 1),
        "{% endif %}".repeat(MAX_DEPTH + 1)
    );
    assert!(validate(&source).is_err());
}

#[test]
fn bounded_subset_rejects_weighted_loop_body_data_and_shape_budgets() {
    let source = format!(
        "{{% for rule in agent.rules %}}{}{{% endfor %}}",
        "{% if rule.name %}{% endif %}".repeat(40)
    );
    let work = validate(&source).unwrap();
    assert!(
        work.validate_data(Some(&agent(128, Value::Null)), None)
            .is_err()
    );
    let source = format!(
        "{{% for rule in agent.rules %}}{}{{% endfor %}}",
        "{{ agent.payload }}".repeat(10)
    );
    let work = validate(&source).unwrap();
    assert!(
        work.validate_data(Some(&agent(64, json!("x".repeat(220000)))), None)
            .is_err()
    );
    let work = validate("{% for rule in agent.rules %}literal{% endfor %}").unwrap();
    for source in [json!("x".repeat(200000)), json!({"name":"value"})] {
        let mut info = agent(0, Value::Null);
        info.variables.inject("rules", source).unwrap();
        assert!(work.validate_data(Some(&info), None).is_err());
    }
    let mut info = agent(0, Value::Null);
    info.variables
        .inject("hints", json!({"name":"value"}))
        .unwrap();
    assert!(
        validate("{% for hint in agent.hints %}literal{% endfor %}")
            .unwrap()
            .validate_data(Some(&info), None)
            .is_err()
    );
    let work = validate("{{ agent.payload }}").unwrap();
    assert!(
        work.validate_data(Some(&agent(129, Value::Null)), None)
            .is_err()
    );
    assert!(
        work.validate_data(Some(&agent(0, json!("x".repeat(MAX_DATA_BYTES + 1)))), None)
            .is_err()
    );
    assert!(
        work.validate_data(Some(&agent(0, json!(vec![0; MAX_DATA_NODES + 1]))), None)
            .is_err()
    );
    let mut nested = Value::Null;
    for _ in 0..MAX_DEPTH + 1 {
        nested = json!([nested]);
    }
    assert!(work.validate_data(Some(&agent(0, nested)), None).is_err());
}

#[test]
fn bounded_subset_limits_rendered_output_from_an_admitted_known_list() {
    let source = format!(
        "{{% for rule in agent.rules %}}{}{{% endfor %}}",
        "x".repeat(10000)
    );
    let info = agent(128, Value::Null);
    validate(&source)
        .unwrap()
        .validate_data(Some(&info), None)
        .unwrap();
    assert!(render(&source, Some(&info)).is_err());
}
