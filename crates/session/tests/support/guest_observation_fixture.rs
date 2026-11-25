//! Inline wire fixture. Keep its discriminants aligned with the canonical WIT.
pub const IMPORT: &str = r#"
  (import "microcrab:session/observation@0.2.2" (instance $observation
    (type $level-t (enum "debug" "info" "warn" "error"))
    (export "level" (type $level (eq $level-t)))
    (type $code-t (enum "checkpoint" "failed" "limit-exceeded" "invalid-envelope" "unexpected-reply" "effect-uncertain"))
    (export "event-code" (type $code (eq $code-t)))
    (type $phase-t (enum "initialize" "connect" "send" "receive" "classify" "decode" "dispatch" "cleanup"))
    (export "phase" (type $phase (eq $phase-t)))
    (type $method-t (enum "none" "target" "page" "runtime" "dom" "input" "web-mcp" "browser"
        "target-get-targets" "target-attach" "target-detach" "runtime-enable" "runtime-disable"
        "runtime-call-function-on" "page-capture-screenshot" "web-mcp-enable" "web-mcp-invoke"))
    (export "method" (type $method (eq $method-t)))
    (type $counter-t (enum "none" "raw-messages" "raw-bytes" "decoded-bytes" "elapsed-ms" "decoded-tokens"))
    (export "counter" (type $counter (eq $counter-t)))
    (type $confirmation-t (enum "unknown" "not-confirmed" "confirmed"))
    (export "confirmation" (type $confirmation (eq $confirmation-t)))
    (type $event-t (record (field "severity" $level) (field "code" $code) (field "stage" $phase)
        (field "method-family" $method) (field "metric" $counter) (field "measured" u32)
        (field "limit" u32) (field "confirmation" $confirmation)))
    (export "event" (type $event (eq $event-t)))
    (export "record-event" (func (param "value" $event)))
  ))
"#;

pub fn instrument(text: &str, describe: &str, invoke: &str) -> String {
    text
        .replacen(
            "(component",
            &format!(r#"(component
            {IMPORT}
            (core func $obs (canon lower (func $observation "record-event")))
            (core instance $sink (export "record-event" (func $obs)))"#),
            1,
        )
        .replacen(
            "(core module $m",
            r#"(core module $m
            (import "observer" "record-event" (func $record (param i32 i32 i32 i32 i32 i32 i32 i32)))"#,
            1,
        )
        .replace("(instantiate $m)", "(instantiate $m (with \"observer\" (instance $sink)))")
        .replace(
            "(func (export \"describe\") (param i32 i32) (result i32)",
            &format!("(func (export \"describe\") (param i32 i32) (result i32) {describe}"),
        )
        .replace("(local $remaining i32)", &format!("(local $remaining i32) {invoke}"))
}

pub const NOTE: &str = "i32.const 2 i32.const 1 i32.const 3 i32.const 11 i32.const 2 i32.const 42 i32.const 100 i32.const 1 call $record";

/// The typed init fixture has its own core module. Thread the same lowered
/// observer through its source instance without adding any guest export.
pub fn component_with_reporting_init(text: &str) -> Vec<u8> {
    let init = super::typed_agent_fixture::READY_INIT_BODY
        .replace("  i32.const 0", &format!("  {NOTE}\n  i32.const 0"));
    let typed = super::typed_agent_fixture::component_text_with_init(text, &init)
        .replace(
            "(core module $fixture_adapter_module",
            r#"(core module $fixture_adapter_module
              (import "source" "record-event" (func $record (param i32 i32 i32 i32 i32 i32 i32 i32)))"#,
        )
        .replace(
            r#"(export "memory" (memory $fixture_source_memory))"#,
            r#"(export "memory" (memory $fixture_source_memory))
              (export "record-event" (func $obs))"#,
        );
    wat::parse_str(typed).expect("valid typed observation fixture")
}
