#![cfg(feature = "wasm")]
//! Metadata-only agents retain their modes without gaining Host execution ports.
use crate::typed_agent_fixture;
use async_trait::async_trait;
use microcrab_host_api::{Host, HostAPI, HostInfo, OsFamily, ToolError};
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::{
    Auditor, Client, Error, Session, VariableView,
    context::{Content, ContentItem},
    runtime::{
        Runtime, ToolRegistration,
        execution::ToolOutput,
        tool::{AllowTools, Tool, ToolContext},
        wasm::{WasmAgent, WasmLimits},
    },
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

struct Hostless;
impl microcrab_host_api::ProcessApi for Hostless {}
impl microcrab_host_api::ShellApi for Hostless {}
impl microcrab_host_api::FsApi for Hostless {}
impl microcrab_host_api::NetApi for Hostless {}
impl HostAPI for Hostless {}
#[async_trait]
impl Host for Hostless {
    fn info(&self) -> Result<HostInfo, ToolError> {
        Ok(HostInfo {
            os_family: OsFamily::Other,
            os: "hostless-fixture".into(),
            arch: "wasm32".into(),
            shell: String::new(),
            path_separator: "/".into(),
            newline: "\n".into(),
            capabilities: vec![],
            resources: None,
            virtual_shell: None,
            description: Some("No filesystem, process, shell or network execution ports.".into()),
        })
    }
}

fn catalog() -> Value {
    json!([{"name":"chat-agent", "modes":[{"name":"chat", "role":"both"}],
        "variables":{}, "tools":[]}])
}
fn component(catalog: &Value) -> Vec<u8> {
    let catalog = catalog.to_string();
    let escaped: String = catalog
        .bytes()
        .map(|byte| format!("\\{byte:02x}"))
        .collect();
    // Keep catalog data and canonical ABI allocation regions disjoint, including
    // fixtures at and above the tool-count bound. Invoke is never advertised.
    typed_agent_fixture::component(&format!(
        r#"(component
          (core module $m
            (memory (export "memory") 4)
            (global $heap (mut i32) (i32.const 65536))
            (data (i32.const 128) "{escaped}")
            (func (export "realloc") (param i32 i32 i32 i32) (result i32)
              (local $old i32) global.get $heap local.tee $old
              local.get 3 i32.add global.set $heap local.get $old)
            (func (export "describe") (param i32 i32) (result i32)
              i32.const 0 i32.const 0 i32.store
              i32.const 4 i32.const 128 i32.store
              i32.const 8 i32.const {length} i32.store i32.const 0)
            (func (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32) unreachable))
          (core instance $i (instantiate $m))
          (func (export "describe") (param "host-info" string) (result (result string (error string)))
            (canon lift (core func $i "describe") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
          (func (export "invoke") (param "host-info" string) (param "name" string) (param "arguments" string)
            (result (result string (error string)))
            (canon lift (core func $i "invoke") (memory (core memory $i "memory")) (realloc (core func $i "realloc")))))"#,
        length = catalog.len()
    ))
}
fn load(catalog: &Value) -> Result<WasmAgent, Error> {
    WasmAgent::load(
        &component(catalog),
        WasmLimits::default(),
        Arc::new(Hostless),
    )
}

#[test]
fn hostless_agent_keeps_validated_metadata_without_declared_tools() {
    let host = Hostless;
    assert!(host.tool_capabilities().is_empty());
    assert!(host.capabilities().supported_operations.is_empty());
    assert!(host.info().unwrap().capabilities.is_empty());
    let agent = load(&catalog()).unwrap();
    assert_eq!(agent.agents().len(), 1);
    assert_eq!(agent.agents()[0].name, "chat-agent");
    assert_eq!(agent.modes(), &["chat"]);
    assert!(agent.agents()[0].tools.is_empty());
    assert!(agent.tools().is_empty());
}

fn rejected(catalog: &Value, reason: &str) {
    let error = load(catalog).err().expect("malformed catalog must fail");
    assert!(error.to_string().contains(reason), "{error}");
}
#[test]
fn zero_tools_does_not_remove_agent_mode_name_schema_or_count_validation() {
    rejected(&json!([]), "1..=32 agents");
    let too_many_agents = Value::Array(
        (0..33)
            .map(|index| {
                let mut agent = catalog()[0].clone();
                agent["name"] = json!(format!("agent_{index}"));
                agent
            })
            .collect(),
    );
    rejected(&too_many_agents, "1..=32 agents");
    let mut invalid = catalog();
    invalid[0]["modes"] = Value::Array(
        (0..33)
            .map(|index| json!({"name":format!("mode_{index}"),"role":"both"}))
            .collect(),
    );
    rejected(&invalid, "invalid or duplicate agent mode");
    invalid = catalog();
    invalid[0]["modes"] = json!([]);
    rejected(&invalid, "invalid or duplicate agent mode");
    invalid = catalog();
    invalid[0]["name"] = json!("");
    rejected(&invalid, "invalid or duplicate agent name");
    invalid = catalog();
    invalid[0]["modes"][0]["name"] = json!("");
    rejected(&invalid, "invalid or duplicate agent mode");
    invalid = catalog();
    let duplicate = invalid[0].clone();
    invalid.as_array_mut().unwrap().push(duplicate);
    rejected(&invalid, "invalid or duplicate agent name");
    invalid = catalog();
    invalid[0]["modes"] = json!([{"name":"chat","role":"both"},{"name":"chat","role":"both"}]);
    rejected(&invalid, "invalid or duplicate agent mode");
    invalid = catalog();
    invalid[0]["tools"] =
        json!([{"name":"bad name", "description":"", "parameters":{"type":"object"}}]);
    rejected(&invalid, "invalid or duplicate catalog tool name");
    invalid[0]["tools"][0]["name"] = json!("valid");
    invalid[0]["tools"][0]["parameters"] = json!({"type":"string"});
    rejected(&invalid, "tool parameters must declare type object");
    invalid[0]["tools"][0]["parameters"] =
        json!({"type":"object", "properties":{"bad":{"type":"not-a-json-type"}}});
    rejected(&invalid, "invalid tool schema");

    let mut bounded = catalog();
    bounded[0]["tools"] = Value::Array(
        (0..128)
            .map(|index| {
                json!({
                    "name":format!("tool_{index}"), "description":"", "parameters":{"type":"object"}
                })
            })
            .collect(),
    );
    assert_eq!(load(&bounded).unwrap().tools().len(), 128);
    bounded[0]["tools"].as_array_mut().unwrap().push(json!({
        "name":"too_many", "description":"", "parameters":{"type":"object"}
    }));
    rejected(&bounded, "at most 128 tools");
}

struct ClientTool(Arc<AtomicUsize>);
#[async_trait]
impl Tool for ClientTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "client_echo".into(),
            description: "External client tool".into(),
            parameters: json!({"type":"object", "properties":{}, "additionalProperties":false}),
        }
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::text("client-result"))
    }
}
struct Model(AtomicUsize);
#[async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        assert_eq!(
            content
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            vec!["client_echo"]
        );
        let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        if !first {
            assert!(content.items.iter().any(|item| matches!(item,
                ContentItem::Tool { call_id, content } if call_id == "client-call" && content == "client-result")));
        }
        Ok(Response {
            provider_metadata: None,
            content: if first { String::new() } else { "done".into() },
            tool_calls: if first {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "client-call".into(),
                    name: "client_echo".into(),
                    arguments: json!({}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}

#[tokio::test]
async fn zero_declared_host_tools_preserve_external_tool_registration_and_execution() {
    let agent = load(&catalog()).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registrations: Vec<_> = agent
        .tools()
        .into_iter()
        .map(ToolRegistration::Wasm)
        .collect();
    registrations.push(ToolRegistration::External(Arc::new(ClientTool(
        calls.clone(),
    ))));
    let runtime = Runtime::new(registrations, Arc::new(AllowTools::new(["client_echo"]))).unwrap();
    let session = Session::builder(Model(AtomicUsize::new(0)), runtime)
        .host(Arc::new(Hostless))
        .agent(agent.agents()[0].clone())
        .build()
        .unwrap();
    session.select_mode("chat").await.unwrap();
    session.submit("Use the allowed client tool").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        session
            .context_snapshot()
            .unwrap()
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["client_echo"]
    );
    session.close().await.unwrap();
}
