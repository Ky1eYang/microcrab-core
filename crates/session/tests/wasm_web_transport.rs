//! Component requirements follow exact imports without changing ordinary linkage.
#![cfg(feature = "wasm")]

use crate::typed_agent_fixture;
use microcrab_session::{
    ComponentCapability,
    runtime::wasm::{WasmAgent, WasmLimits},
};
use std::sync::Arc;

fn component(agent_name: &str, extra_import: &str, import_web_transport: bool) -> Vec<u8> {
    component_with_tools(
        agent_name,
        extra_import,
        import_web_transport,
        &["connect", "status", "list_pages", "snapshot"],
    )
}

fn component_with_tools(
    agent_name: &str,
    extra_import: &str,
    import_web_transport: bool,
    tools: &[&str],
) -> Vec<u8> {
    let tools: Vec<_> = tools.iter().map(|name| {
        serde_json::json!({"name":name,"description":"fixture","parameters":{"type":"object"}})
    }).collect();
    let mut agent = serde_json::json!({"name":agent_name,"modes":[{"name":"observe","role":"both"}],"variables":{},"tools":tools});
    if import_web_transport && tools.iter().any(|tool| tool["name"] == "connect") {
        agent["tool_bindings"] = serde_json::json!({"connect":{"web":{"kind":"open","endpoint":"/endpoint"}}});
    }
    let catalog = serde_json::json!([agent]).to_string();
    let escaped: String = catalog.bytes().map(|b| format!("\\{b:02x}")).collect();
    let web_import = if import_web_transport {
        r#"(import "microcrab:host/web-transport@0.1.0" (instance
        (type $error_raw (enum "unsupported" "denied" "invalid-request" "limit" "cancelled" "timeout" "stale" "closed" "outcome-unknown" "io"))
        (export "transport-error" (type $error (eq $error_raw)))
        (type $response_raw (record (field "status" u16) (field "body" string)))
        (export "http-response" (type $response (eq $response_raw)))
        (type $received_raw (variant (case "message" string) (case "timeout") (case "closed")))
        (export "ws-receive-result" (type $received (eq $received_raw)))
        (type $id_raw string)
        (export "connection-id" (type $id (eq $id_raw)))
        (export "http-get" (func (param "url" string) (param "max-bytes" u32) (result (result $response (error $error)))))
        (export "ws-open" (func (param "url" string) (result (result $id (error $error)))))
        (export "ws-status" (func (param "id" $id) (result (result (error $error)))))
        (export "ws-send" (func (param "id" $id) (param "text" string) (result (result (error $error)))))
        (export "ws-receive" (func (param "id" $id) (param "max-bytes" u32) (result (result $received (error $error)))))
        (export "ws-close" (func (param "id" $id) (result (result (error $error)))))
      ))"#
    } else {
        ""
    };
    typed_agent_fixture::component(&format!(
        r#"(component
      {extra_import}
      {web_import}
      (core module $m
        (memory (export "memory") 1)
        (global $heap (mut i32) (i32.const 4096))
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

#[test]
fn required_capabilities_follow_imports_not_guest_names_or_host_support() {
    let temp = tempfile::tempdir().unwrap();
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(temp.path(), Default::default()).unwrap());
    let load = |bytes: &[u8]| WasmAgent::load(bytes, WasmLimits::default(), host.clone());

    let agent = load(&component("inspection", "", true)).unwrap();
    assert_eq!(agent.agents()[0].name, "inspection");
    assert_eq!(
        agent.required_capabilities(),
        [ComponentCapability::WebTransport]
    );
    assert!(!agent.required_capabilities()[0].available_on_project(host.as_ref(), Some("project")));
    assert_eq!(agent.tools().len(), 4);
    let connect = agent
        .tools()
        .into_iter()
        .find(|tool| tool.definition().name == "connect")
        .unwrap();
    assert_eq!(
        connect.permission_defaults()[0].action,
        microcrab_session::policy::Action::Ask
    );
    assert_eq!(
        connect.secret_delivery(),
        microcrab_session::runtime::tool::SecretDelivery::ForwardPlaceholders
    );
    assert!(connect.uses_literal_sources());
    assert!(
        agent
            .tools()
            .iter()
            .filter(|tool| tool.definition().name != "connect")
            .all(|tool| !tool.uses_literal_sources())
    );

    // Neither the name nor the tool names imply any component requirement.
    let ordinary = load(&component("browser", "", false)).unwrap();
    assert_eq!(ordinary.agents()[0].name, "browser");
    assert!(ordinary.required_capabilities().is_empty());
    assert!(
        ordinary
            .tools()
            .iter()
            .all(|tool| !tool.uses_literal_sources() && tool.permission_defaults().is_empty())
    );
}

#[test]
fn observation_is_additive_in_the_restricted_web_transport_profile() {
    let temp = tempfile::tempdir().unwrap();
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(temp.path(), Default::default()).unwrap());
    let valid = crate::guest_observation_fixture::IMPORT;
    // These are link-only fixtures. Old guests do not gain an observer requirement.
    for observation in ["", valid] {
        let agent = WasmAgent::load(
            &component("inspection", observation, true),
            WasmLimits::default(),
            host.clone(),
        )
        .unwrap();
        assert_eq!(
            agent.required_capabilities(),
            [ComponentCapability::WebTransport]
        );
    }
    let extra_effect = valid.replace("(export \"record-event\" (func (param \"value\" $event)))", "(export \"record-event\" (func (param \"value\" $event))) (export \"read-back\" (func (result string)))");
    assert_ne!(extra_effect, valid);
    for invalid in [extra_effect, valid.replace("@0.2.2", "@0.2.3")] {
        let error = WasmAgent::load(
            &component("inspection", &invalid, true),
            WasmLimits::default(),
            host.clone(),
        )
        .err()
        .unwrap();
        assert!(error.public_reason().unwrap().contains("unexpected import"));
    }
}

#[test]
fn scoped_transport_import_requires_the_exact_legacy_companion() {
    let temp = tempfile::tempdir().unwrap();
    let host = Arc::new(microcrab_host_local::LocalHost::new(temp.path(), Default::default()).unwrap());
    let scoped = r#"(import "microcrab:host/web-transport-scoped@0.1.1" (instance))"#;
    for (legacy, reason) in [(false, "exact legacy companion"), (true, "complete scoped web transport")] {
        let error = WasmAgent::load(&component("inspection", scoped, legacy), WasmLimits::default(), host.clone())
            .err().unwrap();
        assert!(error.public_reason().unwrap().contains(reason), "{error}");
    }
}

#[test]
fn discovery_validates_capability_imports_without_prescribing_a_tool_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(temp.path(), Default::default()).unwrap());
    for (import, web, reason) in [
        // This additional import precedes the capability import in the fixture.
        (
            r#"(import "microcrab:host/net@0.1.0" (instance))"#,
            true,
            "unexpected import",
        ),
        (
            r#"(import "microcrab:host/web-transport@0.1.0" (instance))"#,
            false,
            "complete web transport interface",
        ),
        (
            r#"(import "microcrab:host/web-transport@0.1.1" (instance))"#,
            false,
            "unsupported component import",
        ),
        (
            r#"(import "microcrab:host/web-transport@0.1.1" (instance))"#,
            true,
            "unexpected import",
        ),
    ] {
        let error = WasmAgent::load(
            &component("inspection", import, web),
            WasmLimits::default(),
            host.clone(),
        )
        .err()
        .unwrap();
        assert!(error.public_reason().unwrap().contains(reason), "{error}");
    }

    let bytes = component_with_tools(
        "inspection",
        "",
        true,
        &["connect", "status", "list_pages", "unrelated"],
    );
    let agent = WasmAgent::load(&bytes, WasmLimits::default(), host).unwrap();
    assert_eq!(agent.required_capabilities(), [ComponentCapability::WebTransport]);
    assert!(agent.agents()[0].tools.iter().any(|tool| tool.name == "unrelated"));
}

/// Optional artifact gate. It discovers all three shipped guests against the
/// same Host without dialing anything or requiring registration metadata.
#[test]
#[ignore = "set MICROCRAB_BROWSER_WASM, MICROCRAB_CODING_WASM and MICROCRAB_UNIVERSAL_WASM to staged guest bytes"]
fn delivered_components_link_through_the_canonical_loader() {
    let temp = tempfile::tempdir().unwrap();
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(temp.path(), Default::default()).unwrap());
    let load = |variable: &str| {
        let path = std::env::var_os(variable).unwrap_or_else(|| panic!("set {variable}"));
        WasmAgent::load(
            &std::fs::read(path).unwrap(),
            WasmLimits::default(),
            host.clone(),
        )
        .unwrap()
    };
    for variable in ["MICROCRAB_CODING_WASM", "MICROCRAB_UNIVERSAL_WASM"] {
        let agent = load(variable);
        assert!(!agent.agents().is_empty());
        assert!(!agent.tools().is_empty());
        assert!(agent.required_capabilities().is_empty());
    }
    let agent = load("MICROCRAB_BROWSER_WASM");
    assert_eq!(
        agent.required_capabilities(),
        [ComponentCapability::WebTransport]
    );
    assert!(!agent.required_capabilities()[0].available_on_project(host.as_ref(), Some("project")));
    let names = agent
        .tools()
        .into_iter()
        .map(|tool| tool.definition().name)
        .collect::<std::collections::BTreeSet<_>>();
    let basic = ["connect", "list_pages", "snapshot", "status"];
    let expected = if std::env::var_os("MICROCRAB_BROWSER_INTERACTION").is_some() {
        assert!(agent.agents()[0].modes.iter().any(|mode| mode.name == "interaction"));
        let current = ["press_key", "screenshot"].into_iter().filter(|_| std::env::var_os("MICROCRAB_BROWSER_PRIVATE_BYTES").is_some());
        let tabs = ["new_tab", "select_tab", "history_navigation"].into_iter().filter(|_| std::env::var_os("MICROCRAB_BROWSER_TABS").is_some());
        let scoped = ["close_tab", "scroll", "find", "set_checked", "select_option", "wait_for", "webmcp_list", "webmcp_invoke"].into_iter().filter(|_| std::env::var_os("MICROCRAB_BROWSER_SCOPED").is_some());
        let jobs = ["configure_connection", "read_network_detail"].into_iter().filter(|_| std::env::var_os("MICROCRAB_BROWSER_CONNECTION_JOBS").is_some());
        let scripts = ["execute_js"].into_iter().filter(|_| std::env::var_os("MICROCRAB_BROWSER_EXECUTE_JS").is_some());
        basic.into_iter().chain(["click", "type_text", "navigate"]).chain(current).chain(tabs).chain(scoped).chain(jobs).chain(scripts).map(str::to_owned).collect()
    } else {
        basic.into_iter().map(str::to_owned).collect()
    };
    assert_eq!(names, expected);
    if std::env::var_os("MICROCRAB_BROWSER_EXECUTE_JS").is_some() {
        use microcrab_permission::{Action, PermissionConfig, PolicySnapshot, compile, evaluate_compiled};
        use serde_json::json;
        for mode in &agent.agents()[0].modes {
            let snapshot = PolicySnapshot {
                revision: 0,
                config: PermissionConfig {
                    agent: agent.agents()[0].name.clone(),
                    mode: mode.name.clone(),
                    defaults: mode.permissions.clone(),
                    ..Default::default()
                },
            };
            let script = compile(&snapshot, "execute_js").unwrap();
            for (args, interaction_action) in [
                (json!({}), Action::Allow),
                (json!({"world":"isolated"}), Action::Allow),
                (json!({"world":"main"}), Action::Ask),
            ] {
                assert_eq!(evaluate_compiled(&script, &args).unwrap(),
                    if mode.name == "observe" { Action::Deny } else { interaction_action });
            }
            if mode.name == "observe" {
                for name in ["connect", "screenshot", "configure_connection", "read_network_detail"] {
                    assert_eq!(evaluate_compiled(&compile(&snapshot, name).unwrap(), &json!({})).unwrap(), Action::Allow);
                }
            }
        }
    }
}

#[tokio::test]
async fn browser_connect_does_not_dial_from_visible_or_secret_placeholders() {
    use microcrab_session::{
        Auditor, Client, Session, VariableView,
        context::{Content, ContentItem},
        policy::{Action, PermissionConfig},
        runtime::{Runtime, ToolRegistration, tool::AllowTools},
    };
    use std::sync::Mutex;
    struct Model(Mutex<Vec<String>>);
    #[async_trait::async_trait]
    impl Client for Model {
        async fn complete(
            &self,
            content: &Content,
            _: &dyn Auditor,
            _: &dyn VariableView,
            _: tokio_util::sync::CancellationToken,
        ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
            let mut outputs = self.0.lock().unwrap();
            let results = content
                .items
                .iter()
                .filter_map(|item| match item {
                    ContentItem::Tool { content, .. } => Some(content.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let calls = if results.is_empty() {
                ["{{var:target}}", "{{secret:target}}"]
                    .into_iter()
                    .enumerate()
                    .map(|(index, endpoint)| microcrab_protocol::ToolCall {
                        provider_metadata: None,
                        id: format!("literal-{index}"),
                        name: "connect".into(),
                        arguments: serde_json::json!({"endpoint":endpoint}),
                    })
                    .collect()
            } else {
                outputs.extend(results);
                Vec::new()
            };
            Ok(microcrab_protocol::Response {
                provider_metadata: None,
                content: "done".into(),
                tool_calls: calls,
                usage: Default::default(),
            })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(temp.path(), Default::default()).unwrap());
    let bytes = component("inspection", "", true);
    let agent = WasmAgent::load(&bytes, WasmLimits::default(), host).unwrap();
    let permission = Arc::new(AllowTools::new(["connect"]));
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        permission.clone(),
    )
    .unwrap();
    let model = Arc::new(Model(Mutex::new(Vec::new())));
    let session = Session::builder_arc(model.clone(), runtime)
        .permissions(PermissionConfig {
            initialization: Action::Allow,
            overrides: microcrab_session::policy::SessionPermission::initial_overrides(
                permission.as_ref(),
            ),
            ..Default::default()
        })
        .build()
        .unwrap();
    session
        .state_handle()
        .inject("target", serde_json::json!("ws://fixture.invalid:9222"))
        .await
        .unwrap();
    session.submit("Use the browser fixture").await.unwrap();
    let output = model.0.lock().unwrap().clone();
    assert_eq!(output.len(), 2);
    assert!(
        output
            .iter()
            .all(|message| message.contains("web transport request is malformed")),
        "{output:?}"
    );
    assert!(
        output
            .iter()
            .all(|message| !message.contains("fixture.invalid")),
        "{output:?}"
    );
    session.close().await.unwrap();
}

mod browser_connection_job {
    use super::*;
    use microcrab_host_api::{
        Host, HostInfo, OpContext, ToolError,
        web_transport::{
            ConnectionScope, ProjectScope, WebTransportError, WebTransportResult, WsPull,
        },
    };
    use microcrab_protocol::{Response, ToolCall};
    use microcrab_session::{
        Auditor, Client, Session, VariableView,
        context::{Content, ContentItem},
        policy::{PermissionConfig, SessionPermission},
        runtime::{
            BuiltinTool, Runtime, ToolRegistration,
            execution::{ExecutionId, ExecutionStatus},
            tool::AllowTools,
        },
    };
    use serde_json::{Value, json};
    use std::{collections::VecDeque, sync::Mutex, time::Duration};
    use tokio_util::sync::CancellationToken;

    const ENDPOINT: &str = "ws://browser.invalid:9222/devtools/browser/synthetic";
    const SESSION: &str = "browser-connection-job-fixture";
    const IDLE_FRAME: &str =
        r#"{"method":"Target.targetCreated","params":{"targetId":"synthetic"}}"#;
    const TEST_TIMEOUT: Duration = Duration::from_secs(15);

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Event {
        Open(ConnectionScope),
        VersionRequest(ConnectionScope, u64),
        IdlePull(ConnectionScope),
        CloseStarted(ConnectionScope),
        CloseAck(ConnectionScope),
    }

    #[derive(Default)]
    struct Peer {
        events: Vec<Event>,
        active: Option<ConnectionScope>,
        frames: VecDeque<String>,
    }

    #[derive(Default)]
    struct SyntheticHost {
        peer: Mutex<Peer>,
        idle_pulled: CancellationToken,
        close_started: CancellationToken,
        allow_close_ack: CancellationToken,
    }

    impl SyntheticHost {
        fn events(&self) -> Vec<Event> {
            self.peer.lock().unwrap().events.clone()
        }
    }

    impl microcrab_host_api::ProcessApi for SyntheticHost {}
    impl microcrab_host_api::FsApi for SyntheticHost {}
    impl microcrab_host_api::NetApi for SyntheticHost {}
    impl microcrab_host_api::ShellApi for SyntheticHost {}
    impl microcrab_host_api::HostAPI for SyntheticHost {}

    #[async_trait::async_trait]
    impl Host for SyntheticHost {
        fn info(&self) -> Result<HostInfo, ToolError> {
            Ok(HostInfo {
                os_family: microcrab_host_api::OsFamily::Unix,
                os: "linux".into(),
                arch: "x86_64".into(),
                shell: "sh".into(),
                path_separator: "/".into(),
                newline: "\n".into(),
                capabilities: vec![],
                resources: None,
                virtual_shell: None,
                description: None,
            })
        }

        fn web_transport_binding(&self) -> Option<ProjectScope> {
            Some(ProjectScope {
                binding_id: "synthetic-binding".into(),
                backend_id: "synthetic-backend".into(),
                project_id: "synthetic-project".into(),
            })
        }

        async fn ws_open(
            &self,
            _: &OpContext,
            scope: &ConnectionScope,
            url: &str,
        ) -> WebTransportResult<()> {
            assert_eq!(url, ENDPOINT);
            assert_eq!(scope.binding, self.web_transport_binding().unwrap());
            assert_eq!(scope.session_id, SESSION);
            assert!(scope.valid());
            let mut peer = self.peer.lock().unwrap();
            assert!(peer.active.is_none(), "reopened before close ACK");
            assert!(peer.frames.is_empty());
            peer.active = Some(scope.clone());
            peer.events.push(Event::Open(scope.clone()));
            Ok(())
        }

        async fn ws_status(
            &self,
            _: &OpContext,
            scope: &ConnectionScope,
        ) -> WebTransportResult<()> {
            if self.peer.lock().unwrap().active.as_ref() == Some(scope) {
                Ok(())
            } else {
                Err(WebTransportError::Stale)
            }
        }

        async fn ws_send(
            &self,
            _: &OpContext,
            scope: &ConnectionScope,
            text: &str,
        ) -> WebTransportResult<()> {
            let request: Value = serde_json::from_str(text).unwrap();
            assert_eq!(request["method"], "Browser.getVersion");
            let id = request["id"].as_u64().unwrap();
            let mut peer = self.peer.lock().unwrap();
            assert_eq!(peer.active.as_ref(), Some(scope));
            peer.events.push(Event::VersionRequest(scope.clone(), id));
            // The handshake requires an exact reply, not an unsolicited frame.
            peer.frames.push_back(
                json!({"id":id,"result":{"product":"Synthetic","protocolVersion":"1.3"}})
                    .to_string(),
            );
            Ok(())
        }

        async fn ws_pull(
            &self,
            _: &OpContext,
            scope: &ConnectionScope,
            max: u32,
            wait: u32,
        ) -> WebTransportResult<WsPull> {
            let mut peer = self.peer.lock().unwrap();
            assert_eq!(peer.active.as_ref(), Some(scope));
            let Some(frame) = peer.frames.pop_front() else {
                return Ok(WsPull::Empty);
            };
            assert!(frame.len() <= max as usize);
            if frame == IDLE_FRAME {
                assert_eq!(wait, 0, "idle drive must not wait on the peer");
                peer.events.push(Event::IdlePull(scope.clone()));
                self.idle_pulled.cancel();
            }
            Ok(WsPull::Message(frame))
        }

        async fn ws_close(
            &self,
            context: &OpContext,
            scope: &ConnectionScope,
        ) -> WebTransportResult<()> {
            assert!(
                !context.cancel.is_cancelled(),
                "cleanup needs independent authority"
            );
            {
                let mut peer = self.peer.lock().unwrap();
                assert_eq!(peer.active.as_ref(), Some(scope));
                peer.events.push(Event::CloseStarted(scope.clone()));
            }
            self.close_started.cancel();
            self.allow_close_ack.cancelled().await;
            let mut peer = self.peer.lock().unwrap();
            assert_eq!(peer.active.take().as_ref(), Some(scope));
            peer.frames.clear();
            peer.events.push(Event::CloseAck(scope.clone()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct Model(Mutex<Option<ToolCall>>);

    #[async_trait::async_trait]
    impl Client for Model {
        async fn complete(
            &self,
            _: &Content,
            _: &dyn Auditor,
            _: &dyn VariableView,
            _: CancellationToken,
        ) -> Result<Response, microcrab_transport::Error> {
            Ok(Response {
                provider_metadata: None,
                content: "done".into(),
                tool_calls: self.0.lock().unwrap().take().into_iter().collect(),
                usage: Default::default(),
            })
        }
    }

    async fn invoke(
        session: &Session,
        model: &Model,
        call_id: &str,
        name: &str,
        arguments: Value,
    ) -> Value {
        *model.0.lock().unwrap() = Some(ToolCall {
            provider_metadata: None,
            id: call_id.into(),
            name: name.into(),
            arguments,
        });
        tokio::time::timeout(TEST_TIMEOUT, session.submit(call_id))
            .await
            .unwrap_or_else(|_| panic!("{call_id} did not settle"))
            .unwrap_or_else(|error| panic!("{call_id} did not submit: {error:#}"));
        let output = session
            .messages()
            .into_iter()
            .find_map(|item| match item {
                ContentItem::Tool {
                    call_id: id,
                    content,
                    ..
                } if id == call_id => Some(content),
                _ => None,
            })
            .unwrap_or_else(|| panic!("missing tool output for {call_id}"));
        serde_json::from_str(&output).unwrap_or_else(|error| {
            panic!("invalid JSON from {call_id}: {error}; output: {output}")
        })
    }

    fn connected(receipt: &Value) -> (&str, &str, ExecutionId) {
        assert_eq!(receipt["tool"], "connect", "{receipt}");
        assert_eq!(receipt["status"], "running", "{receipt}");
        assert_eq!(receipt["session_id"], SESSION, "{receipt}");
        assert_eq!(receipt["initial"]["closed"], false, "{receipt}");
        let initial = receipt["initial"]["chunks"][0]["content"]
            .as_str()
            .unwrap_or_else(|| panic!("missing Buffered readiness chunk: {receipt}"));
        let ready: Ready<'_> = serde_json::from_str(initial).unwrap();
        assert_eq!(ready.status, "connected", "{receipt}");
        let address = receipt["id"].as_str().unwrap();
        // Keep the returned opaque ID, separate from the Session Job address.
        assert_ne!(address, ready.connect_id);
        (
            ready.connect_id,
            address,
            ExecutionId(receipt["execution_id"].as_u64().unwrap()),
        )
    }

    #[derive(serde::Deserialize)]
    struct Ready<'a> {
        status: &'a str,
        connect_id: &'a str,
    }

    #[tokio::test]
    #[ignore = "set MICROCRAB_BROWSER_WASM to the staged Browser connection-jobs Component"]
    async fn browser_component_connection_job_drives_idle_and_reconnects_after_close_ack() {
        let path = std::env::var_os("MICROCRAB_BROWSER_WASM").expect("set MICROCRAB_BROWSER_WASM");
        let bytes =
            std::fs::read(&path).unwrap_or_else(|error| panic!("cannot read {path:?}: {error}"));
        let host = Arc::new(SyntheticHost::default());
        let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone())
            .unwrap_or_else(|error| panic!("Browser Component did not link: {error:#}"));
        let permission = Arc::new(AllowTools::new(["connect", "status", "job"]));
        let runtime = Runtime::new(
            agent
                .tools()
                .into_iter()
                .map(ToolRegistration::Wasm)
                .chain([ToolRegistration::Builtin(BuiltinTool::Job)])
                .collect(),
            permission.clone(),
        )
        .unwrap();
        let model = Arc::new(Model::default());
        let session = Session::builder_arc(model.clone(), runtime)
            .session_id(SESSION)
            .host(host.clone())
            .agent(agent.agents()[0].clone())
            .permissions(PermissionConfig {
                overrides: SessionPermission::initial_overrides(permission.as_ref()),
                ..Default::default()
            })
            .build()
            .unwrap();
        session.select_mode("observe").await.unwrap();

        let first = invoke(
            &session,
            &model,
            "first-connect",
            "connect",
            json!({"endpoint":ENDPOINT}),
        )
        .await;
        let (first_connection, first_job, first_id) = connected(&first);
        let first_scope = match host.events().as_slice() {
            [Event::Open(scope), Event::VersionRequest(request_scope, 1)]
                if scope == request_scope =>
            {
                scope.clone()
            }
            events => panic!("expected exactly one open and version query: {events:?}"),
        };
        assert_eq!(first_connection, first_scope.connection_id);
        assert_eq!(first_scope.epoch, session.state().epoch_id);

        // Introduce the first unsolicited frame only after submit returns. While
        // the test waits there is no foreground Browser or model invocation.
        host.peer
            .lock()
            .unwrap()
            .frames
            .push_back(IDLE_FRAME.into());
        tokio::time::timeout(TEST_TIMEOUT, host.idle_pulled.cancelled())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "idle drive never pulled: {:?}; {:?}",
                    host.events(),
                    session.executions().get(first_id)
                )
            });
        assert_eq!(
            host.events().last(),
            Some(&Event::IdlePull(first_scope.clone()))
        );

        let cancel = invoke(
            &session,
            &model,
            "cancel-first-job",
            "job",
            json!({"action":"cancel","id":first_job}),
        );
        tokio::pin!(cancel);
        tokio::select! {
            result = &mut cancel => panic!("job.cancel returned before close started: {result}"),
            started = tokio::time::timeout(TEST_TIMEOUT, host.close_started.cancelled()) => started.expect("job.cancel never called exact-scope close"),
        }
        assert_eq!(
            host.events().last(),
            Some(&Event::CloseStarted(first_scope.clone()))
        );
        assert_eq!(
            host.peer.lock().unwrap().active.as_ref(),
            Some(&first_scope)
        );
        let registry = session.executions();
        assert_eq!(
            registry.get(first_id).unwrap().status,
            ExecutionStatus::Cancelling
        );
        let mut changes = registry.subscribe();
        tokio::select! {
            biased;
            result = &mut cancel => panic!("job.cancel returned before close ACK: {result}"),
            _ = std::future::ready(()) => {}
        }
        host.allow_close_ack.cancel();
        let cancelled = cancel.await;
        assert_eq!(cancelled["execution_id"], first_id.0, "{cancelled}");
        assert!(
            matches!(
                cancelled["status"].as_str(),
                Some("cancelled" | "cancelling")
            ),
            "{cancelled}"
        );
        assert_eq!(
            host.events().last(),
            Some(&Event::CloseAck(first_scope.clone()))
        );

        let row = tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let row = registry.get(first_id).unwrap();
                if !row.status.is_pending() {
                    break row;
                }
                changes.changed().await.unwrap();
            }
        })
        .await
        .expect("connection Job did not settle after exact-scope close ACK");
        assert_eq!(row.status, ExecutionStatus::Cancelled, "{row:?}");

        let second = invoke(
            &session,
            &model,
            "second-connect",
            "connect",
            json!({"endpoint":ENDPOINT}),
        )
        .await;
        let (second_connection, second_job, _) = connected(&second);
        assert_ne!(second_connection, first_connection);
        assert_ne!(second_job, first_job);
        let second_scope = host.peer.lock().unwrap().active.clone().unwrap();
        assert_eq!(second_scope.connection_id, second_connection);
        assert_eq!(second_scope.owner(), first_scope.owner());
        let status = invoke(
            &session,
            &model,
            "second-status",
            "status",
            json!({"connect_id":second_connection}),
        )
        .await;
        assert_eq!(status["status"], "connected", "{status}");

        tokio::time::timeout(TEST_TIMEOUT, session.close())
            .await
            .expect("Session close did not settle")
            .unwrap();
        assert!(host.peer.lock().unwrap().active.is_none());
        assert_eq!(
            host.events(),
            [
                Event::Open(first_scope.clone()),
                Event::VersionRequest(first_scope.clone(), 1),
                Event::IdlePull(first_scope.clone()),
                Event::CloseStarted(first_scope.clone()),
                Event::CloseAck(first_scope),
                Event::Open(second_scope.clone()),
                Event::VersionRequest(second_scope.clone(), 1),
                Event::CloseStarted(second_scope.clone()),
                Event::CloseAck(second_scope),
            ]
        );
    }
}
