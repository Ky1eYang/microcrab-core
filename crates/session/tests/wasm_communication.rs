#![cfg(feature = "wasm")]

//! Acceptance fixtures use Component Model WAT, so no separately built guest is needed.
use crate::typed_agent_fixture;
use microcrab_host_api::{Host, HostInfo, OsFamily, ToolError};
use microcrab_protocol::{Response, ToolCall, Usage};
use microcrab_session::{
    Auditor, Client, Error, Session, SessionState,
    context::{Content, ContentItem},
    policy::{Action, PermissionConfig},
    runtime::{
        Runtime, ToolRegistration,
        tool::{AllowTools, DenyAll, Tool, ToolContext},
        wasm::{WasmAgent, WasmLimits},
    },
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;
use typed_agent_fixture::InitializedTool as _;
use typed_agent_fixture::final_text;

#[derive(Clone)]
enum Operation {
    Info,
    Get(String),
    Change(Value),
}

fn get(name: &str, key: &str) -> (String, Operation) {
    (name.into(), Operation::Get(key.into()))
}
fn change(name: &str, value: Value) -> (String, Operation) {
    (name.into(), Operation::Change(value))
}
fn escaped(text: &str) -> String {
    text.bytes().map(|byte| format!("\\{byte:02x}")).collect()
}

/// Import results use a 16-byte canonical area: tag at 0, payload at 4.
/// An error has code at 4 and message pointer/length at 8/12; only code is read.
/// A string success has pointer/length at 4/8. Unit success has no payload.
fn component(operations: &[(String, Operation)], shell: bool, missing_info: bool) -> Vec<u8> {
    let catalog = json!([{
        "name":"communication", "modes":[{"name":"execute","role":"both"}],
        "variables":{"rules":[],"hints":[]},
        "tools":operations.iter().map(|(name, _)| json!({
            "name":name,"description":"Communication acceptance fixture",
            "parameters":{"type":"object"}
        })).collect::<Vec<_>>()
    }])
    .to_string();
    // Static fixture bytes live below the bump allocator, away from return areas.
    let mut data = String::new();
    let mut cursor = 4096;
    let mut intern = |value: &str| {
        let address = cursor;
        cursor += value.len() + 16;
        data.push_str(&format!(
            "(data (i32.const {address}) \"{}\")\n",
            escaped(value)
        ));
        (address, value.len())
    };
    let (catalog_ptr, catalog_len) = intern(&catalog);
    let (probe_ptr, probe_len) = intern("discovery_probe");
    let (write_ptr, write_len) = intern(r#"{"set":{"discovery_probe":true},"remove":[]}"#);
    let (prefix_ptr, prefix_len) = intern(r#"{"ok":true,"value":"#);
    let (unit_ptr, unit_len) = intern(r#"{"ok":true}"#);
    let errors: Vec<_> = ["absent", "unavailable", "rejected", "cancelled"]
        .into_iter()
        .map(|code| intern(&json!({"ok":false,"code":code}).to_string()))
        .collect();
    let (marker_ptr, marker_len) = intern("typed host-info rejected");
    let mut dispatch = String::new();
    for (name, operation) in operations {
        let (name_ptr, name_len) = intern(name);
        let body = match operation {
            Operation::Info => "i32.const 64 call $host_info i32.const 0 call $encode".into(),
            Operation::Get(key) => {
                let (ptr, len) = intern(key);
                format!(
                    "i32.const {ptr} i32.const {len} i32.const 64 call $get i32.const 0 call $encode"
                )
            }
            Operation::Change(value) => {
                let (ptr, len) = intern(&value.to_string());
                format!(
                    "i32.const {ptr} i32.const {len} i32.const 64 call $change i32.const 1 call $encode"
                )
            }
        };
        dispatch.push_str(&format!(
            "local.get 2 local.get 3 i32.const {name_ptr} i32.const {name_len} call $equal if {body} return end\n"
        ));
    }
    let mut error_dispatch = String::new();
    for (code, (ptr, len)) in errors.into_iter().enumerate() {
        error_dispatch.push_str(&format!(
            "i32.const 68 i32.load8_u i32.const {code} i32.eq if i32.const {ptr} i32.const {len} call $output return end\n"
        ));
    }
    let info_check = if missing_info {
        "i32.const 64 i32.load8_u i32.const 1 i32.ne if unreachable end
         i32.const 68 i32.load8_u i32.const 2 i32.ne if unreachable end"
    } else {
        "i32.const 64 i32.load8_u if unreachable end"
    };
    let describe_info = if missing_info {
        // A deliberate guest error proves that a failed Host was a typed result,
        // rather than an instantiation trap or a synthesized successful HostInfo.
        format!(
            "i32.const 0 i32.const 1 i32.store8
            i32.const 4 i32.const {marker_ptr} i32.store
            i32.const 8 i32.const {marker_len} i32.store i32.const 0 return"
        )
    } else {
        format!("local.get 0 local.get 1 i32.const 68 i32.load i32.const 72 i32.load call $equal i32.eqz if unreachable end
            i32.const {catalog_ptr} i32.const {catalog_len} call $output")
    };
    let shell_import = if shell {
        r#"(import "microcrab:host/types@0.1.0" (instance $types
          (type $raw_id u64)
          (export "handle-id" (type $id (eq $raw_id)))
          (type $category (enum "not-found" "permission" "timeout" "cancelled" "conflict" "unsupported" "io"))
          (export "error-category" (type $category_public (eq $category)))
          (type $error (record (field "category" $category_public) (field "retryable" bool) (field "message" string)))
          (export "host-error" (type $error_public (eq $error)))))
        (alias export $types "handle-id" (type $handle_id))
        (alias export $types "host-error" (type $host_error))
        (import "microcrab:host/process@0.1.0" (instance $process
          (alias outer 1 $handle_id (type $id))
          (export "handle-id" (type $id_public (eq $id)))
          (type $handle (record (field "id" $id_public) (field "pid" (option u32))))
          (export "process-handle" (type $handle_public (eq $handle)))))
        (alias export $process "process-handle" (type $process_handle))
        (import "microcrab:host/shell@0.1.0" (instance $host_shell
          (alias outer 1 $host_error (type $error))
          (export "host-error" (type $error_public (eq $error)))
          (alias outer 1 $process_handle (type $handle))
          (export "process-handle" (type $handle_public (eq $handle)))
          (type $request (record (field "command" string) (field "cwd" (option string))))
          (export "shell-request" (type $request_public (eq $request)))
          (export "launch" (func (param "request" $request_public) (result (result $handle_public (error $error_public)))))))"#
    } else {
        ""
    };
    let text = format!(
        r#"(component
      (import "microcrab:agent/host-info@0.7.0" (instance $host
        (type $code (enum "absent" "unavailable" "rejected" "cancelled"))
        (export "communication-error-code" (type $code_public (eq $code)))
        (type $error (record (field "code" $code_public) (field "message" string)))
        (export "communication-error" (type $error_public (eq $error)))
        (export "host-info" (func (result (result string (error $error_public)))))))
      (alias export $host "communication-error" (type $communication_error))
      (import "microcrab:session/variables@0.1.0" (instance $variables
        (alias outer 1 $communication_error (type $error))
        (export "communication-error" (type $error_public (eq $error)))
        (export "variable-get" (func (param "name" string) (result (result string (error $error_public)))))
        (export "variable-change" (func (param "change" string) (result (result (error $error_public)))))))
      {shell_import}
      (alias export $host "host-info" (func $host_info))
      (alias export $variables "variable-get" (func $get))
      (alias export $variables "variable-change" (func $change))
      (core module $storage
        (memory (export "memory") 4)
        (global $heap (mut i32) (i32.const 65536))
        (func (export "realloc") (param i32 i32 i32 i32) (result i32)
          (local $old i32)
          ;; Start imports allocate byte strings before init returns its align-4
          ;; record. Honor canonical alignment for every subsequent allocation.
          global.get $heap local.get 2 i32.const 1 i32.sub i32.add
          i32.const 0 local.get 2 i32.sub i32.and local.tee $old
          local.get 3 i32.add global.set $heap local.get $old))
      (core instance $memory (instantiate $storage))
      (core func $lower_info (canon lower (func $host_info)
        (memory (core memory $memory "memory")) (realloc (core func $memory "realloc"))))
      (core func $lower_get (canon lower (func $get)
        (memory (core memory $memory "memory")) (realloc (core func $memory "realloc"))))
      (core func $lower_change (canon lower (func $change)
        (memory (core memory $memory "memory")) (realloc (core func $memory "realloc"))))
      (core module $code
        (import "memory" "memory" (memory 4))
        (import "host" "info" (func $host_info (param i32)))
        (import "host" "get" (func $get (param i32 i32 i32)))
        (import "host" "change" (func $change (param i32 i32 i32)))
        {data}
        (func $equal (param $a i32) (param $an i32) (param $b i32) (param $bn i32) (result i32)
          (local $i i32)
          local.get $an local.get $bn i32.ne if i32.const 0 return end
          block $done loop $next
            local.get $i local.get $an i32.ge_u br_if $done
            local.get $a local.get $i i32.add i32.load8_u
            local.get $b local.get $i i32.add i32.load8_u i32.ne if i32.const 0 return end
            local.get $i i32.const 1 i32.add local.set $i br $next
          end end i32.const 1)
        (func $output (param $ptr i32) (param $len i32) (result i32)
          i32.const 0 i32.const 0 i32.store8
          i32.const 4 local.get $ptr i32.store
          i32.const 8 local.get $len i32.store i32.const 0)
        (func $encode (param $unit i32) (result i32)
          (local $len i32)
          i32.const 64 i32.load8_u if
            {error_dispatch}
            unreachable
          end
          local.get $unit if i32.const {unit_ptr} i32.const {unit_len} call $output return end
          i32.const 72 i32.load local.set $len
          i32.const 32768 i32.const {prefix_ptr} i32.const {prefix_len} memory.copy
          i32.const {prefix_end} i32.const 68 i32.load local.get $len memory.copy
          i32.const {prefix_end} local.get $len i32.add i32.const 125 i32.store8
          i32.const 32768 local.get $len i32.const {prefix_plus_end} i32.add call $output)
        (func $initialize
          i32.const 64 call $host_info
          {info_check}
          ;; Initialization must never inherit a previous invocation's binder.
          i32.const {probe_ptr} i32.const {probe_len} i32.const 64 call $get
          call $assert_unavailable
          i32.const {write_ptr} i32.const {write_len} i32.const 64 call $change
          call $assert_unavailable)
        (func $assert_unavailable
          i32.const 64 i32.load8_u i32.const 1 i32.ne if unreachable end
          i32.const 68 i32.load8_u i32.const 1 i32.ne if unreachable end)
        (start $initialize)
        (func (export "describe") (param i32 i32) (result i32)
          i32.const {probe_ptr} i32.const {probe_len} i32.const 64 call $get
          call $assert_unavailable
          i32.const {write_ptr} i32.const {write_len} i32.const 64 call $change
          call $assert_unavailable
          i32.const 64 call $host_info {info_check}
          {describe_info})
        (func (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32)
          i32.const 64 call $host_info {info_check}
          local.get 0 local.get 1 i32.const 68 i32.load i32.const 72 i32.load call $equal i32.eqz if unreachable end
          {dispatch}
          unreachable))
      (core instance $functions
        (export "info" (func $lower_info))
        (export "get" (func $lower_get))
        (export "change" (func $lower_change)))
      (core instance $instance (instantiate $code
        (with "memory" (instance $memory)) (with "host" (instance $functions))))
      (func (export "describe") (param "host-info" string) (result (result string (error string)))
        (canon lift (core func $instance "describe") (memory (core memory $memory "memory"))
          (realloc (core func $memory "realloc"))))
      (func (export "invoke") (param "host-info" string) (param "name" string) (param "arguments" string)
        (result (result string (error string)))
        (canon lift (core func $instance "invoke") (memory (core memory $memory "memory"))
          (realloc (core func $memory "realloc")))))"#,
        prefix_end = 32768 + prefix_len,
        prefix_plus_end = prefix_len + 1,
    );
    typed_agent_fixture::component(&text)
}

#[derive(Default)]
struct RecordingHost {
    shell_calls: AtomicUsize,
}
impl microcrab_host_api::process::ProcessApi for RecordingHost {}
impl microcrab_host_api::filesystem::FsApi for RecordingHost {}
impl microcrab_host_api::net::NetApi for RecordingHost {}
impl microcrab_host_api::shell::ShellApi for RecordingHost {}
impl microcrab_host_api::HostAPI for RecordingHost {
    fn supports(&self, operation: &str) -> bool {
        operation == "shell.launch"
    }
}
#[async_trait::async_trait]
impl Host for RecordingHost {
    fn tool_capabilities(&self) -> Vec<String> {
        vec!["shell".into()]
    }
    fn info(&self) -> Result<HostInfo, ToolError> {
        Ok(HostInfo {
            os_family: OsFamily::Unix,
            os: "linux".into(),
            arch: "fixture-arch".into(),
            shell: "/fixture/bash".into(),
            path_separator: "/".into(),
            newline: "\n".into(),
            // A Host method or a claimed capability is not guest linkage.
            capabilities: vec!["read_file".into(), "shell".into(), "invented".into()],
            resources: None,
            virtual_shell: None,
            description: None,
        })
    }
}

fn load(operations: &[(String, Operation)], host: Arc<RecordingHost>) -> Vec<Arc<dyn Tool>> {
    WasmAgent::load(
        &component(operations, false, false),
        WasmLimits::default(),
        host,
    )
    .unwrap()
    .tools()
}
fn invocation() -> ToolContext {
    ToolContext::detached(
        SessionState::new(Default::default()),
        CancellationToken::new(),
    )
}
fn code(value: &Value, expected: &str) {
    assert_eq!(value, &json!({"ok":false,"code":expected}));
}
fn value(value: &Value) -> &Value {
    assert_eq!(value["ok"], true, "{value}");
    &value["value"]
}

struct FramePosition {
    epoch: u64,
    turn: Option<u64>,
    round: Option<u64>,
}

/// One call per model round keeps mutation acknowledgements observable before
/// the next read. The client captures the actual tool responses, not snapshots.
struct Script {
    names: Vec<String>,
    step: AtomicUsize,
    outputs: Mutex<Vec<Value>>,
    frames: Mutex<Vec<FramePosition>>,
}
impl Script {
    fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Arc<Self> {
        Arc::new(Self {
            names: names.into_iter().map(Into::into).collect(),
            step: AtomicUsize::new(0),
            outputs: Mutex::new(Vec::new()),
            frames: Mutex::new(Vec::new()),
        })
    }
    fn results(&self) -> Vec<Value> {
        self.outputs.lock().unwrap().clone()
    }
}
#[async_trait::async_trait]
impl Client for Script {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let step = self.step.fetch_add(1, Ordering::SeqCst);
        if step > 0 {
            let expected = format!("communication-{step}");
            let output = content
                .items
                .iter()
                .find_map(|item| match item {
                    ContentItem::Tool { call_id, content } if call_id == &expected => Some(content),
                    _ => None,
                })
                .expect("previous tool result must reach the model");
            self.outputs.lock().unwrap().push(
                serde_json::from_str(output).unwrap_or_else(|_| json!({"tool_error":output})),
            );
        }
        let tool_calls = if let Some(name) = self.names.get(step) {
            self.frames.lock().unwrap().push(FramePosition {
                epoch: content.epoch,
                turn: content.turn_id,
                round: content.round,
            });
            vec![ToolCall {
                provider_metadata: None,
                id: format!("communication-{}", step + 1),
                name: name.clone(),
                arguments: json!({}),
            }]
        } else {
            vec![]
        };
        Ok(Response {
            provider_metadata: None,
            content: if tool_calls.is_empty() {
                "done".into()
            } else {
                String::new()
            },
            tool_calls,
            usage: Usage::default(),
        })
    }
}
fn session(tools: &[Arc<dyn Tool>], script: Arc<Script>, denied: bool) -> Session {
    let permission: Arc<dyn microcrab_session::policy::SessionPermission> = if denied {
        Arc::new(DenyAll)
    } else {
        Arc::new(AllowTools::new(
            tools.iter().map(|tool| tool.definition().name),
        ))
    };
    let permissions = PermissionConfig {
        initialization: Action::Allow,
        overrides: permission.initial_overrides(),
        ..Default::default()
    };
    let runtime = Runtime::new(
        tools.iter().cloned().map(ToolRegistration::Wasm).collect(),
        permission,
    )
    .unwrap();
    Session::builder_arc(script, runtime)
        .permissions(permissions)
        .limits(microcrab_session::Limits {
            max_rounds: 64,
            ..Default::default()
        })
        .build()
        .unwrap()
}

#[tokio::test]
async fn host_info_is_available_during_start_discovery_and_invoke_with_actual_imports() {
    let host = Arc::new(RecordingHost::default());
    for (shell, expected) in [(false, json!([])), (true, json!(["shell.launch"]))] {
        let agent = WasmAgent::load(
            &component(&[("environment".into(), Operation::Info)], shell, false),
            WasmLimits::default(),
            host.clone(),
        )
        .unwrap();
        let text = agent.tools()[0]
            .execute_initialized(json!({}), invocation())
            .await
            .unwrap();
        let output: Value = serde_json::from_str(&final_text(text)).unwrap();
        assert_eq!(
            value(&output),
            &json!({
                "os_family":"unix", "os":"linux", "arch":"fixture-arch", "shell":"/fixture/bash",
                "path_separator":"/", "newline":"\n", "capabilities":expected,
                "resources": {
                    "platform": "other",
                    "features": {
                        "process_pty": false, "process_kill_tree": false,
                        "fs_watch": false, "fs_symlink": false,
                        "net_listen": false, "net_udp": false,
                    },
                },
            })
        );
    }
    assert_eq!(host.shell_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn indeterminate_host_info_reaches_guest_as_typed_rejected() {
    struct NoInfo;
    impl microcrab_host_api::process::ProcessApi for NoInfo {}
    impl microcrab_host_api::filesystem::FsApi for NoInfo {}
    impl microcrab_host_api::net::NetApi for NoInfo {}
    impl microcrab_host_api::shell::ShellApi for NoInfo {}
    impl microcrab_host_api::HostAPI for NoInfo {}
    #[async_trait::async_trait]
    impl Host for NoInfo {}
    let result = WasmAgent::load(
        &component(&[("environment".into(), Operation::Info)], false, true),
        WasmLimits::default(),
        Arc::new(NoInfo),
    );
    match result {
        Err(Error::Wasm(message)) => {
            assert!(message.contains("typed host-info rejected"), "{message}")
        }
        Err(error) => panic!("guest must report the typed rejection: {error:?}"),
        Ok(_) => panic!("an indeterminate Host must not load successfully"),
    }
}

#[tokio::test]
async fn discovery_and_direct_execution_have_no_session_variable_binder() {
    let host = Arc::new(RecordingHost::default());
    let tools = load(
        &[
            get("read", "secret"),
            change(
                "write",
                json!({"set":{"secret":"must-not-exist"},"remove":[]}),
            ),
        ],
        host.clone(),
    );
    // Component initialization and describe already asserted unavailable for both
    // imports. Direct execute must not infer a binder from an invocation snapshot.
    for tool in tools {
        let output: Value = serde_json::from_str(&final_text(
            tool.execute_initialized(json!({}), invocation())
                .await
                .unwrap(),
        ))
        .unwrap();
        code(&output, "unavailable");
    }
    assert_eq!(host.shell_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn session_tool_path_reads_refreshed_builtins_and_keeps_public_extract_custom_only() {
    let host = Arc::new(RecordingHost::default());
    let tools = load(
        &[
            get("session_value", "session"),
            get("time_value", "time"),
            get("host_value", "host"),
            get("agent_value", "agent"),
            get("mode_value", "mode"),
            get("null_value", "present_null"),
            get("missing_value", "missing"),
        ],
        host.clone(),
    );
    let script = Script::new([
        "session_value",
        "time_value",
        "host_value",
        "agent_value",
        "mode_value",
        "null_value",
        "missing_value",
        "session_value",
        "time_value",
    ]);
    let session = session(&tools, script.clone(), false);
    let state = session.state_handle();
    state.inject("present_null", Value::Null).await.unwrap();
    assert_eq!(
        session
            .submit("Read Session communication values")
            .await
            .unwrap()
            .content,
        "done"
    );
    let outputs = script.results();
    {
        let frames = script.frames.lock().unwrap();
        for index in [0, 7] {
            let builtin = value(&outputs[index]);
            assert_eq!(builtin["epoch"], json!(frames[index].epoch));
            assert_eq!(builtin["turn"], json!(frames[index].turn));
            assert_eq!(builtin["round"], json!(frames[index].round));
            assert!(builtin["boundary"].is_string());
        }
        assert!(
            value(&outputs[7])["round"].as_u64().unwrap()
                > value(&outputs[0])["round"].as_u64().unwrap()
        );
        let first_time = value(&outputs[1])["unix_millis"].as_u64().unwrap();
        let last_time = value(&outputs[8])["unix_millis"].as_u64().unwrap();
        assert!(first_time > 0 && last_time >= first_time);
        for output in &outputs[2..5] {
            code(output, "absent");
        }
        assert_eq!(value(&outputs[5]), &Value::Null);
        code(&outputs[6], "absent");
        for key in ["session", "time", "host", "agent", "mode"] {
            assert_eq!(state.extract(key), None);
        }
        assert_eq!(state.extract("present_null"), Some(Value::Null));
        assert_eq!(state.extract("discovery_probe"), None);
        assert_eq!(host.shell_calls.load(Ordering::SeqCst), 0);
    }
    session.close().await.unwrap();
}

// Fixture setup uses the same trusted Agent branding as native tool execution.
fn agent_state(session: &Session) -> microcrab_session::State {
    let state = session.state_handle();
    microcrab_session::runtime::tool::ToolContext::new(
        state.clone(),
        state.processes(),
        CancellationToken::new(),
        "fixture".into(),
        "fixture".into(),
        "fixture".into(),
        "fixture".into(),
        None,
    )
    .state
}

#[tokio::test]
async fn acknowledged_set_remove_and_invalid_changes_are_atomic() {
    let host = Arc::new(RecordingHost::default());
    let mut operations = vec![
        change(
            "replace",
            json!({"set":{"created":null,"kept":2},"remove":["old"]}),
        ),
        get("created", "created"),
        get("old", "old"),
        get("kept", "kept"),
    ];
    let mut invalid = vec![
        json!({"set":{"partial":1,"kept":3},"remove":["kept"]}),
        json!({"set":{"partial":1},"remove":["kept","kept"]}),
        json!({"set":{"partial":1,"":2},"remove":["kept"]}),
        json!({"set":{"partial":1,"bad\u{0000}key":2},"remove":["kept"]}),
        json!({"set":[],"remove":[]}),
    ];
    for reserved in ["time", "session", "host", "agent", "mode"] {
        let mut set = serde_json::Map::new();
        set.insert("partial".into(), json!(1));
        set.insert(reserved.into(), json!({"forged":true}));
        invalid.push(json!({"set":set,"remove":["kept"]}));
        invalid.push(json!({"set":{"partial":1},"remove":[reserved,"kept"]}));
    }
    for (index, payload) in invalid.iter().enumerate() {
        operations.push(change(&format!("invalid_{index}"), payload.clone()));
    }
    let tools = load(&operations, host);
    let mut calls: Vec<String> = ["replace", "created", "old", "kept"]
        .into_iter()
        .map(Into::into)
        .collect();
    // Read after each rejection: a later write cannot conceal partial mutation.
    for index in 0..invalid.len() {
        calls.extend([format!("invalid_{index}"), "kept".into(), "created".into()]);
    }
    let script = Script::new(calls);
    let session = session(&tools, script.clone(), false);
    let state = session.state_handle();
    agent_state(&session).inject("old", json!(1)).await.unwrap();
    agent_state(&session)
        .inject("kept", json!(1))
        .await
        .unwrap();
    session
        .submit("Apply atomic changes and reject invalid changes")
        .await
        .unwrap();
    let outputs = script.results();
    assert_eq!(outputs[0], json!({"ok":true}));
    assert_eq!(value(&outputs[1]), &Value::Null);
    code(&outputs[2], "absent");
    assert_eq!(value(&outputs[3]), &json!(2));
    for index in 0..invalid.len() {
        let base = 4 + index * 3;
        code(&outputs[base], "rejected");
        assert_eq!(value(&outputs[base + 1]), &json!(2));
        assert_eq!(value(&outputs[base + 2]), &Value::Null);
    }
    assert_eq!(state.variables(), json!({"created":null,"kept":2}));
    session.close().await.unwrap();
}

#[tokio::test]
async fn legacy_guest_is_agent_bound_and_cannot_spoof_client_or_partially_commit() {
    let host = Arc::new(RecordingHost::default());
    let tools = load(
        &[
            change(
                "unauthorized",
                json!({"set":{"client_owned":2,"partial":true}}),
            ),
            change("spoofed", json!({"set":{"forged":true},"role":"client"})),
            change("create", json!({"set":{"guest_owned":3}})),
        ],
        host,
    );
    let script = Script::new(["unauthorized", "spoofed", "create"]);
    let session = session(&tools, script.clone(), false);
    session
        .state_handle()
        .inject("client_owned", json!(1))
        .await
        .unwrap();
    session.submit("Exercise guest governance").await.unwrap();
    let outputs = script.results();
    code(&outputs[0], "rejected");
    code(&outputs[1], "rejected");
    assert_eq!(outputs[2], json!({"ok":true}));
    let state = session.state_handle();
    assert_eq!(state.extract("client_owned"), Some(json!(1)));
    assert!(state.extract("partial").is_none());
    assert!(state.extract("forged").is_none());
    assert_eq!(
        state.variable_metadata().unwrap()["guest_owned"]["owner"],
        "agent"
    );
    assert!(matches!(
        state.remove("guest_owned").await,
        Err(Error::VariableForbidden(_))
    ));
    session.close().await.unwrap();
}

#[tokio::test]
async fn shared_compiled_tools_and_host_bind_only_the_admitting_session() {
    let host = Arc::new(RecordingHost::default());
    let tools = load(
        &[
            get("read", "owner"),
            change(
                "increment",
                json!({"set":{"written":"by-guest"},"remove":["remove_me"]}),
            ),
            get("written", "written"),
            get("removed", "remove_me"),
        ],
        host.clone(),
    );
    let first_script = Script::new(["read", "increment", "written", "removed"]);
    let second_script = Script::new(["read", "increment", "written", "removed"]);
    let first = session(&tools, first_script.clone(), false);
    let second = session(&tools, second_script.clone(), false);
    first
        .state_handle()
        .inject("owner", json!("first"))
        .await
        .unwrap();
    agent_state(&first)
        .inject("remove_me", json!(1))
        .await
        .unwrap();
    second
        .state_handle()
        .inject("owner", json!("second"))
        .await
        .unwrap();
    agent_state(&second)
        .inject("remove_me", json!(2))
        .await
        .unwrap();
    // First run must not remove or create anything in the peer Session, even
    // though both registries hold exactly the same Arc<dyn Tool> handles.
    first.submit("Write first Session only").await.unwrap();
    assert_eq!(
        second.state_handle().variables(),
        json!({"owner":"second","remove_me":2})
    );
    second.submit("Write second Session only").await.unwrap();
    for (script, owner) in [(first_script, "first"), (second_script, "second")] {
        let outputs = script.results();
        assert_eq!(value(&outputs[0]), &json!(owner));
        assert_eq!(outputs[1], json!({"ok":true}));
        assert_eq!(value(&outputs[2]), &json!("by-guest"));
        code(&outputs[3], "absent");
    }
    assert_eq!(
        first.state_handle().variables(),
        json!({"owner":"first","written":"by-guest"})
    );
    assert_eq!(
        second.state_handle().variables(),
        json!({"owner":"second","written":"by-guest"})
    );
    assert_eq!(host.shell_calls.load(Ordering::SeqCst), 0);
    first.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
async fn epoch_store_is_released_on_recovery_close_and_owner_drop() {
    let host = Arc::new(RecordingHost::default());
    let tools = load(&[get("read", "missing")], host.clone());
    let baseline = Arc::strong_count(&host);
    let script = Script::new(["read"]);
    let first = session(&tools, script, false);
    first.submit("create epoch instance").await.unwrap();
    assert_eq!(Arc::strong_count(&host), baseline + 1);
    let saved = first.checkpoint().await.unwrap();
    first.recover(saved).await.unwrap();
    assert_eq!(Arc::strong_count(&host), baseline);
    first.close().await.unwrap();

    let second = session(&tools, Script::new(["read"]), false);
    second.submit("retain until close").await.unwrap();
    assert_eq!(Arc::strong_count(&host), baseline + 1);
    let retained_state = second.state_handle();
    second.close().await.unwrap();
    assert_eq!(Arc::strong_count(&host), baseline);
    drop(retained_state);

    let third = session(&tools, Script::new(["read"]), false);
    third.submit("retain until owner drop").await.unwrap();
    assert_eq!(Arc::strong_count(&host), baseline + 1);
    let retained_state = third.state_handle();
    drop(third);
    assert_eq!(Arc::strong_count(&host), baseline);
    drop(retained_state);
}

#[tokio::test]
async fn permission_denial_prevents_communication_mutation() {
    let host = Arc::new(RecordingHost::default());
    let tools = load(
        &[change(
            "mutate",
            json!({"set":{"forbidden":true},"remove":["original"]}),
        )],
        host.clone(),
    );
    let script = Script::new(["mutate"]);
    let session = session(&tools, script.clone(), true);
    let state = session.state_handle();
    state.inject("original", json!("retained")).await.unwrap();
    session.submit("Deny the mutation tool").await.unwrap();
    let outputs = script.results();
    assert_eq!(outputs.len(), 1);
    let reason = outputs[0]["error"].as_str().unwrap();
    let authority = reason
        .strip_prefix("permission denied by controller ")
        .unwrap();
    let (controller, decision) = authority.split_once(" revision ").unwrap();
    assert!(controller.parse::<u64>().unwrap() > 0);
    let (revision, decision) = decision.split_once(": ").unwrap();
    assert!(revision.parse::<u64>().unwrap() > 0);
    assert_eq!(
        decision,
        "compiled tool policy for agent [redacted], mode [redacted], tool [redacted] resolved Deny"
    );
    assert_eq!(outputs[0]["diagnostic"]["reason"], reason);
    assert_eq!(outputs[0]["diagnostic"]["code"], "permission_denied");
    assert_eq!(outputs[0]["diagnostic"]["layer"], "runtime");
    assert_eq!(outputs[0]["diagnostic"]["phase"], "authorization");
    assert_eq!(outputs[0]["diagnostic"]["effects"], "unknown");
    assert_eq!(state.variables(), json!({"original":"retained"}));
    assert_eq!(host.shell_calls.load(Ordering::SeqCst), 0);
    session.close().await.unwrap();
}
