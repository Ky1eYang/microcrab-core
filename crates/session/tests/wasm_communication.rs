#![cfg(feature = "wasm")]

//! Acceptance fixtures use Component Model WAT against `microcrab:session/session@0.4.0`,
//! so no separately built guest is needed.
use crate::typed_agent_fixture;
use microcrab_host_api::{Host, HostInfo, OsFamily, ToolError};
use microcrab_protocol::{
    Response, ToolCall, Usage, VariablePersistence, VariableRole, VariableVisibility,
    VariablesChange,
};
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
    Script {
        script: String,
        args: Option<Value>,
    },
}

fn get(name: &str, key: &str) -> (String, Operation) {
    (
        name.into(),
        Operation::Script {
            script: r#"if vars::has(args.key) { #{ ok: true, value: vars::get(args.key) } } else { #{ ok: false, code: "absent" } }"#.into(),
            args: Some(json!({ "key": key })),
        },
    )
}

fn change(name: &str, value: Value) -> (String, Operation) {
    (
        name.into(),
        Operation::Script {
            script: r#"vars::change(args); #{ ok: true }"#.into(),
            args: Some(value),
        },
    )
}

fn call_facts(name: &str) -> (String, Operation) {
    (
        name.into(),
        Operation::Script {
            script: r#"#{ ok: true, value: vars::call_facts() }"#.into(),
            args: None,
        },
    )
}

struct FixtureData {
    wat: String,
    cursor: u32,
}

impl FixtureData {
    fn put(&mut self, bytes: impl AsRef<[u8]>) -> (u32, u32) {
        let bytes = bytes.as_ref();
        self.cursor = self.cursor.next_multiple_of(4);
        let address = self.cursor;
        let len = u32::try_from(bytes.len()).unwrap();
        self.cursor += len;
        let escaped: String = bytes.iter().map(|byte| format!("\\{byte:02x}")).collect();
        self.wat
            .push_str(&format!("(data (i32.const {address}) \"{escaped}\")\n"));
        (address, len)
    }
}

fn component(operations: &[(String, Operation)], shell: bool, missing_info: bool) -> Vec<u8> {
    component_with_extra_import(operations, shell, missing_info, "")
}

fn component_with_extra_import(
    operations: &[(String, Operation)],
    shell: bool,
    missing_info: bool,
    extra_import: &str,
) -> Vec<u8> {
    let catalog = json!([{
        "name":"communication", "modes":[{"name":"execute","role":"both"}],
        "variables":{"rules":[],"hints":[]},
        "tools":operations.iter().map(|(name, _)| json!({
            "name":name,"description":"Communication acceptance fixture",
            "parameters":{"type":"object"}
        })).collect::<Vec<_>>()
    }])
    .to_string();
    let mut data = FixtureData {
        wat: String::new(),
        cursor: 4096,
    };
    let (catalog_ptr, catalog_len) = data.put(&catalog);
    let (probe_script_ptr, probe_script_len) = data.put(r#"vars::get("discovery_probe")"#);
    let (facts_script_ptr, facts_script_len) = data.put("vars::call_facts()");
    let (prefix_ptr, prefix_len) = data.put(r#"{"ok":true,"value":"#);
    let (marker_ptr, marker_len) = data.put("typed host-info rejected");
    let errors: Vec<_> = [
        "absent",
        "unavailable",
        "denied",
        "invalid_argument",
        "limit",
        "cancelled",
        "script_error",
    ]
    .into_iter()
    .map(|code| data.put(json!({"ok":false,"code":code}).to_string()))
    .collect();
    let comm_errors: Vec<_> = ["absent", "unavailable", "rejected", "cancelled"]
        .into_iter()
        .map(|code| data.put(json!({"ok":false,"code":code}).to_string()))
        .collect();

    let mut dispatch = String::new();
    for (name, operation) in operations {
        let (name_ptr, name_len) = data.put(name);
        let body = match operation {
            Operation::Info => "i32.const 64 call $host_info call $encode_host_info".into(),
            Operation::Script { script, args } => {
                let (s_ptr, s_len) = data.put(script);
                let (a_tag, a_ptr, a_len) = if let Some(val) = args {
                    let (p, l) = data.put(val.to_string());
                    (1, p, l)
                } else {
                    (0, 0, 0)
                };
                format!(
                    "i32.const {s_ptr} i32.const {s_len} i32.const {a_tag} i32.const {a_ptr} i32.const {a_len} i32.const 64 call $eval call $encode_eval"
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
            "local.get $code i32.const {code} i32.eq if i32.const {ptr} i32.const {len} call $output return end\n"
        ));
    }
    let mut comm_error_dispatch = String::new();
    for (code, (ptr, len)) in comm_errors.into_iter().enumerate() {
        comm_error_dispatch.push_str(&format!(
            "local.get $code i32.const {code} i32.eq if i32.const {ptr} i32.const {len} call $output return end\n"
        ));
    }

    let info_check = if missing_info {
        "i32.const 64 i32.load8_u i32.const 1 i32.ne if unreachable end
         i32.const 68 i32.load8_u i32.const 2 i32.ne if unreachable end"
    } else {
        "i32.const 64 i32.load8_u if unreachable end"
    };
    let describe_info = if missing_info {
        format!(
            "i32.const 0 i32.const 1 i32.store8
            i32.const 4 i32.const {marker_ptr} i32.store
            i32.const 8 i32.const {marker_len} i32.store i32.const 0 return"
        )
    } else {
        format!(
            "local.get 0 local.get 1 i32.const 68 i32.load i32.const 72 i32.load call $equal i32.eqz if unreachable end
            i32.const {catalog_ptr} i32.const {catalog_len} call $output"
        )
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
    let session_import = r#"(import "microcrab:session/session@0.4.0" (instance $session
          (type $raw_err_code (enum "absent" "unavailable" "denied" "invalid-argument" "limit" "cancelled" "script-error"))
          (export "error-code" (type $err_code (eq $raw_err_code)))
          (type $raw_err (record (field "code" $err_code) (field "message" string)))
          (export "session-error" (type $err (eq $raw_err)))
          (export "eval" (func (param "script" string) (param "args-json" (option string)) (result (result string (error $err)))))))"#;

    let text = format!(
        r#"(component
      (import "microcrab:agent/host-info@0.9.0" (instance $host
        (type $code (enum "absent" "unavailable" "rejected" "cancelled"))
        (export "communication-error-code" (type $code_public (eq $code)))
        (type $error (record (field "code" $code_public) (field "message" string)))
        (export "communication-error" (type $error_public (eq $error)))
        (export "host-info" (func (result (result string (error $error_public)))))))
      (alias export $host "communication-error" (type $communication_error))
      {session_import}
      {extra_import}
      {shell_import}
      (alias export $host "host-info" (func $host_info))
      (alias export $session "eval" (func $eval))
      (core module $storage
        (memory (export "memory") 4)
        (global $heap (mut i32) (i32.const 65536))
        (func (export "realloc") (param i32 i32 i32 i32) (result i32)
          (local $old i32)
          global.get $heap local.get 2 i32.const 1 i32.sub i32.add
          i32.const 0 local.get 2 i32.sub i32.and local.tee $old
          local.get 3 i32.add global.set $heap local.get $old))
      (core instance $memory (instantiate $storage))
      (core func $lower_info (canon lower (func $host_info)
        (memory (core memory $memory "memory")) (realloc (core func $memory "realloc"))))
      (core func $lower_eval (canon lower (func $eval)
        (memory (core memory $memory "memory")) (realloc (core func $memory "realloc"))))
      (core module $code
        (import "memory" "memory" (memory 4))
        (import "host" "info" (func $host_info (param i32)))
        (import "host" "eval" (func $eval (param i32 i32 i32 i32 i32 i32)))
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
        (func $eval_error (param $code i32) (result i32)
          {error_dispatch}
          unreachable)
        (func $comm_error (param $code i32) (result i32)
          {comm_error_dispatch}
          unreachable)
        (func $encode_host_info (result i32)
          (local $len i32)
          i32.const 64 i32.load8_u if
            i32.const 68 i32.load8_u call $comm_error return
          end
          i32.const 72 i32.load local.set $len
          i32.const 32768 i32.const {prefix_ptr} i32.const {prefix_len} memory.copy
          i32.const {prefix_end} i32.const 68 i32.load local.get $len memory.copy
          i32.const {prefix_end} local.get $len i32.add i32.const 125 i32.store8
          i32.const 32768 local.get $len i32.const {prefix_plus_end} i32.add call $output)
        (func $encode_eval (result i32)
          i32.const 64 i32.load8_u if
            i32.const 68 i32.load8_u call $eval_error return
          end
          i32.const 68 i32.load i32.const 72 i32.load call $output)
        (func $assert_eval_unavailable
          i32.const 64 i32.load8_u i32.const 1 i32.ne if unreachable end
          i32.const 68 i32.load8_u i32.const 1 i32.ne if unreachable end)
        (func $initialize
          i32.const {facts_script_ptr} i32.const {facts_script_len} i32.const 0 i32.const 0 i32.const 0 i32.const 64 call $eval
          call $assert_eval_unavailable
          i32.const 64 call $host_info
          {info_check}
          i32.const {probe_script_ptr} i32.const {probe_script_len} i32.const 0 i32.const 0 i32.const 0 i32.const 64 call $eval
          call $assert_eval_unavailable)
        (start $initialize)
        (func (export "describe") (param i32 i32) (result i32)
          i32.const {facts_script_ptr} i32.const {facts_script_len} i32.const 0 i32.const 0 i32.const 0 i32.const 64 call $eval
          call $assert_eval_unavailable
          i32.const {probe_script_ptr} i32.const {probe_script_len} i32.const 0 i32.const 0 i32.const 0 i32.const 64 call $eval
          call $assert_eval_unavailable
          i32.const 64 call $host_info {info_check}
          {describe_info})
        (func (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32)
          local.get 3 i32.eqz if
            i32.const {facts_script_ptr} i32.const {facts_script_len} i32.const 0 i32.const 0 i32.const 0 i32.const 64 call $eval
            call $assert_eval_unavailable
            i32.const 0 return
          end
          i32.const 64 call $host_info {info_check}
          local.get 0 local.get 1 i32.const 68 i32.load i32.const 72 i32.load call $equal i32.eqz if unreachable end
          {dispatch}
          unreachable))
      (core instance $functions
        (export "info" (func $lower_info))
        (export "eval" (func $lower_eval)))
      (core instance $instance (instantiate $code
        (with "memory" (instance $memory)) (with "host" (instance $functions))))
      (func (export "describe") (param "host-info" string) (result (result string (error string)))
        (canon lift (core func $instance "describe") (memory (core memory $memory "memory"))
          (realloc (core func $memory "realloc"))))
      (func (export "invoke") (param "host-info" string) (param "name" string) (param "arguments" string)
        (result (result string (error string)))
        (canon lift (core func $instance "invoke") (memory (core memory $memory "memory"))
          (realloc (core func $memory "realloc")))))"#,
        data = data.wat,
        prefix_end = 32768 + prefix_len,
        prefix_plus_end = prefix_len + 1,
    );
    let init = typed_agent_fixture::READY_INIT_BODY.replace(
        "(local $result i32)",
        "(local $result i32) i32.const 0 i32.const 0 i32.const 0 i32.const 0 i32.const 0 i32.const 0 call $invoke drop",
    );
    typed_agent_fixture::component_with_init(&text, &init)
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
                    ContentItem::Tool {
                        call_id, content, ..
                    } if call_id == &expected => Some(content),
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

#[test]
fn obsolete_session_variables_and_references_imports_are_rejected_before_discovery() {
    let operations = [("environment".into(), Operation::Info)];
    let host = Arc::new(RecordingHost::default());
    WasmAgent::load(
        &component(&operations, false, false),
        WasmLimits::default(),
        host.clone(),
    )
    .unwrap();

    for (version, extra_exports) in [
        ("0.1.0", ""),
        (
            "0.2.0",
            r#"(export "variable-get-all" (func (result (result string (error $error)))))"#,
        ),
    ] {
        let obsolete = format!(
            r#"(import "microcrab:session/variables@{version}" (instance
                (alias outer 1 $communication_error (type $raw_error))
                (export "communication-error" (type $error (eq $raw_error)))
                (export "variable-get" (func (param "name" string) (result (result string (error $error)))))
                (export "variable-change" (func (param "change" string) (result (result (error $error)))))
                {extra_exports}))"#,
        );
        let bytes = component_with_extra_import(&operations, false, false, &obsolete);
        match WasmAgent::load(&bytes, WasmLimits::default(), host.clone()) {
            Err(Error::Wasm(message)) => assert!(
                message.as_str().starts_with("unsupported component import;"),
                "{version}: {message}"
            ),
            Err(error) => panic!("{version}: expected unsupported import, got {error:?}"),
            Ok(_) => panic!("obsolete Variables {version} loaded successfully"),
        }
    }

    let obsolete_refs = r#"(import "microcrab:session/references@0.3.0" (instance
        (export "create" (func (param "title" string) (param "media-type" string) (param "data" (list u8)) (result (result string (error string)))))))"#;
    let bytes = component_with_extra_import(&operations, false, false, obsolete_refs);
    match WasmAgent::load(&bytes, WasmLimits::default(), host) {
        Err(Error::Wasm(message)) => assert!(message.as_str().starts_with("unsupported component import;")),
        Err(err) => panic!("expected obsolete references rejection, got {err}"),
        Ok(_) => panic!("expected obsolete references rejection, got Ok"),
    }
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

#[tokio::test]
async fn tool_call_facts_are_unavailable_through_init_but_return_admitted_identity_to_invoke() {
    let tools = load(&[call_facts("call_facts_probe")], Arc::new(RecordingHost::default()));
    let unix_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    };
    for (session_id, mode) in [
        ("fixture-session-a", Some("admitted-mode")),
        ("fixture-session-b", None),
    ] {
        let mut context = invocation();
        context.session_id = session_id.into();
        context.mode = mode.map(str::to_owned);
        let before = unix_ms();
        let raw = tools[0].execute_initialized(json!({}), context).await.unwrap();
        let after = unix_ms();
        let output: Value = serde_json::from_str(&final_text(raw)).unwrap();
        let facts = value(&output);
        assert_eq!(facts["session_id"], session_id);
        assert_eq!(facts["admitted_mode"], json!(mode));
        let observed = facts["observed_at_unix_ms"].as_u64().unwrap() as u128;
        assert!(observed >= before && observed <= after);
    }
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
            change("write", json!({"set":{"secret":"must-not-exist"},"remove":[]})),
        ],
        host.clone(),
    );
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
    session.close().await.unwrap();
}

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
async fn typed_guest_changes_json_values_with_agent_ownership() {
    use VariablePersistence::{Ephemeral, Persisted};
    use VariableRole::{Agent, Client};
    use VariableVisibility::{Invisible, Visible};
    use microcrab_protocol::ScopedVariableSet;

    let original = json!({"nested":[null, false, 2, "雪"]});
    let edited = json!(["client may edit the value", true]);
    let check_meta = r#"
        let m = vars::metadata()[args.name];
        if m == () { return #{ ok: false, code: "absent" }; }
        assert_eq(m.visibility, args.visibility);
        assert_eq(m.persistence, args.persistence);
        assert_eq(m.owner, args.owner);
        #{ ok: true }
    "#;
    let operations = [
        change(
            "unauthorized",
            json!({"set":{"client_owned":2,"partial":true},"remove":[]}),
        ),
        get("unchanged", "client_owned"),
        get("no_partial", "partial"),
        change(
            "create",
            json!(VariablesChange {
                set: serde_json::Map::from_iter([("null_value".into(), Value::Null)]),
                remove: vec!["old".into()],
                scoped_set: vec![
                    ScopedVariableSet {
                        name: "guest_owned".into(),
                        value: original.clone(),
                        visibility: Visible,
                        persistence: Persisted,
                        editors: Some(vec![Client]),
                    },
                    ScopedVariableSet {
                        name: "private".into(),
                        value: json!("not model-visible"),
                        visibility: Invisible,
                        persistence: Ephemeral,
                        editors: None,
                    },
                ],
            }),
        ),
        get("read_guest", "guest_owned"),
        get("read_null", "null_value"),
        get("read_old", "old"),
        (
            "guest_metadata".into(),
            Operation::Script {
                script: r#"let m = vars::metadata().guest_owned; if m.visibility == "visible" && m.persistence == "persisted" && m.owner == "agent" { #{ ok: true } } else { #{ ok: false, code: "mismatch", meta: m } }"#.into(),
                args: None,
            },
        ),
        (
            "private_metadata".into(),
            Operation::Script {
                script: r#"let m = vars::metadata()["private"]; if m.visibility == "invisible" && m.persistence == "ephemeral" && m.owner == "agent" { #{ ok: true } } else { #{ ok: false, code: "mismatch", meta: m } }"#.into(),
                args: None,
            },
        ),
    ];
    let _ = (check_meta, Agent, Visible, Invisible, Persisted, Ephemeral);
    let tools = load(&operations, Arc::new(RecordingHost::default()));
    let script = Script::new(operations.iter().map(|(name, _)| name.clone()));
    let session = session(&tools, script.clone(), false);
    let state = session.state_handle();
    state.inject("client_owned", json!(1)).await.unwrap();
    agent_state(&session).inject("old", json!(0)).await.unwrap();
    session
        .submit("Exercise typed variable ownership")
        .await
        .unwrap();

    let outputs = script.results();
    assert_eq!(outputs.len(), operations.len());
    code(&outputs[0], "invalid_argument");
    assert_eq!(value(&outputs[1]), &json!(1));
    code(&outputs[2], "absent");
    assert_eq!(outputs[3], json!({"ok":true}));
    assert_eq!(value(&outputs[4]), &original);
    assert_eq!(value(&outputs[5]), &Value::Null);
    code(&outputs[6], "absent");
    assert_eq!(outputs[7], json!({"ok":true}));
    assert_eq!(outputs[8], json!({"ok":true}));

    state.inject("guest_owned", edited.clone()).await.unwrap();
    assert_eq!(state.extract("guest_owned"), Some(edited));
    assert!(matches!(
        state.remove("guest_owned").await,
        Err(Error::VariableForbidden(_))
    ));
    session.close().await.unwrap();
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
    for index in 0..invalid.len() {
        calls.extend([format!("invalid_{index}"), "kept".into(), "created".into()]);
    }
    let script = Script::new(calls);
    let session = session(&tools, script.clone(), false);
    let state = session.state_handle();
    agent_state(&session).inject("old", json!(1)).await.unwrap();
    agent_state(&session).inject("kept", json!(1)).await.unwrap();
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
        code(&outputs[base], "invalid_argument");
        assert_eq!(value(&outputs[base + 1]), &json!(2));
        assert_eq!(value(&outputs[base + 2]), &Value::Null);
    }
    assert_eq!(state.variables(), json!({"created":null,"kept":2}));
    session.close().await.unwrap();
}

#[tokio::test]
async fn typed_guest_is_agent_bound_and_cannot_partially_commit() {
    let host = Arc::new(RecordingHost::default());
    let tools = load(
        &[
            change(
                "unauthorized",
                json!({"set":{"client_owned":2,"partial":true},"remove":[]}),
            ),
            change("create", json!({"set":{"guest_owned":3},"remove":[]})),
        ],
        host,
    );
    let script = Script::new(["unauthorized", "create"]);
    let session = session(&tools, script.clone(), false);
    session
        .state_handle()
        .inject("client_owned", json!(1))
        .await
        .unwrap();
    session.submit("Exercise guest governance").await.unwrap();
    let outputs = script.results();
    code(&outputs[0], "invalid_argument");
    assert_eq!(outputs[1], json!({"ok":true}));
    let state = session.state_handle();
    assert_eq!(state.extract("client_owned"), Some(json!(1)));
    assert!(state.extract("partial").is_none());
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
