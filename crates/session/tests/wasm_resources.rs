#![cfg(feature = "wasm")]

//! Real linked resource interfaces exercise canonical ABI types and Runtime scope.
use crate::typed_agent_fixture;
use microcrab_host_api::{HostAPI, Platform};
use microcrab_host_local::LocalHost;
use microcrab_session::{
    SessionState,
    runtime::{
        tool::ToolContext,
        wasm::{WasmAgent, WasmLimits},
    },
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use typed_agent_fixture::{InitializedTool as _, final_text};

#[derive(Clone, Copy)]
enum Resource {
    Net,
    Filesystem,
}

/// Each guest imports only the resource function it exercises. Shared error
/// identities come from host/types rather than a duplicate private record.
fn component(resource: Option<Resource>, invoke_probe: &str) -> Vec<u8> {
    let catalog = json!([{
        "name":"resource-fixture", "modes":[{"name":"execute","role":"both"}],
        "variables":{"rules":[],"hints":[]},
        "tools":[{"name":"read","description":"Inspect a resource","parameters":{"type":"object"}}]
    }])
    .to_string();
    let escaped: String = catalog
        .bytes()
        .map(|byte| format!("\\{byte:02x}"))
        .collect();
    let resource_interface = match resource {
        None => "",
        Some(Resource::Net) => {
            r#"
      (import "microcrab:host/net@0.1.0" (instance $resource
        (alias outer 1 $host_error (type $error))
        (export "host-error" (type $error_public (eq $error)))
        (export "resolve" (func (param "name" string) (result (result (list string) (error $error_public)))))))
      (alias export $resource "resolve" (func $operation))"#
        }
        Some(Resource::Filesystem) => {
            r#"
      (import "microcrab:host/filesystem@0.1.0" (instance $resource
        (alias outer 1 $host_error (type $error))
        (export "host-error" (type $error_public (eq $error)))
        (type $kind (enum "file" "directory" "symlink" "other"))
        (export "file-kind" (type $kind_public (eq $kind)))
        (type $meta (record (field "kind" $kind_public) (field "size" u64) (field "modified-ms" (option u64))))
        (export "file-meta" (type $meta_public (eq $meta)))
        (export "stat" (func (param "path" string) (result (result $meta_public (error $error_public)))))))
      (alias export $resource "stat" (func $operation))"#
        }
    };
    let resource_imports = if resource.is_some() {
        format!(
            r#"(import "microcrab:host/types@0.1.0" (instance $types
        (type $category (enum "not-found" "permission" "timeout" "cancelled" "conflict" "unsupported" "io"))
        (export "error-category" (type $category_public (eq $category)))
        (type $error (record (field "category" $category_public) (field "retryable" bool) (field "message" string)))
        (export "host-error" (type $error_public (eq $error)))))
      (alias export $types "host-error" (type $host_error))
      {resource_interface}"#
        )
    } else {
        String::new()
    };
    let (lower_operation, core_import, core_export) = if resource.is_some() {
        (
            r#"(core func $lower_operation (canon lower (func $operation)
        (memory (core memory $memory "memory")) (realloc (core func $memory "realloc"))))"#,
            r#"(import "host" "operation" (func $operation (param i32 i32 i32)))"#,
            r#"(export "operation" (func $lower_operation))"#,
        )
    } else {
        ("", "", "")
    };
    let text = format!(
        r#"(component
      (import "microcrab:agent/host-info@0.8.0" (instance $host
        (type $code (enum "absent" "unavailable" "rejected" "cancelled"))
        (export "communication-error-code" (type $code_public (eq $code)))
        (type $error (record (field "code" $code_public) (field "message" string)))
        (export "communication-error" (type $error_public (eq $error)))
        (export "host-info" (func (result (result string (error $error_public)))))))
      (alias export $host "host-info" (func $host_info))
      {resource_imports}
      (core module $storage
        (memory (export "memory") 2)
        (global $heap (mut i32) (i32.const 32768))
        (func (export "realloc") (param i32 i32 i32 i32) (result i32)
          (local $old i32)
          global.get $heap local.tee $old
          local.get 3 i32.add global.set $heap local.get $old))
      (core instance $memory (instantiate $storage))
      (core func $lower_info (canon lower (func $host_info)
        (memory (core memory $memory "memory")) (realloc (core func $memory "realloc"))))
      {lower_operation}
      (core module $code
        (import "memory" "memory" (memory 2))
        (import "host" "info" (func $host_info (param i32)))
        {core_import}
        (data (i32.const 1024) "{escaped}")
        (data (i32.const 4096) "localhost")
        (data (i32.const 4128) "allowed.txt")
        (data (i32.const 4160) "other.txt")
         (data (i32.const 4200) "../outside.txt")
        (func $info
          i32.const 64 call $host_info
          i32.const 64 i32.load8_u if unreachable end)
        (func (export "describe") (param i32 i32) (result i32)
          call $info
          i32.const 0 i32.const 0 i32.store
          i32.const 4 i32.const 1024 i32.store
          i32.const 8 i32.const {catalog_len} i32.store i32.const 0)
        (func (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32)
          call $info
          ;; Preserve HostInfo before a resource overwrites the shared return area.
          i32.const 32 i32.const 0 i32.store
          i32.const 36 i32.const 68 i32.load i32.store
          i32.const 40 i32.const 72 i32.load i32.store
          {invoke_probe}
          ;; Return HostInfo after checking the typed resource outcome.
          i32.const 32))
      (core instance $functions
        (export "info" (func $lower_info))
        {core_export})
      (core instance $instance (instantiate $code
        (with "memory" (instance $memory)) (with "host" (instance $functions))))
      (func (export "describe") (param "host-info" string) (result (result string (error string)))
        (canon lift (core func $instance "describe") (memory (core memory $memory "memory"))
          (realloc (core func $memory "realloc"))))
      (func (export "invoke") (param "host-info" string) (param "name" string) (param "arguments" string)
        (result (result string (error string)))
        (canon lift (core func $instance "invoke") (memory (core memory $memory "memory"))
          (realloc (core func $memory "realloc")))))"#,
        catalog_len = catalog.len(),
    );
    typed_agent_fixture::component(&text)
}

fn context() -> ToolContext {
    ToolContext::detached(
        SessionState::new(Default::default()),
        CancellationToken::new(),
    )
}

#[tokio::test]
async fn host_info_only_standard_guest_projects_features_without_resource_capabilities() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(LocalHost::new(root.path(), Default::default()).unwrap());
    let features = host.capabilities().clone();
    let platform = match host.platform() {
        Platform::Linux => "linux",
        Platform::MacOS => "macos",
        Platform::Windows => "windows",
        Platform::Other => "other",
    };
    let agent = WasmAgent::load(&component(None, ""), WasmLimits::default(), host).unwrap();
    let text = agent.tools()[0]
        .execute_initialized(json!({}), context())
        .await
        .unwrap();
    let info: Value = serde_json::from_str(&final_text(text)).unwrap();
    assert_eq!(info["capabilities"], json!([]));
    assert_eq!(
        info["resources"],
        json!({
            "platform":platform,
            "features":{
                "process_pty":features.process_pty,
                "process_kill_tree":features.process_kill_tree,
                "fs_watch":features.fs_watch,
                "fs_symlink":features.fs_symlink,
                "net_listen":features.net_listen,
                "net_udp":features.net_udp,
            },
        })
    );
}

#[tokio::test]
async fn linked_net_resolve_returns_typed_unsupported_and_is_not_advertised() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(LocalHost::new(root.path(), Default::default()).unwrap());
    // list<string> and host-error align to four bytes: error payload starts at 68.
    let probe = r#"
          i32.const 4096 i32.const 9 i32.const 64 call $operation
          i32.const 64 i32.load8_u i32.const 1 i32.ne if unreachable end
          i32.const 68 i32.load8_u i32.const 5 i32.ne if unreachable end
          i32.const 69 i32.load8_u if unreachable end
          i32.const 76 i32.load i32.eqz if unreachable end"#;
    let agent = WasmAgent::load(
        &component(Some(Resource::Net), ""),
        WasmLimits::default(),
        host.clone(),
    )
    .unwrap();
    let text = agent.tools()[0]
        .execute_initialized(json!({}), context())
        .await
        .unwrap();
    let info: Value = serde_json::from_str(&final_text(text)).unwrap();
    assert_eq!(info["capabilities"], json!([]));
    assert_eq!(info["resources"]["features"]["net_listen"], false);
    assert_eq!(info["resources"]["features"]["net_udp"], false);

    let agent = WasmAgent::load(
        &component(Some(Resource::Net), probe),
        WasmLimits::default(),
        host,
    )
    .unwrap();
    // The guest checks canonical typed Unsupported. Runtime preserves the known
    // unsupported mapping as a concrete Host failure when the guest swallows it.
    let error = agent.tools()[0]
        .execute_initialized(json!({}), context())
        .await
        .unwrap_err();
    let diagnostic = error.tool_diagnostic();
    use microcrab_session::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticOperation, DiagnosticPhase, EffectKnowledge,
    };
    assert_eq!(diagnostic.code, DiagnosticCode::Unsupported);
    assert_eq!(diagnostic.layer, DiagnosticLayer::Host);
    assert_eq!(diagnostic.phase, DiagnosticPhase::HostOperation);
    assert_eq!(diagnostic.operation, Some(DiagnosticOperation::Resource));
    assert_eq!(diagnostic.effects, EffectKnowledge::Unknown);
    assert_eq!(
        diagnostic.summary(),
        "Host operation unavailable: no scope-preserving Runtime mapping"
    );
    assert_eq!(diagnostic.retryable, Some(false));
}

#[tokio::test]
async fn admitted_component_can_stat_an_internal_target_distinct_from_submitted_args() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("allowed.txt"), b"resource fixture").unwrap();
    std::fs::write(root.path().join("other.txt"), b"off-target fixture").unwrap();
    let host = Arc::new(LocalHost::new(root.path(), Default::default()).unwrap());
    // file-meta aligns to eight bytes: payload starts at 72 and size is at 80.
    for (offset, length, size) in [(4128, 11, 16), (4160, 9, 18)] {
        let probe = format!(
            r#"
          i32.const {offset} i32.const {length} i32.const 64 call $operation
          i32.const 64 i32.load8_u if unreachable end
          i32.const 72 i32.load8_u if unreachable end
          i32.const 80 i64.load i64.const {size} i64.ne if unreachable end"#
        );
        let agent = WasmAgent::load(
            &component(Some(Resource::Filesystem), &probe),
            WasmLimits::default(),
            host.clone(),
        )
        .unwrap();
        let output = agent.tools()[0]
            .execute_initialized(json!({"path":"allowed.txt"}), context())
            .await
            .unwrap();
        let info: Value = serde_json::from_str(&final_text(output)).unwrap();
        assert_eq!(info["capabilities"], json!(["filesystem.stat"]));
    }
}

#[tokio::test]
async fn linked_target_boundary_and_policy_denial_remain_distinct_in_context_and_audit() {
    use microcrab_protocol::{Response, ToolCall, Usage};
    use microcrab_session::{
        AuditBody, Auditor, Client, Moment, Session,
        context::{Content, ContentItem},
        runtime::{Runtime, ToolRegistration, tool::AllowTools},
    };
    struct Model(Mutex<bool>);
    #[async_trait::async_trait]
    impl Client for Model {
        async fn complete(
            &self,
            _: &Content,
            _: &dyn Auditor,
            _: &dyn microcrab_session::VariableView,
            _: CancellationToken,
        ) -> Result<Response, microcrab_transport::Error> {
            let mut called = self.0.lock().unwrap();
            let first = !*called;
            *called = true;
            Ok(Response {
                provider_metadata: None,
                content: if first { String::new() } else { "done".into() },
                tool_calls: if first {
                    vec![ToolCall {
                        provider_metadata: None,
                        id: "path-probe".into(),
                        name: "read".into(),
                        arguments: json!({"path":"allowed.txt"}),
                    }]
                } else {
                    vec![]
                },
                usage: Usage::default(),
            })
        }
    }
    #[derive(Default)]
    struct Audit(Mutex<Vec<AuditBody>>);
    impl Auditor for Audit {
        fn is_enabled(&self) -> bool {
            true
        }
        fn publish(&self, moment: Moment, body: AuditBody) {
            if moment == Moment::ToolResult {
                self.0.lock().unwrap().push(body);
            }
        }
    }
    for outside in [true, false] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("other.txt"), "policy protected").unwrap();
        let host = Arc::new(LocalHost::new(root.path(), Default::default()).unwrap());
        // Outside-root imports fail structurally; the other case denies the whole tool.
        // The old ABI represents boundary validation as Conflict, never Permission.
        let probe = if outside {
            "i32.const 4200 i32.const 14 i32.const 64 call $operation\n i32.const 64 i32.load8_u i32.const 1 i32.ne if unreachable end\n i32.const 72 i32.load8_u i32.const 4 i32.ne if unreachable end"
        } else {
            "i32.const 4160 i32.const 9 i32.const 64 call $operation\n i32.const 64 i32.load8_u i32.const 1 i32.ne if unreachable end\n i32.const 72 i32.load8_u i32.const 1 i32.ne if unreachable end"
        };
        let agent = WasmAgent::load(
            &component(Some(Resource::Filesystem), probe),
            WasmLimits::default(),
            host.clone(),
        )
        .unwrap();
        let runtime = Runtime::new(
            agent
                .tools()
                .into_iter()
                .map(ToolRegistration::Wasm)
                .collect(),
            Arc::new(AllowTools::new(["read"])),
        )
        .unwrap();
        let audit = Arc::new(Audit::default());
        let permissions = serde_json::from_value(json!({
            "initialization":"allow",
            "overrides":[{"tool":{"kind":"exact","value":"read"},"action":if outside {"allow"} else {"deny"}}]
        })).unwrap();
        let session = Session::builder(Model(Mutex::new(false)), runtime)
            .permissions(permissions)
            .host(host)
            .agent(agent.agents()[0].clone())
            .auditor(audit.clone())
            .build()
            .unwrap();
        session.start().await.unwrap();
        session.submit("Inspect the resource").await.unwrap();
        let output = session
            .messages()
            .into_iter()
            .find_map(|item| match item {
                ContentItem::Tool { call_id, content, .. } if call_id == "path-probe" => Some(content),
                _ => None,
            })
            .unwrap();
        let value: Value = serde_json::from_str(&output).unwrap();
        let diagnostic = &value["diagnostic"];
        assert_eq!(
            diagnostic["code"],
            if outside {
                "path_outside_root"
            } else {
                "permission_denied"
            }
        );
        assert_eq!(
            diagnostic["layer"],
            if outside { "host" } else { "runtime" }
        );
        assert_eq!(
            diagnostic["phase"],
            if outside {
                "validation"
            } else {
                "authorization"
            }
        );
        assert_eq!(diagnostic["effects"], "unknown");
        if outside {
            assert_eq!(diagnostic["operation_effects"], "not_started");
            assert!(
                value["error"]
                    .as_str()
                    .unwrap()
                    .contains("outside the configured")
            );
        }
        assert!(!output.contains("outside.txt"));
        assert!(!output.contains(&root.path().to_string_lossy().to_string()));
        {
            let records = audit.0.lock().unwrap();
            let record = records
                .iter()
                .find(|record| record.tool_call_id.as_deref() == Some("path-probe"))
                .unwrap();
            let payload = serde_json::to_value(&record.payload).unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(payload["error"].as_str().unwrap()).unwrap(),
                value
            );
        }
        assert_eq!(
            std::fs::read_to_string(root.path().join("other.txt")).unwrap(),
            "policy protected"
        );
        assert!(!session.process_control().has_running());
        session.close().await.unwrap();
    }
}
