//! Typed 0.7 fixture ABI for Final-only handwritten components.
//!
//! Older fixture bodies deliberately exercise small core functions. This adapter
//! preserves their content/trap/import behavior while lifting the actual typed
//! Final result. It is test construction code, never runtime result inference.
use microcrab_session::runtime::execution::ToolOutput;

/// Direct embedding fixtures register the actual tool and obtain the same opaque
/// Runtime admission used by Session. Initialization has independent authority.
#[async_trait::async_trait]
pub trait InitializedTool: Send + Sync {
    async fn execute_initialized(
        &self,
        arguments: serde_json::Value,
        context: microcrab_session::runtime::tool::ToolContext,
    ) -> Result<ToolOutput, microcrab_session::Error>;
    async fn admitted_context(
        &self,
        arguments: serde_json::Value,
        context: microcrab_session::runtime::tool::ToolContext,
    ) -> Result<microcrab_session::runtime::tool::ToolContext, microcrab_session::Error>;
}
#[async_trait::async_trait]
impl InitializedTool for std::sync::Arc<dyn microcrab_session::runtime::tool::Tool> {
    async fn execute_initialized(
        &self,
        arguments: serde_json::Value,
        context: microcrab_session::runtime::tool::ToolContext,
    ) -> Result<ToolOutput, microcrab_session::Error> {
        let context = self.admitted_context(arguments.clone(), context).await?;
        let result = self.execute(arguments, context.clone()).await;
        match &result {
            Ok(output) => context.complete_detached_output(output).await?,
            Err(error) => context.fail_detached_execution(error).await?,
        }
        result
    }
    async fn admitted_context(
        &self,
        arguments: serde_json::Value,
        mut context: microcrab_session::runtime::tool::ToolContext,
    ) -> Result<microcrab_session::runtime::tool::ToolContext, microcrab_session::Error> {
        use microcrab_session::{
            policy::Action,
            runtime::{Runtime, ToolRegistration, tool::AllowTools},
        };
        static ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let runtime = Runtime::new(
            vec![ToolRegistration::Wasm(self.clone())],
            std::sync::Arc::new(AllowTools::new([self.definition().name])),
        )?;
        let controller = runtime.permission_controller();
        let snapshot = controller.snapshot().map_err(admission_error)?;
        let mut config = snapshot.config;
        config.initialization = Action::Allow;
        runtime
            .replace_permissions(snapshot.revision, config)
            .map_err(admission_error)?;
        if context.call_id.is_empty() {
            context.call_id = format!("fixture-call-{attempt}");
        }
        context.tool = self.definition().name;
        context.ensure_detached_execution()?;
        let issuer = issuer();
        runtime
            .admit_invocation(
                &microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: context.call_id.clone(),
                    name: context.tool.clone(),
                    arguments: arguments.clone(),
                },
                &mut context,
                &issuer,
            )
            .await
            .map_err(admission_error)?;
        if let Some(lifecycle) = self.wasm_lifecycle() {
            let mut init = context.clone();
            init.subject = microcrab_session::policy::InvocationSubject::AgentInit {
                component: lifecycle.component().into(),
                generation: lifecycle.generation(&context.state)?,
                attempt,
            };
            init.tool.clear();
            init.call_id = format!("fixture-init-{attempt}");
            init.execution_id = None;
            init.execution_progress = None;
            init.cancellation = context.cancellation.child_token();
            init.admission = None;
            init.ensure_detached_execution()?;
            runtime
                .admit_initialization(&lifecycle, &mut init, &issuer)
                .await
                .map_err(admission_error)?;
            match lifecycle.initialize(init.clone()).await {
                Ok(()) => {
                    init.complete_detached_output(&ToolOutput::text("ready"))
                        .await?
                }
                Err(error) => {
                    init.fail_detached_execution(&error).await?;
                    return Err(error);
                }
            }
        }
        Ok(context)
    }
}

#[allow(dead_code)]
pub fn admission_error(
    error: microcrab_session::policy::PermissionError,
) -> microcrab_session::Error {
    match error {
        microcrab_session::policy::PermissionError::Cancelled => {
            microcrab_session::Error::Cancelled
        }
        error => {
            microcrab_session::Error::denied_with_private(error.public_reason(), error.to_string())
        }
    }
}

pub fn issuer() -> microcrab_session::context::Content {
    microcrab_session::context::Content {
        epoch: 0,
        subagent: None,
        system: "Direct WASM fixture admission".into(),
        items: vec![],
        tools: vec![],
        max_output_tokens: 1024,
        max_bytes: 65536,
        turn_id: None,
        round: None,
    }
}

// Shared WAT fixture targets use different subsets of the helpers.
#[allow(dead_code)]
pub fn final_text(output: ToolOutput) -> String {
    match output {
        ToolOutput::Final(output) => {
            assert!(
                output.failure.is_none(),
                "expected a successful Final fixture result"
            );
            output.content
        }
        ToolOutput::Buffered(_) => panic!("expected Final fixture output"),
    }
}

pub const READY_INIT_BODY: &str = r#"
  (local $result i32)
  i32.const 0 i32.const 0 i32.const 4 i32.const 20 call $realloc
  local.tee $result i32.const 0 i32.const 20 memory.fill
  local.get $result
"#;

pub fn component(text: &str) -> Vec<u8> {
    component_with_init(text, READY_INIT_BODY)
}

/// The supplied core body must return a canonical init-result pointer. It may
/// call $invoke (the original fixture entry) to exercise imported operations.
pub fn component_with_init(text: &str, init_body: &str) -> Vec<u8> {
    let start = text
        .find("(func (export \"invoke\") (param \"host-info\"")
        .expect("fixture exports invoke");
    let invoke = &text[start..];
    let core_start = invoke
        .find("(canon lift (core func ")
        .expect("invoke canonical lift")
        + "(canon lift (core func ".len();
    let source = &invoke[core_start..];
    let source_instance = source.split_whitespace().next().unwrap();
    let memory_start = invoke.find("(memory (core memory ").expect("invoke memory")
        + "(memory (core memory ".len();
    let memory_instance = invoke[memory_start..].split_whitespace().next().unwrap();
    let realloc_start =
        invoke.find("(realloc (core func ").expect("invoke realloc") + "(realloc (core func ".len();
    let realloc_instance = invoke[realloc_start..].split_whitespace().next().unwrap();
    let types = r#"
      (import "microcrab:agent/execution@0.7.0" (instance $fixture_execution
        (export "buffer" (type $buffer (sub resource)))
        (type $kind_raw (enum "failed" "timeout" "cancelled"))
        (export "failure-kind" (type $kind (eq $kind_raw)))
        (type $failure_raw (record (field "kind" $kind)
          (field "code" string) (field "message" string) (field "retryable" bool)))
        (export "tool-failure" (type $failure (eq $failure_raw)))
        (type $final_raw (record (field "content" string) (field "failure" (option $failure))))
        (export "final-output" (type $final (eq $final_raw)))
        (type $buffered_raw (record (field "output" (own $buffer)) (field "include-initial" bool)))
        (export "buffered-output" (type $buffered (eq $buffered_raw)))
        (type $output_raw (variant (case "final" $final) (case "buffered" $buffered)))
        (export "tool-output" (type (eq $output_raw)))
        (type $step_raw (record (field "state" (list u8)) (field "wake-after-ms" u64) (field "finished" bool)))
        (export "driver-step" (type (eq $step_raw)))))
      (alias export $fixture_execution "buffer" (type $fixture_buffer_alias))
      (import "buffer" (type $fixture_buffer (eq $fixture_buffer_alias)))
      (alias export $fixture_execution "tool-failure" (type $fixture_failure_alias))
      (import "tool-failure" (type $fixture_failure (eq $fixture_failure_alias)))
      (alias export $fixture_execution "tool-output" (type $fixture_output_alias))
      (import "tool-output" (type $fixture_output (eq $fixture_output_alias)))
      (alias export $fixture_execution "driver-step" (type $fixture_step_alias))
      (import "driver-step" (type $fixture_step (eq $fixture_step_alias)))
      (type $fixture_init_error_raw (record (field "code" string) (field "message" string)))
      (import "init-error" (type $fixture_init_error (eq $fixture_init_error_raw)))
      (type $fixture_init_result_raw (variant (case "ready") (case "failed" $fixture_init_error)))
      (import "init-result" (type $fixture_init_result (eq $fixture_init_result_raw)))
"#;
    let adapter = format!(
        r#"
      (core module $fixture_adapter_module
        (import "source" "memory" (memory 0))
        (import "source" "invoke" (func $invoke (param i32 i32 i32 i32 i32 i32) (result i32)))
        (import "source" "realloc" (func $realloc (param i32 i32 i32 i32) (result i32)))
        (func (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32)
          (local $old i32) (local $new i32)
          local.get 0 local.get 1 local.get 2 local.get 3 local.get 4 local.get 5
          call $invoke local.set $old
          i32.const 0 i32.const 0 i32.const 4 i32.const 64 call $realloc
          i32.const 3 i32.add i32.const -4 i32.and local.set $new
          local.get $new i32.const 0 i32.const 44 memory.fill
          local.get $old i32.load8_u
          if
            ;; Typed Err(tool-failure), fixed code and original fixture message.
            local.get $new i32.const 1 i32.store8
            local.get $new i32.const 44 i32.add i64.const 0x5f65727574786966 i64.store
            local.get $new i32.const 52 i32.add i32.const 0x6c696166 i32.store
            local.get $new i32.const 56 i32.add i32.const 0x6465 i32.store16
            local.get $new i32.const 8 i32.add local.get $new i32.const 44 i32.add i32.store
            local.get $new i32.const 12 i32.add i32.const 14 i32.store
            local.get $new i32.const 16 i32.add local.get $old i32.const 4 i32.add i32.load i32.store
            local.get $new i32.const 20 i32.add local.get $old i32.const 8 i32.add i32.load i32.store
          else
            ;; Outer Ok, tool-output Final, content pointer/length, failure None.
            local.get $new i32.const 8 i32.add local.get $old i32.const 4 i32.add i32.load i32.store
            local.get $new i32.const 12 i32.add local.get $old i32.const 8 i32.add i32.load i32.store
          end
          local.get $new)
        (func (export "init") (result i32) {init_body})
        (func (export "drive") (param i32 i32 i32 i32) (result i32) unreachable))
      (alias core export {source_instance} "invoke" (core func $fixture_source_invoke))
      (alias core export {memory_instance} "memory" (core memory $fixture_source_memory))
      (alias core export {realloc_instance} "realloc" (core func $fixture_source_realloc))
      (core instance $fixture_adapter (instantiate $fixture_adapter_module
        (with "source" (instance
          (export "memory" (memory $fixture_source_memory))
          (export "invoke" (func $fixture_source_invoke))
          (export "realloc" (func $fixture_source_realloc))))))
      (func (export "init") (result $fixture_init_result)
        (canon lift (core func $fixture_adapter "init")
          (memory (core memory {memory_instance} "memory"))
          (realloc (core func {realloc_instance} "realloc"))))
      (func (export "drive") (param "output" (borrow $fixture_buffer)) (param "state" (list u8))
        (param "cancelled" bool) (result (result $fixture_step (error $fixture_failure)))
        (canon lift (core func $fixture_adapter "drive")
          (memory (core memory {memory_instance} "memory"))
          (realloc (core func {realloc_instance} "realloc"))))
"#
    );
    let invoke = invoke
        .replacen(
            "(result (result string (error string)))",
            "(result (result $fixture_output (error $fixture_failure)))",
            1,
        )
        .replacen(
            &format!("(canon lift (core func {source_instance} \"invoke\")"),
            "(canon lift (core func $fixture_adapter \"invoke\")",
            1,
        );
    let mut typed = String::with_capacity(text.len() + types.len() + adapter.len());
    typed.push_str(&text[..start]);
    typed.push_str(&adapter);
    typed.push_str(&invoke);
    typed = typed.replacen("(component", &format!("(component{types}"), 1);
    wat::parse_str(typed).expect("valid typed 0.7 fixture component")
}
