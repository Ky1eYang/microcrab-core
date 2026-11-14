//! Shared typed shell-import component construction for Session and Server regressions.
pub fn shell_component_source(discovery_calls_host: bool, catalog: &str, repeat: usize) -> String {
    let escaped: String = catalog.bytes().map(|b| format!("\\{b:02x}")).collect();
    let discovery = if discovery_calls_host {
        "call $execute drop i32.const 64 i32.load8_u if unreachable end"
    } else {
        ""
    };
    let text = format!(
        r#"(component
      {shell_import}
      (alias export $host_shell "launch" (func $shell))
      (core module $storage
        (memory (export "memory") 1)
        (global $heap (mut i32) (i32.const 4096))
        (func (export "realloc") (param $old_ptr i32) (param $old_size i32) (param $align i32) (param $size i32) (result i32)
          (local $new i32)
          global.get $heap local.get $align i32.const 1 i32.sub i32.add
          i32.const 0 local.get $align i32.sub i32.and local.tee $new
          local.get $size i32.add global.set $heap
          local.get $old_ptr if
            local.get $new local.get $old_ptr
            local.get $old_size local.get $size
            local.get $old_size local.get $size i32.lt_u select
            memory.copy
          end
          local.get $new))
      (core instance $memory (instantiate $storage))
      (core func $lower (canon lower (func $shell)
        (memory (core memory $memory "memory")) (realloc (core func $memory "realloc"))))
      (core module $code
        (import "memory" "memory" (memory 1))
        (import "host" "shell" (func $shell (param i32 i32 i32 i32 i32 i32)))
        (data (i32.const 128) "{escaped}")
        (data (i32.const 2048) "[\22execute\22]")
        (data (i32.const 3000) "typed-fixture")
        (data (i32.const 3100) "{{\22stdout\22:\22typed-fixture\22}}")
        (data (i32.const 3200) "permission denied")
        (func $execute (result i32)
          i32.const 3000 i32.const 13 i32.const 0 i32.const 0 i32.const 0 i32.const 64 call $shell
          i32.const 64 i32.load8_u)
        (func (export "describe") (param i32 i32) (result i32)
          local.get 1 i32.eqz if unreachable end
          {discovery}
          i32.const 0 i32.const 0 i32.store
          i32.const 4 i32.const 128 i32.store
          i32.const 8 i32.const {length} i32.store i32.const 0)
        (func (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32)
          local.get 1 i32.eqz if unreachable end
          {repeated}
          call $execute
          if
            i32.const 16 i32.const 1 i32.store
            i32.const 20 i32.const 3200 i32.store
            i32.const 24 i32.const 17 i32.store
          else
            i32.const 16 i32.const 0 i32.store
            i32.const 20 i32.const 3100 i32.store
            i32.const 24 i32.const 26 i32.store
          end i32.const 16))
      (core instance $functions (export "shell" (func $lower)))
      (core instance $instance (instantiate $code
        (with "memory" (instance $memory)) (with "host" (instance $functions))))
      (func (export "describe") (param "host-info" string) (result (result string (error string)))
        (canon lift (core func $instance "describe") (memory (core memory $memory "memory"))
          (realloc (core func $memory "realloc"))))
      (func (export "invoke") (param "host-info" string) (param "name" string) (param "arguments" string)
        (result (result string (error string)))
        (canon lift (core func $instance "invoke") (memory (core memory $memory "memory"))
          (realloc (core func $memory "realloc")))))"#,
        length = catalog.len(),
        shell_import = r#"(import "microcrab:host/types@0.1.0" (instance $types
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
          (export "launch" (func (param "request" $request_public) (result (result $handle_public (error $error_public)))))))"#,
        repeated = "call $execute drop ".repeat(repeat),
    );
    text
}
