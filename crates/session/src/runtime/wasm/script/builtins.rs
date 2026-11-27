//! Rhai native bindings for `vars`, `refs`, `state`, and `session` modules.
use super::{
    CapabilityGroup, ScriptFault, ScriptRuntimeContext, block_on_session, with_ctx,
};
use crate::runtime::execution::job;
use crate::runtime::tool::{Tool, ToolContext, delegate, messaging, sleep, switch_mode};
use crate::runtime::wasm::admission_error;
use crate::state::reference::ReferencePayload;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use microcrab_protocol::{
    ScopedVariableSet, VariablePersistence, VariableRole, VariableVisibility, VariablesChange,
};
use rhai::{
    Array, Dynamic, Map, Module,
    serde::{from_dynamic, to_dynamic},
};
use serde_json::{Map as JsonMap, Value, json};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn register_vars(module: &mut Module) {
    module.set_native_fn("has", |name: &str| {
        with_ctx(CapabilityGroup::VarsReadWrite, |ctx| {
            let binding = ctx
                .binding
                .as_ref()
                .ok_or_else(|| ScriptFault::unavailable("no active Session binding"))?;
            let present = binding
                .get(name)
                .map_err(ScriptFault::from_session_error)?
                .is_some();
            Ok(Dynamic::from(present))
        })
    });

    module.set_native_fn("get", |name: &str| {
        with_ctx(CapabilityGroup::VarsReadWrite, |ctx| {
            let binding = ctx
                .binding
                .as_ref()
                .ok_or_else(|| ScriptFault::unavailable("no active Session binding"))?;
            match binding.get(name) {
                Ok(Some(val)) => to_dynamic(val).map_err(|e| ScriptFault::invalid(e.to_string())),
                Ok(None) => Ok(Dynamic::UNIT),
                Err(err) => Err(ScriptFault::from_session_error(err)),
            }
        })
    });

    module.set_native_fn("get_all", || {
        with_ctx(CapabilityGroup::VarsReadWrite, |ctx| {
            let binding = ctx
                .binding
                .as_ref()
                .ok_or_else(|| ScriptFault::unavailable("no active Session binding"))?;
            let val = binding
                .get_all()
                .map_err(ScriptFault::from_session_error)?;
            to_dynamic(val).map_err(|e| ScriptFault::invalid(e.to_string()))
        })
    });

    module.set_native_fn("metadata", || {
        with_ctx(CapabilityGroup::VarsReadWrite, |ctx| {
            let binding = ctx
                .binding
                .as_ref()
                .ok_or_else(|| ScriptFault::unavailable("no active Session binding"))?;
            let val = binding
                .metadata()
                .map_err(ScriptFault::from_session_error)?;
            to_dynamic(val).map_err(|e| ScriptFault::invalid(e.to_string()))
        })
    });

    module.set_native_fn("set", |name: &str, value: Dynamic| {
        with_ctx(CapabilityGroup::VarsReadWrite, |ctx| {
            let binding = ctx
                .binding
                .clone()
                .ok_or_else(|| ScriptFault::unavailable("no active Session binding"))?;
            let json_val: Value =
                from_dynamic(&value).map_err(|e| ScriptFault::invalid(e.to_string()))?;
            let mut set = JsonMap::new();
            set.insert(name.to_owned(), json_val);
            let change = VariablesChange {
                set,
                remove: Vec::new(),
                scoped_set: Vec::new(),
            };
            block_on_session(ctx, "vars::set", |cancel| async move {
                binding.change(change, cancel).await
            })?;
            Ok(Dynamic::UNIT)
        })
    });

    module.set_native_fn(
        "set_scoped",
        |name: &str, value: Dynamic, visibility: &str, persistence: &str| {
            commit_scoped_var(name, value, visibility, persistence, None)
        },
    );

    module.set_native_fn(
        "set_scoped",
        |name: &str, value: Dynamic, visibility: &str, persistence: &str, editors: Array| {
            commit_scoped_var(name, value, visibility, persistence, Some(editors))
        },
    );

    module.set_native_fn("remove", |name: &str| {
        with_ctx(CapabilityGroup::VarsReadWrite, |ctx| {
            let binding = ctx
                .binding
                .clone()
                .ok_or_else(|| ScriptFault::unavailable("no active Session binding"))?;
            let change = VariablesChange {
                set: JsonMap::new(),
                remove: vec![name.to_owned()],
                scoped_set: Vec::new(),
            };
            block_on_session(ctx, "vars::remove", |cancel| async move {
                binding.change(change, cancel).await
            })?;
            Ok(Dynamic::UNIT)
        })
    });

    module.set_native_fn("change", |raw: Map| {
        with_ctx(CapabilityGroup::VarsReadWrite, |ctx| {
            let binding = ctx
                .binding
                .clone()
                .ok_or_else(|| ScriptFault::unavailable("no active Session binding"))?;
            let dyn_map = Dynamic::from_map(raw);
            let change: VariablesChange =
                from_dynamic(&dyn_map).map_err(|e| ScriptFault::invalid(e.to_string()))?;
            block_on_session(ctx, "vars::change", |cancel| async move {
                binding.change(change, cancel).await
            })?;
            Ok(Dynamic::UNIT)
        })
    });

    module.set_native_fn("call_facts", || {
        with_ctx(CapabilityGroup::CallFacts, |ctx| {
            let context = ctx
                .tool_context
                .as_ref()
                .filter(|tc| tc.subject.is_tool())
                .ok_or_else(|| {
                    ScriptFault::unavailable("vars::call_facts requires an active tool invocation")
                })?;
            let observed_at_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ScriptFault::unavailable("system clock unavailable"))?
                .as_millis() as u64;
            let facts = json!({
                "session_id": context.session_id,
                "admitted_mode": context.mode,
                "observed_at_unix_ms": observed_at_unix_ms,
            });
            to_dynamic(facts).map_err(|e| ScriptFault::invalid(e.to_string()))
        })
    });
}

fn commit_scoped_var(
    name: &str,
    value: Dynamic,
    visibility: &str,
    persistence: &str,
    editors: Option<Array>,
) -> Result<Dynamic, Box<rhai::EvalAltResult>> {
    with_ctx(CapabilityGroup::VarsReadWrite, |ctx| {
        let binding = ctx
            .binding
            .clone()
            .ok_or_else(|| ScriptFault::unavailable("no active Session binding"))?;
        let json_val: Value =
            from_dynamic(&value).map_err(|e| ScriptFault::invalid(e.to_string()))?;
        let vis = match visibility {
            "visible" => VariableVisibility::Visible,
            "invisible" => VariableVisibility::Invisible,
            "secret" => VariableVisibility::Secret,
            _ => {
                return Err(ScriptFault::invalid(
                    "visibility must be `visible`, `invisible`, or `secret`",
                ));
            }
        };
        let pers = match persistence {
            "persisted" => VariablePersistence::Persisted,
            "ephemeral" => VariablePersistence::Ephemeral,
            _ => {
                return Err(ScriptFault::invalid(
                    "persistence must be `persisted` or `ephemeral`",
                ));
            }
        };
        let parsed_editors = match editors {
            Some(list) => {
                let mut roles = Vec::with_capacity(list.len());
                for item in list {
                    let s = item
                        .into_string()
                        .map_err(|_| ScriptFault::invalid("editor role must be a string"))?;
                    roles.push(match s.as_str() {
                        "runtime" => VariableRole::Runtime,
                        "client" => VariableRole::Client,
                        "agent" => VariableRole::Agent,
                        _ => {
                            return Err(ScriptFault::invalid(
                                "editor role must be `runtime`, `client`, or `agent`",
                            ));
                        }
                    });
                }
                Some(roles)
            }
            None => None,
        };
        let change = VariablesChange {
            set: JsonMap::new(),
            remove: Vec::new(),
            scoped_set: vec![ScopedVariableSet {
                name: name.to_owned(),
                value: json_val,
                visibility: vis,
                persistence: pers,
                editors: parsed_editors,
            }],
        };
        block_on_session(ctx, "vars::set_scoped", |cancel| async move {
            binding.change(change, cancel).await
        })?;
        Ok(Dynamic::UNIT)
    })
}

pub(super) fn register_refs(module: &mut Module) {
    module.set_native_fn("list", || list_refs_filtered(None));
    module.set_native_fn("list", |kind: &str| list_refs_filtered(Some(kind)));

    module.set_native_fn("expand", |id: &str| {
        with_ctx(CapabilityGroup::RefsReadCreate, |ctx| {
            let state = ctx
                .tool_context
                .as_ref()
                .map(|tc| tc.state.clone())
                .ok_or_else(|| ScriptFault::unavailable("no active Session state"))?;
            let id_owned = id.to_owned();
            let content = block_on_session(ctx, "refs::expand", |cancel| async move {
                state.expand_reference(&id_owned, cancel).await
            })?;
            let payload = match content.payload {
                ReferencePayload::Text(text) => json!({
                    "id": content.id,
                    "media_type": content.media_type,
                    "text": text,
                }),
                ReferencePayload::Binary {
                    data,
                    width,
                    height,
                } => json!({
                    "id": content.id,
                    "media_type": content.media_type,
                    "base64": STANDARD.encode(&data),
                    "byte_length": data.len(),
                    "width": width,
                    "height": height,
                }),
            };
            to_dynamic(payload).map_err(|e| ScriptFault::invalid(e.to_string()))
        })
    });

    module.set_native_fn("refresh", || {
        with_ctx(CapabilityGroup::RefsManageAndMount, |ctx| {
            let state = ctx
                .tool_context
                .as_ref()
                .map(|tc| tc.state.clone())
                .ok_or_else(|| ScriptFault::unavailable("no active Session state"))?;
            block_on_session(ctx, "refs::refresh", |cancel| async move {
                state.refresh_references_async(cancel).await
            })?;
            Ok(Dynamic::UNIT)
        })
    });

    module.set_native_fn(
        "create",
        |title: &str, media_type: &str, data: Dynamic| stage_create_impl(title, media_type, data),
    );

    module.set_native_fn(
        "create_base64",
        |title: &str, media_type: &str, b64: &str| {
            with_ctx(CapabilityGroup::RefsReadCreate, |ctx| {
                let ingress = ctx
                    .references
                    .as_ref()
                    .ok_or_else(|| {
                        ScriptFault::unavailable("references staging is not active for this invocation")
                    })?;
                let raw = STANDARD
                    .decode(b64.trim())
                    .map_err(|_| ScriptFault::invalid("invalid base64 payload"))?;
                ingress
                    .stage_create(title.to_owned(), media_type.to_owned(), raw)
                    .map(Dynamic::from)
                    .map_err(ScriptFault::invalid)
            })
        },
    );

    module.set_native_fn("mount_host", |specs: Dynamic| mount_host_impl(specs));

    module.set_native_fn(
        "mount_inline",
        |title: &str, media_type: &str, content: &str, format: &str| {
            mount_inline_impl("references", title, media_type, content, format)
        },
    );

    module.set_native_fn(
        "mount_inline",
        |target_kind: &str, title: &str, media_type: &str, content: &str, format: &str| {
            mount_inline_impl(target_kind, title, media_type, content, format)
        },
    );

    module.set_native_fn("remove", |id: &str| {
        with_ctx(CapabilityGroup::RefsManageAndMount, |ctx| {
            let state = ctx
                .tool_context
                .as_ref()
                .map(|tc| tc.state.clone())
                .ok_or_else(|| ScriptFault::unavailable("no active Session state"))?;
            let id_owned = id.to_owned();
            let removed = block_on_session(ctx, "refs::remove", |cancel| async move {
                state.remove_reference(&id_owned, cancel).await
            })?;
            Ok(Dynamic::from(removed))
        })
    });
}

fn list_refs_filtered(kind: Option<&str>) -> Result<Dynamic, Box<rhai::EvalAltResult>> {
    with_ctx(CapabilityGroup::RefsReadCreate, |ctx| {
        let state = ctx
            .tool_context
            .as_ref()
            .map(|tc| tc.state.clone())
            .ok_or_else(|| ScriptFault::unavailable("no active Session state"))?;
        let listed = state
            .list_references()
            .map_err(ScriptFault::from_session_error)?;
        let rows: Vec<Value> = listed
            .into_iter()
            .filter(|item| kind.is_none_or(|k| item.kind == k))
            .map(|item| {
                json!({
                    "kind": item.kind,
                    "id": item.entry.id,
                    "node_kind": item.entry.node_kind,
                    "parent_id": item.entry.parent_id,
                    "title": item.entry.title,
                    "location": item.entry.location,
                    "description": item.entry.description,
                    "media_type": item.entry.media_type,
                    "revision": item.entry.revision,
                    "writable": item.entry.writable,
                    "metadata": item.entry.metadata,
                })
            })
            .collect();
        to_dynamic(rows).map_err(|e| ScriptFault::invalid(e.to_string()))
    })
}

fn stage_create_impl(
    title: &str,
    media_type: &str,
    data: Dynamic,
) -> Result<Dynamic, Box<rhai::EvalAltResult>> {
    with_ctx(CapabilityGroup::RefsReadCreate, |ctx| {
        let ingress = ctx.references.as_ref().ok_or_else(|| {
            ScriptFault::unavailable("references staging is not active for this invocation")
        })?;
        let bytes = if data.is_blob() {
            data.into_blob().expect("checked blob")
        } else if data.is_string() {
            data.into_string().expect("checked string").into_bytes()
        } else if data.is_map() {
            let val: Value =
                from_dynamic(&data).map_err(|e| ScriptFault::invalid(e.to_string()))?;
            let enc = val
                .get("encoding")
                .and_then(Value::as_str)
                .unwrap_or("utf8");
            let raw = val
                .get("data")
                .and_then(Value::as_str)
                .ok_or_else(|| ScriptFault::invalid("data map requires string `data` field"))?;
            match enc {
                "utf8" => raw.as_bytes().to_vec(),
                "base64" => STANDARD
                    .decode(raw.trim())
                    .map_err(|_| ScriptFault::invalid("invalid base64 `data`"))?,
                other => {
                    return Err(ScriptFault::invalid(format!(
                        "unsupported data encoding `{other}`"
                    )));
                }
            }
        } else {
            return Err(ScriptFault::invalid(
                "refs::create data must be a string, blob, or {encoding, data} map",
            ));
        };
        ingress
            .stage_create(title.to_owned(), media_type.to_owned(), bytes)
            .map(Dynamic::from)
            .map_err(ScriptFault::invalid)
    })
}

fn parse_plain_text_flag(format: &str, path_hint: Option<&str>) -> Result<bool, ScriptFault> {
    match format {
        "markdown" | "md" => Ok(false),
        "text" | "plain_text" => Ok(true),
        "auto" => {
            let is_md = path_hint.is_some_and(microcrab_host_api::markdown::is_markdown_path);
            Ok(!is_md)
        }
        other => Err(ScriptFault::invalid(format!(
            "reference format must be `auto`, `markdown`, or `text` (got `{other}`)"
        ))),
    }
}

fn mount_host_impl(specs: Dynamic) -> Result<Dynamic, Box<rhai::EvalAltResult>> {
    with_ctx(CapabilityGroup::RefsManageAndMount, |ctx| {
        let host = ctx
            .host
            .clone()
            .ok_or_else(|| ScriptFault::unavailable("Host is unavailable"))?;
        let ingress = ctx.references.clone().ok_or_else(|| {
            ScriptFault::unavailable("references staging is not active for this invocation")
        })?;
        let val: Value = from_dynamic(&specs).map_err(|e| ScriptFault::invalid(e.to_string()))?;
        let items = match val {
            Value::Array(arr) => arr,
            single => vec![single],
        };
        let mut ids = Vec::with_capacity(items.len());
        for item in items {
            let (path, title, target_kind, format) = match item {
                Value::String(path) => (path, None, "references".to_owned(), "auto".to_owned()),
                Value::Object(map) => {
                    let path = map
                        .get("path")
                        .and_then(Value::as_str)
                        .ok_or_else(|| ScriptFault::invalid("host reference spec requires string `path`"))?
                        .to_owned();
                    let title = map.get("title").and_then(Value::as_str).map(str::to_owned);
                    let target_kind = map
                        .get("target_kind")
                        .or_else(|| map.get("kind"))
                        .and_then(Value::as_str)
                        .unwrap_or("references")
                        .to_owned();
                    let format = map
                        .get("format")
                        .and_then(Value::as_str)
                        .unwrap_or("auto")
                        .to_owned();
                    (path, title, target_kind, format)
                }
                _ => {
                    return Err(ScriptFault::invalid(
                        "mount_host entry must be a path string or spec object",
                    ));
                }
            };
            if path.is_empty() || path.contains('\0') {
                return Err(ScriptFault::invalid("host path must be nonempty without NUL"));
            }
            let host_clone = host.clone();
            let path_clone = path.clone();
            let normalized = block_on_session(ctx, "refs::mount_host", |cancel| async move {
                host_clone
                    .normalize_resource(PathBuf::from(path_clone), cancel)
                    .await
                    .map_err(crate::Error::from_host_error)
            })?;
            let plain_text = parse_plain_text_flag(&format, Some(&normalized))?;
            let id = ingress
                .stage_host(target_kind, normalized, title, plain_text)
                .map_err(ScriptFault::invalid)?;
            ids.push(id);
        }
        to_dynamic(ids).map_err(|e| ScriptFault::invalid(e.to_string()))
    })
}

fn mount_inline_impl(
    target_kind: &str,
    title: &str,
    media_type: &str,
    content: &str,
    format: &str,
) -> Result<Dynamic, Box<rhai::EvalAltResult>> {
    with_ctx(CapabilityGroup::RefsManageAndMount, |ctx| {
        let ingress = ctx.references.as_ref().ok_or_else(|| {
            ScriptFault::unavailable("references staging is not active for this invocation")
        })?;
        let plain_text = match format {
            "markdown" | "md" => false,
            "text" | "plain_text" => true,
            "auto" => media_type != "text/markdown",
            other => {
                return Err(ScriptFault::invalid(format!(
                    "inline reference format must be `auto`, `markdown`, or `text` (got `{other}`)"
                )));
            }
        };
        ingress
            .stage_inline(
                target_kind.to_owned(),
                title.to_owned(),
                media_type.to_owned(),
                content.to_owned(),
                plain_text,
            )
            .map(Dynamic::from)
            .map_err(ScriptFault::invalid)
    })
}

pub(super) fn register_state(module: &mut Module) {
    module.set_native_fn("mode", || {
        with_ctx(CapabilityGroup::StateRead, |ctx| {
            let Some(tc) = ctx.tool_context.as_ref() else {
                return Ok(Dynamic::UNIT);
            };
            match tc.state.snapshot().selected_mode {
                Some(mode) => Ok(Dynamic::from(mode)),
                None => Ok(Dynamic::UNIT),
            }
        })
    });

    module.set_native_fn("session_id", || {
        with_ctx(CapabilityGroup::StateRead, |ctx| {
            Ok(Dynamic::from(
                ctx.tool_context
                    .as_ref()
                    .map_or_else(|| "detached".to_owned(), |tc| tc.session_id.clone()),
            ))
        })
    });
}

pub(super) fn register_session(module: &mut Module) {
    module.set_native_fn("switch_mode", |arg: Dynamic| {
        with_ctx(CapabilityGroup::SessionControlBuiltin, |ctx| {
            let tc = invoke_tool_context(ctx)?;
            let args = if arg.is_string() {
                json!({ "mode": arg.into_string().expect("checked string") })
            } else {
                from_dynamic(&arg).map_err(|e| ScriptFault::invalid(e.to_string()))?
            };
            let control = tc.state.builtin_control();
            let output = switch_mode::execute_admitted(control.as_deref(), &args, &tc)
                .map_err(ScriptFault::from_session_error)?;
            tool_output_to_dynamic(output)
        })
    });

    module.set_native_fn("sleep", |args: Map| {
        with_ctx(CapabilityGroup::SessionControlBuiltin, |ctx| {
            let tc = invoke_tool_context(ctx)?;
            let args_json: Value =
                from_dynamic(&Dynamic::from_map(args)).map_err(|e| ScriptFault::invalid(e.to_string()))?;
            let output = block_on_session(ctx, "session::sleep", |_cancel| async move {
                sleep::Sleep.execute(args_json, tc).await
            })?;
            tool_output_to_dynamic(output)
        })
    });

    module.set_native_fn("job", |args: Map| {
        with_ctx(CapabilityGroup::SessionControlBuiltin, |ctx| {
            let tc = invoke_tool_context(ctx)?;
            let args_json: Value =
                from_dynamic(&Dynamic::from_map(args)).map_err(|e| ScriptFault::invalid(e.to_string()))?;
            let output = block_on_session(ctx, "session::job", |_cancel| async move {
                job::execute_admitted(args_json, tc).await
            })?;
            tool_output_to_dynamic(output)
        })
    });

    module.set_native_fn("delegate", |args: Map| {
        with_ctx(CapabilityGroup::SessionControlBuiltin, |ctx| {
            let tc = invoke_tool_context(ctx)?;
            let executor = tc.state.delegate_executor();
            let args_json: Value =
                from_dynamic(&Dynamic::from_map(args)).map_err(|e| ScriptFault::invalid(e.to_string()))?;
            let output = block_on_session(ctx, "session::delegate", |_cancel| async move {
                delegate::execute_admitted(executor.as_deref(), &args_json, tc).await
            })?;
            tool_output_to_dynamic(output)
        })
    });

    module.set_native_fn("send_message", |args: Map| {
        with_ctx(CapabilityGroup::SessionControlBuiltin, |ctx| {
            let tc = invoke_tool_context(ctx)?;
            let args_json: Value =
                from_dynamic(&Dynamic::from_map(args)).map_err(|e| ScriptFault::invalid(e.to_string()))?;
            let output = block_on_session(ctx, "session::send_message", |_cancel| async move {
                messaging::send_message_admitted(args_json, tc).await
            })?;
            tool_output_to_dynamic(output)
        })
    });

    module.set_native_fn("escalate", |args: Map| {
        with_ctx(CapabilityGroup::SessionControlBuiltin, |ctx| {
            let tc = invoke_tool_context(ctx)?;
            let args_json: Value =
                from_dynamic(&Dynamic::from_map(args)).map_err(|e| ScriptFault::invalid(e.to_string()))?;
            let output = block_on_session(ctx, "session::escalate", |_cancel| async move {
                messaging::escalate_admitted(args_json, tc).await
            })?;
            tool_output_to_dynamic(output)
        })
    });
}

fn invoke_tool_context(ctx: &ScriptRuntimeContext) -> Result<ToolContext, ScriptFault> {
    let tc = ctx
        .tool_context
        .as_ref()
        .filter(|tc| tc.subject.is_tool())
        .ok_or_else(|| {
            ScriptFault::denied("Session Builtin calls require an active admitted tool invocation")
        })?;
    tc.ensure_admitted(None)
        .map_err(|e| ScriptFault::from_session_error(admission_error(e)))?;
    Ok(tc.clone())
}

fn tool_output_to_dynamic(output: crate::runtime::tool::ToolOutput) -> Result<Dynamic, ScriptFault> {
    match output {
        crate::runtime::tool::ToolOutput::Final(f) => {
            if let Some(failure) = f.failure {
                return Err(ScriptFault::invalid(format!(
                    "{}: {}",
                    failure.code, failure.message
                )));
            }
            if let Ok(parsed) = serde_json::from_str::<Value>(&f.content) {
                to_dynamic(parsed).map_err(|e| ScriptFault::invalid(e.to_string()))
            } else {
                Ok(Dynamic::from(f.content))
            }
        }
        crate::runtime::tool::ToolOutput::Buffered(_) => {
            Err(ScriptFault::invalid("buffered output cannot be returned synchronously inside Rhai"))
        }
    }
}
