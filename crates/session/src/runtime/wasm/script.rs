//! Unified `microcrab:session/session@0.4.0` Rhai runtime for `SessionState` and Core `Builtin`s.
//!
//! Permissions stay anchored to Tool admission (`AgentInit` for `init`, admitted Tool call for `invoke`).
//! This engine validates AST and runtime calls per lifecycle phase and capability profile, and
//! restricts execution strictly to `SessionState` and Core Session `Builtin` primitives (never External/Server tools).
mod builtins;

use super::{
    CancelOnDrop, Phase, RuntimeState,
    microcrab::session0_4_0::session::{ErrorCode, SessionError},
    references,
};
use crate::Error;
use crate::state::wasm::SessionBinding;
use rhai::{
    AST, ASTNode, Dynamic, Engine, EvalAltResult, Expr, Module, Scope,
    serde::{from_dynamic, to_dynamic},
};
use serde_json::Value;
use std::cell::RefCell;
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use wasmtime::{
    Engine as WasmEngine, StoreContextMut,
    component::{Linker, types::ComponentItem},
};

pub(super) const INTERFACE: &str = "microcrab:session/session@0.4.0";

const MAX_RHAI_OPERATIONS: u64 = 50_000;
const MAX_RHAI_CALL_LEVELS: usize = 32;
const MAX_RHAI_EXPR_DEPTH: usize = 32;
const MAX_RHAI_ARRAY_SIZE: usize = 4096;
const MAX_RHAI_MAP_SIZE: usize = 1024;
const MAX_RHAI_STRING_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScriptProfile {
    Standard,
    WebTransport,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CapabilityGroup {
    VarsReadWrite,
    CallFacts,
    RefsReadCreate,
    RefsManageAndMount,
    StateRead,
    SessionControlBuiltin,
}

#[derive(Clone)]
pub(super) struct ScriptRuntimeContext {
    pub(super) phase: Phase,
    pub(super) profile: ScriptProfile,
    pub(super) binding: Option<SessionBinding>,
    pub(super) tool_context: Option<super::ToolContext>,
    pub(super) references: Option<references::Ingress>,
    pub(super) host: Option<Arc<dyn microcrab_host_api::Host>>,
    pub(super) runtime: Option<tokio::runtime::Handle>,
    pub(super) cancellation: CancellationToken,
    pub(super) epoch_cancellation: CancellationToken,
    pub(super) waited_ns: u64,
}

thread_local! {
    static ACTIVE_SCRIPT_CTX: RefCell<Option<ScriptRuntimeContext>> = const { RefCell::new(None) };
}

struct ContextGuard {
    previous: Option<ScriptRuntimeContext>,
}

impl ContextGuard {
    fn enter(ctx: ScriptRuntimeContext) -> Self {
        let previous = ACTIVE_SCRIPT_CTX.with(|slot| slot.borrow_mut().replace(ctx));
        Self { previous }
    }
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        ACTIVE_SCRIPT_CTX.with(|slot| {
            *slot.borrow_mut() = self.previous.take();
        });
    }
}

fn with_ctx<R>(
    group: CapabilityGroup,
    f: impl FnOnce(&mut ScriptRuntimeContext) -> Result<R, ScriptFault>,
) -> Result<R, Box<EvalAltResult>> {
    ACTIVE_SCRIPT_CTX.with(|slot| {
        let mut borrow = slot.borrow_mut();
        let ctx = borrow.as_mut().ok_or_else(|| {
            ScriptFault::unavailable("no active Session script context").into_rhai()
        })?;
        check_capability(ctx.phase, ctx.profile, group).map_err(ScriptFault::into_rhai)?;
        f(ctx).map_err(ScriptFault::into_rhai)
    })
}

#[derive(Debug)]
struct ScriptFault {
    code: ErrorCode,
    message: String,
}

impl ScriptFault {
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unavailable, message)
    }
    fn denied(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Denied, message)
    }
    fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArgument, message)
    }
    fn limit(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Limit, message)
    }
    fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Cancelled, message)
    }
    fn from_session_error(err: Error) -> Self {
        match err {
            Error::Cancelled => Self::cancelled("operation cancelled"),
            Error::Limit(limit) => Self::limit(limit.safe_message()),
            Error::Denied(reason) => {
                Self::denied(reason.public_reason().unwrap_or("permission denied"))
            }
            other => Self::invalid(other.to_string()),
        }
    }
    fn into_rhai(self) -> Box<EvalAltResult> {
        let tag = match self.code {
            ErrorCode::Absent => "ABSENT",
            ErrorCode::Unavailable => "UNAVAILABLE",
            ErrorCode::Denied => "DENIED",
            ErrorCode::InvalidArgument => "INVALID_ARGUMENT",
            ErrorCode::Limit => "LIMIT",
            ErrorCode::Cancelled => "CANCELLED",
            ErrorCode::ScriptError => "SCRIPT_ERROR",
        };
        Box::new(EvalAltResult::ErrorRuntime(
            Dynamic::from(format!("{tag}:{}", self.message)),
            rhai::Position::NONE,
        ))
    }
    fn from_rhai(err: EvalAltResult) -> SessionError {
        if let EvalAltResult::ErrorRuntime(ref val, _) = err
            && let Ok(text) = val.clone().into_string()
            && let Some((tag, msg)) = text.split_once(':')
        {
            let code = match tag {
                "ABSENT" => ErrorCode::Absent,
                "UNAVAILABLE" => ErrorCode::Unavailable,
                "DENIED" => ErrorCode::Denied,
                "INVALID_ARGUMENT" => ErrorCode::InvalidArgument,
                "LIMIT" => ErrorCode::Limit,
                "CANCELLED" => ErrorCode::Cancelled,
                _ => ErrorCode::ScriptError,
            };
            return SessionError {
                code,
                message: msg.to_owned(),
            };
        }
        match err {
            EvalAltResult::ErrorTooManyOperations(_) => SessionError {
                code: ErrorCode::Limit,
                message: "Rhai script exceeded maximum operation limit".into(),
            },
            EvalAltResult::ErrorTerminated(_, _) => SessionError {
                code: ErrorCode::Cancelled,
                message: "Rhai script terminated".into(),
            },
            other => SessionError {
                code: ErrorCode::ScriptError,
                message: other.to_string(),
            },
        }
    }
}

fn check_capability(
    phase: Phase,
    profile: ScriptProfile,
    group: CapabilityGroup,
) -> Result<(), ScriptFault> {
    if profile == ScriptProfile::WebTransport {
        return match group {
            CapabilityGroup::CallFacts => {
                if phase == Phase::Invoke {
                    Ok(())
                } else {
                    Err(ScriptFault::unavailable(
                        "vars::call_facts requires an active tool invocation",
                    ))
                }
            }
            CapabilityGroup::RefsReadCreate => Ok(()),
            _ => Err(ScriptFault::denied(
                "WebTransport capability restricts Session script access to call_facts and library reference creation",
            )),
        };
    }
    match group {
        CapabilityGroup::CallFacts => {
            if phase == Phase::Invoke {
                Ok(())
            } else {
                Err(ScriptFault::unavailable(
                    "vars::call_facts requires an active tool invocation",
                ))
            }
        }
        CapabilityGroup::SessionControlBuiltin => {
            if phase == Phase::Invoke {
                Ok(())
            } else {
                Err(ScriptFault::denied(
                    "Session Builtin control calls are not allowed during init",
                ))
            }
        }
        CapabilityGroup::VarsReadWrite
        | CapabilityGroup::RefsReadCreate
        | CapabilityGroup::RefsManageAndMount
        | CapabilityGroup::StateRead => Ok(()),
    }
}

fn classify_call(ns: &str, name: &str) -> Result<CapabilityGroup, ScriptFault> {
    match (ns, name) {
        ("vars", "call_facts") => Ok(CapabilityGroup::CallFacts),
        (
            "vars",
            "has" | "get" | "get_all" | "metadata" | "set" | "set_scoped" | "remove" | "change",
        ) => Ok(CapabilityGroup::VarsReadWrite),
        ("refs", "list" | "expand" | "create" | "create_base64") => {
            Ok(CapabilityGroup::RefsReadCreate)
        }
        ("refs", "refresh" | "mount_host" | "mount_inline" | "remove") => {
            Ok(CapabilityGroup::RefsManageAndMount)
        }
        ("state", "mode" | "session_id") => Ok(CapabilityGroup::StateRead),
        (
            "session",
            "switch_mode" | "delegate" | "job" | "send_message" | "escalate" | "sleep",
        ) => Ok(CapabilityGroup::SessionControlBuiltin),
        _ => Err(ScriptFault::denied(format!(
            "unknown or disallowed Session script call `{ns}::{name}`"
        ))),
    }
}

fn validate_ast(ast: &AST, phase: Phase, profile: ScriptProfile) -> Result<(), ScriptFault> {
    let mut rejected: Option<ScriptFault> = None;
    ast.walk(&mut |nodes| {
        if let Some(ASTNode::Expr(Expr::FnCall(call, _))) = nodes.last() {
            if !call.namespace.is_empty() {
                let ns = call
                    .namespace
                    .path
                    .iter()
                    .map(|ident| ident.as_str())
                    .collect::<Vec<_>>()
                    .join("::");
                match classify_call(&ns, call.name.as_str())
                    .and_then(|group| check_capability(phase, profile, group))
                {
                    Ok(()) => {}
                    Err(err) => {
                        rejected = Some(err);
                        return false;
                    }
                }
            }
        }
        true
    });
    match rejected {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

static SHARED_ENGINE: LazyLock<Arc<Engine>> = LazyLock::new(|| {
    let mut engine = Engine::new();
    engine.set_max_operations(MAX_RHAI_OPERATIONS);
    engine.set_max_call_levels(MAX_RHAI_CALL_LEVELS);
    engine.set_max_expr_depths(MAX_RHAI_EXPR_DEPTH, MAX_RHAI_EXPR_DEPTH);
    engine.set_max_array_size(MAX_RHAI_ARRAY_SIZE);
    engine.set_max_map_size(MAX_RHAI_MAP_SIZE);
    engine.set_max_string_size(MAX_RHAI_STRING_BYTES);

    // Disable dynamic eval, prints, and function-pointer reflection symbols.
    for symbol in ["eval", "print", "debug", "Fn", "call"] {
        engine.disable_symbol(symbol);
    }
    engine.on_print(|_| {});
    engine.on_debug(|_, _, _| {});

    let mut vars = Module::new();
    builtins::register_vars(&mut vars);
    engine.register_static_module("vars", vars.into());

    let mut refs = Module::new();
    builtins::register_refs(&mut refs);
    engine.register_static_module("refs", refs.into());

    let mut state = Module::new();
    builtins::register_state(&mut state);
    engine.register_static_module("state", state.into());

    let mut session = Module::new();
    builtins::register_session(&mut session);
    engine.register_static_module("session", session.into());

    Arc::new(engine)
});

pub(super) fn valid_import(item: &ComponentItem, engine: &WasmEngine) -> bool {
    let ComponentItem::ComponentInstance(interface) = item else {
        return false;
    };
    let mut eval_func = None;
    for (name, export) in interface.exports(engine) {
        match (name, export.ty) {
            ("eval", ComponentItem::ComponentFunc(func)) if eval_func.is_none() => {
                eval_func = Some(func);
            }
            ("error-code" | "session-error", ComponentItem::Type(_)) => {}
            _ => return false,
        }
    }
    let Some(func) = eval_func else {
        return false;
    };
    let mut dummy = Linker::<RuntimeState>::new(engine);
    if link(&mut dummy).is_err() {
        return false;
    }
    let params: Vec<_> = func.params().map(|(_, ty)| ty).collect();
    let matches_params = params.len() == 2
        && params[0] == wasmtime::component::Type::String
        && matches!(
            &params[1],
            wasmtime::component::Type::Option(opt) if opt.ty() == wasmtime::component::Type::String
        );
    let results: Vec<_> = func.results().collect();
    matches_params
        && results.len() == 1
        && matches!(results[0], wasmtime::component::Type::Result(_))
}

pub(super) fn link(linker: &mut Linker<RuntimeState>) -> wasmtime::Result<()> {
    let mut instance = linker.instance(INTERFACE)?;
    instance.func_wrap(
        "eval",
        |mut caller: StoreContextMut<'_, RuntimeState>,
         (script, args_json): (String, Option<String>)| {
            let max_in = caller.data().max_input_bytes;
            let max_out = caller.data().max_output_bytes;
            let in_bytes = script
                .len()
                .saturating_add(args_json.as_ref().map_or(0, String::len));
            if crate::budget::check(crate::LimitKind::ToolArguments, in_bytes, max_in).is_err() {
                return bounded(
                    Err(SessionError {
                        code: ErrorCode::Limit,
                        message: "session script input byte limit exceeded".into(),
                    }),
                    max_out,
                );
            }
            let phase = caller
                .data()
                .tool_context
                .as_ref()
                .map(|ctx| {
                    if ctx.subject.is_tool() {
                        Phase::Invoke
                    } else {
                        Phase::Init
                    }
                })
                .unwrap_or(Phase::Init);
            let ctx = ScriptRuntimeContext {
                phase,
                profile: caller.data().script_profile,
                binding: caller.data().binding.clone(),
                tool_context: caller.data().tool_context.clone(),
                references: caller.data().references.clone(),
                host: caller.data().host.clone(),
                runtime: caller.data().runtime.clone(),
                cancellation: caller.data().cancellation.clone(),
                epoch_cancellation: caller.data().epoch_cancellation.clone(),
                waited_ns: 0,
            };
            let result = evaluate_script(ctx, &script, args_json.as_deref());
            let (reply, waited_ns) = match result {
                Ok((json, waited)) => (Ok(json), waited),
                Err((err, waited)) => (Err(err), waited),
            };
            if waited_ns > 0 {
                caller.data_mut().deadline += std::time::Duration::from_nanos(waited_ns);
            }
            bounded(reply, max_out)
        },
    )?;
    Ok(())
}

fn bounded(
    result: Result<String, SessionError>,
    limit: usize,
) -> wasmtime::Result<(Result<String, SessionError>,)> {
    let result = match result {
        Ok(value) => {
            if crate::budget::check(crate::LimitKind::WasmBytes, value.len(), limit).is_err() {
                Err(SessionError {
                    code: ErrorCode::Limit,
                    message: "session script response byte limit exceeded".into(),
                })
            } else {
                Ok(value)
            }
        }
        Err(err) => Err(err),
    };
    let result = result.map_err(|mut err| {
        if crate::budget::check(crate::LimitKind::WasmBytes, err.message.len(), limit).is_err() {
            err.message.clear();
        }
        err
    });
    Ok((result,))
}

fn evaluate_script(
    ctx: ScriptRuntimeContext,
    script: &str,
    args_json: Option<&str>,
) -> Result<(String, u64), (SessionError, u64)> {
    let phase = ctx.phase;
    let profile = ctx.profile;
    if ctx.cancellation.is_cancelled() || ctx.epoch_cancellation.is_cancelled() {
        return Err((
            SessionError {
                code: ErrorCode::Cancelled,
                message: "script evaluation cancelled".into(),
            },
            0,
        ));
    }
    let args_value: Value = match args_json {
        Some(raw) if !raw.trim().is_empty() => serde_json::from_str(raw).map_err(|e| {
            (
                SessionError {
                    code: ErrorCode::InvalidArgument,
                    message: format!("invalid args-json: {e}"),
                },
                0,
            )
        })?,
        _ => Value::Object(Default::default()),
    };
    let engine = SHARED_ENGINE.clone();
    let ast = engine.compile(script).map_err(|e| {
        (
            SessionError {
                code: ErrorCode::ScriptError,
                message: e.to_string(),
            },
            0,
        )
    })?;
    if let Err(fault) = validate_ast(&ast, phase, profile) {
        return Err((
            SessionError {
                code: fault.code,
                message: fault.message,
            },
            0,
        ));
    }
    let args_dynamic = to_dynamic(args_value).map_err(|e| {
        (
            SessionError {
                code: ErrorCode::InvalidArgument,
                message: e.to_string(),
            },
            0,
        )
    })?;
    let mut scope = Scope::new();
    scope.push("args", args_dynamic);

    let _guard = ContextGuard::enter(ctx);
    let eval_result = engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast);
    let waited = ACTIVE_SCRIPT_CTX.with(|slot| {
        slot.borrow()
            .as_ref()
            .map_or(0, |active| active.waited_ns)
    });
    let output = eval_result.map_err(|e| (ScriptFault::from_rhai(*e), waited))?;
    let json_val: Value = from_dynamic(&output).map_err(|e| {
        (
            SessionError {
                code: ErrorCode::ScriptError,
                message: format!("script result is not JSON serializable: {e}"),
            },
            waited,
        )
    })?;
    let encoded = serde_json::to_string(&json_val).map_err(|e| {
        (
            SessionError {
                code: ErrorCode::ScriptError,
                message: e.to_string(),
            },
            waited,
        )
    })?;
    Ok((encoded, waited))
}

fn block_on_session<T, F, Fut>(
    ctx: &mut ScriptRuntimeContext,
    op_label: &'static str,
    build: F,
) -> Result<T, ScriptFault>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let runtime = ctx.runtime.clone().ok_or_else(|| {
        ScriptFault::unavailable("Session operations require a Tokio runtime")
    })?;
    let cancel = ctx.cancellation.child_token();
    let _drop_guard = CancelOnDrop(cancel.clone());
    let epoch = ctx.epoch_cancellation.clone();
    let started = Instant::now();
    let out = runtime.block_on(async {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(ScriptFault::cancelled(format!("{op_label} cancelled"))),
            _ = epoch.cancelled() => Err(ScriptFault::cancelled(format!("{op_label} cancelled"))),
            res = build(cancel.clone()) => res.map_err(ScriptFault::from_session_error),
        }
    });
    let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    ctx.waited_ns = ctx.waited_ns.saturating_add(nanos);
    out
}

#[cfg(test)]
#[path = "script/tests.rs"]
mod tests;
