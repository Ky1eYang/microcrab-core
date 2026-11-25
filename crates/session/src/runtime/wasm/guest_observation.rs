//! Fixed guest diagnostics ABI; this lane has no access to Session or Host effects.
use super::{
    Phase, RuntimeState, ToolContext, microcrab::session0_2_2::observation as wit,
    microcrab::session0_2_3::browser_observation as browser,
};
use crate::observation::wasm::digest;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
use wasmtime::{
    Engine, StoreContextMut,
    component::{Linker, types::ComponentItem},
};

pub(super) const INTERFACE: &str = "microcrab:session/observation@0.2.2";
pub(super) const BROWSER_INTERFACE: &str = "microcrab:session/browser-observation@0.2.3";
const FUNCTION: &str = "record-event";
const CHECKPOINT_FUNCTION: &str = "record-checkpoint";
const MAX_EVENTS: u8 = 32;
// Canonical memory shape: five byte enums, alignment padding, two u32s and
// a final byte enum, rounded to 20 bytes including trailing alignment padding.
const EVENT_BYTES: u16 = 20;
const MAX_BYTES: u16 = 640;
const CHECKPOINT_BYTES: u16 = 4; // Three byte enums and one u8.
const MAX_HANDLING: Duration = Duration::from_millis(1);
const FIRST_WARNING_HANDLING: Duration = Duration::from_micros(100);
const QUEUE_CAPACITY: usize = 256;
const DEBUG_QUEUE_CAPACITY: usize = QUEUE_CAPACITY - 32;

pub(super) fn valid_import(name: &str, item: &ComponentItem, engine: &Engine) -> bool {
    let (function, types): (&str, &[&str]) = match name {
        INTERFACE => (
            FUNCTION,
            &[
                "level",
                "event-code",
                "phase",
                "method",
                "counter",
                "confirmation",
                "event",
            ],
        ),
        BROWSER_INTERFACE => (
            CHECKPOINT_FUNCTION,
            &["method", "phase", "confirmation", "checkpoint"],
        ),
        _ => return false,
    };
    let ComponentItem::ComponentInstance(interface) = item else {
        return false;
    };
    let mut functions = 0;
    for (name, export) in interface.exports(engine) {
        match export.ty {
            ComponentItem::ComponentFunc(_) if name == function => functions += 1,
            ComponentItem::Type(_) if types.contains(&name) => (),
            _ => return false,
        }
    }
    // The typed linker checks the actual record layout and function signature.
    functions == 1
}

pub(super) fn link(linker: &mut Linker<RuntimeState>) -> wasmtime::Result<()> {
    linker.instance(INTERFACE)?.func_wrap(
        FUNCTION,
        |caller: StoreContextMut<'_, RuntimeState>, (event,): (wit::Event,)| {
            if let Some((scope, phase)) = &caller.data().guest_observation {
                scope.record(*phase, event);
            }
            Ok(())
        },
    )?;
    linker.instance(BROWSER_INTERFACE)?.func_wrap(
        CHECKPOINT_FUNCTION,
        |caller: StoreContextMut<'_, RuntimeState>, (checkpoint,): (browser::Checkpoint,)| {
            if let Some((scope, phase)) = &caller.data().guest_observation
                && checkpoint.exchange != 0
            {
                scope.record_entry(*phase, Entry::Browser(checkpoint));
            }
            Ok(())
        },
    )?;
    Ok(())
}

struct ToolAttribution {
    session: String,
    operation: String,
    epoch: u64,
    execution: Option<u64>,
}

struct Attribution {
    component: String,
    tool: Option<ToolAttribution>,
    dispatch: tracing::Dispatch,
}

struct Queued {
    attribution: Arc<Attribution>,
    phase: Phase,
    entry: Entry,
    _debug_queue_slot: Option<DebugQueueSlot>,
}

enum Entry {
    Guest(wit::Event),
    Browser(browser::Checkpoint),
    Suppressed(u64),
    BrowserSuppressed(u64),
}

impl Entry {
    fn is_debug(&self) -> bool {
        matches!(self, Self::Browser(_) | Self::BrowserSuppressed(_))
            || matches!(self, Self::Guest(event) if matches!(event.severity, wit::Level::Debug))
    }

    fn is_warning(&self) -> bool {
        matches!(self, Self::Guest(event) if matches!(event.severity, wit::Level::Warn | wit::Level::Error))
    }

    fn bytes(&self) -> u16 {
        match self {
            Self::Guest(_) => EVENT_BYTES,
            Self::Browser(_) => CHECKPOINT_BYTES,
            Self::Suppressed(_) | Self::BrowserSuppressed(_) => 0,
        }
    }
}

// Keep one FIFO, but prevent DEBUG alone from filling it. High-priority
// traffic and a slow/broken subscriber can still exhaust delivery headroom.
static DEBUG_QUEUED: AtomicUsize = AtomicUsize::new(0);
struct DebugQueueSlot;

impl DebugQueueSlot {
    fn try_acquire() -> Option<Self> {
        if DEBUG_QUEUED.fetch_add(1, Ordering::Relaxed) < DEBUG_QUEUE_CAPACITY {
            Some(Self)
        } else {
            DEBUG_QUEUED.fetch_sub(1, Ordering::Relaxed);
            None
        }
    }
}

impl Drop for DebugQueueSlot {
    fn drop(&mut self) {
        DEBUG_QUEUED.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(super) fn prepare_sink() {
    // The sole process-lifetime worker is started before guest wall accounting.
    let _ = sink();
}

fn sink() -> Option<&'static mpsc::SyncSender<Queued>> {
    static SINK: OnceLock<Option<mpsc::SyncSender<Queued>>> = OnceLock::new();
    SINK.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel::<Queued>(QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("microcrab-wasm-observation".into())
            .spawn(move || {
                while let Ok(value) = rx.recv() {
                    // A subscriber is embedder code; its slowness or panic
                    // cannot unwind the guest or require a new drain thread.
                    let _ = catch_unwind(AssertUnwindSafe(|| value.emit()));
                }
            })
            .ok()
            .map(|_| tx)
    })
    .as_ref()
}

pub(super) struct Scope {
    sink: &'static mpsc::SyncSender<Queued>,
    attribution: Arc<Attribution>,
    budget: Mutex<Budget>,
}

struct Budget {
    remaining_events: u8,
    remaining_bytes: u16,
    remaining_time: Duration,
    first_warning_pending: bool,
    first_warning_time: Duration,
    suppressed: u64,
    browser_suppressed: u64,
    last_phase: Phase,
}

impl Budget {
    fn new(phase: Phase) -> Self {
        Self {
            remaining_events: MAX_EVENTS,
            remaining_bytes: MAX_BYTES,
            remaining_time: MAX_HANDLING - FIRST_WARNING_HANDLING,
            first_warning_pending: true,
            first_warning_time: FIRST_WARNING_HANDLING,
            suppressed: 0,
            browser_suppressed: 0,
            last_phase: phase,
        }
    }

    fn admit(&self, entry: &Entry) -> Option<bool> {
        let first_warning = self.first_warning_pending && entry.is_warning();
        let reserve = self.first_warning_pending && !first_warning;
        let time = if first_warning {
            self.first_warning_time
        } else {
            self.remaining_time
        };
        (!time.is_zero()
            && self.remaining_events > u8::from(reserve)
            && self.remaining_bytes >= entry.bytes() + if reserve { EVENT_BYTES } else { 0 })
        .then_some(first_warning)
    }

    fn charge(&mut self, first_warning: bool, bytes: u16, elapsed: Duration, queued: bool) {
        if first_warning {
            self.first_warning_time = self.first_warning_time.saturating_sub(elapsed);
        } else {
            self.remaining_time = self.remaining_time.saturating_sub(elapsed);
        }
        if queued {
            self.remaining_events -= 1;
            self.remaining_bytes -= bytes;
            if first_warning {
                self.first_warning_pending = false;
                self.remaining_time += self.first_warning_time;
            }
        }
    }

    fn suppress(&mut self, browser: bool) {
        let count = if browser {
            &mut self.browser_suppressed
        } else {
            &mut self.suppressed
        };
        *count = count.saturating_add(1);
    }
}

impl Scope {
    pub(super) fn new(
        component: &str,
        phase: Phase,
        context: Option<&ToolContext>,
    ) -> Option<Arc<Self>> {
        let sink = sink()?;
        let tool = context
            .filter(|context| context.subject.is_tool())
            .map(|context| ToolAttribution {
                // Full hashes use the exact Session and Host OpContext trace IDs;
                // neither provider-authored call text nor guest attribution enters logs.
                session: digest(context.session_id.as_bytes()),
                operation: digest(context.call_id.as_bytes()),
                epoch: context.state.snapshot().epoch_id,
                execution: context.execution_id.map(|id| id.0),
            });
        Some(Arc::new(Self {
            sink,
            attribution: Arc::new(Attribution {
                component: component.into(),
                tool,
                dispatch: tracing::dispatcher::get_default(Clone::clone),
            }),
            budget: Mutex::new(Budget::new(phase)),
        }))
    }

    fn record(&self, phase: Phase, mut event: wit::Event) {
        if let wit::Counter::None = event.metric {
            event.measured = 0;
            event.limit = 0;
        }
        self.record_entry(phase, Entry::Guest(event));
    }

    fn record_entry(&self, phase: Phase, entry: Entry) {
        // Invoke and its Drive continuations borrow the same scope serially.
        // Even if that changes, observing must never wait for a lock.
        let Ok(mut budget) = self.budget.try_lock() else {
            return;
        };
        budget.last_phase = phase;
        let browser = matches!(entry, Entry::Browser(_));
        let Some(first_warning) = budget.admit(&entry) else {
            budget.suppress(browser);
            return;
        };
        let start = Instant::now();
        let bytes = entry.bytes();
        // Admission to the process-local queue is publication, not a tracing
        // flush. It happens here, before canonical return, even if the guest
        // subsequently traps. No diagnostic work is awaited for cleanup.
        let queued = self.enqueue(phase, entry);
        budget.charge(first_warning, bytes, start.elapsed(), queued);
        if !queued {
            budget.suppress(browser);
        }
    }

    fn enqueue(&self, phase: Phase, entry: Entry) -> bool {
        let debug_slot = if entry.is_debug() {
            let Some(slot) = DebugQueueSlot::try_acquire() else {
                return false;
            };
            Some(slot)
        } else {
            None
        };
        self.sink
            .try_send(Queued {
                attribution: self.attribution.clone(),
                phase,
                entry,
                _debug_queue_slot: debug_slot,
            })
            .is_ok()
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        let Ok(budget) = self.budget.get_mut() else {
            return;
        };
        let (phase, legacy, browser) = (
            budget.last_phase,
            budget.suppressed,
            budget.browser_suppressed,
        );
        if legacy != 0 {
            self.enqueue(phase, Entry::Suppressed(legacy));
        }
        if browser != 0 {
            self.enqueue(phase, Entry::BrowserSuppressed(browser));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint() -> Entry {
        Entry::Browser(browser::Checkpoint {
            command: browser::Method::BrowserGetVersion,
            stage: browser::Phase::Receive,
            exchange: 1,
            reply: browser::Confirmation::NotConfirmed,
        })
    }

    fn warning() -> Entry {
        Entry::Guest(wit::Event {
            severity: wit::Level::Warn,
            code: wit::EventCode::Failed,
            stage: wit::Phase::Receive,
            method_family: wit::Method::Browser,
            metric: wit::Counter::None,
            measured: 0,
            limit: 0,
            confirmation: wit::Confirmation::NotConfirmed,
        })
    }

    #[test]
    fn checkpoints_leave_first_warning_slot() {
        let mut budget = Budget::new(Phase::Invoke);
        let entry = checkpoint();
        for _ in 1..MAX_EVENTS {
            assert_eq!(budget.admit(&entry), Some(false));
            budget.charge(false, entry.bytes(), Duration::ZERO, true);
        }
        assert_eq!(budget.admit(&entry), None);
        assert_eq!(budget.admit(&warning()), Some(true));
        budget.charge(true, EVENT_BYTES, Duration::ZERO, true);
        assert_eq!(budget.admit(&warning()), None);
    }

    #[test]
    fn checkpoint_time_cannot_exhaust_first_warning_reserve() {
        let mut budget = Budget::new(Phase::Invoke);
        let entry = checkpoint();
        budget.charge(false, entry.bytes(), MAX_HANDLING, true);
        assert_eq!(budget.admit(&entry), None);
        assert_eq!(budget.admit(&warning()), Some(true));
        // A failed enqueue spends time but does not spend the protected slot.
        budget.charge(true, EVENT_BYTES, Duration::ZERO, false);
        assert_eq!(budget.admit(&warning()), Some(true));
    }
}

fn browser_command(method: browser::Method) -> &'static str {
    use browser::Method::*;
    match method {
        BrowserGetVersion => "Browser.getVersion",
        TargetGetTargets => "Target.getTargets",
        TargetAttachToTarget => "Target.attachToTarget",
        TargetDetachFromTarget => "Target.detachFromTarget",
        TargetCreateTarget => "Target.createTarget",
        TargetActivateTarget => "Target.activateTarget",
        TargetCloseTarget => "Target.closeTarget",
        AccessibilityGetFullAxTree => "Accessibility.getFullAXTree",
        RuntimeEnable => "Runtime.enable",
        RuntimeDisable => "Runtime.disable",
        RuntimeCallFunctionOn => "Runtime.callFunctionOn",
        PageCreateIsolatedWorld => "Page.createIsolatedWorld",
        PageNavigate => "Page.navigate",
        PageGetFrameTree => "Page.getFrameTree",
        PageReload => "Page.reload",
        PageGetNavigationHistory => "Page.getNavigationHistory",
        PageNavigateToHistoryEntry => "Page.navigateToHistoryEntry",
        PageGetLayoutMetrics => "Page.getLayoutMetrics",
        PageCaptureScreenshot => "Page.captureScreenshot",
        WebMcpEnable => "WebMCP.enable",
        WebMcpInvokeTool => "WebMCP.invokeTool",
        WebMcpCancelInvocation => "WebMCP.cancelInvocation",
    }
}

impl Queued {
    fn emit(&self) {
        let a = &self.attribution;
        let tool = a.tool.as_ref();
        let session = tool.map(|tool| tool.session.as_str());
        let operation = tool.map(|tool| tool.operation.as_str());
        let epoch = tool.map(|tool| tool.epoch);
        let execution = tool.and_then(|tool| tool.execution);
        let stage = self.phase.label();
        tracing::dispatcher::with_default(&a.dispatch, || {
            macro_rules! emit {
                ($level:ident, $event:ident) => {
                    tracing::$level!(target: "microcrab_session::runtime::wasm",
                        observation = "wasm_guest_event", source = "guest", lifecycle_stage = stage,
                        component_sha256 = %a.component, session_sha256 = session,
                        operation_sha256 = operation, epoch, execution_id = execution,
                        code = ?$event.code, guest_phase = ?$event.stage,
                        method = ?$event.method_family, metric = ?$event.metric,
                        measured = $event.measured, limit = $event.limit,
                        guest_confirmation = ?$event.confirmation, "WASM guest breadcrumb")
                };
            }
            match &self.entry {
                Entry::Guest(event) => match event.severity {
                    wit::Level::Debug => emit!(debug, event),
                    wit::Level::Info => emit!(info, event),
                    wit::Level::Warn => emit!(warn, event),
                    wit::Level::Error => emit!(error, event),
                },
                Entry::Browser(checkpoint) => tracing::debug!(
                    target: "microcrab_session::runtime::wasm",
                    observation = "browser_cdp_checkpoint", source = "guest", lifecycle_stage = stage,
                    component_sha256 = %a.component, session_sha256 = session,
                    operation_sha256 = operation, epoch, execution_id = execution,
                    cdp_command = browser_command(checkpoint.command), guest_phase = ?checkpoint.stage,
                    exchange = checkpoint.exchange, guest_confirmation = ?checkpoint.reply,
                    "Browser CDP exchange checkpoint"
                ),
                Entry::BrowserSuppressed(suppressed) => tracing::debug!(
                    target: "microcrab_session::runtime::wasm",
                    observation = "browser_cdp_checkpoints_suppressed", source = "runtime", lifecycle_stage = stage,
                    component_sha256 = %a.component, session_sha256 = session,
                    operation_sha256 = operation, epoch, execution_id = execution,
                    suppressed, "Browser CDP exchange checkpoints suppressed"
                ),
                Entry::Suppressed(suppressed) => tracing::warn!(
                    target: "microcrab_session::runtime::wasm",
                    observation = "wasm_guest_events_suppressed", source = "runtime", lifecycle_stage = stage,
                    component_sha256 = %a.component, session_sha256 = session,
                    operation_sha256 = operation, epoch, execution_id = execution,
                    suppressed, "WASM guest breadcrumbs suppressed"
                ),
            }
        });
    }
}
