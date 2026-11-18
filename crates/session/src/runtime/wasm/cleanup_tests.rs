//! Scope-owned cleanup can outlive its ToolContext and never reaches the generic
//! execution result sanitizer. Its tracing fields need their own masking boundary.
use super::report_untransferred_process_cleanup_failure;
use crate::runtime::execution::InvocationRedaction;
use crate::{Error, Limits, State};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, atomic::AtomicBool};

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<BTreeMap<String, String>>>>);
impl tracing::Subscriber for Captured {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "microcrab_session::runtime::wasm"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().into(), format!("{value:?}"));
            }
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

#[tokio::test]
async fn asynchronous_scope_cleanup_masks_actual_tracing_fields_after_state_is_dropped() {
    let (state, endpoint) = State::new(Limits::default()).bind_writer(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    state
        .prepare_secret_write(BTreeMap::from([(
            "cleanup_token".into(),
            "PRIVATE_CLEANUP_TOKEN".into(),
        )]))
        .await
        .unwrap()
        .commit()
        .unwrap();
    let redaction = InvocationRedaction::from_snapshot(state.secret_snapshot()).unwrap();
    // Scope drop clears its ToolContext before this separately scheduled cleanup
    // runs. Only the opaque captured handle can still recognize the old literal.
    drop(state);
    drop(endpoint);
    let captured = Captured::default();
    let dispatch = tracing::Dispatch::new(captured.clone());
    tokio::spawn(async move {
        tracing::dispatcher::with_default(&dispatch, || {
            report_untransferred_process_cleanup_failure(
                &Error::tool("cleanup backend denied PRIVATE_CLEANUP_TOKEN"),
                &redaction,
            );
        });
    })
    .await
    .unwrap();
    let events = captured.0.lock().unwrap();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert!(event["message"].contains("Untransferred WASM process cleanup failed"));
    assert!(event["reason"].contains("cleanup backend denied {{secret:cleanup_token}}"));
    assert!(!format!("{events:?}").contains("PRIVATE_CLEANUP_TOKEN"));
}

#[test]
fn cleanup_warning_preserves_nonsecret_diagnostic_cause() {
    let captured = Captured::default();
    let dispatch = tracing::Dispatch::new(captured.clone());
    tracing::dispatcher::with_default(&dispatch, || {
        report_untransferred_process_cleanup_failure(
            &Error::tool("cleanup backend did not acknowledge release"),
            &InvocationRedaction::default(),
        );
    });
    let events = captured.0.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0]["reason"].contains("cleanup backend did not acknowledge release"));
}
