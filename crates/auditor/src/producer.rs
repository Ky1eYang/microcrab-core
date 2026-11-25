//! Serializes audit sequencing and request baselines; never waits on a consumer.
use super::{AuditBody, AuditRecord, diff::request_diff};
use microcrab_protocol::Moment;
use std::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

/// Optional producer only. Caller chooses whether/how to consume sensitive wire bodies.
pub struct ChannelAuditor {
    sender: Option<UnboundedSender<AuditRecord>>,
    state: Mutex<ProducerState>,
}

#[derive(Default)]
struct ProducerState {
    sequence: u64,
    previous_request: Option<(u64, String)>,
}

impl ChannelAuditor {
    /// No channel, serialization, diff computation, or audit history is allocated.
    pub fn disconnected() -> Self {
        Self {
            sender: None,
            state: Mutex::new(ProducerState::default()),
        }
    }

    pub fn from_sender(sender: UnboundedSender<AuditRecord>) -> Self {
        Self {
            sender: Some(sender),
            state: Mutex::new(ProducerState::default()),
        }
    }

    /// Allows producers to skip constructing body copies when auditing is disabled.
    pub fn is_enabled(&self) -> bool {
        self.sender
            .as_ref()
            .is_some_and(|sender| !sender.is_closed())
    }

    /// A closed or absent consumer never fails the caller. Sending is synchronous.
    pub fn publish(&self, event: Moment, body: AuditBody) {
        let Some(sender) = self.sender.as_ref().filter(|sender| !sender.is_closed()) else {
            return;
        };
        // Keep sequence, baseline, and send in the same critical section so concurrent
        // producers cannot enqueue diffs in an order different from their baselines.
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if sender.is_closed() {
            return;
        }
        if matches!(event, Moment::SessionCreated | Moment::SessionRestored) {
            state.previous_request = None;
        }
        let request_diff = body.sent_request.as_deref().map(|request| {
            let previous = state
                .previous_request
                .as_ref()
                .filter(|(epoch, _)| *epoch == body.epoch);
            let diff = request_diff(previous.map_or("", |(_, text)| text.as_str()), request);
            state.previous_request = Some((body.epoch, request.to_owned()));
            diff
        });
        state.sequence += 1;
        let record = AuditRecord {
            sequence: state.sequence,
            event,
            epoch: body.epoch,
            subagent: body.subagent,
            turn_id: body.turn_id,
            round: body.round,
            call_id: body.call_id,
            context_fingerprint: body.context_fingerprint,
            context_target: body.context_target,
            tool_call_id: body.tool_call_id,
            payload: body.payload,
            sent_request: body.sent_request,
            request_diff,
            response: body.response,
            response_bytes: body.response_bytes,
            response_complete: body.response_complete,
            response_status: body.response_status,
            response_content_type: body.response_content_type,
            started_at_unix_ms: body.started_at_unix_ms,
            elapsed_ms: body.elapsed_ms,
            response_completed_at_ms: body.response_completed_at_ms,
            first_token_ms: body.first_token_ms,
            generation_ms: body.generation_ms,
            timing_source: body.timing_source,
            outcome: body.outcome,
            usage: body.usage,
            protected_tool_call_ids: body.protected_tool_call_ids,
        };
        let _ = sender.send(record);
    }
}

impl super::Auditor for ChannelAuditor {
    fn is_enabled(&self) -> bool {
        ChannelAuditor::is_enabled(self)
    }
    fn publish(&self, event: Moment, body: AuditBody) {
        ChannelAuditor::publish(self, event, body);
    }
}
