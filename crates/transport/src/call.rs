//! Provider-call guard: one start/end pair, including future cancellation by drop.
use crate::Content;
use crate::error::Error;
use microcrab_auditor::{AuditBody, AuditOutcome, Auditor, Moment};
use microcrab_protocol::{Response, Usage};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn observer_enabled(auditor: &dyn Auditor) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| auditor.is_enabled())).unwrap_or_else(
        |_| {
            tracing::warn!("audit observer panicked; observation dropped");
            false
        },
    )
}

fn publish_observation(auditor: &dyn Auditor, moment: Moment, body: AuditBody) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        auditor.publish(moment, body)
    }))
    .is_err()
    {
        tracing::warn!("audit observer panicked; observation dropped");
    }
}

pub(crate) struct ProviderCallAudit<'a> {
    auditor: &'a dyn Auditor,
    terminal: Option<AuditBody>,
    started: Option<Instant>,
    call_id: uuid::Uuid,
    finished: bool,
    streamed_chars: u64,
    stream_timing: crate::observation::StreamTiming,
    /// Also the transport decode buffer; no second body copy is needed for auditing.
    pub(crate) received: Vec<u8>,
}

impl<'a> ProviderCallAudit<'a> {
    pub(crate) fn start(
        auditor: &'a dyn Auditor,
        content: &Content,
        request: &str,
        call_id: String,
    ) -> Self {
        let enabled = observer_enabled(auditor);
        let started = Some(Instant::now());
        let identity = uuid::Uuid::parse_str(&call_id).unwrap_or(uuid::Uuid::nil());
        let terminal = enabled.then(|| {
            let body = AuditBody {
                epoch: content.epoch,
                subagent: content.subagent.clone(),
                turn_id: content.turn_id,
                round: content.round,
                call_id: Some(call_id),
                started_at_unix_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|elapsed| millis(elapsed.as_millis())),
                ..AuditBody::default()
            };
            let mut start = body.clone();
            start.sent_request = Some(request.to_owned());
            publish_observation(auditor, Moment::RequestPrepared, start);
            body
        });
        Self {
            auditor,
            terminal,
            started,
            call_id: identity,
            finished: false,
            streamed_chars: 0,
            stream_timing: crate::observation::StreamTiming::default(),
            received: Vec::new(),
        }
    }

    /// Called only for decoded provider SSE events, before preview delivery.
    pub(crate) fn stream_delta(&mut self, event: &crate::ModelEvent) {
        self.stream_timing.delta(event, Instant::now());
        if let crate::ModelEvent::TextDelta { text } | crate::ModelEvent::ReasoningDelta { text } =
            event
        {
            self.streamed_chars = self
                .streamed_chars
                .saturating_add(millis(text.chars().count() as u128));
        }
    }

    /// Successful SSE EOF and final frame flush, before normalized validation.
    pub(crate) fn stream_complete(&mut self) {
        self.stream_timing.complete(Instant::now());
    }

    pub(crate) fn received_status(&mut self, status: u16) {
        if let Some(body) = &mut self.terminal {
            body.response_status = Some(status);
            body.response_complete = Some(false);
        }
    }

    pub(crate) fn received_content_type(&mut self, content_type: String) {
        if let Some(body) = &mut self.terminal {
            body.response_content_type = Some(content_type);
        }
    }

    pub(crate) fn received_complete(&mut self) {
        if let Some(body) = &mut self.terminal {
            body.response_complete = Some(true);
        }
    }

    pub(crate) fn measured_error(&self, error: Error) -> Error {
        match error {
            Error::ProviderIdleTimeout(mut facts) => {
                facts.elapsed_ms = self
                    .started
                    .map_or(0, |start| millis(start.elapsed().as_millis()));
                facts.first_token_ms = self
                    .started
                    .and_then(|start| self.stream_timing.measured(start).0);
                facts.streamed_bytes = u64::try_from(self.received.len()).unwrap_or(u64::MAX);
                facts.streamed_chars = self.streamed_chars;
                Error::ProviderIdleTimeout(facts)
            }
            other => other,
        }
    }

    pub(crate) fn finish(mut self, result: &Result<Response, Error>) {
        let (outcome, usage) = match result {
            Ok(response) => (AuditOutcome::Accepted, Some(response.usage.clone())),
            Err(error) => (outcome_of(error), None),
        };
        if let Some(body) = &mut self.terminal {
            body.payload = microcrab_auditor::HookPayload::RoundCompleted {
                response: result.as_ref().ok().cloned(),
                status: body.response_status,
                usage: usage.clone(),
            };
        }
        self.publish_terminal(outcome, usage);
    }

    fn publish_terminal(&mut self, outcome: AuditOutcome, usage: Option<Usage>) {
        if self.finished {
            return;
        }
        self.finished = true;
        let first_token_ms = self
            .started
            .and_then(|started| self.stream_timing.measured(started).0);
        tracing::warn!(
            call_id = %self.call_id,
            outcome = ?outcome,
            elapsed_ms = self.started.map(|started| millis(started.elapsed().as_millis())),
            first_token_ms,
            streamed_bytes = self.received.len(),
            streamed_chars = self.streamed_chars,
            "Provider attempt completed"
        );
        let Some(mut body) = self.terminal.take() else {
            return;
        };
        if !observer_enabled(self.auditor) {
            return;
        }
        body.outcome = Some(outcome);
        body.usage = usage;
        body.elapsed_ms = self
            .started
            .map(|started| millis(started.elapsed().as_millis()));
        if let Some(started) = self.started {
            let (first, generation) = self.stream_timing.measured(started);
            body.first_token_ms = first;
            body.generation_ms = generation;
            body.timing_source = first.map(|_| "transport_stream".to_owned());
        }
        if body.response_status.is_some() {
            match String::from_utf8(std::mem::take(&mut self.received)) {
                Ok(text) => body.response = Some(text),
                Err(error) => body.response_bytes = Some(error.into_bytes()),
            }
        }
        publish_observation(self.auditor, Moment::RoundCompleted, body);
    }
}

impl Drop for ProviderCallAudit<'_> {
    fn drop(&mut self) {
        // No await/spawn: aborting or dropping a polled provider future still enqueues
        // its terminal audit. An unpolled future never started a logical call.
        self.publish_terminal(AuditOutcome::Cancelled, None);
    }
}

fn millis(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn outcome_of(error: &Error) -> AuditOutcome {
    match error {
        Error::Cancelled => AuditOutcome::Cancelled,
        Error::Limit(_) => AuditOutcome::LimitExceeded,
        Error::Protocol(_) => AuditOutcome::InvalidResponse,
        Error::ProviderTimeout | Error::ProviderIdleTimeout(_) => AuditOutcome::TimedOut,
        _ => AuditOutcome::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PanickingObserver {
        enabled_panics: bool,
    }
    impl Auditor for PanickingObserver {
        fn is_enabled(&self) -> bool {
            assert!(!self.enabled_panics, "broken observer enablement");
            true
        }
        fn publish(&self, _: Moment, _: AuditBody) {
            panic!("broken observer publish");
        }
    }
    fn content() -> Content {
        Content {
            epoch: 0,
            subagent: None,
            system: "system".into(),
            items: vec![],
            tools: vec![],
            max_output_tokens: 32,
            max_bytes: 4096,
            turn_id: Some(1),
            round: Some(1),
        }
    }
    #[test]
    fn functional_identity_reaches_both_audit_records_without_changing_wire_body() {
        for subagent in [None, Some("compact"), Some("rename")] {
            for cancel in [false, true] {
                let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
                let auditor = microcrab_auditor::ChannelAuditor::from_sender(sender);
                let mut request = content();
                request.subagent = subagent.map(str::to_owned);
                if subagent.is_some() {
                    request.turn_id = None;
                    request.round = None;
                }
                let call = ProviderCallAudit::start(
                    &auditor,
                    &request,
                    "{\"messages\":[]}",
                    "call".into(),
                );
                if cancel {
                    drop(call);
                } else {
                    call.finish(&Ok(Response {
                        provider_metadata: None,
                        content: "title".into(),
                        tool_calls: vec![],
                        usage: Usage::default(),
                    }));
                }
                let start = receiver.try_recv().unwrap();
                let end = receiver.try_recv().unwrap();
                assert_eq!(start.event, Moment::RequestPrepared);
                assert_eq!(end.event, Moment::RoundCompleted);
                assert_eq!(start.subagent.as_deref(), subagent);
                assert_eq!(end.subagent.as_deref(), subagent);
                assert_eq!(start.turn_id, request.turn_id);
                assert_eq!(end.round, request.round);
                assert_eq!(start.call_id, end.call_id);
                assert_eq!(start.sent_request.as_deref(), Some("{\"messages\":[]}"));
                assert_eq!(
                    end.outcome,
                    Some(if cancel {
                        AuditOutcome::Cancelled
                    } else {
                        AuditOutcome::Accepted
                    })
                );
                assert_eq!(
                    serde_json::to_value(&end).unwrap()["subagent"],
                    serde_json::to_value(subagent).unwrap()
                );
                assert!(receiver.try_recv().is_err());
            }
        }
    }

    #[test]
    fn stream_clocks_are_metadata_on_the_existing_terminal_record() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let auditor = microcrab_auditor::ChannelAuditor::from_sender(sender);
        let mut call = ProviderCallAudit::start(&auditor, &content(), "{}", "stream".into());
        let start = Instant::now() - std::time::Duration::from_millis(200);
        call.started = Some(start);
        call.stream_timing.delta(
            &crate::ModelEvent::TextDelta { text: "a".into() },
            start + std::time::Duration::from_millis(40),
        );
        call.stream_timing
            .complete(start + std::time::Duration::from_millis(140));
        call.finish(&Ok(Response {
            provider_metadata: None,
            content: "a".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        }));
        let prepared = receiver.try_recv().unwrap();
        let terminal = receiver.try_recv().unwrap();
        assert_eq!(prepared.event, Moment::RequestPrepared);
        assert_eq!(terminal.event, Moment::RoundCompleted);
        assert_eq!(prepared.call_id, terminal.call_id);
        assert_eq!(terminal.first_token_ms, Some(40));
        assert_eq!(terminal.generation_ms, Some(100));
        assert_eq!(terminal.timing_source.as_deref(), Some("transport_stream"));
        assert_eq!(terminal.usage.as_ref().unwrap().output_tokens, None);
        assert!(prepared.first_token_ms.is_none());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn observer_panics_cannot_fail_completion_or_drop() {
        for enabled_panics in [false, true] {
            let auditor = PanickingObserver { enabled_panics };
            let call = ProviderCallAudit::start(&auditor, &content(), "{}", "a".into());
            call.finish(&Ok(Response {
                provider_metadata: None,
                content: "done".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            }));
            let unfinished = ProviderCallAudit::start(&auditor, &content(), "{}", "b".into());
            drop(unfinished);
        }
    }
}
