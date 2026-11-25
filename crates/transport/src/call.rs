//! Provider-call guard: one start/end pair, including future cancellation by drop.
use crate::error::Error;
use crate::{Content, ModelAttempt, ResponseCompletedAt};
use microcrab_auditor::{
    AuditBody, AuditOutcome, Auditor, HookContext, HookPayload, Moment, VariableView,
};
use microcrab_protocol::{Response, Usage};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

pub(crate) async fn intervene_request(
    auditor: &dyn Auditor,
    content: &Content,
    variables: &dyn VariableView,
    cancellation: &CancellationToken,
    call_id: &str,
    request: String,
) -> Result<String, Error> {
    let injection = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(Error::Cancelled),
        result = auditor.intervene(
            Moment::RequestPrepared,
            HookContext {
                epoch: content.epoch,
                subagent: content.subagent.clone(),
                turn: content.turn_id,
                round: content.round,
                call_id: Some(call_id.to_owned()),
                request_body: Some(request.as_str()),
                max_request_bytes: Some(content.max_bytes),
                variables,
                payload: HookPayload::None,
            },
        ) => result?,
    };
    match injection.request_body {
        Some(replacement) => {
            crate::validate_request_rewrite(&request, &replacement, content.max_bytes)?;
            Ok(replacement)
        }
        None => Ok(request),
    }
}

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
    started: Instant,
    started_at_unix_ms: Option<u64>,
    normalized_at: Option<Instant>,
    call_id: String,
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
        let started = Instant::now();
        let started_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok());
        let terminal = enabled.then(|| {
            let body = AuditBody {
                epoch: content.epoch,
                subagent: content.subagent.clone(),
                turn_id: content.turn_id,
                round: content.round,
                call_id: Some(call_id.clone()),
                started_at_unix_ms,
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
            started_at_unix_ms,
            normalized_at: None,
            call_id,
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
                facts.elapsed_ms = millis(self.started.elapsed().as_millis());
                facts.first_token_ms = self.stream_timing.measured(self.started).0;
                facts.streamed_bytes = u64::try_from(self.received.len()).unwrap_or(u64::MAX);
                facts.streamed_chars = self.streamed_chars;
                Error::ProviderIdleTimeout(facts)
            }
            other => other,
        }
    }

    /// Record successful normalization before synthetic preview/Finished delivery
    /// can wait on an observer or a stream consumer. No error can use this sample
    /// as completion evidence, including cancellation during that delivery.
    pub(crate) fn normalized(&mut self) {
        self.normalized_at.get_or_insert_with(Instant::now);
    }

    pub(crate) fn finish(mut self, result: &Result<Response, Error>) -> Option<ModelAttempt> {
        let (outcome, usage) = match result {
            Ok(response) => (AuditOutcome::Accepted, Some(response.usage.clone())),
            Err(error) => (outcome_of(error), None),
        };
        if let Some(body) = &mut self.terminal {
            body.payload = HookPayload::RoundCompleted {
                response: result.as_ref().ok().cloned(),
                status: body.response_status,
                usage: usage.clone(),
            };
        }
        let elapsed_ms = if result.is_ok() {
            self.normalized_at.and_then(|end| self.elapsed_ms_at(end))
        } else {
            self.elapsed_ms_at(Instant::now())
        };
        let attempt = result.is_ok().then(|| ModelAttempt {
            call_id: self.call_id.clone(),
            response_completed_at_ms: self
                .started_at_unix_ms
                .zip(elapsed_ms)
                .and_then(|(start, elapsed)| start.checked_add(elapsed))
                .and_then(ResponseCompletedAt::new),
        });
        self.publish_terminal(
            outcome,
            usage,
            elapsed_ms,
            attempt
                .as_ref()
                .and_then(|attempt| attempt.response_completed_at_ms),
        );
        attempt
    }

    fn elapsed_ms_at(&self, terminal: Instant) -> Option<u64> {
        let elapsed = terminal.checked_duration_since(self.started)?;
        u64::try_from(elapsed.as_millis()).ok()
    }

    fn publish_terminal(
        &mut self,
        outcome: AuditOutcome,
        usage: Option<Usage>,
        elapsed_ms: Option<u64>,
        response_completed_at_ms: Option<ResponseCompletedAt>,
    ) {
        if self.finished {
            return;
        }
        self.finished = true;
        let (first_token_ms, generation_ms) = self.stream_timing.measured(self.started);
        tracing::warn!(
            call_id = %self.call_id,
            outcome = ?outcome,
            elapsed_ms,
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
        body.elapsed_ms = elapsed_ms;
        body.response_completed_at_ms = response_completed_at_ms.map(ResponseCompletedAt::get);
        body.first_token_ms = first_token_ms;
        body.generation_ms = generation_ms;
        body.timing_source = first_token_ms.map(|_| "transport_stream".to_owned());
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
        if !self.finished {
            let elapsed_ms = self.elapsed_ms_at(Instant::now());
            self.publish_terminal(AuditOutcome::Cancelled, None, elapsed_ms, None);
        }
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
        let start = Instant::now();
        call.started = start;
        call.started_at_unix_ms = Some(1_000);
        call.normalized_at = Some(start + std::time::Duration::from_millis(150));
        call.stream_timing.delta(
            &crate::ModelEvent::TextDelta { text: "a".into() },
            start + std::time::Duration::from_millis(40),
        );
        call.stream_timing
            .complete(start + std::time::Duration::from_millis(140));
        call.normalized(); // A second observation must not replace the terminal sample.
        let attempt = call
            .finish(&Ok(Response {
                provider_metadata: None,
                content: "a".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            }))
            .unwrap();
        let prepared = receiver.try_recv().unwrap();
        let terminal = receiver.try_recv().unwrap();
        assert_eq!(prepared.event, Moment::RequestPrepared);
        assert_eq!(terminal.event, Moment::RoundCompleted);
        assert_eq!(prepared.call_id, terminal.call_id);
        assert_eq!(terminal.call_id.as_deref(), Some(attempt.call_id.as_str()));
        assert_eq!(
            attempt
                .response_completed_at_ms
                .map(ResponseCompletedAt::get),
            Some(1_150)
        );
        assert_eq!(terminal.response_completed_at_ms, Some(1_150));
        assert_eq!(terminal.elapsed_ms, Some(150));
        assert_eq!(terminal.first_token_ms, Some(40));
        assert_eq!(terminal.generation_ms, Some(100));
        assert_eq!(terminal.timing_source.as_deref(), Some("transport_stream"));
        assert_eq!(terminal.usage.as_ref().unwrap().output_tokens, None);
        assert!(prepared.first_token_ms.is_none());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn normalized_terminal_sample_is_audit_independent_and_validated() {
        use microcrab_auditor::NoopAuditor;
        use std::time::Duration;
        let response = Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        };
        for (wall_start, terminal_offset, expected) in [
            (Some(1_000), Some(150), Some(1_150)),
            (None, Some(150), None),
            (Some(1_000), None, None),
            (Some(1_000), Some(-1), None),
            (Some(u64::MAX), Some(150), None),
            (Some(0), Some(0), None),
            (Some(8_640_000_000_000_000), Some(1), None),
        ] {
            let mut call = ProviderCallAudit::start(&NoopAuditor, &content(), "{}", "call".into());
            let start = Instant::now();
            call.started = start;
            call.started_at_unix_ms = wall_start;
            call.normalized_at = terminal_offset.map(|offset: i64| {
                let elapsed = Duration::from_millis(offset.unsigned_abs());
                if offset < 0 {
                    start - elapsed
                } else {
                    start + elapsed
                }
            });
            let attempt = call.finish(&Ok(response.clone())).unwrap();
            assert_eq!(attempt.call_id, "call");
            assert_eq!(
                attempt
                    .response_completed_at_ms
                    .map(ResponseCompletedAt::get),
                expected
            );
        }
        let mut call = ProviderCallAudit::start(&NoopAuditor, &content(), "{}", "cancelled".into());
        call.normalized();
        assert!(call.finish(&Err(Error::Cancelled)).is_none());
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
