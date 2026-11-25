use super::*;
use serde_json::{Value, json};
use std::io::Write;
mod response_wire;
mod turn_usage;

fn selector() -> AuditQuery {
    AuditQuery {
        epoch: 2,
        turn: Some(3),
        round: Some(1),
        request_hash: None,
        history_head: None,
        subagent: None,
        call_id: None,
        tool_call_id: None,
    }
}
fn effective(call: &str, parent: Option<String>) -> EffectiveRequest {
    EffectiveRequest {
        call_id: call.into(),
        epoch: 2,
        turn: Some(3),
        round: Some(1),
        parent_request: parent,
        history_head: None,
        subagent: None,
        request_body: "effective-only-secret".into(),
    }
}
fn send(recorder: &mut Recorder, call: &str, wire: &str) {
    recorder
        .publish(json!({"event":"request_prepared","call_id":call,"sent_request":wire}))
        .unwrap();
}
#[test]
fn legacy_bodyless_lifecycle_requires_an_exact_unique_call_and_never_fabricates_a_request() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let start = json!({"event":"request_prepared","call_id":"legacy","epoch":2,
        "turn_id":3,"round":1,"started_at_unix_ms":100});
    let terminal = json!({"event":"round_completed","call_id":"legacy","epoch":2,
        "turn_id":3,"round":1,"started_at_unix_ms":100,"elapsed_ms":12,"outcome":"accepted"});
    recorder.publish(start).unwrap();
    recorder.publish(terminal.clone()).unwrap();
    let exact = AuditQuery {
        call_id: Some("legacy".into()),
        ..selector()
    };
    let result = query(directory.path(), &exact).unwrap();
    assert!(result.attempts.is_empty());
    assert_eq!(result.status, AuditStatus::Missing);
    let legacy = result.legacy_unlinked_attempt.unwrap();
    assert_eq!(legacy.call_id, "legacy");
    assert_eq!(legacy.completion, "succeeded");
    assert_eq!(legacy.ended_at_ms, Some(112));
    assert!(
        query(directory.path(), &selector())
            .unwrap()
            .legacy_unlinked_attempt
            .is_none()
    );
    let wrong = AuditQuery {
        round: Some(2),
        ..exact.clone()
    };
    assert!(
        query(directory.path(), &wrong)
            .unwrap()
            .legacy_unlinked_attempt
            .is_none()
    );
    recorder.publish(terminal).unwrap();
    assert!(
        query(directory.path(), &exact)
            .unwrap()
            .legacy_unlinked_attempt
            .is_none()
    );
}

#[test]
fn cancelled_completion_survives_durable_query_and_metadata_projection() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    for (call, outcome, expected) in [
        ("cancelled", "cancelled", "cancelled"),
        ("accepted", "accepted", "succeeded"),
        ("unavailable", "unavailable", "failed"),
    ] {
        let hash = recorder.effective(effective(call, None)).unwrap();
        send(&mut recorder, call, "{}");
        recorder
            .publish(json!({"event":"round_completed","call_id":call,"outcome":outcome}))
            .unwrap();
        let mut exact = selector();
        exact.request_hash = Some(hash);
        let full = query(directory.path(), &exact).unwrap();
        let metadata = query_attempt_metadata(directory.path(), &exact).unwrap();
        assert_eq!(full.attempts[0].completion, expected);
        assert_eq!(metadata.attempts[0].completion, expected);
        assert_eq!(full.selected_request_hash, exact.request_hash);
        assert_eq!(
            metadata.attempts[0].cancel_reason,
            (outcome == "cancelled").then_some("cancelled")
        );
        assert_eq!(
            serde_json::to_value(&metadata.attempts[0]).unwrap()["completion"],
            expected
        );
    }
}

#[test]
fn selected_response_completion_requires_unambiguous_terminal_timing() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    for (call, outcome, timing, expected) in [
        (
            "normalized",
            "accepted",
            json!({"response_completed_at_ms":1150}),
            Some(1150),
        ),
        (
            "legacy",
            "accepted",
            json!({"started_at_unix_ms":1000,"elapsed_ms":150}),
            Some(1150),
        ),
        ("missing-start", "accepted", json!({"elapsed_ms":150}), None),
        (
            "invalid-explicit",
            "accepted",
            json!({"response_completed_at_ms":0,"started_at_unix_ms":1000,"elapsed_ms":150}),
            None,
        ),
        (
            "contradictory-clocks",
            "accepted",
            json!({"response_completed_at_ms":1160,"started_at_unix_ms":1000,"elapsed_ms":150}),
            None,
        ),
        (
            "overflow",
            "accepted",
            json!({"started_at_unix_ms":u64::MAX,"elapsed_ms":150}),
            None,
        ),
        (
            "outside-date",
            "accepted",
            json!({"response_completed_at_ms":8_640_000_000_000_001_u64}),
            None,
        ),
        (
            "cancelled",
            "cancelled",
            json!({"response_completed_at_ms":1150}),
            None,
        ),
    ] {
        let head = recorder.effective(effective(call, None)).unwrap();
        recorder.publish(json!({"event":"request_prepared","call_id":call,"sent_request":"{}","started_at_unix_ms":1000})).unwrap();
        let mut terminal = json!({"event":"round_completed","call_id":call,"outcome":outcome,"response":"not projected"});
        terminal
            .as_object_mut()
            .unwrap()
            .extend(timing.as_object().unwrap().clone());
        recorder.publish(terminal).unwrap();
        let result =
            query_selected_attempt_metadata(directory.path(), &head, &[(2, 3, 1)]).unwrap();
        assert!(result.selected_ancestry_complete && result.selected_identities_complete);
        if outcome == "accepted" {
            let attempt = &result.attempts[0];
            assert!(attempt.response.is_none());
            assert_eq!(attempt.response_completed_at_ms, expected, "{call}");
        } else {
            assert!(result.attempts.is_empty());
        }
    }

    let head = recorder.effective(effective("conflict", None)).unwrap();
    send(&mut recorder, "conflict", "{}");
    for (sample, complete) in [(1150, true), (1150, true), (1160, false), (1150, false)] {
        recorder.publish(json!({"event":"round_completed","call_id":"conflict","outcome":"accepted","response_completed_at_ms":sample})).unwrap();
        let result =
            query_selected_attempt_metadata(directory.path(), &head, &[(2, 3, 1)]).unwrap();
        assert_eq!(result.selected_identities_complete, complete);
        assert_eq!(
            result.attempts[0].response_completed_at_ms,
            complete.then_some(1150)
        );
    }
}

#[test]
fn stream_timing_metadata_survives_durable_queries_without_body_inference() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    for (call, metadata) in [
        ("legacy", json!({})),
        (
            "stream",
            json!({"first_token_ms":40,"generation_ms":80,"timing_source":"transport_stream"}),
        ),
        (
            "partial",
            json!({"first_token_ms":40,"timing_source":"transport_stream"}),
        ),
        (
            "untrusted",
            json!({"first_token_ms":40,"generation_ms":80,"timing_source":"aggregate_preview"}),
        ),
        (
            "invalid",
            json!({"first_token_ms":140,"generation_ms":80,"timing_source":"transport_stream"}),
        ),
    ] {
        let request_hash = recorder.effective(effective(call, None)).unwrap();
        send(&mut recorder, call, "{}");
        let mut terminal = json!({"event":"round_completed","call_id":call,"outcome":"accepted","started_at_unix_ms":1000,"elapsed_ms":150,"usage":{"input_tokens":12},"response":"sensitive response body"});
        terminal
            .as_object_mut()
            .unwrap()
            .extend(metadata.as_object().unwrap().clone());
        recorder.publish(terminal).unwrap();
        let mut exact = selector();
        exact.request_hash = Some(request_hash);
        let result = query_attempt_metadata(directory.path(), &exact).unwrap();
        let attempt = &result.attempts[0];
        assert!(attempt.response.is_none());
        assert!(attempt.sent_request.is_none());
        assert_eq!(
            attempt.usage.as_ref().unwrap()["output_tokens"],
            Value::Null
        );
        match call {
            "stream" => {
                assert_eq!(attempt.first_token_ms, Some(40));
                assert_eq!(attempt.generation_ms, Some(80));
                assert_eq!(attempt.timing_source.as_deref(), Some("transport_stream"));
            }
            "partial" | "invalid" => {
                assert!(attempt.first_token_ms.is_some());
                assert!(attempt.generation_ms.is_none());
            }
            _ => {
                assert!(attempt.first_token_ms.is_none());
                assert!(attempt.generation_ms.is_none());
                assert!(attempt.timing_source.is_none());
            }
        }
    }
}

#[test]
fn selected_internal_timeline_follows_journal_ancestry_not_latest_success() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let root = recorder.effective(effective("root", None)).unwrap();
    let mut selected = effective("selected", Some(root.clone()));
    selected.history_head = Some("a".repeat(64));
    let selected = recorder.effective(selected).unwrap();
    let mut sibling = effective("newer-sibling", Some(root.clone()));
    sibling.history_head = Some("b".repeat(64));
    let sibling = recorder.effective(sibling).unwrap();
    recorder.publish(json!({"call_id":"newer-sibling","event":"round_completed","outcome":{"status":"succeeded"}})).unwrap();
    let result = query_internal_selected(
        directory.path(),
        &selector(),
        Some(&selected),
        Some(&"a".repeat(64)),
    )
    .unwrap();
    assert!(result.timeline.iter().all(|row| row.branch_selected));
    assert_eq!(
        result
            .timeline
            .iter()
            .map(|row| row.request_hash.as_str())
            .collect::<Vec<_>>(),
        vec![root.as_str(), selected.as_str()]
    );
    assert!(
        !result
            .timeline
            .iter()
            .any(|row| row.request_hash == sibling)
    );
    let evidence = query_selected(
        directory.path(),
        &selector(),
        Some(&selected),
        Some(&"a".repeat(64)),
    )
    .unwrap();
    assert!(
        evidence
            .attempts
            .iter()
            .any(|attempt| attempt.request_hash == selected)
    );
    assert!(
        !evidence
            .attempts
            .iter()
            .any(|attempt| attempt.request_hash == sibling)
    );
    let empty = query_internal_selected(directory.path(), &selector(), None, None).unwrap();
    assert!(empty.timeline.is_empty());
}

#[test]
fn verified_intermediate_helper_membership_keeps_competing_main_anchors_excluded() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let a = "a".repeat(64);
    let b = "b".repeat(64);
    let c = "c".repeat(64);
    let mut main = effective("main", None);
    main.history_head = Some(a.clone());
    let main = recorder.effective(main).unwrap();
    let mut helper = effective("review", None);
    helper.history_head = Some(b.clone());
    helper.subagent = Some("action_review".into());
    let helper = recorder.effective(helper).unwrap();
    let ancestry = vec![c.clone(), b.clone(), a];
    let result = query_internal_selected_with_ancestry(
        directory.path(),
        &selector(),
        Some(&main),
        Some(&c),
        &ancestry,
    )
    .unwrap();
    assert_eq!(result.attempts[0].request_hash, helper);
    assert!(
        query_internal_selected_with_ancestry(
            directory.path(),
            &selector(),
            Some(&main),
            Some(&c),
            &[]
        )
        .is_err()
    );
    let mut sibling = effective("competing-main", Some(main.clone()));
    sibling.history_head = Some(b);
    recorder.effective(sibling).unwrap();
    let result = query_internal_selected_with_ancestry(
        directory.path(),
        &selector(),
        Some(&main),
        Some(&c),
        &ancestry,
    )
    .unwrap();
    assert!(result.attempts.is_empty());
}

#[test]
fn query_returns_causal_attempts_and_exact_historical_schema_without_writer_lock() {
    let directory = tempfile::tempdir().unwrap();
    let wire = r#"{"messages":[],"tools":[{"type":"function","function":{"name":"historical","parameters":{"type":"object"}}}]}"#;
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = recorder.effective(effective("first", None)).unwrap();
    send(&mut recorder, "first", wire);
    let second = recorder
        .effective(effective("second", Some(first.clone())))
        .unwrap();
    send(&mut recorder, "second", wire);
    let result = query(directory.path(), &selector()).unwrap();
    assert_eq!(result.attempts.len(), 2);
    assert_eq!(result.attempts[0].baseline.as_deref(), Some(""));
    assert_eq!(result.attempts[1].status, AuditStatus::Unchanged);
    assert_eq!(result.attempts[1].base_request.as_ref(), Some(&first));
    assert_eq!(result.attempts[1].delta.as_deref(), Some(""));
    assert_eq!(
        result.attempts[1].tools.as_ref().unwrap()[0]["function"]["name"],
        "historical"
    );
    let mut selected = selector();
    selected.request_hash = Some(second);
    assert_eq!(
        query(directory.path(), &selected).unwrap().attempts.len(),
        1
    );
    assert!(!directory.path().join("audit").exists());
}
#[test]
fn query_distinguishes_pending_omission_missing_and_rejects_corruption() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(
        directory.path(),
        RecorderOptions {
            retention: Retention::None,
            ..Default::default()
        },
    )
    .unwrap();
    recorder.effective(effective("pending", None)).unwrap();
    assert_eq!(
        query(directory.path(), &selector()).unwrap().status,
        AuditStatus::Pending
    );
    send(&mut recorder, "pending", "not retained");
    let result = query(directory.path(), &selector()).unwrap();
    assert_eq!(result.status, AuditStatus::Omitted);
    assert!(result.attempts[0].sent_request.is_none());
    assert!(result.attempts[0].request_document.is_none());
    let mut absent = selector();
    absent.round = Some(99);
    assert_eq!(
        query(directory.path(), &absent).unwrap().status,
        AuditStatus::Missing
    );
    let path = directory.path().join("audit.jsonl");
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(b"{\"incomplete\":").unwrap();
    assert!(
        query(directory.path(), &selector())
            .unwrap()
            .incomplete_tail
    );
    file.write_all(b"true}\n").unwrap();
    assert!(query(directory.path(), &selector()).is_err());
}
#[test]
fn query_context_delta_replays_selected_parent_and_keeps_metadata_separate() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let wire = |answer: &str| {
        json!({
            "system": (0..20).map(|i| format!("frozen-system-{i}")).collect::<Vec<_>>(),
            "messages": [{"role":"user","content":answer}],
            "tools": [{"name":"historical"}]
        })
        .to_string()
    };
    let first = recorder.effective(effective("first", None)).unwrap();
    let first_wire = wire("one");
    send(&mut recorder, "first", &first_wire);
    let unrelated = recorder
        .effective(effective("unrelated", Some(first.clone())))
        .unwrap();
    send(&mut recorder, "unrelated", &wire("unrelated-branch"));
    let selected = recorder
        .effective(effective("selected", Some(first.clone())))
        .unwrap();
    let selected_wire = wire("two");
    send(&mut recorder, "selected", &selected_wire);
    let result = query(directory.path(), &selector()).unwrap();
    let initial = &result.attempts[0];
    assert!(initial.base_request.is_none());
    assert_eq!(initial.baseline.as_deref(), Some(""));
    assert!(initial.delta.as_deref().unwrap().starts_with("--- empty\n"));
    let attempt = result
        .attempts
        .iter()
        .find(|a| a.request_hash == selected)
        .unwrap();
    assert_eq!(attempt.base_request.as_ref(), Some(&first));
    assert_ne!(attempt.base_request.as_ref(), Some(&unrelated));
    assert_eq!(
        attempt.sent_request.as_deref(),
        Some(selected_wire.as_str())
    );
    let delta = attempt.delta.as_deref().unwrap();
    assert!(delta.contains("-      \"content\": \"one\"\n+      \"content\": \"two\"\n"));
    assert!(!delta.contains("frozen-system-0"));
    assert!(!delta.contains("unrelated-branch"));
    assert!(!delta.contains("effective-only-secret"));
    assert!(!delta.contains("base_request"));
    assert_eq!(
        super::diff::replay_patch(attempt.baseline.as_deref().unwrap(), delta),
        attempt.request_document.as_deref().unwrap()
    );
    assert_eq!(
        super::diff::replay_patch("", initial.delta.as_deref().unwrap()),
        initial.request_document.as_deref().unwrap()
    );
    // The same patch the query projects replays from the same base after flush,
    // because flush no longer rewrites any readable projection.
    recorder.flush().unwrap();
    assert_eq!(
        super::diff::replay_patch(attempt.baseline.as_deref().unwrap(), delta),
        attempt.request_document.as_deref().unwrap()
    );
    assert!(!directory.path().join("audit").exists());
}

#[test]
fn query_initial_diff_never_borrows_unrelated_epoch_scope_or_missing_parent() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = recorder.effective(effective("first", None)).unwrap();
    send(&mut recorder, "first", "{\"before\":true}");
    let mut old_epoch = effective("old-epoch", Some(first.clone()));
    old_epoch.epoch = 3;
    recorder.effective(old_epoch).unwrap();
    send(&mut recorder, "old-epoch", "{\"after\":true}");
    let mut foreign = effective("foreign", Some(first.clone()));
    foreign.subagent = Some("other".into());
    recorder.effective(foreign).unwrap();
    send(&mut recorder, "foreign", "{\"after\":true}");
    let missing = recorder.effective(effective("missing", None)).unwrap();
    recorder
        .effective(effective("missing-child", Some(missing)))
        .unwrap();
    send(&mut recorder, "missing-child", "{\"after\":true}");
    recorder.effective(effective("root", None)).unwrap();
    send(&mut recorder, "root", "{\"after\":true}");
    for (epoch, subagent) in [(2, None), (3, None), (2, Some("other".to_owned()))] {
        let mut selected = selector();
        selected.epoch = epoch;
        selected.subagent = subagent;
        for attempt in query(directory.path(), &selected).unwrap().attempts {
            if attempt.sent_request.is_none() {
                continue;
            }
            assert!(attempt.base_request.is_none());
            assert_eq!(attempt.baseline.as_deref(), Some(""));
            assert!(attempt.delta.as_deref().unwrap().starts_with("--- empty\n"));
            assert_eq!(
                super::diff::replay_patch("", attempt.delta.as_deref().unwrap()),
                attempt.request_document.as_deref().unwrap()
            );
        }
    }
}

#[test]
fn durable_observer_can_read_the_announced_commit() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let path = directory.path().to_owned();
    let observations = std::sync::Arc::new(std::sync::Mutex::new(vec![]));
    let captured = observations.clone();
    recorder.observer = Some(std::sync::Arc::new(move |commit| {
        let result = query(&path, &selector()).unwrap();
        assert_eq!(result.committed_sequence, commit.sequence);
        captured.lock().unwrap().push(commit);
    }));
    recorder.effective(effective("observed", None)).unwrap();
    send(&mut recorder, "observed", "{\"messages\":[]}");
    let observed = observations.lock().unwrap();
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].kind, "effective");
    assert_eq!(observed[1].kind, "sent");
    assert_eq!(observed[0].request_hash, observed[1].request_hash);
}
#[test]
fn tool_result_selector_follows_first_causal_admission() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = recorder.effective(effective("first", None)).unwrap();
    let wire = r#"{"messages":[{"role":"tool","tool_call_id":"tool-1","content":"done"}]}"#;
    send(&mut recorder, "first", wire);
    let mut later = effective("later", Some(first.clone()));
    later.round = Some(2);
    recorder.effective(later).unwrap();
    send(&mut recorder, "later", wire);
    let mut selected = selector();
    selected.turn = None;
    selected.round = None;
    selected.tool_call_id = Some("tool-1".into());
    let result = query(directory.path(), &selected).unwrap();
    assert_eq!(result.attempts.len(), 1);
    assert_eq!(result.attempts[0].request_hash, first);
}

#[test]
fn tool_result_membership_handles_all_transport_envelopes_without_user_data_search() {
    let wires = [
        json!({"messages":[{"role":"tool","tool_call_id":"tool-1","content":"done"}]}),
        json!({"messages":[{"role":"tool","call_id":"tool-1","content":"done"}]}),
        json!({"input":[{"type":"function_call_output","call_id":"tool-1","output":"done"}]}),
        json!({"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"done"}]}]}),
        json!({"contents":[{"role":"user","parts":[{"functionResponse":{"id":"tool-1","name":"lookup","response":{"output":"done"}}}]}]}),
        json!({"contents":[{"role":"user","parts":[{"function_response":{"id":"tool-1","name":"lookup","response":{"output":"done"}}}]}]}),
        json!({"contents":[{"role":"user","parts":[{"function_response":{"name":"tool-1","response":{"output":"done"}}}]}]}),
    ];
    for wire in wires {
        let directory = tempfile::tempdir().unwrap();
        let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
        let identity = recorder.effective(effective("envelope", None)).unwrap();
        send(&mut recorder, "envelope", &wire.to_string());
        let mut selected = selector();
        selected.tool_call_id = Some("tool-1".into());
        let result = query(directory.path(), &selected).unwrap();
        assert_eq!(result.attempts.len(), 1, "{wire}");
        assert_eq!(result.attempts[0].request_hash, identity);
        selected.tool_call_id = Some("other".into());
        assert!(
            query(directory.path(), &selected)
                .unwrap()
                .attempts
                .is_empty()
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    recorder.effective(effective("untrusted", None)).unwrap();
    send(&mut recorder, "untrusted", &json!({"messages":[{"role":"user","content":"{\"role\":\"tool\",\"tool_call_id\":\"tool-1\"}"}],"data":{"role":"tool","tool_call_id":"tool-1"}}).to_string());
    let mut selected = selector();
    selected.tool_call_id = Some("tool-1".into());
    assert!(
        query(directory.path(), &selected)
            .unwrap()
            .attempts
            .is_empty()
    );
}

#[test]
fn canonical_google_result_membership_uses_id_never_function_name() {
    for part in [
        json!({"functionResponse":{"id":"different-call","name":"tool-1","response":{}}}),
        json!({"functionResponse":{"name":"tool-1","response":{}}}),
        json!({"function_response":{"id":"different-call","name":"tool-1","response":{}}}),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
        recorder.effective(effective("not-a-match", None)).unwrap();
        let wire = json!({"contents":[{"role":"user","parts":[part]}]});
        send(&mut recorder, "not-a-match", &wire.to_string());
        let mut selected = selector();
        selected.tool_call_id = Some("tool-1".into());
        assert!(
            query(directory.path(), &selected)
                .unwrap()
                .attempts
                .is_empty()
        );
    }
}

#[test]
fn prepared_membership_preserves_pending_and_omitted_without_fabricating_sent_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(
        directory.path(),
        RecorderOptions {
            retention: Retention::Effective,
            ..Default::default()
        },
    )
    .unwrap();
    let wire =
        json!({"input":[{"type":"function_call_output","call_id":"tool-1","output":"done"}]})
            .to_string();
    let mut request = effective("prepared", None);
    request.request_body = wire.clone();
    recorder.effective(request).unwrap();
    let mut selected = selector();
    selected.tool_call_id = Some("tool-1".into());
    let pending = query(directory.path(), &selected).unwrap();
    assert_eq!(pending.status, AuditStatus::Pending);
    assert!(pending.attempts[0].sent_request.is_none());
    assert!(pending.attempts[0].tools.is_none());
    send(&mut recorder, "prepared", &wire);
    let omitted = query(directory.path(), &selected).unwrap();
    assert_eq!(omitted.status, AuditStatus::Omitted);
    assert!(omitted.attempts[0].baseline.is_none());
    assert!(omitted.attempts[0].delta.is_none());
}

#[test]
fn tool_result_selector_keeps_first_position_retries_but_not_later_position_retries() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let wire =
        json!({"messages":[{"role":"tool","tool_call_id":"tool-1","content":"done"}]}).to_string();
    let first = recorder.effective(effective("first", None)).unwrap();
    send(&mut recorder, "first", &wire);
    let retry = recorder
        .effective(effective("retry", Some(first.clone())))
        .unwrap();
    send(&mut recorder, "retry", &wire);
    let mut later = effective("later", Some(retry.clone()));
    later.round = Some(2);
    let later_id = recorder.effective(later).unwrap();
    send(&mut recorder, "later", &wire);
    let mut later_retry = effective("later-retry", Some(later_id));
    later_retry.round = Some(2);
    recorder.effective(later_retry).unwrap();
    send(&mut recorder, "later-retry", &wire);
    let mut selected = selector();
    selected.turn = None;
    selected.round = None;
    selected.tool_call_id = Some("tool-1".into());
    let result = query(directory.path(), &selected).unwrap();
    assert_eq!(
        result
            .attempts
            .iter()
            .map(|a| &a.request_hash)
            .collect::<Vec<_>>(),
        vec![&first, &retry]
    );
}

#[test]
fn selected_step_uses_actual_completion_and_persisted_usage() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(dir.path(), RecorderOptions::default()).unwrap();
    let first = recorder.effective(effective("failed", None)).unwrap();
    send(&mut recorder, "failed", r#"{"messages":[]}"#);
    recorder
        .publish(json!({"event":"round_completed","call_id":"failed","outcome":"timed_out"}))
        .unwrap();
    let success = recorder
        .effective(effective("success", Some(first.clone())))
        .unwrap();
    send(&mut recorder, "success", r#"{"messages":[]}"#);
    recorder.publish(json!({"event":"round_completed","call_id":"success","payload":{"status":200,"response":{"content":"answer","tool_calls":[]}},"usage":{"input_tokens":17,"output_tokens":3,"total_tokens":20,"cache_read_tokens":null}})).unwrap();
    recorder
        .effective(effective("pending", Some(success.clone())))
        .unwrap();
    let result = query(dir.path(), &selector()).unwrap();
    assert_eq!(result.attempts.len(), 3);
    assert_eq!(result.selected_request_hash.as_ref(), Some(&success));
    assert_eq!(result.attempts[0].completion, "failed");
    assert_eq!(
        result.attempts[1].response.as_ref().unwrap()["content"],
        "answer"
    );
    assert_eq!(
        result.attempts[1].usage.as_ref().unwrap()["input_tokens"],
        17
    );
    assert!(result.attempts[1].token_delta["input_tokens"].is_null());
    assert!(result.attempts[2].usage.is_none());
    assert!(
        result.attempts[2]
            .token_delta
            .as_object()
            .unwrap()
            .values()
            .all(Value::is_null)
    );
}
#[test]
fn internal_listing_is_epoch_scoped_bounded_summary_with_exact_lazy_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(dir.path(), RecorderOptions::default()).unwrap();
    let main = recorder.effective(effective("main", None)).unwrap();
    send(&mut recorder, "main", r#"{"messages":[]}"#);
    let mut helper = effective("rename", None);
    helper.subagent = Some("rename".into());
    helper.turn = None;
    helper.round = None;
    let id = recorder.effective(helper).unwrap();
    send(
        &mut recorder,
        "rename",
        r#"{"messages":[{"role":"user","content":"private-helper-input"}]}"#,
    );
    recorder.publish(json!({"event":"round_completed","call_id":"rename","payload":{"status":200,"response":{"content":"new name"}},"usage":{"input_tokens":4,"output_tokens":2,"total_tokens":6}})).unwrap();
    let mut q = selector();
    q.turn = None;
    q.round = None;
    let listing = query_internal(dir.path(), &q).unwrap();
    assert_eq!(listing.attempts.len(), 1);
    assert_eq!(listing.attempts[0].request_hash, id);
    assert!(listing.attempts[0].sent_request.is_none());
    assert_eq!(
        listing.attempts[0].response.as_ref().unwrap()["content"],
        "new name"
    );
    assert_eq!(listing.attempts[0].token_delta["input_tokens"], 4);
    q.subagent = Some("rename".into());
    q.call_id = Some("rename".into());
    let detail = query(dir.path(), &q).unwrap();
    assert!(
        detail.attempts[0]
            .sent_request
            .as_ref()
            .unwrap()
            .contains("private-helper-input")
    );
    q.history_head = Some(main);
    assert!(query_internal(dir.path(), &q).unwrap().attempts.is_empty());
}
#[test]
fn token_deltas_keep_unknown_fields_and_signed_decreases() {
    let current =
        json!({"input_tokens":10,"output_tokens":8,"cache_read_tokens":null,"total_tokens":18});
    let base = json!({"input_tokens":15,"output_tokens":2,"cache_read_tokens":3,"total_tokens":17});
    let delta = query::token_delta(Some(&current), Some(&base));
    assert_eq!(delta["input_tokens"], -5);
    assert_eq!(delta["output_tokens"], 6);
    assert_eq!(delta["total_tokens"], 1);
    assert!(delta["cache_read_tokens"].is_null());
}

#[test]
fn step_selection_ignores_later_round_and_rejects_http_success_error_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(dir.path(), RecorderOptions::default()).unwrap();
    let a = recorder.effective(effective("a", None)).unwrap();
    send(&mut recorder, "a", r#"{"messages":[]}"#);
    recorder.publish(json!({"body":{"event":"round_completed","call_id":"a","payload":{"status":503,"response":{"error":"unavailable"}}}})).unwrap();
    let b = recorder.effective(effective("b", Some(a))).unwrap();
    send(&mut recorder, "b", r#"{"messages":[]}"#);
    recorder.publish(json!({"event":"round_completed","call_id":"b","payload":{"status":200,"response":{"error":"bad envelope"}}})).unwrap();
    let mut later = effective("later", Some(b.clone()));
    later.round = Some(2);
    recorder.effective(later).unwrap();
    send(&mut recorder, "later", r#"{"messages":[]}"#);
    recorder.publish(json!({"event":"round_completed","call_id":"later","outcome":"accepted","usage":{"input_tokens":99},"payload":{"response":{"content":"later result"}}})).unwrap();
    let mut q = selector();
    q.round = None;
    let result = query(dir.path(), &q).unwrap();
    assert_eq!(result.attempts.len(), 3);
    assert_eq!(result.selected_request_hash, Some(b));
    assert_eq!(result.attempts[1].completion, "failed");
    assert_eq!(result.attempts[2].completion, "succeeded");
}
#[test]
fn internal_response_summary_omits_oversized_body_but_exact_detail_retains_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(dir.path(), RecorderOptions::default()).unwrap();
    let mut e = effective("large", None);
    e.subagent = Some("rename".into());
    e.turn = None;
    e.round = None;
    recorder.effective(e).unwrap();
    send(&mut recorder, "large", r#"{"messages":[]}"#);
    recorder.publish(json!({"event":"round_completed","call_id":"large","outcome":"accepted","payload":{"response":{"content":"x".repeat(9000)}}})).unwrap();
    let mut q = selector();
    q.turn = None;
    q.round = None;
    let list = query_internal(dir.path(), &q).unwrap();
    assert!(list.attempts[0].response_omitted);
    assert!(list.attempts[0].response.is_none());
    q.subagent = Some("rename".into());
    q.call_id = Some("large".into());
    let detail = query(dir.path(), &q).unwrap();
    assert!(!detail.attempts[0].response_omitted);
    assert_eq!(
        detail.attempts[0].response.as_ref().unwrap()["content"]
            .as_str()
            .unwrap()
            .len(),
        9000
    );
}

#[test]
fn actual_transport_timing_is_projected_for_main_helpers_and_pending_calls() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(dir.path(), RecorderOptions::default()).unwrap();
    let main = recorder.effective(effective("timed-main", None)).unwrap();
    recorder.publish(json!({"event":"request_prepared","call_id":"timed-main","sent_request":"{}","started_at_unix_ms":1000})).unwrap();
    recorder.publish(json!({"event":"round_completed","call_id":"timed-main","outcome":"accepted","started_at_unix_ms":1000,"elapsed_ms":125})).unwrap();
    let mut helper = effective("timed-helper", None);
    helper.subagent = Some("rename".into());
    helper.turn = None;
    helper.round = None;
    recorder.effective(helper).unwrap();
    recorder.publish(json!({"event":"request_prepared","call_id":"timed-helper","sent_request":"{}","started_at_unix_ms":1200})).unwrap();
    // A terminal carries the actual transport start and supersedes the prepared value.
    recorder.publish(json!({"body":{"event":"round_completed","call_id":"timed-helper","outcome":"accepted","started_at_unix_ms":1201,"elapsed_ms":49}})).unwrap();
    recorder
        .effective(effective("pending-timing", Some(main.clone())))
        .unwrap();
    recorder.publish(json!({"event":"request_prepared","call_id":"pending-timing","sent_request":"{}","started_at_unix_ms":1300})).unwrap();
    recorder
        .effective(effective(
            "terminal-with-prepared-start",
            Some(main.clone()),
        ))
        .unwrap();
    recorder.publish(json!({"event":"request_prepared","call_id":"terminal-with-prepared-start","sent_request":"{}","started_at_unix_ms":1400})).unwrap();
    recorder.publish(json!({"event":"round_completed","call_id":"terminal-with-prepared-start","outcome":"accepted","elapsed_ms":0})).unwrap();
    recorder
        .effective(effective("untimed", Some(main.clone())))
        .unwrap();
    send(&mut recorder, "untimed", "{}");
    recorder.publish(json!({"event":"round_completed","call_id":"untimed","outcome":"accepted","elapsed_ms":17})).unwrap();
    recorder
        .effective(effective("overflow", Some(main)))
        .unwrap();
    recorder.publish(json!({"event":"request_prepared","call_id":"overflow","sent_request":"{}","started_at_unix_ms":u64::MAX})).unwrap();
    recorder.publish(json!({"event":"round_completed","call_id":"overflow","outcome":"accepted","started_at_unix_ms":u64::MAX,"elapsed_ms":1})).unwrap();

    let result = query(dir.path(), &selector()).unwrap();
    assert!(result.timeline.is_empty());
    let times: Vec<_> = result
        .attempts
        .iter()
        .map(|a| (a.call_id.as_str(), a.started_at_ms, a.ended_at_ms))
        .collect();
    assert_eq!(
        times,
        vec![
            ("timed-main", Some(1000), Some(1125)),
            ("pending-timing", Some(1300), None),
            ("terminal-with-prepared-start", Some(1400), Some(1400)),
            ("untimed", None, None),
            ("overflow", Some(u64::MAX), None),
        ]
    );
    let listing = query_internal(dir.path(), &selector()).unwrap();
    assert_eq!(listing.attempts.len(), 1);
    assert_eq!(listing.attempts[0].started_at_ms, Some(1201));
    assert_eq!(listing.attempts[0].ended_at_ms, Some(1250));
    assert_eq!(listing.timeline.len(), 5);
    assert_eq!(listing.timeline[0].call_id, "timed-main");
    assert_eq!(listing.timeline[0].started_at_ms, Some(1000));
    assert_eq!(listing.timeline[0].ended_at_ms, Some(1125));
    assert_eq!(listing.timeline[0].completion, "succeeded");
    assert_eq!(listing.timeline[1].completion, "pending");
    assert!(listing.timeline.iter().all(|row| row.epoch == 2));
    let mut exact = selector();
    exact.turn = None;
    exact.round = None;
    exact.subagent = Some("rename".into());
    exact.call_id = Some("timed-helper".into());
    let detail = query(dir.path(), &exact).unwrap();
    assert_eq!(
        detail.attempts[0].started_at_ms,
        listing.attempts[0].started_at_ms
    );
    assert_eq!(
        detail.attempts[0].ended_at_ms,
        listing.attempts[0].ended_at_ms
    );
}

#[test]
fn internal_main_timeline_is_epoch_scoped_and_explicitly_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(dir.path(), RecorderOptions::default()).unwrap();
    let mut other = effective("other-epoch", None);
    other.epoch = 1;
    recorder.effective(other).unwrap();
    for index in 0..256 {
        recorder
            .effective(effective(&format!("bounded-{index}"), None))
            .unwrap();
    }
    let listing = query_internal(dir.path(), &selector()).unwrap();
    assert_eq!(listing.timeline.len(), 256);
    assert!(listing.timeline.iter().all(|row| row.epoch == 2));
    assert!(
        listing
            .timeline
            .iter()
            .all(|row| row.started_at_ms.is_none() && row.ended_at_ms.is_none())
    );
    recorder.effective(effective("one-too-many", None)).unwrap();
    assert!(
        query_internal(dir.path(), &selector())
            .unwrap_err()
            .to_string()
            .contains("timeline budget exceeded")
    );
}

#[test]
fn tool_stages_pair_actual_clocks_without_provider_aliases_or_scope_borrowing() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(dir.path(), RecorderOptions::default()).unwrap();
    let branch = "a".repeat(64);
    let other_branch = "b".repeat(64);
    let mut main = effective("same-id", None);
    main.history_head = Some(branch.clone());
    recorder.effective(main).unwrap();
    send(&mut recorder, "same-id", r#"{"messages":[]}"#);
    let publish = |recorder: &mut Recorder,
                   event: &str,
                   tool: &str,
                   head: &str,
                   start: Option<u64>,
                   elapsed: Option<u64>,
                   failure: bool| {
        recorder.publish(json!({"event":event,"epoch":2,"turn_id":3,"round":1,
            "tool_call_id":tool,"history_head":head,"started_at_unix_ms":start,"elapsed_ms":elapsed,
            "payload":if event=="tool_call" {json!({"type":"tool_call","name":"pwsh","arguments":{}})} else {json!({"type":"tool_result","result":"receipt","error":if failure {json!("denied")}else{Value::Null}})}})).unwrap();
    };
    publish(
        &mut recorder,
        "tool_call",
        "same-id",
        &branch,
        Some(100),
        None,
        false,
    );
    publish(
        &mut recorder,
        "tool_call",
        "parallel",
        &branch,
        Some(110),
        None,
        false,
    );
    publish(
        &mut recorder,
        "tool_result",
        "parallel",
        &branch,
        Some(110),
        Some(5),
        true,
    );
    publish(
        &mut recorder,
        "tool_result",
        "same-id",
        &branch,
        Some(100),
        Some(30),
        false,
    );
    publish(
        &mut recorder,
        "tool_call",
        "same-id",
        &other_branch,
        Some(200),
        None,
        false,
    );
    publish(
        &mut recorder,
        "tool_result",
        "same-id",
        &other_branch,
        Some(200),
        Some(7),
        false,
    );
    recorder.publish(json!({"event":"tool_call","epoch":99,"tool_call_id":"wrong-epoch","started_at_unix_ms":1})).unwrap();
    recorder.publish(json!({"event":"tool_call","epoch":2,"subagent":"rename","tool_call_id":"wrong-scope","started_at_unix_ms":1})).unwrap();
    recorder.publish(json!({"event":"tool_call","epoch":2,"turn_id":3,"round":1,"payload":{"type":"tool_call","name":"legacy"}})).unwrap();
    recorder.publish(json!({"event":"round_completed","call_id":"same-id","outcome":"accepted","started_at_unix_ms":80,"elapsed_ms":15,"payload":{"response":{"content":"ok","tool_calls":[]}}})).unwrap();
    let result = query_internal(dir.path(), &selector()).unwrap();
    assert_eq!(result.stages.len(), 3);
    assert_eq!(
        (result.stages[0].started_at_ms, result.stages[0].ended_at_ms),
        (Some(100), Some(130))
    );
    assert_eq!(
        (result.stages[1].started_at_ms, result.stages[1].ended_at_ms),
        (Some(110), Some(115))
    );
    assert_eq!(result.stages[1].completion, "failed");
    assert_eq!(
        result.stages[2].history_head.as_deref(),
        Some(other_branch.as_str())
    );
    assert_eq!(
        result.timeline[0].history_head.as_deref(),
        Some(branch.as_str())
    );
    let normal = query(dir.path(), &selector()).unwrap();
    assert!(normal.stages.is_empty());
    assert_eq!(normal.attempts[0].completion, "succeeded");
    let mut exact = selector();
    exact.history_head = Some(branch);
    assert_eq!(query_internal(dir.path(), &exact).unwrap().stages.len(), 2);
}

#[test]
fn stage_unknown_clocks_overflow_and_explicit_budget_are_retained() {
    let dir = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(dir.path(), RecorderOptions::default()).unwrap();
    for (id, start, elapsed) in [
        ("pending", Some(1), None),
        ("unknown", None, Some(99)),
        ("overflow", Some(u64::MAX), Some(1)),
    ] {
        recorder.publish(json!({"event":if elapsed.is_none(){"tool_call"}else{"tool_result"},"epoch":2,"turn_id":3,"round":1,"tool_call_id":id,"started_at_unix_ms":start,"elapsed_ms":elapsed,"payload":{"type":"tool_result","result":"ok","error":null}})).unwrap();
    }
    let result = query_internal(dir.path(), &selector()).unwrap();
    assert_eq!(result.stages.len(), 3);
    assert_eq!(result.stages[0].completion, "pending");
    assert!(
        result
            .stages
            .iter()
            .all(|stage| stage.ended_at_ms.is_none())
    );
    assert!(result.stages[1].started_at_ms.is_none());
    for index in 3..256 {
        recorder
            .publish(
                json!({"event":"tool_call","epoch":2,"tool_call_id":format!("bounded-{index}")}),
            )
            .unwrap();
    }
    assert_eq!(
        query_internal(dir.path(), &selector())
            .unwrap()
            .stages
            .len(),
        256
    );
    recorder
        .publish(json!({"event":"tool_call","epoch":2,"tool_call_id":"too-many"}))
        .unwrap();
    assert!(
        query_internal(dir.path(), &selector())
            .unwrap_err()
            .to_string()
            .contains("stage budget exceeded")
    );
}
