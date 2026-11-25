use super::*;

#[test]
fn selected_detail_roundtrips_exact_sse_and_keeps_partial_and_binary_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let history_head = "a".repeat(64);
    let sent = "{ \"messages\": [] }";
    let complete =
        ": a comment  \r\n\r\ndata:  {\"delta\":\"界\"} \r\ndata: {}\r\n\r\ndata: [DONE]\r\n\r\n";
    let partial = "data: {\"delta\":\"界\"}\r\n\r\ndata: {\"unclosed\":";
    let content_type = "text/event-stream; charset=utf-8";
    let mut hashes = Vec::new();
    for (round, (call, terminal)) in [
        (
            "complete",
            json!({"outcome":"accepted", "response":complete, "response_complete":true,
                "response_status":200, "response_content_type":content_type,
                "payload":{"response":{"content":"normalized output","tool_calls":[]}}}),
        ),
        (
            "partial",
            json!({"outcome":"cancelled", "response":partial, "response_complete":false,
                "response_status":200, "response_content_type":content_type}),
        ),
        (
            "binary",
            json!({"outcome":"invalid_response", "response_bytes":[0, 255, 1], "response_complete":true,
                "response_status":200, "response_content_type":"application/octet-stream"}),
        ),
        (
            "legacy",
            json!({"outcome":"accepted", "response":{"content":"old normalized output"}}),
        ),
        ("pending", Value::Null),
    ]
    .into_iter()
    .enumerate()
    {
        let identity = recorder
            .effective(EffectiveRequest {
                round: Some(round as u64 + 1),
                history_head: Some(history_head.clone()),
                ..effective(call, hashes.last().cloned())
            })
            .unwrap();
        hashes.push(identity);
        send(&mut recorder, call, sent);
        if !terminal.is_null() {
            let mut terminal = terminal;
            terminal["event"] = json!("round_completed");
            terminal["call_id"] = json!(call);
            recorder.publish(terminal).unwrap();
        }
    }
    let helper = recorder
        .effective(EffectiveRequest {
            history_head: Some(history_head.clone()),
            subagent: Some("review".into()),
            ..effective("helper", None)
        })
        .unwrap();
    send(&mut recorder, "helper", sent);
    recorder.publish(json!({"event":"round_completed", "call_id":"helper", "outcome":"cancelled", "response":partial,
        "response_complete":false, "response_status":200, "response_content_type":content_type})).unwrap();
    let off_branch = recorder.effective(effective("off-branch", None)).unwrap();
    send(&mut recorder, "off-branch", sent);
    recorder
        .publish(json!({"event":"round_completed", "call_id":"off-branch", "outcome":"accepted", "response":"off-branch raw"}))
        .unwrap();
    recorder.flush().unwrap();
    drop(recorder);

    let selection = AuditQuery {
        round: None,
        ..selector()
    };
    let result = query_selected(
        directory.path(),
        &selection,
        hashes.last().map(String::as_str),
        Some(&history_head),
    )
    .unwrap();
    // Exercise the wire DTO through JSON serialization and parsing, not just its Rust fields.
    let wire: Value = serde_json::from_slice(&serde_json::to_vec(&result).unwrap()).unwrap();
    let attempts = wire["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 5);
    for (attempt, (raw, complete)) in attempts
        .iter()
        .zip([(Some(complete), true), (Some(partial), false)])
    {
        assert_eq!(attempt["sent_request"], sent);
        assert_eq!(
            attempt["response_raw"].as_str().map(str::as_bytes),
            raw.map(str::as_bytes)
        );
        assert_eq!(attempt["response_complete"], complete);
        assert_eq!(attempt["response_status"], 200);
        assert_eq!(attempt["response_content_type"], content_type);
    }
    assert_eq!(attempts[0]["response"]["content"], "normalized output");
    assert!(attempts[1]["response"].is_null());
    assert_eq!(attempts[1]["completion"], "cancelled");
    assert!(attempts[2]["response_raw"].is_null());
    assert!(attempts[2].get("response_bytes").is_none());
    assert_eq!(attempts[2]["response_complete"], true);
    assert_eq!(attempts[3]["response"]["content"], "old normalized output");
    for attempt in &attempts[3..] {
        assert_eq!(attempt.get("response_raw"), Some(&Value::Null));
        assert_eq!(attempt.get("response_complete"), Some(&Value::Null));
        assert_eq!(attempt.get("response_status"), Some(&Value::Null));
        assert_eq!(attempt.get("response_content_type"), Some(&Value::Null));
    }
    let off_branch_query = AuditQuery {
        request_hash: Some(off_branch),
        ..selection
    };
    assert!(
        query_selected(
            directory.path(),
            &off_branch_query,
            hashes.last().map(String::as_str),
            Some(&history_head)
        )
        .unwrap()
        .attempts
        .is_empty()
    );

    let helper_query = AuditQuery {
        subagent: Some("review".into()),
        request_hash: Some(helper),
        ..selector()
    };
    let summary = query_internal_selected_with_ancestry(
        directory.path(),
        &helper_query,
        hashes.last().map(String::as_str),
        Some(&history_head),
        std::slice::from_ref(&history_head),
    )
    .unwrap();
    assert_eq!(summary.attempts.len(), 1);
    let helper = &summary.attempts[0];
    assert_eq!(helper.completion, "cancelled");
    assert!(helper.response.is_none());
    assert!(helper.response_raw.is_none());
    assert!(helper.response_complete.is_none());
    assert!(helper.response_status.is_none());
    assert!(helper.response_content_type.is_none());

    let exact = AuditQuery {
        request_hash: Some(hashes[0].clone()),
        ..selector()
    };
    let metadata = query_attempt_metadata(directory.path(), &exact).unwrap();
    let selected_usage =
        query_selected_turn_metadata(directory.path(), hashes.last().unwrap(), &[(2, 3)]).unwrap();
    for attempt in metadata.attempts.iter().chain(&selected_usage.attempts) {
        assert!(attempt.response.is_none());
        assert!(attempt.response_raw.is_none());
        assert!(attempt.response_complete.is_none());
        assert!(attempt.response_status.is_none());
        assert!(attempt.response_content_type.is_none());
    }
}

#[test]
fn raw_sse_detail_counts_json_escapes_toward_aggregate_response_budget() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    // Less than 32 MiB unescaped in total, but more on the wire; each
    // terminal remains below the recorder's 16 MiB serialized record limit.
    let raw = "\r\n".repeat(3 * 1024 * 1024);
    let mut head = None;
    for call in ["first", "second", "third"] {
        head = Some(recorder.effective(effective(call, head)).unwrap());
        send(&mut recorder, call, "{}");
        recorder
            .publish(
                json!({"event":"round_completed", "call_id":call, "outcome":"accepted",
            "response":raw, "response_complete":true, "payload":{"response":{"content":"ok"}}}),
            )
            .unwrap();
    }
    let head = head.unwrap();
    let error = query_selected(directory.path(), &selector(), Some(&head), None).unwrap_err();
    assert!(error.to_string().contains("audit response budget exceeded"));
    let exact = AuditQuery {
        request_hash: Some(head.clone()),
        ..selector()
    };
    let detail = query_selected(directory.path(), &exact, Some(&head), None).unwrap();
    assert_eq!(
        detail.attempts[0].response_raw.as_deref(),
        Some(raw.as_str())
    );
    let metadata = query_selected_turn_metadata(directory.path(), &head, &[(2, 3)]).unwrap();
    assert_eq!(metadata.attempts.len(), 3);
    assert!(
        metadata
            .attempts
            .iter()
            .all(|attempt| attempt.response_raw.is_none())
    );
}
