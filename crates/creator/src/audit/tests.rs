use super::recorder::{FORMAT, canonical, digest_prefix, hash, hash_record};
use super::*;
use microcrab_auditor::{AuditBody, AuditOutcome, Auditor, EmptyVariableView, HookContext, Moment};
use microcrab_storage::AuditAnchor;
use serde_json::{Value, json};
use std::{
    fs,
    sync::{Arc, Mutex},
};

const WIRE: &str =
    "{\"temperature\":1e-05,\"messages\":[{\"role\":\"user\",\"content\":\"你好\"}]}";
/// Independently derived with Node SHA-256 over recursively sorted compact UTF-8
/// JSON, after reproducing all three original-domain Python golden hashes. WIRE
/// stays an opaque string so its numeric lexemes remain exact. The chain identity
/// of every retained record is part of the cross-implementation contract.
const FORMAT_RECORD: &str = "5d2eeaf78d97f0eccbc1441a5d701ad2828939c0f4c820e961aaa7a2c732dd73";
const EFFECTIVE_RECORD: &str = "843b65c1536fb08de1932b25e9f4cfa4197834dcbe0a8d9cfd7bac79fcf6b48c";
const PUBLISHED_RECORD: &str = "2fc59fc7fe9a3af8b6792b3cab71748233bd1bc109abad10c74179ace6a8cfe6";
const READABLE_WIRE: &str = "{\n  \"temperature\": 1e-05,\n  \"messages\": [\n    {\n      \"role\": \"user\",\n      \"content\": \"你好\"\n    }\n  ]\n}\n";

fn request(call: &str, parent: Option<String>, epoch: u64) -> EffectiveRequest {
    EffectiveRequest {
        call_id: call.into(),
        epoch,
        turn: Some(3),
        round: Some(1),
        parent_request: parent,
        history_head: None,
        subagent: None,
        request_body: WIRE.into(),
    }
}
fn sent(call: &str, wire: &str) -> Value {
    json!({"call_id":call,"sent_request":wire,"event":"request_prepared"})
}

#[test]
fn python_v3_golden_chain_and_readable_wire_parity() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let rows = recorder.rows().unwrap();
    assert_eq!(rows[0]["sha256"], FORMAT_RECORD);
    assert_eq!(rows[0]["data"]["format"], FORMAT);
    assert_eq!(rows[0]["data"]["retention"], "both");
    let id = recorder.effective(request("golden-call", None, 2)).unwrap();
    assert_eq!(id, EFFECTIVE_RECORD);
    recorder.publish(sent("golden-call", WIRE)).unwrap();
    assert_eq!(recorder.rows().unwrap()[2]["sha256"], PUBLISHED_RECORD);
    recorder.flush().unwrap();
    drop(recorder);
    // The readable document is projected from the authority, never derived to disk.
    let result = query(
        directory.path(),
        &AuditQuery {
            epoch: 2,
            turn: Some(3),
            round: Some(1),
            request_hash: None,
            history_head: None,
            subagent: None,
            call_id: None,
            tool_call_id: None,
        },
    )
    .unwrap();
    assert_eq!(result.attempts.len(), 1);
    assert_eq!(
        result.attempts[0].request_document.as_deref(),
        Some(READABLE_WIRE)
    );
    assert_eq!(
        result.attempts[0].sent_request.as_deref(),
        Some(WIRE),
        "sent evidence stays the exact submitted bytes"
    );
    let reopened = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    assert_eq!(
        reopened.request_head("golden-call"),
        Some(EFFECTIVE_RECORD.to_owned())
    );
}

#[test]
fn canonical_floats_and_unicode_follow_python_v3() {
    for (number, expected) in [
        (1e-5, "1e-05"),
        (1e-4, "0.0001"),
        (1e16, "1e+16"),
        (1e20, "1e+20"),
        (1e-7, "1e-07"),
        (-0.0, "-0.0"),
        (1.0, "1.0"),
    ] {
        assert_eq!(
            String::from_utf8(canonical(&json!(number)).unwrap()).unwrap(),
            expected
        );
    }
    assert_eq!(
        String::from_utf8(canonical(&json!({"z":1,"a":["你好","\n"]})).unwrap()).unwrap(),
        "{\"a\":[\"你好\",\"\\n\"],\"z\":1}"
    );
}

#[test]
fn retention_never_fabricates_sent_and_reopen_requires_same_policy() {
    for retention in [
        Retention::Both,
        Retention::Effective,
        Retention::Sent,
        Retention::None,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let options = RecorderOptions {
            retention,
            ..Default::default()
        };
        let mut recorder = Recorder::open(directory.path(), options.clone()).unwrap();
        let id = recorder.effective(request("one", None, 0)).unwrap();
        assert_eq!(recorder.request_head("one"), Some(id));
        recorder.publish(sent("one", WIRE)).unwrap();
        recorder
            .publish(json!({"call_id":"one","outcome":"accepted"}))
            .unwrap();
        assert!(recorder.request_head("one").is_none());
        recorder.flush().unwrap();
        let rows = recorder.rows().unwrap();
        assert_eq!(
            rows[1]["data"].get("request_body").is_some(),
            matches!(retention, Retention::Effective | Retention::Both)
        );
        assert_eq!(
            rows[2]["data"]["record"].get("sent_request").is_some(),
            matches!(retention, Retention::Sent | Retention::Both)
        );
        drop(recorder);
        let reopened = Recorder::open(directory.path(), options).unwrap();
        assert!(reopened.request_head("one").is_none());
        drop(reopened);
        let before = fs::read(directory.path().join("audit.jsonl")).unwrap();
        let wrong = if retention == Retention::Both {
            Retention::None
        } else {
            Retention::Both
        };
        assert!(
            Recorder::open(
                directory.path(),
                RecorderOptions {
                    retention: wrong,
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert_eq!(
            before,
            fs::read(directory.path().join("audit.jsonl")).unwrap(),
            "a rejected policy must not rewrite evidence"
        );
    }
}

/// Opening verifies raw stored bytes instead of re-serializing every record, so the
/// digest prefix must stay an exact byte range of the line.
#[test]
fn raw_line_digest_prefix_matches_the_canonical_form() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    recorder.effective(request("one", None, 0)).unwrap();
    recorder.publish(sent("one", WIRE)).unwrap();
    drop(recorder);
    let path = directory.path().join("audit.jsonl");
    let original = fs::read(&path).unwrap();
    let mut lines = 0;
    for line in original
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        lines += 1;
        let (payload, digest) = digest_prefix(line).expect("canonical digest suffix");
        assert_eq!(hash_record(payload), digest);
        let mut row: Value = serde_json::from_slice(line).unwrap();
        let claimed = row
            .as_object_mut()
            .unwrap()
            .remove("sha256")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(claimed, digest);
        assert_eq!(hash(canonical(&row).unwrap()), digest);
    }
    assert_eq!(lines, 3);
    // One flipped byte inside a retained body must still be refused.
    let at = original
        .windows(5)
        .position(|window| window == b"role\\")
        .expect("body byte")
        + 1;
    let mut tampered = original;
    tampered[at] ^= 1;
    fs::write(&path, &tampered).unwrap();
    assert!(Recorder::open(directory.path(), RecorderOptions::default()).is_err());
    assert_eq!(fs::read(&path).unwrap(), tampered);
}

#[test]
fn tampered_and_incomplete_authority_is_refused_without_repair() {
    let directory = tempfile::tempdir().unwrap();
    let recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    drop(recorder);
    let path = directory.path().join("audit.jsonl");
    let original = fs::read(&path).unwrap();
    let mut incomplete = original.clone();
    incomplete.extend(b"{\"sequence\":2");
    fs::write(&path, &incomplete).unwrap();
    assert!(Recorder::open(directory.path(), RecorderOptions::default()).is_err());
    assert_eq!(fs::read(&path).unwrap(), incomplete);
    let tampered = String::from_utf8(original).unwrap().replace("both", "sent");
    fs::write(&path, tampered.as_bytes()).unwrap();
    assert!(Recorder::open(directory.path(), RecorderOptions::default()).is_err());
    assert_eq!(fs::read(&path).unwrap(), tampered.as_bytes());
}

#[test]
fn oversized_append_is_sticky_and_legacy_evidence_is_not_rewritten() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(
        directory.path(),
        RecorderOptions {
            max_record_bytes: 1024,
            ..Default::default()
        },
    )
    .unwrap();
    let mut oversized = request("large", None, 0);
    oversized.request_body = "x".repeat(2048);
    assert!(recorder.effective(oversized).is_err());
    assert!(recorder.publish(sent("other", "{}")).is_err());
    assert!(recorder.flush().is_err());
    // Superseded layouts fail closed instead of being reinterpreted. A current
    // `audit/` view directory is Python-owned and stays readable here.
    for legacy in [
        "plugin-audit.jsonl",
        "plugin-audit",
        "events.jsonl",
        "request",
    ] {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join(legacy), "old").unwrap();
        assert!(Recorder::open(directory.path(), RecorderOptions::default()).is_err());
        assert!(!directory.path().join("audit.jsonl").exists());
    }
}

#[tokio::test]
async fn native_lane_updates_only_sent_anchor_and_flushes_terminal_identity() {
    let directory = tempfile::tempdir().unwrap();
    let anchor = Arc::new(Mutex::new(AuditAnchor::default()));
    let auditor = NativeAuditor::open(directory.path(), anchor.clone(), RecorderOptions::default())
        .await
        .unwrap();
    auditor
        .intervene(
            Moment::RequestPrepared,
            HookContext {
                epoch: 0,
                turn: Some(1),
                round: Some(1),
                call_id: Some("call".into()),
                request_body: Some(WIRE),
                max_request_bytes: Some(65536),
                variables: &EmptyVariableView,
                payload: Default::default(),
                subagent: None,
            },
        )
        .await
        .unwrap();
    let head = auditor.request_head("call").unwrap();
    assert!(anchor.lock().unwrap().request_head.is_none());
    auditor.publish(
        Moment::RequestPrepared,
        AuditBody {
            epoch: 0,
            turn_id: Some(1),
            round: Some(1),
            call_id: Some("call".into()),
            sent_request: Some(WIRE.into()),
            ..Default::default()
        },
    );
    assert_eq!(anchor.lock().unwrap().request_head, Some(head));
    auditor.publish(
        Moment::RoundCompleted,
        AuditBody {
            epoch: 0,
            call_id: Some("call".into()),
            outcome: Some(AuditOutcome::Accepted),
            ..Default::default()
        },
    );
    auditor.flush().await.unwrap();
    assert!(auditor.request_head("call").is_none());
    auditor.close().await.unwrap();
    let reopened = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    assert!(reopened.request_head("call").is_none());
}

#[tokio::test]
async fn concurrent_close_and_flush_join_one_retained_clean_result() {
    let directory = tempfile::tempdir().unwrap();
    let auditor = NativeAuditor::open(
        directory.path(),
        Arc::new(Mutex::new(AuditAnchor::default())),
        RecorderOptions::default(),
    )
    .await
    .unwrap();
    auditor.publish(Moment::SessionCreated, AuditBody::default());
    let mut tasks = vec![];
    for index in 0..16 {
        let auditor = auditor.clone();
        tasks.push(tokio::spawn(async move {
            if index % 2 == 0 {
                auditor.close().await
            } else {
                auditor.flush().await
            }
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    auditor.close().await.unwrap();
    auditor.flush().await.unwrap();
    auditor.publish(Moment::SessionTerminated, AuditBody::default());
    assert!(auditor.failure().is_none());
    auditor.flush().await.unwrap();
    // All Arc owners still exist; ACK must already have released file ownership.
    let reopened = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    assert_eq!(reopened.rows().unwrap().len(), 2);
}

#[tokio::test]
async fn cancelled_close_waiter_does_not_abandon_shutdown_or_other_joiners() {
    use std::{future::Future, task::Poll};
    let directory = tempfile::tempdir().unwrap();
    let auditor = NativeAuditor::open(
        directory.path(),
        Arc::new(Mutex::new(AuditAnchor::default())),
        RecorderOptions::default(),
    )
    .await
    .unwrap();
    let mut closing = Box::pin(auditor.close());
    // Poll exactly once before the current-thread actor can execute. The command
    // is queued and close awaits its retained result, then that waiter is dropped.
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    let (close, flush) = tokio::join!(auditor.close(), auditor.flush());
    close.unwrap();
    flush.unwrap();
    auditor.close().await.unwrap();
    let _reopened = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
}

#[tokio::test]
async fn failed_close_keeps_the_same_error_for_all_cleanup_paths() {
    let directory = tempfile::tempdir().unwrap();
    let auditor = NativeAuditor::open(
        directory.path(),
        Arc::new(Mutex::new(AuditAnchor::default())),
        RecorderOptions {
            pending_records: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for _ in 0..4 {
        auditor.publish(Moment::SessionCreated, AuditBody::default());
    }
    let failure = auditor.failure().unwrap();
    let (close, flush) = tokio::join!(auditor.close(), auditor.flush());
    assert_eq!(close.unwrap_err(), failure);
    assert_eq!(flush.unwrap_err(), failure);
    assert_eq!(auditor.close().await.unwrap_err(), failure);
    assert_eq!(auditor.flush().await.unwrap_err(), failure);
    // A failed audit is still closed: lock ownership is not leaked by sticky errors.
    let _reopened = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
}

#[tokio::test]
async fn google_raw_stream_and_partial_prefix_survive_flush_close_and_reopen_exactly() {
    let directory = tempfile::tempdir().unwrap();
    let auditor = NativeAuditor::open(
        directory.path(),
        Arc::new(Mutex::new(AuditAnchor::default())),
        RecorderOptions::default(),
    )
    .await
    .unwrap();
    let first = format!(
        "data: {}\r\n\r\n",
        json!({
            "candidates":[{"content":{"role":"model","parts":[{"text":"synthetic retained 界"}]}}],
            "usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":40}
        })
    );
    let tail = format!(
        "data: {}\r\n\r\ndata: {}\r\n\r\n",
        json!({
            "candidates":[{"content":{"role":"model","parts":[{"text":"","thoughtSignature":"opaque-synthetic-tail"}]},"finishReason":"STOP"}]
        }),
        json!({"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":2,"totalTokenCount":102}})
    );
    let complete = format!(": retained raw comment\r\n\r\n{first}{tail}");
    let partial = first;
    for (index, raw) in [&complete, &partial].into_iter().enumerate() {
        let call = format!("synthetic-google-{index}");
        auditor
            .intervene(
                Moment::RequestPrepared,
                HookContext {
                    epoch: 0,
                    turn: Some(1),
                    round: Some(index as u64 + 1),
                    call_id: Some(call.clone()),
                    request_body: Some("{\"contents\":[]}"),
                    max_request_bytes: Some(65536),
                    variables: &EmptyVariableView,
                    payload: Default::default(),
                    subagent: None,
                },
            )
            .await
            .unwrap();
        auditor.publish(
            Moment::RequestPrepared,
            AuditBody {
                call_id: Some(call.clone()),
                sent_request: Some("{\"contents\":[]}".into()),
                ..Default::default()
            },
        );
        auditor.publish(
            Moment::RoundCompleted,
            AuditBody {
                call_id: Some(call),
                response: Some(raw.clone()),
                response_complete: Some(index == 0),
                response_status: Some(200),
                response_content_type: Some("text/event-stream; charset=utf-8".into()),
                outcome: Some(if index == 0 {
                    AuditOutcome::Accepted
                } else {
                    AuditOutcome::Cancelled
                }),
                ..Default::default()
            },
        );
    }
    auditor.flush().await.unwrap();
    auditor.close().await.unwrap();
    // Closing ACK releases the lock; all original auditor handles still exist.
    let reopened = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let rows = reopened.rows().unwrap();
    let terminal = rows
        .iter()
        .filter_map(|row| {
            let record = &row["data"]["record"];
            (!record["outcome"].is_null()).then_some(record)
        })
        .collect::<Vec<_>>();
    assert_eq!(terminal.len(), 2);
    for (index, expected) in [&complete, &partial].into_iter().enumerate() {
        let record = terminal[index];
        assert_eq!(
            record["response"].as_str().unwrap().as_bytes(),
            expected.as_bytes()
        );
        assert!(record["response_bytes"].is_null());
        assert_eq!(record["response_complete"], index == 0);
        assert_eq!(record["response_status"], 200);
        assert_eq!(
            record["response_content_type"],
            "text/event-stream; charset=utf-8"
        );
    }
    assert_eq!(
        terminal[0]["response"]
            .as_str()
            .unwrap()
            .matches("usageMetadata")
            .count(),
        2
    );
    assert_eq!(terminal[0]["outcome"], "accepted");
    assert_eq!(terminal[1]["outcome"], "cancelled");
}

#[test]
fn oversized_raw_response_is_rejected_without_publishing_a_truncated_record() {
    let directory = tempfile::tempdir().unwrap();
    let options = RecorderOptions {
        max_record_bytes: 1024,
        ..Default::default()
    };
    let mut recorder = Recorder::open(directory.path(), options.clone()).unwrap();
    recorder
        .effective(request("synthetic-budget", None, 0))
        .unwrap();
    recorder.publish(sent("synthetic-budget", WIRE)).unwrap();
    let before = fs::read(directory.path().join("audit.jsonl")).unwrap();
    assert!(
        recorder
            .publish(json!({
                "call_id":"synthetic-budget", "outcome":"accepted", "response_complete":true,
                "response":format!("data: {}\n\n", "x".repeat(2048))
            }))
            .is_err()
    );
    assert!(
        recorder.flush().is_err(),
        "budget failure remains observable"
    );
    assert_eq!(
        fs::read(directory.path().join("audit.jsonl")).unwrap(),
        before
    );
    drop(recorder);
    let reopened = Recorder::open(directory.path(), options).unwrap();
    assert!(
        reopened
            .rows()
            .unwrap()
            .iter()
            .all(|row| row["data"]["record"]["response"].is_null())
    );
}
