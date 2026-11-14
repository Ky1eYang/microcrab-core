//! Regression coverage for the same canonical writer/projections on both stores.
use super::*;
use microcrab_storage::audit::{AuditScope, AuditSource};
use serde_json::json;
use std::{
    io::Read,
    sync::{Arc, Mutex},
};

fn request(call: &str, parent: Option<String>) -> EffectiveRequest {
    EffectiveRequest {
        call_id: call.into(),
        epoch: 1,
        turn: Some(1),
        round: Some(1),
        parent_request: parent,
        history_head: Some("a".repeat(64)),
        subagent: None,
        request_body: "{\"messages\":[{\"role\":\"user\",\"content\":\"你好\"}]}".into(),
    }
}
fn bytes(source: &AuditSource) -> Vec<u8> {
    let mut bytes = vec![];
    source
        .reader()
        .unwrap()
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    bytes
}
fn selector() -> AuditQuery {
    AuditQuery {
        epoch: 1,
        turn: Some(1),
        round: None,
        request_hash: None,
        history_head: None,
        subagent: None,
        call_id: None,
        tool_call_id: None,
    }
}
#[test]
fn sqlite_reopen_exclusivity_scope_and_observers_follow_commits() {
    let temp = tempfile::tempdir().unwrap();
    let source = AuditSource::Sqlite {
        database: temp.path().join("storage.sqlite"),
        scope: AuditScope::new("owner", "root", "root"),
    };
    let observed = Arc::new(Mutex::new(Vec::new()));
    let mut recorder = Recorder::open_source(source.clone(), RecorderOptions::default()).unwrap();
    assert!(Recorder::open_source(source.clone(), RecorderOptions::default()).is_err());
    let inspect = source.clone();
    let callbacks = observed.clone();
    recorder.observer = Some(Arc::new(move |commit| {
        let rows = bytes(&inspect);
        let row: serde_json::Value = serde_json::from_slice(
            rows.split(|byte| *byte == b'\n')
                .rfind(|line| !line.is_empty())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(row["sequence"], commit.sequence);
        callbacks.lock().unwrap().push(commit.sequence);
    }));
    let head = recorder.effective(request("first", None)).unwrap();
    recorder.publish(json!({"event":"request_prepared","epoch":1,"turn_id":1,"round":1,"call_id":"first","sent_request":"wire"})).unwrap();
    recorder.publish(json!({"event":"round_completed","epoch":1,"turn_id":1,"round":1,"call_id":"first","outcome":"accepted","usage":{"samples":1,"input_tokens":0,"output_tokens":3}})).unwrap();
    recorder.flush().unwrap();
    let historical = bytes(&source);
    let result = query_selected(
        source.clone(),
        &selector(),
        Some(&head),
        Some(&"a".repeat(64)),
    )
    .unwrap();
    assert_eq!(result.attempts.len(), 1);
    assert_eq!(
        result.attempts[0].usage.as_ref().unwrap()["input_tokens"],
        0
    );
    assert_eq!(*observed.lock().unwrap(), vec![2, 3, 4]);
    drop(recorder);
    let mut reopened = Recorder::open_source(source.clone(), RecorderOptions::default()).unwrap();
    assert_eq!(bytes(&source), historical);
    reopened.effective(request("second", Some(head))).unwrap();
    assert!(bytes(&source).starts_with(&historical));
    assert_eq!(
        temp.path()
            .read_dir()
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains("audit"))
            .count(),
        0
    );
    let unauthorized = AuditSource::Sqlite {
        database: temp.path().join("storage.sqlite"),
        scope: AuditScope::new("other", "root", "root"),
    };
    assert!(unauthorized.reader().unwrap().is_none());
    assert!(Recorder::open_source(unauthorized, RecorderOptions::default()).is_err());
}

#[cfg(feature = "storage-file")]
#[test]
fn canonical_bytes_retention_and_selected_queries_match_file() {
    for retention in [
        Retention::Both,
        Retention::Effective,
        Retention::Sent,
        Retention::None,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let file = AuditSource::File(temp.path().join("file"));
        let sqlite = AuditSource::Sqlite {
            database: temp.path().join("storage.sqlite"),
            scope: AuditScope::new("owner", "root", "root"),
        };
        let mut heads = Vec::new();
        for source in [&file, &sqlite] {
            let mut recorder = Recorder::open_source(
                source.clone(),
                RecorderOptions {
                    retention,
                    ..Default::default()
                },
            )
            .unwrap();
            let selected = recorder.effective(request("selected", None)).unwrap();
            recorder.publish(json!({"event":"request_prepared","epoch":1,"turn_id":1,"round":1,"call_id":"selected","sent_request":"exact sent bytes"})).unwrap();
            recorder.publish(json!({"event":"round_completed","epoch":1,"turn_id":1,"round":1,"call_id":"selected","outcome":"accepted","usage":{"samples":1,"input_tokens":9,"output_tokens":4}})).unwrap();
            recorder.effective(request("unselected", None)).unwrap();
            recorder.flush().unwrap();
            heads.push(selected);
        }
        assert_eq!(heads[0], heads[1]);
        assert_eq!(bytes(&file), bytes(&sqlite));
        let left = query_selected_with_ancestry(
            file,
            &selector(),
            Some(&heads[0]),
            Some(&"a".repeat(64)),
            &["a".repeat(64)],
        )
        .unwrap();
        let right = query_selected_with_ancestry(
            sqlite.clone(),
            &selector(),
            Some(&heads[1]),
            Some(&"a".repeat(64)),
            &["a".repeat(64)],
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(left).unwrap(),
            serde_json::to_value(right).unwrap()
        );
        assert!(
            Recorder::open_source(
                sqlite,
                RecorderOptions {
                    retention: if retention == Retention::None {
                        Retention::Both
                    } else {
                        Retention::None
                    },
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn sqlite_native_writer_close_releases_stream_and_flush_acknowledges() {
    use microcrab_auditor::{AuditBody, Auditor, Moment};
    let temp = tempfile::tempdir().unwrap();
    let source = AuditSource::Sqlite {
        database: temp.path().join("storage.sqlite"),
        scope: AuditScope::new("owner", "root", "root"),
    };
    let anchor = Arc::new(Mutex::new(microcrab_storage::AuditAnchor::default()));
    let auditor = NativeAuditor::open_source_with_observer(
        source.clone(),
        anchor.clone(),
        RecorderOptions::default(),
        None,
    )
    .await
    .unwrap();
    auditor.publish(Moment::SessionRestored, AuditBody::default());
    auditor.flush().await.unwrap();
    auditor.close().await.unwrap();
    auditor.close().await.unwrap();
    let historical = bytes(&source);
    let reopened = NativeAuditor::open_source_with_observer(
        source.clone(),
        anchor,
        RecorderOptions::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(bytes(&source), historical);
    reopened.close().await.unwrap();
    assert!(!temp.path().join("audit.jsonl").exists());
    assert!(!temp.path().join(".audit.lock").exists());
}
