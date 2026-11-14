use microcrab_auditor::{AuditBody, Auditor, ChannelAuditor};
use microcrab_protocol::Moment;
use microcrab_session::runtime::Runtime;
use microcrab_session::runtime::tool::DenyAll;
use microcrab_session::{Client, Session};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;

struct Answer;
#[async_trait::async_trait]
impl Client for Answer {
    async fn complete(
        &self,
        _: &microcrab_session::context::Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        Ok(microcrab_protocol::Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: microcrab_protocol::Usage::default(),
        })
    }
}

fn diff_between(previous: &str, current: &str) -> String {
    let (sender, mut receiver) = unbounded_channel();
    let auditor = ChannelAuditor::from_sender(sender);
    for request in [previous, current] {
        auditor.publish(
            Moment::RequestPrepared,
            AuditBody {
                sent_request: Some(request.into()),
                ..AuditBody::default()
            },
        );
    }
    receiver.try_recv().unwrap();
    receiver.try_recv().unwrap().request_diff.unwrap()
}

#[test]
fn request_diff_is_a_unified_hunk_with_unchanged_context() {
    assert_eq!(
        diff_between(
            "head\nsame\nold\nsame\nfoot\n",
            "head\nsame\nnew\nsame\nfoot\n"
        ),
        "--- baseline\n+++ request\n@@ -1,5 +1,5 @@\n head\n same\n-old\n+new\n same\n foot\n"
    );
    assert_eq!(diff_between("identical", "identical"), "");
    assert_eq!(
        diff_between("", "one\n"),
        "--- baseline\n+++ request\n@@ -0,0 +1,1 @@\n+one\n"
    );
    assert_eq!(
        diff_between("one\n", ""),
        "--- baseline\n+++ request\n@@ -1,1 +0,0 @@\n-one\n"
    );
}

#[test]
fn request_diff_preserves_missing_final_newlines() {
    assert_eq!(
        diff_between("same", "same\n"),
        "--- baseline\n+++ request\n@@ -1,1 +1,1 @@\n-same\n\\ No newline at end of file\n+same\n"
    );
}

#[test]
fn request_diff_never_aligns_changed_fields_with_siblings() {
    let diff = diff_between(
        r#"{"messages":["shared","old"],"tools":["shared","old"],"nested":{"first":"old","second":"old"}}"#,
        r#"{"messages":["shared","new"],"tools":["shared","old"],"nested":{"first":"new","second":"old"}}"#,
    );
    assert!(diff.contains("--- baseline/messages\n+++ request/messages\n@@"));
    assert!(diff.contains("--- baseline/nested/first\n+++ request/nested/first\n@@"));
    assert!(!diff.contains("tools"));
    assert!(!diff.contains("second"));
    assert_eq!(diff.matches("@@ -").count(), 2);
}

#[test]
fn request_diff_keeps_literal_body_headers_and_escapes_field_paths() {
    let diff = diff_between(
        r#"{"a/b":{"~key":"--- baseline"},"a":{"b":"unchanged"}}"#,
        r#"{"a/b":{"~key":"+++ request"},"a":{"b":"unchanged"}}"#,
    );
    assert!(diff.starts_with("--- baseline/a~1b/~0key\n+++ request/a~1b/~0key\n@@"));
    assert!(diff.contains("-\"--- baseline\"\n+\"+++ request\"\n"));
    assert!(!diff.contains("unchanged"));
}

#[test]
fn request_baseline_does_not_cross_producers_or_epochs() {
    let (sender, mut receiver) = unbounded_channel();
    let auditor = ChannelAuditor::from_sender(sender);
    for (epoch, request) in [(0, r#"{"user":"before"}"#), (1, r#"{"user":"epoch"}"#)] {
        auditor.publish(
            Moment::RequestPrepared,
            AuditBody {
                epoch,
                sent_request: Some(request.into()),
                ..AuditBody::default()
            },
        );
    }
    for expected in ["before", "epoch"] {
        let record = receiver.try_recv().unwrap();
        let diff = record.request_diff.unwrap();
        assert!(diff.contains("@@ -0,0 +1,1 @@"));
        assert!(diff.contains(&format!("+\"{expected}\"")));
        assert_eq!(
            record.sent_request,
            Some(format!(r#"{{"user":"{expected}"}}"#))
        );
    }

    // Each Session's creator supplies its own producer and receiver.
    let (sender, mut independent_receiver) = unbounded_channel();
    let independent = ChannelAuditor::from_sender(sender);
    independent.publish(
        Moment::RequestPrepared,
        AuditBody {
            epoch: 1,
            sent_request: Some(r#"{"user":"session"}"#.into()),
            ..AuditBody::default()
        },
    );
    let record = independent_receiver.try_recv().unwrap();
    let diff = record.request_diff.unwrap();
    assert!(diff.contains("@@ -0,0 +1,1 @@"));
    assert!(diff.contains("+\"session\""));
    assert_eq!(
        record.sent_request.as_deref(),
        Some(r#"{"user":"session"}"#)
    );
    assert!(receiver.try_recv().is_err());
}

#[test]
fn concurrent_publish_order_matches_sequence_and_request_baseline() {
    let (sender, mut receiver) = unbounded_channel();
    let auditor = Arc::new(ChannelAuditor::from_sender(sender));
    let handles = (0..16)
        .map(|index| {
            let auditor = auditor.clone();
            std::thread::spawn(move || {
                auditor.publish(
                    Moment::RequestPrepared,
                    AuditBody {
                        sent_request: Some(json!({"user":index}).to_string()),
                        ..AuditBody::default()
                    },
                );
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
    let mut previous = None;
    for sequence in 1..=16 {
        let record = receiver.try_recv().unwrap();
        assert_eq!(record.sequence, sequence);
        let request: serde_json::Value =
            serde_json::from_str(record.sent_request.as_ref().unwrap()).unwrap();
        let diff = record.request_diff.unwrap();
        if let Some(previous) = previous {
            assert!(diff.contains(&format!("-{previous}\n")));
        }
        let current = request["user"].as_u64().unwrap();
        assert!(diff.contains(&format!("+{current}\n")));
        previous = Some(current);
    }
}

#[tokio::test]
async fn caller_receives_lifecycle_points_and_absent_consumer_still_runs() {
    let (sender, mut receiver) = unbounded_channel();
    let runtime = Runtime::new(vec![], Arc::new(DenyAll)).unwrap();
    let session = Session::builder_arc(Arc::new(Answer), runtime)
        .audit_sender(sender)
        .build()
        .unwrap();
    session.submit("inspect").await.unwrap();
    session.compact("summary", json!({})).await.unwrap();
    session.close().await.unwrap();
    let mut events = Vec::new();
    while let Ok(record) = receiver.try_recv() {
        events.push(record.event);
    }
    assert_eq!(
        events,
        [
            Moment::SessionCreated,
            Moment::InputAdmitted,
            Moment::RoundCompleted,
            Moment::SessionTerminated,
        ]
    );
    let runtime = Runtime::new(vec![], Arc::new(DenyAll)).unwrap();
    let quiet = Session::builder_arc(Arc::new(Answer), runtime)
        .build()
        .unwrap();
    assert_eq!(quiet.submit("inspect").await.unwrap().content, "done");
}
