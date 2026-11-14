use super::*;
use microcrab_storage::display::DisplayScope;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
#[path = "graph_tests.rs"]
mod graph_tests;

fn observation(phase: ObservationState) -> ModelObservation {
    ModelObservation {
        call_id: "exact-attempt".into(),
        epoch: 2,
        turn: Some(3),
        round: Some(4),
        subagent: None,
        state: phase,
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn full_unicode_body_fixed_metadata_and_terminal_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let source = DisplaySource::File {
        directory: directory.path().join("display"),
        scope: DisplayScope::new("owner", "root", "session"),
    };
    let snapshots = Arc::new(AtomicUsize::new(0));
    let counter = snapshots.clone();
    let commits = Arc::new(Mutex::new(Vec::new()));
    let published = commits.clone();
    let recorder = DisplayRecorder::open(source.clone(), Arc::new(move |_| {
        json!({"history_head": format!("head-{}", counter.fetch_add(1, Ordering::SeqCst))})
    }), Arc::new(move |record| published.lock().unwrap().push(record.sequence)), Arc::new(|e| panic!("{e}"))).await.unwrap();
    recorder
        .record(observation(ObservationState::Started))
        .await
        .unwrap();
    let body = format!("{}END 思考尾部", "完整中文 reasoning\n".repeat(40_000));
    recorder
        .record(observation(ObservationState::ReasoningDelta {
            text: body.clone(),
        }))
        .await
        .unwrap();
    recorder
        .record(observation(ObservationState::Validated {
            text: "answer".into(),
        }))
        .await
        .unwrap();
    recorder.close().await.unwrap();
    let page = tokio::task::spawn_blocking(move || source.query(0, None, 10))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(page.high_water, 3);
    assert!(
        page.records
            .iter()
            .all(|record| record.event["display_graph_version"] == 1)
    );
    assert_eq!(page.records[1].event["text"], body);
    assert_eq!(page.records[1].event["stream_offset"], 0);
    assert_eq!(page.records[2].event["reasoning_bytes"], body.len() as u64);
    assert_eq!(page.records[2].event["archive_complete"], true);
    assert_eq!(page.records[2].event["phase"], "validated");
    assert!(
        page.records
            .iter()
            .all(|r| r.event["history_head"] == "head-0")
    );
    assert_eq!(snapshots.load(Ordering::SeqCst), 1);
    assert_eq!(*commits.lock().unwrap(), vec![1, 2, 3]);
}

#[cfg(feature = "storage-file")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_body_and_drop_terminal_outlive_cancelled_ack_waiter() {
    let directory = tempfile::tempdir().unwrap();
    let source = DisplaySource::File {
        directory: directory.path().join("display"),
        scope: DisplayScope::new("owner", "root", "session"),
    };
    let (blocked, waiting) = oneshot::channel();
    let blocked = Mutex::new(Some(blocked));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let recorder = DisplayRecorder::open(
        source.clone(),
        Arc::new(|_| json!({"history_head":"selected"})),
        Arc::new(move |record| {
            if record.sequence == 2 {
                blocked.lock().unwrap().take().unwrap().send(()).unwrap();
                released.lock().unwrap().recv().unwrap();
            }
        }),
        Arc::new(|e| panic!("{e}")),
    )
    .await
    .unwrap();
    recorder
        .record(observation(ObservationState::Started))
        .await
        .unwrap();
    let task_recorder = recorder.clone();
    let pending = tokio::spawn(async move {
        task_recorder
            .record(observation(ObservationState::ReasoningDelta {
                text: "accepted despite cancellation".into(),
            }))
            .await
    });
    waiting.await.unwrap();
    pending.abort();
    let _ = pending.await;
    recorder.dropped(observation(ObservationState::Invalidated {
        reason: "provider_call_dropped".into(),
    }));
    release.send(()).unwrap();
    recorder.close().await.unwrap();
    let page = tokio::task::spawn_blocking(move || source.query(0, None, 10))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(page.high_water, 3);
    assert_eq!(
        page.records[1].event["text"],
        "accepted despite cancellation"
    );
    assert_eq!(page.records[2].event["reason"], "provider_call_dropped");
}

struct BrokenStore;
impl DisplayStore for BrokenStore {
    fn append(&mut self, _: u64, _: &ArchiveRecord) -> Result<(), Error> {
        Err(Error::Audit("disk full".into()))
    }
    fn revision(&mut self) -> Result<u64, Error> {
        Ok(0)
    }
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
#[test]
fn failed_archive_never_publishes_complete_and_reports_once() {
    let errors = Arc::new(AtomicUsize::new(0));
    let count = errors.clone();
    let mut writer = Writer {
        store: Box::new(BrokenStore),
        sequence: 0,
        graph: DisplayGraph::empty(DisplayScope::new("owner", "root", "session")),
        offsets: HashMap::new(),
        state: Arc::new(Mutex::new(State::default())),
        committed: Arc::new(|_| panic!("failed append published")),
        failed: Arc::new(move |error| {
            assert!(error.contains("disk full"));
            count.fetch_add(1, Ordering::SeqCst);
        }),
    };
    assert!(
        writer
            .append("attempt".into(), json!({"phase":"validated"}))
            .is_err()
    );
    assert!(
        writer
            .append("attempt".into(), json!({"phase":"validated"}))
            .is_err()
    );
    assert_eq!(writer.sequence, 0);
    assert_eq!(errors.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "storage-file")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_first_close_and_repeated_close_wait_for_the_same_durable_ack() {
    let directory = tempfile::tempdir().unwrap();
    let source = DisplaySource::File {
        directory: directory.path().join("display"),
        scope: DisplayScope::new("owner", "root", "session"),
    };
    let (blocked, waiting) = oneshot::channel();
    let blocked = Mutex::new(Some(blocked));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let recorder = DisplayRecorder::open(
        source.clone(),
        Arc::new(|_| json!({})),
        Arc::new(move |_| {
            blocked.lock().unwrap().take().unwrap().send(()).unwrap();
            released.lock().unwrap().recv().unwrap();
        }),
        Arc::new(|error| panic!("{error}")),
    )
    .await
    .unwrap();
    let accepted = recorder.clone();
    let append = tokio::spawn(async move {
        accepted
            .append("exact-binding".into(), json!({"phase":"admission_binding"}))
            .await
    });
    waiting.await.unwrap();
    let closing = recorder.clone();
    let first = tokio::spawn(async move { closing.close().await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if recorder.state.lock().unwrap().closed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.abort();
    let _ = first.await;
    let second = recorder.close();
    tokio::pin!(second);
    assert!(
        futures_util::poll!(second.as_mut()).is_pending(),
        "repeated close falsely acknowledged a blocked writer"
    );
    release.send(()).unwrap();
    assert_eq!(append.await.unwrap().unwrap(), 1);
    second.await.unwrap();
    recorder.close().await.unwrap();
    assert!(
        recorder
            .append("too-late".into(), json!({"phase":"admission_binding"}))
            .await
            .is_err()
    );
    let page = tokio::task::spawn_blocking(move || source.query(0, None, 10))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(page.high_water, 1);
    assert_eq!(page.records[0].attempt_id, "exact-binding");
}
