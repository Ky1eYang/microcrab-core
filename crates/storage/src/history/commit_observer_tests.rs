use super::*;
use microcrab_session::{Limits, SessionState, context::Context};

struct Projection {
    observed: std::sync::Mutex<Vec<JournalCursor>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl CommitObserver for Projection {
    async fn committed(&self, cursor: &JournalCursor) {
        self.observed.lock().unwrap().push(cursor.clone());
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
}
fn checkpoint(text: &str) -> Recover {
    let limits = Limits::default();
    let mut context = Context::new("system".into(), vec![], limits.max_context_bytes).unwrap();
    context.admit_user(text, &serde_json::json!({})).unwrap();
    Recover {
        state: SessionState::new(limits),
        context: context.snapshot().unwrap(),
        continuation: None,
        workspace: None,
        cache_report: None,
        agent: None,
        prompt_config: None,
    }
}
#[tokio::test]
async fn secondary_projection_observes_acknowledged_new_commits_in_order() {
    let (journal, _) = CreatorJournal::open("unused", "projection".into(), false, false)
        .await
        .unwrap();
    let observer = Arc::new(Projection {
        observed: Default::default(),
        entered: Default::default(),
        release: tokio::sync::Semaphore::new(0),
    });
    journal.install_commit_observer(observer.clone()).unwrap();
    assert!(journal.install_commit_observer(observer.clone()).is_err());
    let writer = journal.clone();
    let first = tokio::spawn(async move { writer.commit(checkpoint("first")).await });
    observer.entered.notified().await;
    assert!(
        journal.selection.try_lock().is_err(),
        "projection stays inside commit serialization"
    );
    assert_eq!(
        journal
            .history_anchor()
            .lock()
            .unwrap()
            .history_head
            .as_ref(),
        Some(&observer.observed.lock().unwrap()[0].checkpoint_id)
    );
    let writer = journal.clone();
    let second = tokio::spawn(async move { writer.commit(checkpoint("second")).await });
    observer.release.add_permits(1);
    first.await.unwrap().unwrap();
    observer.entered.notified().await;
    assert_eq!(observer.observed.lock().unwrap().len(), 2);
    observer.release.add_permits(1);
    second.await.unwrap().unwrap();
    let latest = journal.cursor().await.unwrap();
    assert_eq!(observer.observed.lock().unwrap().last(), Some(&latest));
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        journal.commit(checkpoint("second")),
    )
    .await
    .expect("an identical commit must not emit another projection")
    .unwrap();
    assert_eq!(observer.observed.lock().unwrap().len(), 2);
}
