use super::*;
use crate::{
    Client, Session,
    runtime::{Runtime, tool::DenyAll},
};
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::{Response, Usage};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

struct BlockingClient {
    entered: Notify,
    release: Semaphore,
    issued: Mutex<Option<ContextMeasurement>>,
}
#[async_trait::async_trait]
impl Client for BlockingClient {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        *self.issued.lock().unwrap() = Some(ContextMeasurement::for_content(content).unwrap());
        self.entered.notify_one();
        let permit = tokio::select! { _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled), permit = self.release.acquire() => permit.unwrap() };
        permit.forget();
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
async fn session_measurement_tracks_issued_context_while_provider_active_and_clears_overlay() {
    let client = Arc::new(BlockingClient {
        entered: Notify::new(),
        release: Semaphore::new(0),
        issued: Mutex::new(None),
    });
    let runtime = Runtime::new(vec![], Arc::new(DenyAll)).unwrap();
    let session = Session::builder_arc(client.clone(), runtime)
        .build()
        .unwrap();
    let mut changes = session.context_measurement_changes();
    let run = tokio::spawn({
        let session = session.clone();
        async move { session.submit("hello 中文").await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), client.entered.notified())
        .await
        .unwrap();
    let active = session.context_measurement().unwrap();
    assert_eq!(active.counts.user_messages, 1);
    assert_eq!(active.counts.turns, Some(1));
    assert_eq!(
        active.fingerprint,
        client.issued.lock().unwrap().as_ref().unwrap().fingerprint
    );
    assert!(changes.has_changed().unwrap());
    changes.borrow_and_update();
    client.release.add_permits(1);
    run.await.unwrap().unwrap();
    let settled = session.context_measurement().unwrap();
    assert_eq!(settled.counts.steps, 1);
    assert_ne!(active.fingerprint, settled.fingerprint);
    session
        .compact("retained summary", serde_json::json!({}))
        .await
        .unwrap();
    let compacted = session.context_measurement().unwrap();
    assert!(compacted.pending_epoch);
    assert_ne!(compacted.fingerprint, settled.fingerprint);
    session.close().await.unwrap();
}
