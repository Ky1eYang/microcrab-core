//! Contract tests exercise dynamic dispatch and lane separation without Session.
use crate::*;
use serde_json::json;
use std::sync::{Arc, Mutex};

struct Intervention {
    seen: Mutex<Vec<Moment>>,
    sink: ChannelAuditor,
}
impl Auditor for Intervention {
    fn is_enabled(&self) -> bool {
        self.sink.is_enabled()
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        self.sink.publish(moment, body);
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(moment);
            assert_eq!(context.variables.get("sample"), None);
            Ok(Injection {
                variables: VariablesChange {
                    set: serde_json::Map::from_iter([("sample".into(), json!(1))]),
                    remove: vec![],
                    ..Default::default()
                },
                request_body: context.request_body.map(|_| "replacement".into()),
                ..Default::default()
            })
        })
    }
}

#[tokio::test]
async fn dyn_auditor_intervention_is_separate_from_nonblocking_observation() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let concrete = Arc::new(Intervention {
        seen: Mutex::new(vec![]),
        sink: ChannelAuditor::from_sender(tx),
    });
    let auditor: Arc<dyn Auditor> = concrete.clone();
    let injection = auditor
        .intervene(
            Moment::RequestPrepared,
            HookContext {
                epoch: 0,
                subagent: None,
                turn: Some(1),
                round: Some(1),
                call_id: Some("call".into()),
                request_body: Some("authored"),
                max_request_bytes: None,
                variables: &EmptyVariableView,
                payload: HookPayload::None,
            },
        )
        .await
        .unwrap();
    assert_eq!(injection.request_body.as_deref(), Some("replacement"));
    assert_eq!(injection.variables.set["sample"], json!(1));
    assert!(rx.try_recv().is_err());
    auditor.publish(
        Moment::RequestPrepared,
        AuditBody {
            call_id: Some("call".into()),
            sent_request: Some("replacement".into()),
            ..Default::default()
        },
    );
    let record = rx.try_recv().unwrap();
    let value = serde_json::to_value(record).unwrap();
    assert_eq!(value["sent_request"], "replacement");
    assert!(value.get("request").is_none());
    assert!(value.get("authored_request").is_none());
    assert!(!value.to_string().contains("authored"));
    auditor.flush().await.unwrap();
    drop(rx);
    auditor.publish(Moment::ToolResult, AuditBody::default());
    assert_eq!(
        *concrete.seen.lock().unwrap(),
        vec![Moment::RequestPrepared]
    );
}

#[test]
fn restored_same_epoch_restarts_request_diff_baseline() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = ChannelAuditor::from_sender(tx);
    sink.publish(
        Moment::RequestPrepared,
        AuditBody {
            epoch: 4,
            sent_request: Some("{\"value\":1}".into()),
            ..Default::default()
        },
    );
    rx.try_recv().unwrap();
    sink.publish(
        Moment::SessionRestored,
        AuditBody {
            epoch: 4,
            ..Default::default()
        },
    );
    rx.try_recv().unwrap();
    sink.publish(
        Moment::RequestPrepared,
        AuditBody {
            epoch: 4,
            sent_request: Some("{\"value\":2}".into()),
            ..Default::default()
        },
    );
    let patch = rx.try_recv().unwrap().request_diff.unwrap();
    assert!(!patch.contains("-1"));
    assert!(patch.contains("+2"));
}
