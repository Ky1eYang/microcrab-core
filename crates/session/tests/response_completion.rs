//! Only Session's actual Context append can promote normalized attempt evidence.
use microcrab_auditor::{AuditBody, AuditOutcome, Auditor, Moment, VariableView};
use microcrab_protocol::{Response, Usage};
use microcrab_session::{
    Client, Error, Session,
    runtime::{Runtime, tool::DenyAll},
};
use microcrab_transport::{
    Content, ContentItem, ModelAttempt, ModelEvent, ResponseCompletedAt, ResponseOutcome,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

enum Candidate {
    Rejected,
    Cancelled,
    Accepted,
    Untimed,
}

#[derive(Debug)]
struct Admitted {
    epoch: u64,
    assistant_index: usize,
    attempt: Option<ModelAttempt>,
}

struct Script {
    candidates: Mutex<VecDeque<Candidate>>,
    admitted: Mutex<Vec<Admitted>>,
    requests: Mutex<Vec<Vec<u8>>>,
}

#[async_trait::async_trait]
impl Client for Script {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        panic!("primary runtime must use the outcome port")
    }

    async fn stream_admitted_outcome(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        _: &dyn VariableView,
        cancellation: CancellationToken,
        _: Option<tokio::sync::mpsc::Sender<ModelEvent>>,
    ) -> Result<ResponseOutcome, microcrab_transport::Error> {
        self.requests
            .lock()
            .unwrap()
            .push(serde_json::to_vec(content).unwrap());
        let candidate = self.candidates.lock().unwrap().pop_front().unwrap();
        let (answer, attempt) = match candidate {
            Candidate::Rejected => (" ", Some(("rejected", 11))),
            Candidate::Cancelled => {
                cancellation.cancel();
                ("done", Some(("cancelled", 12)))
            }
            Candidate::Accepted => ("done", Some(("accepted", 13))),
            Candidate::Untimed => ("done", None),
        };
        // Even the same scoped successful diagnostic record cannot supply the
        // producer or completion fact if it was not returned with this response.
        auditor.publish(
            Moment::RoundCompleted,
            AuditBody {
                epoch: content.epoch,
                turn_id: content.turn_id,
                round: content.round,
                call_id: Some("unrelated".into()),
                outcome: Some(AuditOutcome::Accepted),
                response_completed_at_ms: Some(99),
                ..AuditBody::default()
            },
        );
        Ok(ResponseOutcome {
            response: Response {
                provider_metadata: None,
                content: answer.into(),
                tool_calls: vec![],
                usage: Usage::default(),
            },
            attempt: attempt.map(|(call_id, ms)| ModelAttempt {
                call_id: call_id.into(),
                response_completed_at_ms: ResponseCompletedAt::new(ms),
            }),
        })
    }

    async fn observe_response_admitted(
        &self,
        epoch: u64,
        items: &[ContentItem],
        assistant_index: usize,
        attempt: Option<&ModelAttempt>,
    ) {
        assert_eq!(assistant_index, items.len() - 1);
        assert!(
            matches!(&items[assistant_index], ContentItem::Assistant { content, .. } if content == "done")
        );
        self.admitted.lock().unwrap().push(Admitted {
            epoch,
            assistant_index,
            attempt: attempt.cloned(),
        });
    }
}

#[tokio::test]
async fn only_exact_admitted_attempt_survives_rejection_cancellation_and_retry() {
    let client = Arc::new(Script {
        candidates: Mutex::new(VecDeque::from([
            Candidate::Rejected,
            Candidate::Cancelled,
            Candidate::Accepted,
            Candidate::Untimed,
        ])),
        admitted: Mutex::new(Vec::new()),
        requests: Mutex::new(Vec::new()),
    });
    let session = Session::builder_arc(
        client.clone(),
        Runtime::new(vec![], Arc::new(DenyAll)).unwrap(),
    )
    .auditor(microcrab_auditor::NoopAuditor)
    .build()
    .unwrap();

    assert!(matches!(
        session.submit("first").await,
        Err(Error::Protocol(_))
    ));
    assert!(client.admitted.lock().unwrap().is_empty());
    assert!(matches!(session.resume().await, Err(Error::Cancelled)));
    assert!(client.admitted.lock().unwrap().is_empty());
    assert_eq!(session.resume().await.unwrap().content, "done");

    let snapshot = session.context_snapshot().unwrap();
    let snapshot_json = serde_json::to_string(&snapshot).unwrap();
    assert!(!snapshot_json.contains("response_completed_at_ms"));
    assert!(!snapshot_json.contains("unrelated"));
    {
        let admitted = client.admitted.lock().unwrap();
        let [first] = admitted.as_slice() else {
            panic!("{admitted:?}")
        };
        assert_eq!(first.attempt.as_ref().unwrap().call_id, "accepted");
        assert_eq!(
            first
                .attempt
                .as_ref()
                .unwrap()
                .response_completed_at_ms
                .unwrap()
                .get(),
            13
        );
        let requests = client.requests.lock().unwrap();
        assert_eq!(requests[0], requests[1]);
        assert_eq!(requests[1], requests[2]);
    }

    assert_eq!(session.submit("second").await.unwrap().content, "done");
    {
        let admitted = client.admitted.lock().unwrap();
        let [first, second] = admitted.as_slice() else {
            panic!("{admitted:?}")
        };
        assert_eq!(first.epoch, second.epoch);
        assert!(first.assistant_index < second.assistant_index);
        assert!(second.attempt.is_none());
    }
    session.close().await.unwrap();
}
