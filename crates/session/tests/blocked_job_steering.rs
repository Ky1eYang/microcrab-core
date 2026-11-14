//! Regression coverage for steering a live builtin job observation.
use super::users;
use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::{
    Runtime, ToolRegistration,
    execution::{
        CancellationController, ExecutionId, ExecutionStatus, OutputOptions, OutputWriter,
    },
    journal::MemoryJournal,
    tool::{AllowTools, BuiltinTool, Tool, ToolContext, ToolOutput},
};
use microcrab_session::{Auditor, Client, Error, PendingInputMode, Session};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

struct Producer {
    outputs: Mutex<Vec<(ExecutionId, OutputWriter)>>,
    cancelled: CancellationToken,
}
#[async_trait::async_trait]
impl Tool for Producer {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "producer".into(),
            description: "live producer".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let (writer, output) = context.open_output(
            OutputOptions::default(),
            Arc::new(CancellationController::new(self.cancelled.clone())),
        )?;
        self.outputs
            .lock()
            .unwrap()
            .push((context.execution_id.unwrap(), writer));
        Ok(ToolOutput::Buffered(output))
    }
}
struct WaitClient {
    seen: Mutex<Vec<Content>>,
    arguments: Mutex<Value>,
    count: usize,
    entered: Semaphore,
    release: Semaphore,
}
impl WaitClient {
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
}
#[async_trait::async_trait]
impl Client for WaitClient {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let index = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(content.clone());
            seen.len()
        };
        self.entered.add_permits(1);
        tokio::select! { biased;
            _ = cancellation.cancelled() => return Err(microcrab_transport::Error::Cancelled),
            permit = self.release.acquire() => permit.unwrap().forget(),
        }
        let tool_calls = match index {
            1 => (0..self.count)
                .map(|index| ToolCall {
                    id: format!("producer-{index}"),
                    name: "producer".into(),
                    arguments: json!({}),
                    provider_metadata: None,
                })
                .collect(),
            2 => vec![ToolCall {
                id: "wait-observer".into(),
                name: "job".into(),
                arguments: self.arguments.lock().unwrap().clone(),
                provider_metadata: None,
            }],
            _ => vec![],
        };
        Ok(Response {
            content: "answer".into(),
            usage: Usage::default(),
            provider_metadata: None,
            tool_calls,
        })
    }
}

async fn steering_case(batch: bool, convert: bool, before_wait: bool) {
    let client = Arc::new(WaitClient {
        seen: Mutex::new(vec![]),
        arguments: Mutex::new(Value::Null),
        count: if batch { 10 } else { 1 },
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let producer = Arc::new(Producer {
        outputs: Mutex::new(vec![]),
        cancelled: CancellationToken::new(),
    });
    let journal = Arc::new(MemoryJournal::new());
    let runtime = Runtime::new(
        vec![
            ToolRegistration::Builtin(BuiltinTool::Job),
            ToolRegistration::External(producer.clone()),
        ],
        Arc::new(AllowTools::new(["job", "producer"])),
    )
    .unwrap();
    let session = Session::builder_arc(client.clone(), runtime)
        .limits(microcrab_session::Limits {
            max_execution_buffers: 16,
            max_pending_executions: 16,
            ..Default::default()
        })
        .journal_arc(journal.clone())
        .build()
        .unwrap();
    let registry = session.executions();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("first").await });
    client.entered().await;
    client.release.add_permits(1);
    client.entered().await;
    let session_id = session.process_control().session_id().to_owned();
    let handles = producer
        .outputs
        .lock()
        .unwrap()
        .iter()
        .map(|(id, _)| format!("{session_id}/{}", id.0))
        .collect::<Vec<_>>();
    assert_eq!(handles.len(), client.count);
    let id = if batch {
        json!(handles)
    } else {
        json!(handles[0])
    };
    *client.arguments.lock().unwrap() = json!({"id":id,"action":"wait"});
    session.queue("queued").unwrap();
    if before_wait {
        session.steer("intervene").unwrap();
    }
    client.release.add_permits(1);
    if !before_wait {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if registry.snapshot().iter().any(|row| {
                    row.call_id == "wait-observer" && row.status == ExecutionStatus::Running
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Ordinary queue must not settle an unchanged-target observation.
        assert!(
            tokio::time::timeout(Duration::from_millis(30), client.entered.acquire())
                .await
                .is_err()
        );
        if convert {
            let snapshot = session.inbox_snapshot();
            let queued = &snapshot.items[0];
            session
                .update_pending_input(
                    &snapshot.inbox_id,
                    queued.id,
                    queued.revision,
                    Some("intervene".into()),
                    Some(PendingInputMode::Steer),
                )
                .unwrap();
        } else {
            session.steer("intervene").unwrap();
        }
    }
    client.entered().await;
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(users(&seen[2]), ["first", "intervene"]);
        assert_eq!(seen[0].turn_id, seen[2].turn_id);
        let result = seen[2]
            .items
            .iter()
            .find_map(|item| match item {
                ContentItem::Tool {
                    call_id, content, ..
                } if call_id == "wait-observer" => Some(content),
                _ => None,
            })
            .expect("wait result precedes steered user input");
        let result: Value = serde_json::from_str(result).unwrap();
        assert_eq!(result["reason"], "user_steer");
        assert_eq!(result["targets_cancelled"], false);
    }
    assert!(!producer.cancelled.is_cancelled());
    {
        let outputs = producer.outputs.lock().unwrap();
        for (id, writer) in outputs.iter() {
            assert!(registry.get(*id).unwrap().status.is_pending());
            writer.append(None, "still live").unwrap();
            assert_eq!(
                registry.read(*id, 0, 1024).unwrap().chunks[0].content,
                "still live"
            );
        }
    }
    let observer = registry
        .snapshot()
        .into_iter()
        .find(|row| row.call_id == "wait-observer")
        .unwrap();
    assert_eq!(observer.status, ExecutionStatus::Succeeded);
    assert_eq!(observer.tool, "job");
    client.release.add_permits(1);
    // Appending live output above emits a Continue notice. The current Turn
    // must observe that new metadata before it settles; release that legitimate
    // request rather than waiting for settlement while the mock blocks it.
    client.entered().await;
    {
        let seen = client.seen.lock().unwrap();
        assert_eq!(seen[2].turn_id, seen[3].turn_id);
        assert_eq!(users(&seen[3]), ["first", "intervene"]);
    }
    client.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), turn)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    if !convert {
        client.entered().await;
        {
            let seen = client.seen.lock().unwrap();
            assert_ne!(seen[3].turn_id, seen[4].turn_id);
            assert_eq!(users(&seen[4]).last().unwrap(), "queued");
        }
        client.release.add_permits(1);
    }
    super::quiet(&session).await;
    let recovered = journal.latest().unwrap();
    recovered.validate().unwrap();
    assert!(
        recovered
            .state
            .executions
            .iter()
            .any(|row| row.call_id == "wait-observer" && row.invocation_result.is_some())
    );
    for (_, writer) in producer.outputs.lock().unwrap().iter() {
        writer.finish(Ok(())).unwrap();
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn scalar_wait_yields_to_steer_and_retains_live_output() {
    steering_case(false, false, false).await;
}
#[tokio::test]
async fn batch_wait_yields_with_more_targets_than_scheduler_slots() {
    steering_case(true, false, false).await;
}
#[tokio::test]
async fn converting_queue_to_steer_wakes_wait() {
    steering_case(false, true, false).await;
}
#[tokio::test]
async fn steer_before_wait_registration_is_not_lost() {
    steering_case(false, false, true).await;
}
