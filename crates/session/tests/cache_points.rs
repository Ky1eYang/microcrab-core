//! Full-workspace methods must be inert during conversation persistence and travel.
use async_trait::async_trait;
use microcrab_host_api::{
    FileCachePoint, FsApi, Host, HostAPI, NetApi, ProcessApi, ShellApi, WorkspaceFile,
    WorkspaceSnapshot,
};
use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::context::Content;
use microcrab_session::runtime::execution::ToolOutput;
use microcrab_session::runtime::journal::{Journal, Recover};
use microcrab_session::runtime::tool::{AllowTools, DenyAll, Tool, ToolContext};
use microcrab_session::runtime::{Runtime, ToolRegistration};
use microcrab_session::{Auditor, Client, Error, Session};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct NoScans;
impl ProcessApi for NoScans {}
impl FsApi for NoScans {}
impl NetApi for NoScans {}
impl ShellApi for NoScans {}
impl HostAPI for NoScans {}
impl Host for NoScans {}
struct Answer;
#[async_trait]
impl Client for Answer {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "answer".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
fn builder() -> microcrab_session::SessionBuilder {
    Session::builder(Answer, Runtime::new(vec![], Arc::new(DenyAll)).unwrap())
        .host(Arc::new(NoScans))
}
#[tokio::test]
async fn legacy_and_new_recovery_restore_conversation_without_touching_files() {
    let session = builder().build().unwrap();
    session.start().await.unwrap();
    session.submit("first").await.unwrap();
    let mut legacy = session.checkpoint().await.unwrap();
    assert!(legacy.workspace.is_none());
    legacy.workspace = Some(WorkspaceSnapshot {
        files: vec![WorkspaceFile {
            path: "historical.txt".into(),
            data: b"inert old bytes".to_vec(),
        }],
    });
    let historical = serde_json::to_vec(&legacy).unwrap();
    let decoded: Recover = serde_json::from_slice(&historical).unwrap();
    decoded.validate().unwrap();
    // Attaching old history must succeed without global comparison or mutation.
    let restored = Session::restore(builder(), decoded.clone()).await.unwrap();
    restored.start().await.unwrap();
    assert_eq!(restored.messages().len(), session.messages().len());
    restored.submit("continue").await.unwrap();
    restored.recover(decoded).await.unwrap();
    assert_eq!(restored.messages().len(), session.messages().len());
    assert!(restored.checkpoint().await.unwrap().workspace.is_none());
    assert_eq!(serde_json::to_vec(&legacy).unwrap(), historical);
    restored.close().await.unwrap();
    session.close().await.unwrap();
}

#[derive(Default)]
struct Records(Mutex<Vec<Recover>>);
#[async_trait]
impl Journal for Records {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.0.lock().unwrap().push(recover);
        Ok(())
    }
}
struct SelectPoints;
#[async_trait]
impl Tool for SelectPoints {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "select_points".into(),
            description: "Select explicit cache points".into(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        _: serde_json::Value,
        invocation: ToolContext,
    ) -> Result<ToolOutput, Error> {
        invocation.record_cache_point(FileCachePoint {
            path: "selected.txt".into(),
            data: None,
            version: None,
        })?;
        invocation.record_cache_point(FileCachePoint {
            path: "selected.txt".into(),
            data: Some(vec![]),
            version: Some(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
            ),
        })?;
        Ok(ToolOutput::text("selected"))
    }
}
struct WithTool(std::sync::atomic::AtomicBool);
#[async_trait]
impl Client for WithTool {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let first = !self.0.swap(true, std::sync::atomic::Ordering::SeqCst);
        Ok(Response {
            provider_metadata: None,
            content: "answer".into(),
            tool_calls: if first {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "selected-call".into(),
                    name: "select_points".into(),
                    arguments: serde_json::json!({}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
async fn round_journal_summarizes_only_tool_selected_points_with_invocation_identity() {
    let records = Arc::new(Records::default());
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(SelectPoints))],
        Arc::new(AllowTools::new(["select_points"])),
    )
    .unwrap();
    let session = Session::builder(WithTool(std::sync::atomic::AtomicBool::new(false)), runtime)
        .host(Arc::new(NoScans))
        .journal_arc(records.clone())
        .build()
        .unwrap();
    session.submit("select explicitly").await.unwrap();
    {
        let recorded = records.0.lock().unwrap();
        let first_round = recorded
            .iter()
            .filter_map(|recover| recover.cache_report.as_ref())
            .find(|report| report.round_id == 1)
            .unwrap();
        assert_eq!(first_round.points.len(), 2);
        assert_eq!(first_round.points[0].call_id, "selected-call");
        assert_eq!(first_round.points[0].tool, "select_points");
        assert_eq!(first_round.points[1].sequence, 1);
        assert!(first_round.points[0].point.data.is_none());
        assert_eq!(first_round.points[1].point.data, Some(vec![]));
        assert!(recorded.iter().all(|recover| recover.workspace.is_none()));
    }
    let checkpoint = session.checkpoint().await.unwrap();
    assert!(checkpoint.cache_report.unwrap().points.is_empty());
    session.close().await.unwrap();
}
