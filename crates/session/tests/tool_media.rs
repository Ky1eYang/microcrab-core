//! Native typed media dispatch, pre-effect reservation, and metadata observation.
use microcrab_protocol::{
    HookPayload, ImageSource, InputPart, Response, ToolCall, ToolDefinition, Usage,
};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::execution::{FinalOutput, MediaAttachment, ToolOutput};
use microcrab_session::runtime::tool::{AllowTools, Tool, ToolContext};
use microcrab_session::runtime::{Runtime, ToolRegistration};
use microcrab_session::{Auditor, Client, Error, LimitKind, Limits, Session};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;
fn png() -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    bytes
}
struct ImageTool {
    effects: Arc<AtomicUsize>,
    declaration: usize,
}
#[async_trait::async_trait]
impl Tool for ImageTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "native_image".into(),
            description: "inspect native image".into(),
            parameters: json!({"type":"object"}),
        }
    }
    fn media_output_bytes(&self) -> usize {
        self.declaration
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput, Error> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::Final(FinalOutput {
            content: "caption".into(),
            failure: None,
            attachments: vec![MediaAttachment {
                media_type: "image/png".into(),
                data: png(),
            }],
        }))
    }
}
struct Model {
    seen: Arc<Mutex<Vec<Content>>>,
}
#[async_trait::async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut seen = self.seen.lock().unwrap();
        seen.push(content.clone());
        Ok(Response {
            content: "answer".into(),
            tool_calls: if seen.len() == 1 {
                vec![ToolCall {
                    id: "image-call".into(),
                    name: "native_image".into(),
                    arguments: json!({}),
                    provider_metadata: None,
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
            provider_metadata: None,
        })
    }
}
fn runtime(effects: Arc<AtomicUsize>, declaration: usize) -> Runtime {
    Runtime::new(
        vec![ToolRegistration::External(Arc::new(ImageTool {
            effects,
            declaration,
        }))],
        Arc::new(AllowTools::new(["native_image"])),
    )
    .unwrap()
}
#[tokio::test]
async fn media_reservation_rejects_whole_batch_before_native_effects() {
    let effects = Arc::new(AtomicUsize::new(0));
    let session = Session::builder(
        Model {
            seen: Arc::new(Mutex::new(vec![])),
        },
        runtime(effects.clone(), 1024 * 1024),
    )
    .limits(Limits {
        max_context_bytes: 1024 * 1024,
        ..Limits::default()
    })
    .build()
    .unwrap();
    let Error::Limit(limit) = session.submit("inspect").await.unwrap_err() else {
        panic!("expected pre-effect reservation error")
    };
    assert_eq!(limit.kind, LimitKind::ToolResultReservation);
    assert!(limit.is_pre_effect());
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(session.execution_snapshots().is_empty());
    session.close().await.unwrap();
}
#[tokio::test]
async fn native_image_enters_context_as_data_and_audit_as_metadata_only() {
    let effects = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(vec![]));
    let (sender, mut records) = tokio::sync::mpsc::unbounded_channel();
    let session = Session::builder(Model { seen: seen.clone() }, runtime(effects.clone(), 64))
        .limits(Limits {
            max_tool_output_bytes: 8,
            ..Limits::default()
        })
        .auditor(Arc::new(microcrab_auditor::ChannelAuditor::from_sender(
            sender,
        )))
        .build()
        .unwrap();
    session.submit("inspect").await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    {
        let messages = seen.lock().unwrap();
        let result = messages[1]
            .items
            .iter()
            .find_map(|item| {
                if let ContentItem::Tool { content, parts, .. } = item {
                    Some((content, parts))
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(result.0, "caption");
        assert!(
            matches!(&result.1[0],InputPart::Image {source:ImageSource::Data {data,..}} if data==&png())
        );
    }
    let mut observed = false;
    while let Ok(record) = records.try_recv() {
        if let HookPayload::ToolResult {
            result: Some(result),
            error: None,
        } = record.payload
        {
            let metadata: Value = serde_json::from_str(&result).unwrap();
            assert_eq!(metadata["content"], "caption");
            assert_eq!(metadata["images"][0]["width"], 1);
            assert_eq!(metadata["images"][0]["byte_len"], png().len());
            assert!(metadata["images"][0].get("data").is_none());
            observed = true;
        }
    }
    assert!(observed);
    session.close().await.unwrap();
}
