//! Actual maintained Coding Component media crosses execution, Context and Recover.
#![cfg(feature = "wasm")]
use microcrab_protocol::{ImageSource, InputPart, Response, ToolCall};
use microcrab_session::{
    Auditor, Client, Limits, Session, VariableView,
    context::{Content, ContentItem},
    runtime::{
        Runtime, ToolRegistration,
        execution::ExecutionStatus,
        journal::Recover,
        tool::DenyAll,
        wasm::{WasmAgent, WasmLimits},
    },
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct ImageModel {
    path: String,
    seen: Arc<Mutex<Vec<Content>>>,
}
#[async_trait::async_trait]
impl Client for ImageModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut seen = self.seen.lock().unwrap();
        seen.push(content.clone());
        Ok(Response {
            provider_metadata: None,
            content: "image inspection complete".into(),
            tool_calls: if seen.len() == 1 {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "actual-image-read".into(),
                    name: "read_image".into(),
                    arguments: json!({"path":self.path}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
fn component() -> Vec<u8> {
    std::fs::read(
        std::env::var_os("MICROCRAB_CODING_WASM")
            .expect("parent gate supplies the current ABI0.8 Coding Component"),
    )
    .unwrap()
}
fn session(root: &std::path::Path, path: &str, seen: Arc<Mutex<Vec<Content>>>) -> Session {
    let host = Arc::new(
        microcrab_host_local::LocalHost::new_filesystem(root, Default::default()).unwrap(),
    );
    let agent = WasmAgent::load(&component(), WasmLimits::default(), host.clone()).unwrap();
    assert_eq!(
        agent.agents()[0].tool_media_output_bytes.get("read_image"),
        Some(&(20 * 1024 * 1024))
    );
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(DenyAll),
    )
    .unwrap();
    Session::builder(
        ImageModel {
            path: path.into(),
            seen,
        },
        runtime,
    )
    .host(host)
    .agent(agent.agents()[0].clone())
    .permissions(serde_json::from_value(json!({"initialization":"allow"})).unwrap())
    .build()
    .unwrap()
}
fn image_parts(items: &[ContentItem]) -> &[InputPart] {
    items
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, parts, .. } if call_id == "actual-image-read" => {
                Some(parts.as_slice())
            }
            _ => None,
        })
        .expect("actual image call must settle into Context")
}
fn png() -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=").unwrap()
}
#[tokio::test]
async fn actual_coding_plan_image_reaches_model_and_self_contained_recover() {
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().canonicalize().unwrap();
    let source = png();
    assert!(source.len() < 1024 * 1024);
    // No extension inference: typed bytes, not a filename, identify the image MIME.
    std::fs::write(root.join("picture.bin"), &source).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let session = session(&root, "picture.bin", seen.clone());
    assert_eq!(Limits::default().max_tool_image_bytes, 1024 * 1024);
    session.select_mode("plan").await.unwrap();
    session.submit("inspect the image").await.unwrap();
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            image_parts(&seen[1].items),
            &[InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/png".into(),
                    data: source.clone()
                }
            }]
        );
    }
    let rows = session.execution_snapshots();
    let row = rows
        .iter()
        .find(|row| row.call_id == "actual-image-read")
        .unwrap();
    assert_eq!(row.status, ExecutionStatus::Succeeded);
    assert!(row.failure.is_none());
    let saved = session.checkpoint().await.unwrap();
    let frozen_context = serde_json::to_vec(&saved.context).unwrap();
    let bytes = serde_json::to_vec(&saved).unwrap();
    let restored: Recover = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&restored).unwrap(), bytes);
    assert_eq!(
        image_parts(&restored.context.items),
        &[InputPart::Image {
            source: ImageSource::Data {
                media_type: "image/png".into(),
                data: source
            }
        }]
    );
    // Recovery cannot depend on a still-readable workspace path or attachment store.
    std::fs::remove_file(root.join("picture.bin")).unwrap();
    session.recover(restored).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&session.context_snapshot().unwrap()).unwrap(),
        frozen_context
    );
    session.close().await.unwrap();
}
#[tokio::test]
async fn actual_coding_bad_images_fail_without_admitting_media_and_close_cleanly() {
    for (data, guest_failure) in [
        (b"not an image".to_vec(), true),
        (b"\x89PNG\r\n\x1a\n".to_vec(), false),
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().canonicalize().unwrap();
        std::fs::write(root.join("bad.png"), data).unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let session = session(&root, "bad.png", seen.clone());
        session.select_mode("plan").await.unwrap();
        session.submit("inspect the invalid image").await.unwrap();
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert!(image_parts(&seen[1].items).is_empty());
        }
        let rows = session.execution_snapshots();
        let row = rows
            .iter()
            .find(|row| row.call_id == "actual-image-read")
            .unwrap();
        assert_eq!(row.status, ExecutionStatus::Failed);
        if guest_failure {
            assert_eq!(
                row.failure
                    .as_ref()
                    .expect("guest reports typed invalid-request failure")
                    .code,
                "invalid_request"
            );
        }
        let saved = session.checkpoint().await.unwrap();
        assert!(image_parts(&saved.context.items).is_empty());
        session.close().await.unwrap();
    }
}
