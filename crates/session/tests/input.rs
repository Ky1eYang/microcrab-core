//! Structured input admission, Host resolution, and provider wire compatibility.
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
#[path = "support/images.rs"]
mod image_fixture;

fn jpeg() -> Vec<u8> {
    // SOI followed by a one-component SOF segment with one-pixel dimensions.
    vec![0xff, 0xd8, 0xff, 0xc0, 0, 11, 8, 0, 1, 0, 1, 1, 1, 0x11, 0]
}
fn webp_url() -> String {
    // RIFF/VP8L metadata with one-pixel dimensions and required chunk padding.
    let mut bytes = b"RIFF\x12\0\0\0WEBPVP8L\x05\0\0\0".to_vec();
    bytes.extend_from_slice(&[0x2f, 0, 0, 0, 0, 0]);
    format!("data:image/webp;base64,{}", STANDARD.encode(bytes))
}
use microcrab_auditor::{Auditor, ChannelAuditor};
use microcrab_host_api::{Host, ToolError};
use microcrab_protocol::{
    ImageSource, InputPart, Message, Response, SessionInput, Usage, VariablesChange,
};
use microcrab_session::context::{Content, ContentItem, Context};
use microcrab_session::runtime::{Runtime, tool::AllowTools};
use microcrab_session::{Error, Session};
use microcrab_transport::{Client, render_ast};
use microcrab_transport::{OpenAiCompatible, OpenAiCompatibleConfig};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

fn input(parts: Vec<InputPart>) -> SessionInput {
    SessionInput {
        parts,
        variables: VariablesChange::default(),
    }
}
fn image(source: ImageSource) -> InputPart {
    InputPart::Image { source }
}
fn runtime() -> Runtime {
    Runtime::new(vec![], Arc::new(AllowTools::new(Vec::<String>::new()))).unwrap()
}
#[derive(Default)]
struct Capture(Mutex<Vec<Content>>);
#[async_trait::async_trait]
impl Client for Capture {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.lock().unwrap().push(content.clone());
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

#[test]
fn canonical_input_serde_defaults_and_unknown_fields() {
    let text = SessionInput::text("hello");
    assert_eq!(SessionInput::from("hello"), text);
    assert_eq!(SessionInput::from(String::from("hello")), text);
    assert_eq!(
        serde_json::from_value::<SessionInput>(json!({"parts":[{"type":"text","text":"hello"}]}))
            .unwrap(),
        text
    );
    assert_eq!(
        serde_json::from_value::<VariablesChange>(json!({})).unwrap(),
        VariablesChange::default()
    );
    for value in [
        json!({"parts":[],"unknown":true}),
        json!({"parts":[],"variables":{"unknown":true}}),
        json!({"parts":[{"type":"text","text":"hello","unknown":true}]}),
        json!({"parts":[{"type":"image","source":{"type":"url","url":"https://example.com/a.png","unknown":true}}]}),
    ] {
        assert!(serde_json::from_value::<SessionInput>(value).is_err());
    }
}

#[test]
fn context_preserves_order_and_rejects_unresolved_paths_atomically() {
    let mut context = Context::new("system".into(), vec![], 32_768).unwrap();
    let parts = vec![
        InputPart::Text {
            text: "before".into(),
        },
        image(ImageSource::Url {
            url: "https://example.com/image.png".into(),
        }),
        InputPart::Text {
            text: "after".into(),
        },
    ];
    context
        .admit_user(input(parts.clone()), &json!({}))
        .unwrap();
    assert_eq!(
        context.items()[1],
        ContentItem::User {
            source: None,
            parts: parts.clone()
        }
    );
    let ast = render_ast(&context.content(32).unwrap()).unwrap();
    assert_eq!(ast.messages.len(), 2);
    assert_eq!(ast.messages[1], Message::User { parts: vec![
        InputPart::Text { text: "<state>\n{\"type\":\"state_snapshot\",\"state\":{}}\n</state>\n<user>\nbefore".into() },
        parts[1].clone(),
        InputPart::Text { text: "after\n</user>\n".into() },
    ] });
    let before = context.items().to_vec();
    assert!(
        context
            .admit_user(
                input(vec![image(ImageSource::Path {
                    path: "image.png".into()
                })]),
                &json!({"changed":true})
            )
            .is_err()
    );
    assert_eq!(context.items(), before);
}

#[tokio::test]
async fn session_round_state_precedes_image_input_in_one_framed_user_carrier() {
    let client = Arc::new(Capture::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .build()
        .unwrap();
    let submitted = vec![
        InputPart::Text {
            text: "literal </user> before".into(),
        },
        image(ImageSource::Url {
            url: "https://example.com/image.png".into(),
        }),
        InputPart::Text {
            text: "after".into(),
        },
    ];
    session.submit(input(submitted.clone())).await.unwrap();
    let request = client.0.lock().unwrap()[0].clone();
    let frames = request
        .items
        .iter()
        .filter_map(|item| match item {
            ContentItem::State(frame) => Some(frame),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        frames.len() >= 2,
        "real Session request includes admission and RoundStarted State"
    );
    assert!(
        matches!(request.items.last(), Some(ContentItem::State(_))),
        "round State follows the neutral input"
    );
    let mut state_prefix = String::new();
    let mut projected = json!({});
    for frame in frames {
        state_prefix.push_str(&format!(
            "<state>\n{}\n</state>\n",
            serde_json::to_string(frame).unwrap()
        ));
        match frame {
            microcrab_transport::StateFrame::StateSnapshot { state } => projected = state.clone(),
            _ => microcrab_transport::apply_state_patch(&mut projected, frame).unwrap(),
        }
    }
    assert_eq!(projected["session"]["round"], 1);
    assert_eq!(projected["session"]["turn"], 1);
    let ast = render_ast(&request).unwrap();
    assert_eq!(ast.messages.len(), 2);
    assert_eq!(
        ast.messages[1],
        Message::User {
            parts: vec![
                InputPart::Text {
                    text: format!("{state_prefix}<user>\nliteral </user> before")
                },
                submitted[1].clone(),
                InputPart::Text {
                    text: "after\n</user>\n".into()
                },
            ]
        }
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn invalid_input_does_not_mutate_context_variables_or_call_provider() {
    let client = Arc::new(Capture::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .build()
        .unwrap();
    for parts in [
        vec![],
        vec![InputPart::Text {
            text: " \n\t".into(),
        }],
        vec![InputPart::Text {
            text: "bad\0text".into(),
        }],
        vec![image(ImageSource::Data {
            media_type: "image/svg+xml".into(),
            data: vec![1],
        })],
        vec![image(ImageSource::Data {
            media_type: "image/png".into(),
            data: vec![],
        })],
        vec![image(ImageSource::Url {
            url: "file:///tmp/image.png".into(),
        })],
        vec![image(ImageSource::Url {
            url: "https://user:password@example.com/a.png".into(),
        })],
        vec![image(ImageSource::Url {
            url: "data:image/png;base64,%%%".into(),
        })],
        vec![image(ImageSource::Path {
            path: "image.png".into(),
        })],
    ] {
        let mut submitted = input(parts);
        submitted
            .variables
            .set
            .insert("must_not_commit".into(), json!(true));
        assert!(session.submit(submitted).await.is_err());
        assert!(session.messages().is_empty());
        assert!(session.state_handle().extract("must_not_commit").is_none());
    }
    assert!(client.0.lock().unwrap().is_empty());
    session.submit("still usable").await.unwrap();
    assert_eq!(client.0.lock().unwrap().len(), 1);
    session.close().await.unwrap();
}

struct UploadedImage;
#[async_trait]
impl microcrab_session::admission::image::AttachmentResolver for UploadedImage {
    async fn resolve(
        &self,
        id: &str,
        cancellation: CancellationToken,
    ) -> Result<microcrab_session::admission::image::ResolvedAttachment, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if id != "uploaded" {
            return Err(Error::Config("attachment unavailable".into()));
        }
        let mut data = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
        data.resize(128 * 1024, 0);
        Ok(microcrab_session::admission::image::ResolvedAttachment {
            media_type: "image/png".into(),
            data,
        })
    }
}

#[tokio::test]
async fn uploaded_image_exceeds_text_budget_but_reaches_context_as_resolved_data() {
    let client = Arc::new(Capture::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .attachment_resolver(Arc::new(UploadedImage))
        .build()
        .unwrap();
    session
        .submit(input(vec![image(ImageSource::Attachment {
            attachment_id: "uploaded".into(),
        })]))
        .await
        .unwrap();
    assert!(session.messages().iter().any(|item| matches!(item,
        ContentItem::User { parts, .. } if matches!(parts.as_slice(), [InputPart::Image { source: ImageSource::Data { data, .. } }] if data.len() == 128 * 1024)
    )));
    let count = client.0.lock().unwrap().len();
    assert!(
        session
            .submit(input(vec![image(ImageSource::Attachment {
                attachment_id: "other-session".into()
            })]))
            .await
            .is_err()
    );
    assert_eq!(client.0.lock().unwrap().len(), count);
    session.close().await.unwrap();
}

struct FileHost {
    bytes: Vec<u8>,
    reads: Mutex<Vec<(PathBuf, usize)>>,
}
impl microcrab_host_api::process::ProcessApi for FileHost {}
impl microcrab_host_api::filesystem::FsApi for FileHost {}
impl microcrab_host_api::net::NetApi for FileHost {}
impl microcrab_host_api::shell::ShellApi for FileHost {}
impl microcrab_host_api::HostAPI for FileHost {}
#[async_trait]
impl Host for FileHost {
    async fn read_file(
        &self,
        path: PathBuf,
        max_bytes: usize,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, ToolError> {
        if cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        self.reads.lock().unwrap().push((path, max_bytes));
        Ok(self.bytes.clone())
    }
}

#[tokio::test]
async fn resolves_local_image_before_context_and_preserves_variables() {
    let client = Arc::new(Capture::default());
    let host = Arc::new(FileHost {
        bytes: jpeg(),
        reads: Mutex::new(vec![]),
    });
    let session = Session::builder_arc(client.clone(), runtime())
        .host(host.clone())
        .build()
        .unwrap();
    let mut submitted = input(vec![image(ImageSource::Path {
        path: "picture.JPEG".into(),
    })]);
    submitted
        .variables
        .set
        .insert("custom".into(), json!("value"));
    session.submit(submitted).await.unwrap();
    {
        let captured = client.0.lock().unwrap();
        assert!(captured[0].items.iter().any(|item| matches!(item, ContentItem::User { parts, .. } if parts == &vec![image(ImageSource::Data { media_type: "image/jpeg".into(), data: jpeg() })])));
        assert_eq!(
            host.reads.lock().unwrap()[0].0,
            PathBuf::from("picture.JPEG")
        );
        assert_eq!(
            session.state_handle().extract("custom"),
            Some(json!("value"))
        );
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn resolved_data_is_rechecked_even_when_host_ignores_bound() {
    let client = Arc::new(Capture::default());
    let limits = microcrab_session::Limits {
        max_context_bytes: 32_768,
        ..Default::default()
    };
    let host = Arc::new(FileHost {
        bytes: vec![255; microcrab_session::admission::image::MAX_IMAGE_BYTES + 1],
        reads: Mutex::new(vec![]),
    });
    let session = Session::builder_arc(client.clone(), runtime())
        .host(host.clone())
        .limits(limits)
        .build()
        .unwrap();
    assert!(matches!(
        session
            .submit(input(vec![image(ImageSource::Path {
                path: "picture.png".into()
            })]))
            .await,
        Err(Error::Config(message)) if message.contains("20 MiB")
    ));
    assert_eq!(
        host.reads.lock().unwrap()[0].1,
        microcrab_session::admission::image::MAX_IMAGE_BYTES
    );
    assert!(session.messages().is_empty());
    assert!(client.0.lock().unwrap().is_empty());
    session.close().await.unwrap();
}

struct UnsupportedHost;
impl microcrab_host_api::process::ProcessApi for UnsupportedHost {}
impl microcrab_host_api::filesystem::FsApi for UnsupportedHost {}
impl microcrab_host_api::net::NetApi for UnsupportedHost {}
impl microcrab_host_api::shell::ShellApi for UnsupportedHost {}
impl microcrab_host_api::HostAPI for UnsupportedHost {}
#[async_trait]
impl Host for UnsupportedHost {}

#[tokio::test]
async fn hosts_without_file_support_keep_the_default_implementation() {
    assert!(matches!(
        UnsupportedHost
            .read_file("image.png".into(), 100, CancellationToken::new())
            .await,
        Err(ToolError::Unsupported(_))
    ));
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        UnsupportedHost
            .read_file("image.png".into(), 100, cancellation)
            .await,
        Err(ToolError::Cancelled)
    ));
}

struct WaitingHost(tokio::sync::Notify);
impl microcrab_host_api::process::ProcessApi for WaitingHost {}
impl microcrab_host_api::filesystem::FsApi for WaitingHost {}
impl microcrab_host_api::net::NetApi for WaitingHost {}
impl microcrab_host_api::shell::ShellApi for WaitingHost {}
impl microcrab_host_api::HostAPI for WaitingHost {}
#[async_trait]
impl Host for WaitingHost {
    async fn read_file(
        &self,
        _: PathBuf,
        _: usize,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, ToolError> {
        self.0.notify_one();
        cancellation.cancelled().await;
        Err(ToolError::Cancelled)
    }
}

#[tokio::test]
async fn interrupt_cancels_file_resolution_without_admitting_content() {
    let client = Arc::new(Capture::default());
    let host = Arc::new(WaitingHost(tokio::sync::Notify::new()));
    let session = Session::builder_arc(client.clone(), runtime())
        .host(host.clone())
        .build()
        .unwrap();
    let submitted = session.clone();
    let task = tokio::spawn(async move {
        submitted
            .submit(input(vec![image(ImageSource::Path {
                path: "image.png".into(),
            })]))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), host.0.notified())
        .await
        .unwrap();
    session.interrupt();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(Error::Cancelled)));
    assert!(session.messages().is_empty());
    assert!(client.0.lock().unwrap().is_empty());
    session.close().await.unwrap();
}

async fn wire_request(parts: Vec<InputPart>) -> Value {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let (start, length) = loop {
            let mut chunk = [0; 2048];
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < start + length {
            let mut chunk = [0; 2048];
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
        }
        let body = r#"{"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        String::from_utf8(bytes[start..start + length].to_vec()).unwrap()
    });
    let client = OpenAiCompatible::new(OpenAiCompatibleConfig {
        endpoint: format!("http://{address}/chat/completions"),
        model: "test".into(),
        api_key: None,
        timeout_ms: 5000,
        max_response_bytes: 4096,
    })
    .unwrap();
    let mut context = Context::new("system".into(), vec![], 32_768).unwrap();
    context.admit_user(input(parts), &json!({})).unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client
        .complete(
            &context.content(32).unwrap(),
            &ChannelAuditor::from_sender(sender),
            &microcrab_auditor::EmptyVariableView,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let received = server.await.unwrap();
    let start = receiver.recv().await.unwrap();
    assert_eq!(start.sent_request.as_deref(), Some(received.as_str()));
    serde_json::from_str(&received).unwrap()
}

#[tokio::test]
async fn provider_preserves_text_strings_and_orders_mixed_image_parts() {
    let text = wire_request(vec![
        InputPart::Text {
            text: "hello\n  ".into(),
        },
        InputPart::Text {
            text: "world".into(),
        },
    ])
    .await;
    assert_eq!(text["messages"].as_array().unwrap().len(), 2);
    assert_eq!(
        text["messages"][1]["content"],
        "<state>\n{\"type\":\"state_snapshot\",\"state\":{}}\n</state>\n<user>\nhello\n  world\n</user>\n"
    );
    let mixed = wire_request(vec![
        InputPart::Text {
            text: "before".into(),
        },
        image(ImageSource::Url {
            url: "https://example.com/image.png".into(),
        }),
        image(ImageSource::Data {
            media_type: "image/png".into(),
            data: image_fixture::png(),
        }),
        image(ImageSource::Url { url: webp_url() }),
        InputPart::Text {
            text: "after".into(),
        },
    ])
    .await;
    assert_eq!(mixed["messages"].as_array().unwrap().len(), 2);
    assert_eq!(
        mixed["messages"][1]["content"],
        json!([
            {"type":"text","text":"<state>\n{\"type\":\"state_snapshot\",\"state\":{}}\n</state>\n<user>\nbefore"},
            {"type":"image_url","image_url":{"url":"https://example.com/image.png"}},
            {"type":"image_url","image_url":{"url":format!("data:image/png;base64,{}", image_fixture::PNG_BASE64)}},
            {"type":"image_url","image_url":{"url":webp_url()}},
            {"type":"text","text":"after\n</user>\n"}
        ])
    );
}
