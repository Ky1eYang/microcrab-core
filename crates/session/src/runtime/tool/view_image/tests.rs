use super::*;
use crate::context::ContentItem;
use crate::runtime::{BuiltinTool, Runtime, ToolRegistration};
use crate::{Auditor, Client, ImageSource, InputPart, Session, SessionKind, VariableView};
use microcrab_auditor::{AuditBody, HookPayload, Moment};
use microcrab_protocol::{Response, ToolCall, Usage};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

// Small valid encoded 1x1 PNG. Assert exact bytes, not an accidental raster re-encode.
const IMAGE: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 4, 0,
    0, 0, 181, 28, 12, 2, 0, 0, 0, 11, 73, 68, 65, 84, 120, 218, 99, 252, 255, 31, 0, 3, 3, 2, 0,
    238, 254, 95, 153, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];
const SELECTOR: &str = "bytes_01234567890123456789012345678901_0123456789012345";

#[derive(Default, Clone)]
struct Script(Arc<Mutex<Option<Value>>>);
#[async_trait]
impl Client for Script {
    async fn complete(
        &self,
        _: &crate::context::Content,
        _: &dyn Auditor,
        variables: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        assert!(variables.get("raster").is_none());
        assert!(variables.get_all("raster").is_none());
        let next = self.0.lock().unwrap().take();
        Ok(Response {
            provider_metadata: None,
            content: if next.is_some() { "" } else { "done" }.into(),
            tool_calls: next
                .into_iter()
                .map(|arguments| ToolCall {
                    id: "inspect".into(),
                    name: NAME.into(),
                    arguments,
                    provider_metadata: None,
                })
                .collect(),
            usage: Usage::default(),
        })
    }
}

#[derive(Default, Clone)]
struct Audit(Arc<Mutex<Vec<(Moment, AuditBody)>>>);
impl Auditor for Audit {
    fn is_enabled(&self) -> bool {
        true
    }

    fn publish(&self, moment: Moment, body: AuditBody) {
        if matches!(moment, Moment::ToolCall | Moment::ToolResult) {
            self.0.lock().unwrap().push((moment, body));
        }
    }
}
impl Audit {
    fn check(&self, id: &str, expected: Value) {
        let stages = self.0.lock().unwrap();
        let matching: Vec<_> = stages
            .iter()
            .filter(|(_, body)| body.tool_call_id.as_deref() == Some(id))
            .collect();
        let [(Moment::ToolCall, call), (Moment::ToolResult, result)] = matching.as_slice() else {
            panic!("expected one linked ToolCall/ToolResult pair for {id}")
        };
        assert!(call.authored_image_arguments);
        assert!(
            matches!(&call.payload, HookPayload::ToolCall { name, arguments } if name == NAME && *arguments == expected)
        );
        assert!(call.tool_execution_id.is_some());
        assert_eq!(call.tool_execution_id, result.tool_execution_id);
        assert_eq!(call.epoch, result.epoch);
        assert_eq!(call.turn_id, result.turn_id);
        assert_eq!(call.round, result.round);
        assert_eq!(call.started_at_unix_ms, result.started_at_unix_ms);
    }
}

fn replay_arguments(snapshot: &crate::context::ContextSnapshot) -> &Value {
    snapshot
        .items
        .iter()
        .find_map(|item| match item {
            ContentItem::Assistant { tool_calls, .. } => {
                tool_calls.first().map(|call| &call.arguments)
            }
            _ => None,
        })
        .unwrap()
}

fn runtime() -> Runtime {
    Runtime::new(
        vec![ToolRegistration::Builtin(BuiltinTool::ViewImage)],
        Arc::new(super::super::AllowTools::new([NAME])),
    )
    .unwrap()
}

#[test]
fn schema_accepts_one_string_only() {
    let schema = ViewImage.definition().parameters;
    let validator = jsonschema::validator_for(&schema).unwrap();
    for allowed in [
        "file:///workspace/a.png",
        "https://example.invalid/a.png",
        "{{var:base64:preview}}",
    ] {
        assert!(validator.is_valid(&json!({"image_url":allowed})));
    }
    for rejected in [
        json!({}),
        json!({"image_url":true}),
        json!({"image_url":null}),
        json!({"image_url":""}),
        json!({"variable":SELECTOR}),
        json!({"image_url":"png","path":"a"}),
    ] {
        assert!(!validator.is_valid(&rejected), "{rejected}");
    }
}

#[test]
fn https_archive_restores_current_and_legacy_frozen_calls_unchanged() {
    use crate::context::Context;
    use microcrab_transport::{CarrierEnvelopeVersion, UserSource};

    let arguments = json!({"image_url":"https://example.org/private-path?signature=opaque-value"});
    let source = Source::Https(
        microcrab_transport::public_https::PublicHttpsUrl::parse(
            arguments["image_url"].as_str().unwrap(),
        )
        .unwrap(),
    );
    let call = ToolCall {
        id: "archive".into(),
        name: NAME.into(),
        arguments,
        provider_metadata: None,
    };
    let approval = approval_preview(&call.arguments, &source);
    assert_eq!(approval.owner["image_url"], call.arguments["image_url"]);
    assert!(!approval.reviewer.to_string().contains("opaque-value"));
    let legacy = ToolCall {
        arguments: source.policy_arguments(),
        ..call.clone()
    };
    let previously_redacted = ToolCall {
        arguments: json!({"image_url":"[HTTPS image URL redacted]"}),
        ..call.clone()
    };
    for call in [call, legacy, previously_redacted] {
        let mut context =
            Context::new("system".into(), vec![ViewImage.definition()], 65_536).unwrap();
        context
            .append_response(&Response {
                content: String::new(),
                tool_calls: vec![call],
                provider_metadata: None,
                usage: Usage::default(),
            })
            .unwrap();
        context
            .append_tool_result_with_parts(
                "archive".into(),
                r#"{"source_kind":"https","status":"awaiting_provider_and_archive"}"#.into(),
                vec![],
            )
            .unwrap();
        context
            .append_retained_tool_image(
                "archive",
                ContentItem::User {
                    parts: vec![
                        InputPart::Text {
                            text: "retained".into(),
                        },
                        InputPart::Image {
                            source: ImageSource::Data {
                                media_type: "image/png".into(),
                                data: IMAGE.to_vec(),
                            },
                        },
                    ],
                    envelope: Some(CarrierEnvelopeVersion::RetainedToolImage {
                        call_id: "archive".into(),
                    }),
                    source: Some(UserSource::Framework),
                },
            )
            .unwrap();
        let frozen = serde_json::to_vec(&context.snapshot().unwrap()).unwrap();
        let restored = Context::from_snapshot(serde_json::from_slice(&frozen).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_vec(&restored.snapshot().unwrap()).unwrap(),
            frozen
        );
    }
}

#[tokio::test]
async fn no_host_root_admits_only_exact_variable_to_canonical_inline_data() {
    let script = Script::default();
    let audit = Audit::default();
    let session = Session::builder(script.clone(), runtime())
        .auditor(audit.clone())
        .build()
        .unwrap();
    let state = session.state_handle();
    let mut upload = state
        .prepare_bytes_write(
            "raster",
            "image/png",
            IMAGE.len() as u64,
            state.inspection().unwrap().witness,
        )
        .unwrap();
    upload.append(IMAGE).unwrap();
    let receipt = state.commit_bytes(upload).await.unwrap();
    assert!(session.context_snapshot().unwrap().items.is_empty());
    *script.0.lock().unwrap() =
        Some(json!({"image_url":format!("{{{{var:base64:@{}}}}}", receipt.metadata.selector)}));
    session
        .submit("Inspect the explicit version")
        .await
        .unwrap();
    let authored =
        json!({"image_url":format!("{{{{var:base64:@{}}}}}", receipt.metadata.selector)});
    audit.check("inspect", authored.clone());
    let context = session.context_snapshot().unwrap();
    assert_eq!(replay_arguments(&context), &authored);
    let (content, parts) = context
        .items
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool { content, parts, .. } => Some((content, parts)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(content).unwrap(),
        json!({"byte_length":IMAGE.len(),"media_type":"image/png","width":1,"height":1,"source_kind":"bytes"})
    );
    assert_eq!(
        parts,
        &[InputPart::Image {
            source: ImageSource::Data {
                media_type: "image/png".into(),
                data: IMAGE.to_vec()
            }
        }]
    );
    let fork = session
        .fork(Session::builder(Script::default(), runtime()))
        .await
        .unwrap();
    assert!(fork.state_handle().inspection().unwrap().bytes.is_empty());
    assert_eq!(fork.context_snapshot().unwrap().items, context.items);
    assert_eq!(state.inspection().unwrap().bytes.len(), 1);
    state
        .remove_bytes_checked("raster", receipt.commit.after)
        .await
        .unwrap();
    assert!(state.inspection().unwrap().bytes.is_empty());
    assert_eq!(session.context_snapshot().unwrap().items, context.items);
    fork.close().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test]
async fn bound_host_uses_signature_and_keeps_confined_file_policy() {
    let area = tempfile::tempdir().unwrap();
    let root = area.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("image name.not-a-png-extension"), IMAGE).unwrap();
    std::fs::write(root.join("protected.png"), IMAGE).unwrap();
    std::fs::write(root.join("large.png"), vec![b'x'; 1024 * 1024 + 1]).unwrap();
    std::fs::write(area.path().join("outside.png"), IMAGE).unwrap();
    let host = Arc::new(
        microcrab_host_local::LocalHost::new_filesystem(&root, Default::default()).unwrap(),
    );
    host.protect_workspace_path(root.join("protected.png"))
        .unwrap();
    let file_url = |path| {
        reqwest::Url::from_file_path(root.join(path))
            .unwrap()
            .to_string()
    };
    for (uri, success) in [
        (file_url("image name.not-a-png-extension"), true),
        (file_url("protected.png"), false),
        (file_url("../outside.png"), false),
        (file_url("large.png"), false),
        ("file://host/workspace/p.png".into(), false),
    ] {
        let script = Script(Arc::new(Mutex::new(Some(json!({"image_url":uri})))));
        let audit = Audit::default();
        let session = Session::builder(script, runtime())
            .auditor(audit.clone())
            .host(host.clone())
            .build()
            .unwrap();
        session.submit("Use this exact path").await.unwrap();
        let snapshot = session.context_snapshot().unwrap();
        let parts = snapshot
            .items
            .iter()
            .find_map(|item| match item {
                ContentItem::Tool { parts, .. } => Some(parts),
                _ => None,
            })
            .unwrap();
        audit.check("inspect", json!({"image_url":uri}));
        assert_eq!(replay_arguments(&snapshot), &json!({"image_url":uri}));
        if success {
            assert_eq!(
                parts,
                &[InputPart::Image {
                    source: ImageSource::Data {
                        media_type: "image/png".into(),
                        data: IMAGE.to_vec()
                    }
                }]
            );
        } else {
            assert!(parts.is_empty());
        }
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn ask_uses_authored_preview_but_permission_matches_bound_bytes() {
    use crate::policy::{
        Action, AskReply, Decision, PermissionError, PermissionFeatures, PermissionRequest,
        PolicySnapshot, SessionPermission,
    };

    #[derive(Clone)]
    struct Approval(Audit);
    #[async_trait]
    impl SessionPermission for Approval {
        fn features(&self) -> PermissionFeatures {
            PermissionFeatures {
                interactive_ask: true,
                ..Default::default()
            }
        }
        async fn evaluate(
            &self,
            _: &PolicySnapshot,
            _: &PermissionRequest,
        ) -> Result<Decision, PermissionError> {
            panic!("admission must evaluate the native selector policy")
        }
        async fn ask(&self, request: &PermissionRequest) -> Result<AskReply, PermissionError> {
            assert_eq!(
                request.arguments,
                json!({"image_url":"{{var:base64:raster}}"})
            );
            let stages = self.0.0.lock().unwrap();
            assert!(
                matches!(stages.as_slice(), [(Moment::ToolCall, body)] if body.tool_call_id.as_deref() == Some(request.call_id.as_str()) && body.authored_image_arguments)
            );
            Ok(AskReply {
                action: Action::Allow,
                reason: "owner_once".into(),
            })
        }
    }

    let audit = Audit::default();
    let runtime = Runtime::new(
        vec![ToolRegistration::Builtin(BuiltinTool::ViewImage)],
        Arc::new(Approval(audit.clone())),
    )
    .unwrap();
    let arguments = json!({"image_url":"{{var:base64:raster}}"});
    let script = Script(Arc::new(Mutex::new(Some(arguments.clone()))));
    let session = Session::builder(script, runtime)
        .auditor(audit.clone())
        .build()
        .unwrap();
    let state = session.state_handle();
    let mut upload = state
        .prepare_bytes_write(
            "raster",
            "image/png",
            IMAGE.len() as u64,
            state.inspection().unwrap().witness,
        )
        .unwrap();
    upload.append(IMAGE).unwrap();
    state.commit_bytes(upload).await.unwrap();
    session.submit("inspect Bytes").await.unwrap();
    audit.check("inspect", arguments.clone());
    let context = session.context_snapshot().unwrap();
    assert_eq!(replay_arguments(&context), &arguments);
    assert!(context.items.iter().any(|item| matches!(item, ContentItem::Tool { parts, .. } if matches!(parts.as_slice(), [InputPart::Image { source: ImageSource::Data { data, .. } }] if data == IMAGE))));
    session.close().await.unwrap();
}

#[tokio::test]
async fn inline_encodings_admit_exact_bytes_and_retain_literal_arguments() {
    let encoded = STANDARD.encode(IMAGE);
    for source in [encoded.clone(), format!("data:image/png;base64,{encoded}")] {
        let script = Script(Arc::new(Mutex::new(Some(json!({"image_url":source})))));
        let audit = Audit::default();
        let session = Session::builder(script, runtime())
            .auditor(audit.clone())
            .build()
            .unwrap();
        session.submit("inspect inline image").await.unwrap();
        audit.check("inspect", json!({"image_url": source}));
        let snapshot = session.context_snapshot().unwrap();
        assert_eq!(replay_arguments(&snapshot), &json!({"image_url":source}));
        assert!(snapshot.items.iter().any(|item| matches!(item, ContentItem::Tool { parts, .. } if matches!(parts.as_slice(), [InputPart::Image { source: ImageSource::Data { data, .. } }] if data == IMAGE))));
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn visible_image_source_is_injected_only_in_core_dispatch() {
    let uri = format!("data:image/png;base64,{}", STANDARD.encode(IMAGE));
    let authored = json!({"image_url":"{{var:string:picture}}"});
    let audit = Audit::default();
    let session = Session::builder(
        Script(Arc::new(Mutex::new(Some(authored.clone())))),
        runtime(),
    )
    .auditor(audit.clone())
    .build()
    .unwrap();
    session
        .state_handle()
        .inject("picture", json!(uri))
        .await
        .unwrap();
    session
        .submit("look at the referenced picture")
        .await
        .unwrap();
    audit.check("inspect", authored.clone());
    let snapshot = session.context_snapshot().unwrap();
    assert_eq!(replay_arguments(&snapshot), &authored);
    assert!(snapshot.items.iter().any(|item| matches!(item, ContentItem::Tool { parts, .. } if matches!(parts.as_slice(), [InputPart::Image { source: ImageSource::Data { data, .. } }] if data == IMAGE))));
    session.close().await.unwrap();
}

#[tokio::test]
async fn identical_boolean_calls_stop_after_three_rounds() {
    struct Booleans(std::sync::atomic::AtomicUsize);
    #[async_trait]
    impl Client for Booleans {
        async fn complete(
            &self,
            _: &crate::context::Content,
            _: &dyn Auditor,
            _: &dyn VariableView,
            _: CancellationToken,
        ) -> Result<Response, microcrab_transport::Error> {
            let count = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            assert!(count < 14);
            Ok(Response {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: format!("view-{count}"),
                    name: NAME.into(),
                    arguments: json!({"image_url":true}),
                    provider_metadata: None,
                }],
                provider_metadata: None,
                usage: Usage::default(),
            })
        }
    }
    let audit = Audit::default();
    let session = Session::builder(Booleans(std::sync::atomic::AtomicUsize::new(0)), runtime())
        .auditor(audit.clone())
        .build()
        .unwrap();
    let result = session.submit("inspect image").await.unwrap();
    assert_eq!(result.rounds, 3);
    assert_eq!(
        result.stop_reason,
        crate::runtime::TurnStopReason::RepeatedInvalidToolArguments
    );
    let snapshot = session.context_snapshot().unwrap();
    assert_eq!(snapshot.items.iter().filter(|item| matches!(item, ContentItem::Tool { content, parts, .. } if parts.is_empty() && content.contains("received a boolean"))).count(), 3);
    for i in 0..3 {
        audit.check(&format!("view-{i}"), json!({"image_url":true}));
    }
    assert_eq!(replay_arguments(&snapshot), &json!({"image_url":true}));
    session.close().await.unwrap();
}

#[tokio::test]
async fn delegated_session_catalog_drops_the_root_builtin() {
    let session = Session::builder(Script::default(), runtime())
        .is_main(false)
        .build()
        .unwrap();
    assert_eq!(session.kind(), SessionKind::Delegated);
    assert!(
        session
            .context_snapshot()
            .unwrap()
            .tools
            .iter()
            .all(|tool| tool.name != NAME)
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn one_shot_cannot_advertise_or_use_the_root_builtin() {
    assert!(
        Session::builder(Script::default(), runtime())
            .kind(SessionKind::OneShot)
            .build()
            .is_err()
    );
    let empty = Runtime::new(vec![], Arc::new(super::super::DenyAll)).unwrap();
    let session = Session::builder(Script::default(), empty)
        .kind(SessionKind::OneShot)
        .build()
        .unwrap();
    let state = session.state_handle();
    assert!(
        state
            .prepare_bytes_write(
                "raster",
                "image/png",
                1,
                state.inspection().unwrap().witness
            )
            .is_err()
    );
    session.close().await.unwrap();
}
