use super::*;
use microcrab_protocol::{ImageSource, ToolCall, Usage};
fn image() -> InputPart {
    let mut data = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    data.extend_from_slice(&1u32.to_be_bytes());
    data.extend_from_slice(&1u32.to_be_bytes());
    data.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    InputPart::Image {
        source: ImageSource::Data {
            media_type: "image/png".into(),
            data,
        },
    }
}
fn pending() -> Context {
    let mut context = Context::new(
        "frozen".into(),
        vec![ToolDefinition {
            name: "image".into(),
            description: "inspect".into(),
            parameters: serde_json::json!({"type":"object"}),
        }],
        65536,
    )
    .unwrap();
    context
        .admit_user("inspect", &serde_json::json!({}))
        .unwrap();
    context
        .append_response(&Response {
            content: String::new(),
            tool_calls: vec![ToolCall {
                id: "call".into(),
                name: "image".into(),
                arguments: serde_json::json!({}),
                provider_metadata: None,
            }],
            usage: Usage::default(),
            provider_metadata: None,
        })
        .unwrap();
    context
}
#[test]
fn tool_media_roundtrips_exact_data_and_measurement() {
    let mut context = pending();
    context
        .append_tool_result_with_parts("call".into(), "caption".into(), vec![image()])
        .unwrap();
    assert!(
        TokenEstimate::conservative_text(&context.content(32).unwrap(), "route".into(), 4096)
            .unwrap()
            .is_none()
    );
    let snapshot = context.snapshot().unwrap();
    let bytes = serde_json::to_vec(&snapshot).unwrap();
    let restored = Context::from_snapshot(serde_json::from_slice(&bytes).unwrap()).unwrap();
    assert_eq!(
        serde_json::to_vec(&restored.snapshot().unwrap()).unwrap(),
        bytes
    );
    assert_eq!(
        context.measure().unwrap().bytes,
        restored.measure().unwrap().bytes
    );
    assert!(
        matches!(&restored.items().last().unwrap(), ContentItem::Tool { parts, .. } if parts == &vec![image()])
    );
    let measurement = context.measure().unwrap();
    let exact =
        serde_json::json!({"system":context.system,"items":context.items,"tools":context.tools})
            .to_string()
            .len();
    assert_eq!(measurement.bytes, exact);
}
#[test]
fn invalid_tool_media_never_mutates_pending_history_and_restore_rejects_route_hints() {
    for parts in [
        vec![InputPart::Image {
            source: ImageSource::Url {
                url: "https://example.invalid/routehint".into(),
            },
        }],
        vec![image(), image()],
        vec![InputPart::Image {
            source: ImageSource::Data {
                media_type: "image/png".into(),
                data: vec![1, 2, 3],
            },
        }],
    ] {
        let mut context = pending();
        let before = context.measure().unwrap().fingerprint;
        assert!(
            context
                .append_tool_result_with_parts("call".into(), "caption".into(), parts)
                .is_err()
        );
        assert_eq!(context.measure().unwrap().fingerprint, before);
        assert_eq!(context.pending_call_ids(), ["call"]);
    }
    let mut context = pending();
    context
        .append_tool_result_with_parts("call".into(), "caption".into(), vec![image()])
        .unwrap();
    let mut snapshot = context.snapshot().unwrap();
    if let Some(ContentItem::Tool { parts, .. }) = snapshot.items.last_mut() {
        *parts = vec![InputPart::Image {
            source: ImageSource::Attachment {
                attachment_id: "ephemeral".into(),
            },
        }];
    }
    assert!(Context::from_snapshot(snapshot).is_err());
}
#[test]
fn worst_case_numeric_image_byte_reservation_bounds_exact_tool_item() {
    for count in [1, 33, 64, 1024] {
        let item = ContentItem::Tool {
            call_id: "call".into(),
            content: String::new(),
            parts: vec![InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/jpeg".into(),
                    data: vec![255; count],
                },
            }],
        };
        let empty = ContentItem::Tool {
            call_id: "call".into(),
            content: String::new(),
            parts: vec![],
        };
        let added =
            serde_json::to_vec(&item).unwrap().len() - serde_json::to_vec(&empty).unwrap().len();
        assert!(added <= crate::admission::image::tool_context_reservation(count));
    }
    assert_eq!(crate::admission::image::tool_context_reservation(0), 0);
}
