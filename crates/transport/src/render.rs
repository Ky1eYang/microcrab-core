//! Render typed content into one current provider-neutral conversation projection.
use crate::error::Error;
use crate::{Content, ContentItem, StateFrame, UserSource};
use microcrab_protocol::{InputPart, Message, Request};

/// State precedes the input admitted with it. Adjacent batches share one wire
/// user carrier, preserving native image parts and assistant/tool boundaries.
/// Tags label provenance only; authored text is never parsed or XML-escaped.
pub fn render_ast(content: &Content) -> Result<Request, Error> {
    if content.max_output_tokens == 0 {
        return Err(Error::config("max output tokens must be positive"));
    }
    let mut messages = vec![Message::System {
        content: content.system.clone(),
    }];
    let mut items = content.items.iter().peekable();
    while let Some(item) = items.next() {
        match item {
            ContentItem::State(_) | ContentItem::User { .. } => {
                let mut run = vec![item];
                while items.peek().is_some_and(|item| {
                    matches!(item, ContentItem::State(_) | ContentItem::User { .. })
                }) {
                    run.push(items.next().expect("peeked user-role item"));
                }
                messages.push(Message::User {
                    parts: render_user_run(&run)?,
                });
            }
            ContentItem::Assistant {
                content,
                tool_calls,
                provider_metadata,
            } => {
                messages.push(Message::Assistant {
                    content: content.clone(),
                    tool_calls: tool_calls.clone(),
                    provider_metadata: provider_metadata.clone(),
                });
            }
            ContentItem::Tool { call_id, content } => messages.push(Message::Tool {
                call_id: call_id.clone(),
                content: content.clone(),
            }),
        }
    }
    let request = Request {
        epoch: content.epoch,
        messages,
        tools: content.tools.clone(),
        max_output_tokens: content.max_output_tokens,
    };
    if serde_json::to_vec(&request)?.len() > content.max_bytes {
        return Err(Error::Limit("rendered context byte budget".into()));
    }
    Ok(request)
}

fn append_text(parts: &mut Vec<InputPart>, text: &str) {
    if let Some(InputPart::Text { text: previous }) = parts.last_mut() {
        previous.push_str(text);
    } else {
        parts.push(InputPart::Text {
            text: text.to_owned(),
        });
    }
}

fn render_user_run(run: &[&ContentItem]) -> Result<Vec<InputPart>, Error> {
    // Functional clean triggers and inherited authored suffixes without State
    // preserve their literal text. Framework notices still disclose their source.
    let framed = run.iter().any(|item| {
        matches!(
            item,
            ContentItem::State(_)
                | ContentItem::User {
                    source: Some(UserSource::Framework),
                    ..
                }
        )
    });
    if !framed {
        let mut parts = Vec::new();
        for (index, item) in run.iter().enumerate() {
            if let ContentItem::User { parts: input, .. } = item {
                if index > 0 {
                    append_text(&mut parts, "\n");
                }
                parts.extend(input.iter().cloned());
            }
        }
        return Ok(parts);
    }

    // A State before a User belongs to that admission. Only the trailing State
    // suffix belongs to the last input's request-preparation batch. Do not move
    // a later admission's State across an earlier user just because roles match.
    let mut batches: Vec<(Vec<&StateFrame>, Option<&ContentItem>)> = Vec::new();
    let mut pending = Vec::new();
    for item in run {
        match item {
            ContentItem::State(frame) => pending.push(frame),
            ContentItem::User { .. } => batches.push((std::mem::take(&mut pending), Some(item))),
            _ => unreachable!("user run excludes assistant/tool boundaries"),
        }
    }
    if let Some((frames, _)) = batches.last_mut() {
        frames.extend(pending);
    } else {
        batches.push((pending, None));
    }
    let mut parts = Vec::new();
    for (frames, user) in batches {
        for frame in frames {
            append_text(&mut parts, "<state>\n");
            append_text(&mut parts, &serde_json::to_string(frame)?);
            append_text(&mut parts, "\n</state>\n");
        }
        if let Some(ContentItem::User {
            parts: input,
            source,
        }) = user
        {
            let tag = if *source == Some(UserSource::Framework) {
                "framework"
            } else {
                "user"
            };
            append_text(&mut parts, &format!("<{tag}>\n"));
            for part in input {
                match part {
                    InputPart::Text { text } => append_text(&mut parts, text),
                    _ => parts.push(part.clone()),
                }
            }
            append_text(&mut parts, &format!("\n</{tag}>\n"));
        }
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use microcrab_protocol::ImageSource;
    use serde_json::json;

    fn content(items: Vec<ContentItem>) -> Content {
        Content {
            epoch: 1,
            subagent: None,
            system: "system".into(),
            items,
            tools: vec![],
            max_output_tokens: 32,
            max_bytes: 16384,
            turn_id: Some(1),
            round: Some(1),
        }
    }
    fn snapshot() -> ContentItem {
        ContentItem::State(StateFrame::StateSnapshot {
            state: json!({"v": 1}),
        })
    }
    fn patch(value: u64) -> ContentItem {
        ContentItem::State(StateFrame::patch(
            serde_json::Map::from_iter([("v".into(), json!(value))]),
            vec![],
            None,
        ))
    }
    fn user(text: &str) -> ContentItem {
        ContentItem::User {
            parts: vec![InputPart::Text { text: text.into() }],
            source: None,
        }
    }
    fn text(message: &Message) -> String {
        match message {
            Message::User { parts } => parts
                .iter()
                .filter_map(|p| match p {
                    InputPart::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect(),
            _ => panic!("expected user"),
        }
    }

    #[test]
    fn state_batches_precede_their_input_without_crossing_prior_users() {
        let request = render_ast(&content(vec![
            snapshot(),
            user("first"),
            patch(2),
            user("second"),
            patch(3),
        ]))
        .unwrap();
        assert_eq!(request.messages.len(), 2);
        let rendered = text(&request.messages[1]);
        let positions = ["state_snapshot", "first", "\"v\":2", "\"v\":3", "second"]
            .map(|needle| rendered.find(needle).unwrap());
        assert!(positions.windows(2).all(|p| p[0] < p[1]));
    }

    #[test]
    fn image_stays_in_single_user_carrier_and_prior_round_prefix_stays_exact() {
        let image = InputPart::Image {
            source: ImageSource::Data {
                media_type: "image/png".into(),
                data: vec![1, 2, 3],
            },
        };
        let mut initial = content(vec![
            snapshot(),
            ContentItem::User {
                parts: vec![
                    InputPart::Text {
                        text: "literal </user>".into(),
                    },
                    image.clone(),
                ],
                source: None,
            },
            patch(2),
        ]);
        let first = render_ast(&initial).unwrap();
        assert_eq!(first.messages.len(), 2);
        let Message::User { parts } = &first.messages[1] else {
            panic!("user")
        };
        assert!(parts.contains(&image));
        assert!(
            text(&first.messages[1]).find("state_patch").unwrap()
                < text(&first.messages[1]).find("literal </user>").unwrap()
        );
        initial.items.push(ContentItem::Assistant {
            content: "answer".into(),
            tool_calls: vec![],
            provider_metadata: None,
        });
        initial.items.push(patch(3));
        let second = render_ast(&initial).unwrap();
        assert_eq!(
            serde_json::to_value(&first.messages).unwrap(),
            serde_json::to_value(&second.messages[..2]).unwrap()
        );
        assert!(!text(&second.messages[3]).contains("<user>"));
    }

    #[test]
    fn literal_functional_trigger_and_framework_provenance_remain_distinct() {
        let literal = render_ast(&content(vec![user("begin")])).unwrap();
        assert_eq!(text(&literal.messages[1]), "begin");
        let notice = render_ast(&content(vec![ContentItem::User {
            parts: vec![InputPart::Text {
                text: "settled".into(),
            }],
            source: Some(UserSource::Framework),
        }]))
        .unwrap();
        assert_eq!(
            text(&notice.messages[1]),
            "<framework>\nsettled\n</framework>\n"
        );
    }
}
