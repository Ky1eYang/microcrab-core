//! Incremental SSE framing and OpenAI delta accumulation. Raw transport bytes stay
//! with the audit guard; this decoder owns only the unfinished line and aggregate.
use crate::ModelEvent;
use crate::error::Error;
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct Decoder {
    pending: Vec<u8>,
    data: Vec<String>,
    content: String,
    parts: Vec<Value>,
    calls: BTreeMap<u32, PartialCall>,
    finish: Option<String>,
    usage: Vec<Value>,
    done: bool,
    unrecognized_delta: bool,
}
#[derive(Default)]
struct PartialCall {
    id: Option<String>,
    name: String,
    arguments: String,
}

impl Decoder {
    pub(super) fn push(&mut self, bytes: &[u8], eof: bool) -> Result<Vec<ModelEvent>, Error> {
        self.pending.extend_from_slice(bytes);
        let mut events = Vec::new();
        let mut consumed = 0;
        loop {
            let rest = &self.pending[consumed..];
            let Some(end) = rest.iter().position(|byte| matches!(byte, b'\r' | b'\n')) else {
                break;
            };
            if rest[end] == b'\r' && end + 1 == rest.len() && !eof {
                break;
            }
            let width = if rest[end] == b'\r' && rest.get(end + 1) == Some(&b'\n') {
                2
            } else {
                1
            };
            let line = std::str::from_utf8(&rest[..end])
                .map_err(|error| {
                    Error::decode_reason(format!("SSE UTF-8 decoding failed: {error}"))
                })?
                .to_owned();
            consumed += end + width;
            self.line(&line, &mut events)?;
        }
        self.pending.drain(..consumed);
        if eof {
            self.completion()?;
        }
        Ok(events)
    }

    /// A gateway can omit the semantic stop reason while explicitly closing a
    /// complete text stream. Preserve that unknown; never infer tool completion.
    fn completion(&self) -> Result<bool, Error> {
        if !self.pending.is_empty() || !self.data.is_empty() {
            return Err(Error::decode_reason(
                "provider SSE ended with an incomplete line or event",
            ));
        }
        if !self.done {
            return Err(Error::decode_reason(
                "provider SSE ended without its DONE terminal marker",
            ));
        }
        if self.finish.is_some() {
            return Ok(false);
        }
        let measured = self
            .usage
            .last()
            .map(|usage| super::decode_usage(Some(usage)))
            .transpose()?;
        if !self.content.is_empty()
            && self.calls.is_empty()
            && !self.unrecognized_delta
            && measured
                .is_some_and(|usage| usage.input_tokens.is_some() && usage.output_tokens.is_some())
        {
            return Ok(true);
        }
        Err(Error::decode_reason(
            "provider SSE received DONE but omitted finish_reason; only nonempty text without tool calls and with measured input/output usage may complete",
        ))
    }

    fn line(&mut self, line: &str, events: &mut Vec<ModelEvent>) -> Result<(), Error> {
        if line.is_empty() {
            if self.data.is_empty() {
                return Ok(());
            }
            let data = std::mem::take(&mut self.data).join("\n");
            if self.done {
                return Err(Error::protocol("data after SSE terminal"));
            }
            if data == "[DONE]" {
                self.done = true;
                return Ok(());
            }
            let value: Value = serde_json::from_str(&data).map_err(Error::decode)?;
            return self.chunk(value, events);
        }
        if line.starts_with(':') {
            return Ok(());
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        if field == "data" {
            self.data
                .push(value.strip_prefix(' ').unwrap_or(value).to_owned());
        }
        Ok(())
    }

    fn chunk(&mut self, value: Value, events: &mut Vec<ModelEvent>) -> Result<(), Error> {
        if value.get("error").is_some_and(|error| !error.is_null()) {
            return Err(crate::failure::provider_failure(&value));
        }
        if let Some(usage) = value.get("usage").filter(|v| !v.is_null()) {
            // Snapshots are cumulative, not separate attempts. Validate each
            // object and retain it within the transport's response byte budget.
            super::decode_usage(Some(usage))?;
            self.usage.push(usage.clone());
        }
        let choices = value
            .get("choices")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::protocol("missing SSE choices"))?;
        if choices.is_empty() {
            return Ok(());
        }
        if choices.len() != 1
            || choices[0]
                .get("index")
                .is_some_and(|index| index.as_u64() != Some(0))
        {
            return Err(Error::protocol("expected one SSE choice at index zero"));
        }
        let choice = &choices[0];
        let delta = choice
            .get("delta")
            .and_then(Value::as_object)
            .ok_or_else(|| Error::protocol("missing SSE delta"))?;
        self.unrecognized_delta |= delta.keys().any(|key| {
            !matches!(
                key.as_str(),
                "role" | "content" | "reasoning_content" | "reasoning" | "tool_calls"
            )
        });
        if let Some(role) = delta.get("role")
            && role.as_str() != Some("assistant")
        {
            return Err(Error::protocol("invalid SSE role"));
        }
        let reason = choice
            .get("finish_reason")
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| Error::protocol("invalid finish reason"))
            })
            .transpose()?;
        // Validate both aliases even when the other supplies display text. A
        // malformed wrapper must not become harmless merely because we finished.
        for key in ["content", "reasoning_content", "reasoning"] {
            if delta
                .get(key)
                .is_some_and(|value| !value.is_null() && !value.is_string())
            {
                return Err(Error::protocol("unsupported SSE content"));
            }
        }
        if let Some(calls) = delta.get("tool_calls").filter(|value| !value.is_null())
            && !calls.is_array()
        {
            return Err(Error::protocol("invalid SSE tool calls"));
        }
        if let Some(finish) = &self.finish {
            if reason.is_some_and(|reason| reason != finish) {
                return Err(Error::protocol("conflicting SSE finish reason"));
            }
            if ["content", "reasoning_content", "reasoning"]
                .iter()
                .any(|key| {
                    delta
                        .get(*key)
                        .and_then(Value::as_str)
                        .is_some_and(|text| !text.is_empty())
                })
                || delta
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .is_some_and(|calls| !calls.is_empty())
            {
                return Err(Error::protocol("content after SSE finish reason"));
            }
            if delta.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "role" | "content" | "reasoning_content" | "reasoning" | "tool_calls"
                )
            }) {
                return Err(Error::protocol(
                    "unexpected SSE delta field after finish reason",
                ));
            }
            // Cumulative usage was retained above. Redundant terminal wrappers
            // never replay text, reasoning, tool deltas, or a completion event.
            return Ok(());
        }
        if let Some(reasoning) = delta
            .get("reasoning_content")
            .filter(|value| !value.is_null())
            .or_else(|| delta.get("reasoning"))
            .filter(|v| !v.is_null())
            && let Some(text) = reasoning.as_str()
            && !text.is_empty()
        {
            self.parts.push(json!({"kind":"reasoning","text":text}));
            events.push(ModelEvent::ReasoningDelta {
                text: text.to_owned(),
            });
        }
        if let Some(text) = delta.get("content").filter(|v| !v.is_null()) {
            let text = text
                .as_str()
                .ok_or_else(|| Error::protocol("unsupported SSE content"))?;
            self.content.push_str(text);
            if !text.is_empty() {
                self.parts.push(json!({"kind":"text","text":text}));
            }
            if !text.is_empty() {
                events.push(ModelEvent::TextDelta {
                    text: text.to_owned(),
                });
            }
        }
        if let Some(calls) = delta.get("tool_calls").filter(|v| !v.is_null()) {
            for call in calls
                .as_array()
                .ok_or_else(|| Error::protocol("invalid SSE tool calls"))?
            {
                let index = call
                    .get("index")
                    .and_then(Value::as_u64)
                    .and_then(|v| u32::try_from(v).ok())
                    .ok_or_else(|| Error::protocol("invalid SSE tool index"))?;
                if let Some(kind) = call.get("type")
                    && kind.as_str() != Some("function")
                {
                    return Err(Error::protocol("unsupported SSE tool type"));
                }
                // Compatible providers may repeat an empty placeholder on argument
                // chunks. It carries no identity update for either the accumulator
                // or observers; nonempty IDs remain exact and conflict-checked.
                let id = optional_text(call.get("id"))?.filter(|id| !id.is_empty());
                let function = call.get("function");
                if function.is_some_and(|f| !f.is_object()) {
                    return Err(Error::protocol("invalid SSE function"));
                }
                let name = optional_text(function.and_then(|f| f.get("name")))?;
                let arguments =
                    optional_text(function.and_then(|f| f.get("arguments")))?.unwrap_or_default();
                if !self.calls.contains_key(&index) {
                    self.parts.push(json!({"kind":"tool_call","index":index}));
                }
                let partial = self.calls.entry(index).or_default();
                if let Some(id) = &id {
                    if partial.id.as_ref().is_some_and(|old| old != id) {
                        return Err(Error::protocol("conflicting SSE tool id"));
                    }
                    partial.id = Some(id.clone());
                }
                if let Some(name) = &name {
                    partial.name.push_str(name);
                }
                partial.arguments.push_str(&arguments);
                events.push(ModelEvent::ToolCallDelta {
                    index,
                    id,
                    name,
                    arguments,
                });
            }
        }
        if let Some(reason) = reason {
            self.finish = Some(reason.to_owned());
        }
        Ok(())
    }

    pub(super) fn into_value(self) -> Result<(Value, Vec<Value>, bool), Error> {
        let gateway_text_complete = self.completion()?;
        let mut calls = Vec::new();
        for (expected, (index, call)) in self.calls.into_iter().enumerate() {
            if usize::try_from(index).ok() != Some(expected) {
                return Err(Error::protocol("noncontiguous SSE tool indices"));
            }
            let id = call
                .id
                .ok_or_else(|| Error::protocol("missing SSE tool id"))?;
            calls.push(json!({"id":id,"type":"function","function":{"name":call.name,"arguments":call.arguments}}));
        }
        let mut parts = self.parts;
        // Wire indices define final call order even when fragments arrive out of
        // order. Keep every text/reasoning part in place and fill only the call
        // slots from the same canonical sequence used for the final call array.
        let mut canonical_calls = calls.iter();
        for part in &mut parts {
            if part["kind"] == "tool_call" {
                let call = canonical_calls
                    .next()
                    .ok_or_else(|| Error::protocol("replay call slot has no canonical call"))?;
                part.as_object_mut()
                    .expect("constructed part object")
                    .remove("index");
                part["call_id"] = call["id"].clone();
            }
        }
        if canonical_calls.next().is_some() {
            return Err(Error::protocol("canonical call has no replay call slot"));
        }
        let reasoning = parts
            .iter()
            .filter(|p| p["kind"] == "reasoning")
            .filter_map(|p| p["text"].as_str())
            .collect::<String>();
        let value = json!({"choices":[{"finish_reason":self.finish,"message":{"role":"assistant","content":self.content,"reasoning_content":reasoning,"tool_calls":calls,"__microcrab_parts":parts}}],"usage":self.usage.last()});
        Ok((value, self.usage, gateway_text_complete))
    }
}

fn optional_text(value: Option<&Value>) -> Result<Option<String>, Error> {
    value
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::protocol("invalid SSE string field"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn out_of_order_tool_fragments_use_canonical_call_slots_without_reordering_text_or_reasoning() {
        use microcrab_protocol::{
            AssistantPart, AssistantProviderMetadata, Request, ToolDefinition,
        };
        let chunks = [
            json!({"choices":[{"delta":{"reasoning":"first thought","content":"before"}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":1,"id":"two","type":"function","function":{"name":"echo","arguments":"{\"b\":"}}]}}]}),
            json!({"choices":[{"delta":{"reasoning":"middle thought","content":"between"}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"one","type":"function","function":{"name":"echo","arguments":"{\"a\":"}}]}}]}),
            json!({"choices":[{"delta":{"content":"after","tool_calls":[{"index":0,"function":{"arguments":"1}"}},{"index":1,"function":{"arguments":"2}"}}]},"finish_reason":"tool_calls"}]}),
            json!({"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":5}}),
        ];
        let wire = chunks
            .iter()
            .map(|chunk| format!("data: {chunk}\r\n\r\n"))
            .collect::<String>()
            + "data: [DONE]\r\n\r\n";
        let mut decoder = Decoder::default();
        let mut events = Vec::new();
        for byte in wire.bytes() {
            events.extend(decoder.push(&[byte], false).unwrap());
        }
        decoder.push(&[], true).unwrap();
        let observed = events
            .iter()
            .filter_map(|event| match event {
                ModelEvent::ToolCallDelta { index, .. } => Some(*index),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(observed, vec![1, 0, 0, 1]);
        let (value, snapshots, _) = decoder.into_value().unwrap();
        let request = Request {
            epoch: 1,
            messages: vec![],
            tools: vec![ToolDefinition {
                name: "echo".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }],
            max_output_tokens: 100,
        };
        let response =
            super::super::decode_response(value, &request, Some(snapshots), false).unwrap();
        assert_eq!(
            response
                .tool_calls
                .iter()
                .map(|call| call.id.as_str())
                .collect::<Vec<_>>(),
            vec!["one", "two"]
        );
        assert_eq!(response.tool_calls[0].arguments, json!({"a":1}));
        assert_eq!(response.tool_calls[1].arguments, json!({"b":2}));
        assert_eq!(response.content, "beforebetweenafter");
        let restored: microcrab_protocol::Response =
            serde_json::from_slice(&serde_json::to_vec(&response).unwrap()).unwrap();
        let metadata = restored.provider_metadata.as_ref().unwrap();
        metadata
            .validate_projection(&restored.content, &restored.tool_calls)
            .unwrap();
        let AssistantProviderMetadata::Neutral { parts, .. } = metadata else {
            panic!("neutral replay")
        };
        assert_eq!(
            parts,
            &vec![
                AssistantPart::Reasoning {
                    text: "first thought".into()
                },
                AssistantPart::Text {
                    text: "before".into()
                },
                AssistantPart::ToolCall {
                    call_id: "one".into()
                },
                AssistantPart::Reasoning {
                    text: "middle thought".into()
                },
                AssistantPart::Text {
                    text: "between".into()
                },
                AssistantPart::ToolCall {
                    call_id: "two".into()
                },
                AssistantPart::Text {
                    text: "after".into()
                },
            ]
        );
    }

    #[test]
    fn empty_tool_id_placeholders_preserve_identity_at_every_byte_split() {
        let chunks = [
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-one","type":"function","function":{"name":"ec","arguments":""}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"","type":"function","function":{"name":"ho","arguments":"{"}},{"index":1,"id":"","function":{"name":"","arguments":""}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call-two","function":{"name":"echo","arguments":"{\"b\":"}},{"index":0,"id":"","function":{"name":"","arguments":"\"a\":1}"}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call-two","function":{"name":"","arguments":"2}"}}]},"finish_reason":"tool_calls"}]}),
        ];
        let wire = chunks
            .iter()
            .map(|chunk| format!("data: {chunk}\r\n\r\n"))
            .collect::<String>()
            + "data: [DONE]\r\n\r\n";
        for split in 0..=wire.len() {
            let mut decoder = Decoder::default();
            let mut events = decoder.push(&wire.as_bytes()[..split], false).unwrap();
            events.extend(decoder.push(&wire.as_bytes()[split..], true).unwrap());
            let ids = events
                .iter()
                .filter_map(|event| match event {
                    ModelEvent::ToolCallDelta { index, id, .. } => Some((*index, id.as_deref())),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(
                ids,
                vec![
                    (0, Some("call-one")),
                    (0, None),
                    (1, None),
                    (1, Some("call-two")),
                    (0, None),
                    (1, Some("call-two"))
                ],
                "split {split}"
            );
            let (value, usage, _) = decoder.into_value().unwrap();
            let request = microcrab_protocol::Request {
                epoch: 0,
                messages: vec![],
                max_output_tokens: 100,
                tools: vec![microcrab_protocol::ToolDefinition {
                    name: "echo".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                }],
            };
            let response =
                super::super::decode_response(value, &request, Some(usage), false).unwrap();
            assert_eq!(response.tool_calls[0].id, "call-one");
            assert_eq!(response.tool_calls[0].name, "echo");
            assert_eq!(response.tool_calls[0].arguments, json!({"a":1}));
            assert_eq!(response.tool_calls[1].id, "call-two");
            assert_eq!(response.tool_calls[1].arguments, json!({"b":2}));
            response
                .provider_metadata
                .as_ref()
                .unwrap()
                .validate_projection(&response.content, &response.tool_calls)
                .unwrap();
        }
    }

    #[test]
    fn tool_id_placeholders_do_not_relax_conflicts_or_field_types() {
        for id in [
            json!("different"),
            json!(" "),
            json!(null),
            json!(0),
            json!(false),
            json!([]),
            json!({}),
        ] {
            let mut decoder = Decoder::default();
            decoder.chunk(json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"original","function":{"name":"echo","arguments":"{"}}]}}]}), &mut Vec::new()).unwrap();
            let mut events = Vec::new();
            let error = decoder.chunk(json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":id,"function":{"arguments":"}"}}]}}]}), &mut events).unwrap_err();
            if id.is_string() {
                assert!(error.to_string().contains("conflicting SSE tool id"));
            } else {
                assert!(error.to_string().contains("invalid SSE string field"));
            }
            assert!(events.is_empty());
        }
        let mut decoder = Decoder::default();
        decoder.push(b"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"\",\"function\":{\"name\":\"echo\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n", true).unwrap();
        assert!(
            decoder
                .into_value()
                .unwrap_err()
                .to_string()
                .contains("missing SSE tool id")
        );
    }

    #[test]
    fn placeholder_normalization_preserves_final_identity_validation() {
        let request = microcrab_protocol::Request {
            epoch: 0,
            messages: vec![],
            max_output_tokens: 100,
            tools: vec![microcrab_protocol::ToolDefinition {
                name: "echo".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }],
        };
        for (ids, reason) in [
            (vec![" "], "tool call ID is empty"),
            (vec!["same", "same"], "duplicate tool call ID"),
        ] {
            let calls = ids
                .iter()
                .enumerate()
                .map(|(index, id)| {
                    json!({
                        "index":index,"id":id,"type":"function",
                        "function":{"name":"echo","arguments":"{}"}
                    })
                })
                .collect::<Vec<_>>();
            let mut decoder = Decoder::default();
            let wire = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{
                    "delta":{"tool_calls":calls},"finish_reason":"tool_calls"
                }]})
            );
            decoder.push(wire.as_bytes(), true).unwrap();
            let (value, usage, _) = decoder.into_value().unwrap();
            let error =
                super::super::decode_response(value, &request, Some(usage), false).unwrap_err();
            assert!(error.to_string().contains(reason), "{error}");
        }
        for field in ["name", "arguments"] {
            for invalid in [Value::Null, json!(0), json!(false), json!([]), json!({})] {
                let mut function = json!({"name":"echo","arguments":"{}"});
                function[field] = invalid;
                assert!(
                    Decoder::default()
                        .chunk(
                            json!({"choices":[{"delta":{"tool_calls":[{
                                "index":0,"id":"","function":function
                            }]}}]}),
                            &mut Vec::new()
                        )
                        .is_err()
                );
            }
        }
        for id in [None, Some("")] {
            let mut call = json!({"index":0,"function":{"name":"echo","arguments":"{}"}});
            if let Some(id) = id {
                call["id"] = id.into();
            }
            let mut decoder = Decoder::default();
            decoder.chunk(json!({"choices":[{"delta":{"tool_calls":[call]},"finish_reason":"tool_calls"}]}), &mut Vec::new()).unwrap();
            decoder.push(b"data: [DONE]\n\n", true).unwrap();
            assert!(
                decoder
                    .into_value()
                    .unwrap_err()
                    .to_string()
                    .contains("missing SSE tool id")
            );
        }
    }

    #[test]
    fn null_primary_reasoning_does_not_hide_valid_semantic_alias() {
        let wire = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":null,\"reasoning\":\"portable\",\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let mut decoder = Decoder::default();
        let events = decoder.push(wire.as_bytes(), true).unwrap();
        assert!(events.iter().any(
            |event| matches!(event, ModelEvent::ReasoningDelta { text } if text == "portable")
        ));
        let (value, _, _) = decoder.into_value().unwrap();
        assert_eq!(
            value["choices"][0]["message"]["reasoning_content"],
            "portable"
        );
    }

    #[test]
    fn every_byte_split_preserves_utf8_multiline_data_and_crlf() {
        let wire = concat!(
            ": comment\r\n\r\n",
            "event: message\r\ndata: {\"choices\":[{\"index\":0,\r\n",
            "data: \"delta\":{\"content\":\"你好\"},\"finish_reason\":\"stop\"}]}\r\n\r\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\r\n\r\n",
            "data: [DONE]\r\n\r\n"
        ).as_bytes();
        for split in 0..=wire.len() {
            let mut decoder = Decoder::default();
            let mut events = decoder.push(&wire[..split], false).unwrap();
            events.extend(decoder.push(&wire[split..], false).unwrap());
            events.extend(decoder.push(&[], true).unwrap());
            assert_eq!(
                events,
                vec![ModelEvent::TextDelta {
                    text: "你好".into()
                }],
                "split {split}"
            );
            let (value, usage_details, _) = decoder.into_value().unwrap();
            assert_eq!(usage_details, vec![value["usage"].clone()]);
            assert_eq!(value["usage"]["completion_tokens"], 3);
            assert_eq!(value["choices"][0]["message"]["content"], "你好");
        }
        let mut decoder = Decoder::default();
        for byte in wire {
            decoder.push(&[*byte], false).unwrap();
        }
        decoder.push(&[], true).unwrap();
    }

    #[test]
    fn every_byte_split_preserves_trailing_usage_without_replaying_events() {
        let wire = concat!(
            "data: {\"error\":null,\"choices\":[{\"delta\":{\"content\":\"你好\",\"reasoning\":\"think\"},\"finish_reason\":\"end_turn\"}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"end_turn\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"\",\"reasoning\":null,\"tool_calls\":[]}}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":4}}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null}]}\n\n",
            "data: [DONE]\n\n"
        ).as_bytes();
        for split in 0..=wire.len() {
            let mut decoder = Decoder::default();
            let mut events = decoder.push(&wire[..split], false).unwrap();
            events.extend(decoder.push(&wire[split..], true).unwrap());
            assert_eq!(
                events,
                vec![
                    ModelEvent::ReasoningDelta {
                        text: "think".into()
                    },
                    ModelEvent::TextDelta {
                        text: "你好".into()
                    },
                ],
                "split {split}"
            );
            let (value, snapshots, _) = decoder.into_value().unwrap();
            assert_eq!(snapshots.len(), 2);
            assert_eq!(value["usage"]["completion_tokens"], 4);
            assert_eq!(value["choices"][0]["message"]["content"], "你好");
        }
        let mut decoder = Decoder::default();
        let mut events = Vec::new();
        for byte in wire {
            events.extend(decoder.push(&[*byte], false).unwrap());
        }
        decoder.push(&[], true).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn fragmented_mixed_gateway_usage_preserves_canonical_counts_and_raw_snapshots() {
        for (input, output) in [(96, 0), (341, 96)] {
            let raw = json!({
                "usage_semantic":"openai","usage_source":"anthropic",
                "prompt_tokens":input,"completion_tokens":output,
                "prompt_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},
                "completion_tokens_details":{"reasoning_tokens":0},
                "input_tokens":0,"output_tokens":0,
                "cache_read_input_tokens":"invalid","cache_creation_input_tokens":-1,
                "input_tokens_details":{"cached_tokens":999},
                "output_tokens_details":{"reasoning_tokens":999},
                "billing_usage":{"input_tokens":96,"output_tokens":0}
            });
            let initial = json!({"choices":[{"delta":{"content":"ok","tool_calls":[{
                "index":0,"id":"call-1","type":"function",
                "function":{"name":"tool","arguments":"{}"}
            }]},"finish_reason":"tool_calls"}]});
            let wire = format!(
                "data: {initial}\n\ndata: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[],"usage":raw})
            );
            let mut decoder = Decoder::default();
            let mut events = Vec::new();
            for byte in wire.as_bytes() {
                events.extend(decoder.push(&[*byte], false).unwrap());
            }
            events.extend(decoder.push(&[], true).unwrap());
            assert_eq!(
                events,
                vec![
                    ModelEvent::TextDelta { text: "ok".into() },
                    ModelEvent::ToolCallDelta {
                        index: 0,
                        id: Some("call-1".into()),
                        name: Some("tool".into()),
                        arguments: "{}".into()
                    },
                ]
            );
            let (value, snapshots, _) = decoder.into_value().unwrap();
            assert_eq!(snapshots, vec![raw.clone()]);
            assert_eq!(value["usage"], raw);
            assert_eq!(value["choices"][0]["finish_reason"], "tool_calls");
            assert_eq!(
                value["choices"][0]["message"]["tool_calls"][0]["id"],
                "call-1"
            );
            let usage = super::super::decode_usage(value.get("usage")).unwrap();
            assert_eq!(usage.input_tokens, Some(input));
            assert_eq!(usage.output_tokens, Some(output));
            assert_eq!(usage.total_tokens, Some(input + output));
            assert_eq!(usage.cache_read_tokens, Some(0));
            assert_eq!(usage.cache_write_tokens, Some(0));
            assert_eq!(usage.reasoning_tokens, Some(0));
            assert_eq!(usage.provider_details, snapshots);
        }
    }

    #[test]
    fn trailing_choices_reject_content_conflicts_and_malformed_wrappers() {
        for choice in [
            json!({"delta":{"content":"late"}}),
            json!({"delta":{"reasoning":"late"}}),
            json!({"delta":{"reasoning_content":"late"}}),
            json!({"delta":{"tool_calls":[{"index":0}]}}),
            json!({"delta":{},"finish_reason":"length"}),
            json!({"delta":{},"finish_reason":5}),
            json!({"delta":null}),
            json!({"delta":[]}),
            json!({"finish_reason":"stop"}),
            json!({"delta":{"content":{}}}),
            json!({"delta":{"reasoning":false}}),
            json!({"delta":{"reasoning_content":[],"reasoning":""}}),
            json!({"delta":{"tool_calls":{}}}),
            json!({"delta":{"role":"user"}}),
            json!({"delta":{"unexpected":"late"}}),
            json!({"delta":{"function_call":{"arguments":"late"}}}),
        ] {
            let mut decoder = Decoder::default();
            decoder
                .chunk(
                    json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
                    &mut Vec::new(),
                )
                .unwrap();
            let mut events = Vec::new();
            assert!(
                decoder
                    .chunk(json!({"choices":[choice]}), &mut events)
                    .is_err()
            );
            assert!(events.is_empty());
        }
    }

    #[test]
    fn explicit_bad_choice_indices_are_never_singleton_fallbacks() {
        for index in [
            Value::Null,
            json!(1),
            json!(-1),
            json!(0.5),
            json!("0"),
            json!(false),
        ] {
            for finished in [false, true] {
                let mut decoder = Decoder::default();
                if finished {
                    decoder
                        .chunk(
                            json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
                            &mut Vec::new(),
                        )
                        .unwrap();
                }
                assert!(
                    decoder
                        .chunk(
                            json!({"choices":[{"index":index,"delta":{},"finish_reason":"stop"}]}),
                            &mut Vec::new()
                        )
                        .is_err()
                );
            }
        }
    }

    #[test]
    fn completion_requires_explicit_finish_or_measured_text_done() {
        for wire in [
            "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: [DONE]\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\ndata: [DONE]\n\n",
            "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thought\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"a\",\"tool_calls\":[{\"index\":0,\"id\":\"call\",\"function\":{\"name\":\"echo\",\"arguments\":\"{}\"}}]}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n",
        ] {
            assert!(Decoder::default().push(wire.as_bytes(), true).is_err());
        }
        for (wire, cause) in [
            ("data: [DONE]", "incomplete line or event"),
            (
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "without its DONE terminal marker",
            ),
            ("data: [DONE]\n\n", "omitted finish_reason"),
        ] {
            assert!(
                Decoder::default()
                    .push(wire.as_bytes(), true)
                    .unwrap_err()
                    .to_string()
                    .contains(cause)
            );
        }
    }

    #[test]
    fn measured_text_done_without_finish_preserves_unknown_at_every_byte_split() {
        use microcrab_protocol::Request;
        let wire = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你好\"},\"finish_reason\":null}]}\r\n\r\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":46588,\"completion_tokens\":1488,\"total_tokens\":48076}}\r\n\r\ndata: [DONE]\r\n\r\n";
        for split in 0..=wire.len() {
            let mut decoder = Decoder::default();
            let mut events = decoder.push(&wire.as_bytes()[..split], false).unwrap();
            events.extend(decoder.push(&wire.as_bytes()[split..], true).unwrap());
            assert_eq!(events.len(), 1);
            let (value, usage, gateway_text_complete) = decoder.into_value().unwrap();
            assert!(gateway_text_complete);
            assert!(value["choices"][0]["finish_reason"].is_null());
            let request = Request {
                epoch: 1,
                messages: vec![],
                tools: vec![],
                max_output_tokens: 2000,
            };
            assert!(
                super::super::decode_response(value.clone(), &request, Some(usage.clone()), false)
                    .is_err()
            );
            assert!(super::super::decode_response(value.clone(), &request, None, true).is_err());
            let response =
                super::super::decode_response(value, &request, Some(usage), gateway_text_complete)
                    .unwrap();
            assert_eq!(response.content, "你好");
            assert!(response.tool_calls.is_empty());
            assert_eq!(response.usage.input_tokens, Some(46588));
            assert_eq!(response.usage.output_tokens, Some(1488));
        }
    }

    #[test]
    fn text_done_compatibility_does_not_accept_unmeasured_or_truncated_output() {
        let text = "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n";
        for usage in [
            json!({}),
            json!({"prompt_tokens":1}),
            json!({"completion_tokens":2}),
            json!({"prompt_tokens":1,"completion_tokens":-1}),
        ] {
            let wire = format!(
                "{text}data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[],"usage":usage})
            );
            assert!(Decoder::default().push(wire.as_bytes(), true).is_err());
        }
        for delta in [
            json!({"content":"answer","function_call":{"name":"echo","arguments":"{}"}}),
            json!({"content":"answer","refusal":"blocked"}),
        ] {
            let wire = format!(
                "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"delta":delta}]}),
                json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}})
            );
            assert!(Decoder::default().push(wire.as_bytes(), true).is_err());
        }
        let measured = format!(
            "{text}data: {}\n\n",
            json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}})
        );
        for tail in ["", "data: [DONE]", "data: [DONE]\n"] {
            assert!(
                Decoder::default()
                    .push(format!("{measured}{tail}").as_bytes(), true)
                    .is_err()
            );
        }
        let refused = format!(
            "{text}data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"delta":{},"finish_reason":"content_filter"}]}),
            json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}})
        );
        let mut decoder = Decoder::default();
        decoder.push(refused.as_bytes(), true).unwrap();
        let (value, usage, gateway) = decoder.into_value().unwrap();
        assert!(!gateway);
        let request = microcrab_protocol::Request {
            epoch: 1,
            messages: vec![],
            tools: vec![],
            max_output_tokens: 100,
        };
        assert!(super::super::decode_response(value, &request, Some(usage), gateway).is_err());
    }

    #[test]
    fn lone_cr_line_endings_work_and_unterminated_events_fail() {
        let mut decoder = Decoder::default();
        decoder.push(b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\r\rdata: [DONE]\r\r", false).unwrap();
        decoder.push(&[], true).unwrap();
        let mut decoder = Decoder::default();
        decoder.push(b"data: [DONE]", false).unwrap();
        assert!(decoder.push(&[], true).is_err());
    }
}
