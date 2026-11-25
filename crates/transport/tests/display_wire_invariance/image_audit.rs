use super::{Recorder, read_request};
use microcrab_auditor::{
    AuditBody, AuditOutcome, AuditRecord, Auditor, BoxFuture, ChannelAuditor, EmptyVariableView,
    HookContext, HookPayload, Injection, Moment,
};
use microcrab_protocol::{ImageSource, InputPart, ToolDefinition};
use microcrab_transport::{
    Client, Content, ContentItem, ModelEvent, ObservationState, Protocol, TransportClient,
    TransportConfig,
};
use serde_json::{Value, json};
use std::{
    io::Write,
    net::TcpListener,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

const REQUEST_URL: &str = "https://fixture.invalid/image?token=request-query-secret";
const RESPONSE_URL: &str = "https://fixture.invalid/image?token=response-query-secret";
const API_KEY: &str = "fixture-provider-auth-secret";
const INLINE: &str = "data:image/png;base64,c2Vuc2l0aXZl";
const PROTOCOLS: [Protocol; 4] = [
    Protocol::OpenaiCompatible,
    Protocol::OpenaiResponses,
    Protocol::Anthropic,
    Protocol::Google,
];

struct Capture {
    records: ChannelAuditor,
    requests: Mutex<Vec<(Option<String>, String)>>,
    rewrite: bool,
}
impl Auditor for Capture {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        self.records.publish(moment, body);
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, microcrab_auditor::AuditorError>> {
        assert_eq!(moment, Moment::RequestPrepared);
        assert!(matches!(context.payload, HookPayload::None));
        assert_eq!(context.max_request_bytes, Some(65536));
        let body = context.request_body.expect("raw authored provider body");
        self.requests
            .lock()
            .unwrap()
            .push((context.call_id, body.to_owned()));
        let request_body = self.rewrite.then(|| format!("{body}\n"));
        Box::pin(async move {
            Ok(Injection {
                request_body,
                ..Injection::default()
            })
        })
    }
}

fn content(url_catalog: bool) -> Content {
    Content {
        epoch: 3,
        subagent: None,
        system: "Frozen synthetic system".into(),
        items: vec![ContentItem::User {
            envelope: None,
            source: None,
            parts: vec![InputPart::Image {
                source: ImageSource::Url {
                    url: REQUEST_URL.into(),
                },
            }],
        }],
        tools: vec![ToolDefinition {
            name: "view_image".into(),
            description: "fixture".into(),
            parameters: if url_catalog {
                json!({"type":"object","properties":{"image_url":{"type":"string"}},"required":["image_url"],"additionalProperties":false})
            } else {
                json!({"type":"object","properties":{"path":{"type":"string"}}})
            },
        }],
        max_output_tokens: 128,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(2),
    }
}

fn body(protocol: Protocol, sse: bool, key: &str, source: &str) -> String {
    let args = json!({key:source});
    let serialized = args.to_string();
    if !sse {
        return match protocol {
            Protocol::OpenaiCompatible => json!({"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call","type":"function","function":{"name":"view_image","arguments":serialized}}]},"finish_reason":"tool_calls"}],"usage":{"completion_tokens":1,"echo":source}}),
            Protocol::OpenaiResponses => json!({"status":"completed","output":[{"type":"function_call","call_id":"call","name":"view_image","arguments":serialized}],"usage":{"output_tokens":1,"echo":source}}),
            Protocol::Anthropic => json!({"content":[{"type":"tool_use","id":"call","name":"view_image","input":args}],"stop_reason":"tool_use","usage":{"output_tokens":1,"echo":source}}),
            Protocol::Google => json!({"candidates":[{"content":{"parts":[{"functionCall":{"id":"call","name":"view_image","args":args}}]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":1,"echo":source}}),
        }.to_string();
    }
    let at = serialized.find(source).unwrap() + source.len() / 2;
    let (first, last) = serialized.split_at(at);
    let events = match protocol {
        // Keep exact argument fragments even before the streamed name is complete.
        Protocol::OpenaiCompatible => vec![
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call","function":{"arguments":first}}]},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"view_"}}]},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"image","arguments":last}}]},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}),
            json!({"choices":[],"usage":{"completion_tokens":1,"echo":source}}),
        ],
        Protocol::OpenaiResponses => vec![
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"item","call_id":"call","name":"view_image","arguments":""}}),
            json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"item","delta":first}),
            json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"item","delta":last}),
            json!({"type":"response.function_call_arguments.done","output_index":0,"item_id":"item","arguments":serialized}),
            json!({"type":"response.completed","response":{"status":"completed","usage":{"output_tokens":1,"echo":source}}}),
        ],
        Protocol::Anthropic => vec![
            json!({"type":"message_start","message":{}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"view_image","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":first}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":last}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1,"echo":source}}),
            json!({"type":"message_stop"}),
        ],
        Protocol::Google => vec![
            json!({"candidates":[{"content":{"parts":[{"functionCall":{"id":"call","name":"view_image","args":args}}]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":1,"echo":source}}),
        ],
    };
    let mut result = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>();
    if protocol == Protocol::OpenaiCompatible {
        result.push_str("data: [DONE]\n\n");
    }
    result
}

fn fixture(
    protocol: Protocol,
    status: u16,
    media: &str,
    response: String,
    recorder: Arc<Recorder>,
) -> (TransportClient, std::thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TransportClient::new(TransportConfig {
        auth: Default::default(),
        protocol,
        endpoint: format!("http://{}/v1", listener.local_addr().unwrap()),
        model: "fixture".into(),
        api_key: Some(API_KEY.into()),
        timeout_ms: 5000,
        max_response_bytes: 65536,
    })
    .unwrap()
    .with_recording(recorder);
    let wire = format!(
        "HTTP/1.1 {status} Fixture\r\nContent-Type: {media}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
        response.len()
    );
    let worker = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let (headers, body) = read_request(&mut socket);
        let (header, value) = match protocol {
            Protocol::OpenaiCompatible | Protocol::OpenaiResponses => {
                ("authorization", format!("Bearer {API_KEY}"))
            }
            Protocol::Anthropic => ("x-api-key", API_KEY.to_owned()),
            Protocol::Google => ("x-goog-api-key", API_KEY.to_owned()),
        };
        assert!(headers.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, body)| {
                name.eq_ignore_ascii_case(header) && body.trim() == value
            })
        }));
        let request = String::from_utf8(body).unwrap();
        socket.write_all(wire.as_bytes()).unwrap();
        request
    });
    (client, worker)
}

fn capture(rewrite: bool) -> (Capture, tokio::sync::mpsc::UnboundedReceiver<AuditRecord>) {
    let (audits, records) = tokio::sync::mpsc::unbounded_channel();
    (
        Capture {
            records: ChannelAuditor::from_sender(audits),
            requests: Mutex::new(Vec::new()),
            rewrite,
        },
        records,
    )
}

#[tokio::test]
async fn image_catalog_uses_default_raw_audit_and_preview() {
    for protocol in PROTOCOLS {
        for (url_catalog, sse, key, source, rewrite) in [
            (true, false, "image_url", RESPONSE_URL, true),
            (true, true, "image_url", INLINE, false),
            (false, true, "path", INLINE, false),
        ] {
            let media = if sse {
                "text/event-stream"
            } else {
                "application/json"
            };
            let response = body(protocol, sse, key, source);
            let recorder = Arc::new(Recorder::default());
            let (client, worker) =
                fixture(protocol, 200, media, response.clone(), recorder.clone());
            let (audit, mut records) = capture(rewrite);
            let (events, mut preview) = tokio::sync::mpsc::channel(64);
            let outcome = client
                .stream_admitted_outcome(
                    &content(url_catalog),
                    &audit,
                    &EmptyVariableView,
                    CancellationToken::new(),
                    Some(events),
                )
                .await
                .unwrap_or_else(|err| panic!("{protocol:?}: {err:?}"));
            let request = worker.join().unwrap();
            assert!(request.contains(REQUEST_URL));
            assert!(!request.contains(API_KEY));
            assert_eq!(request.ends_with('\n'), rewrite);
            assert_eq!(outcome.response.tool_calls[0].arguments[key], source);
            assert!(!outcome.response.usage.provider_details.is_empty());
            assert_eq!(outcome.response.usage.output_tokens, Some(1));

            let prepared = records.try_recv().unwrap();
            let terminal = records.try_recv().unwrap();
            assert!(records.try_recv().is_err());
            assert_eq!(prepared.event, Moment::RequestPrepared);
            assert_eq!(terminal.event, Moment::RoundCompleted);
            assert_eq!(prepared.call_id, terminal.call_id);
            assert_eq!(
                terminal.call_id,
                outcome.attempt.map(|attempt| attempt.call_id)
            );
            assert_eq!(terminal.outcome, Some(AuditOutcome::Accepted));
            assert_eq!(terminal.response_status, Some(200));
            assert_eq!(terminal.response_complete, Some(true));
            assert_eq!(terminal.response_content_type.as_deref(), Some(media));
            assert_eq!(terminal.usage.as_ref().unwrap().output_tokens, Some(1));
            assert_eq!(
                terminal.usage.as_ref().unwrap().provider_details,
                outcome.response.usage.provider_details
            );
            assert_eq!(terminal.first_token_ms.is_some(), sse);
            assert_eq!(prepared.sent_request.as_deref(), Some(request.as_str()));
            assert_eq!(terminal.response.as_deref(), Some(response.as_str()));
            assert!(terminal.response_bytes.is_none());
            assert!(terminal.protected_tool_call_ids.is_none());
            let HookPayload::RoundCompleted { response, .. } = terminal.payload else {
                panic!("missing completion facts")
            };
            let response = response.expect("normalized provider response");
            assert_eq!(response.tool_calls, outcome.response.tool_calls);
            assert_eq!(response.usage, outcome.response.usage);
            let intervention = audit.requests.lock().unwrap();
            let authored = request.strip_suffix('\n').unwrap_or(&request);
            assert_eq!(
                intervention.as_slice(),
                &[(terminal.call_id, authored.into())]
            );

            let mut forwarded = String::new();
            let mut saw_identity = false;
            let mut saw_finished = false;
            while let Ok(event) = preview.try_recv() {
                match event {
                    ModelEvent::ToolCallDelta { id, arguments, .. } => {
                        saw_identity |= id.as_deref() == Some("call");
                        forwarded.push_str(&arguments);
                    }
                    ModelEvent::Finished { usage } => {
                        saw_finished = true;
                        assert_eq!(usage, outcome.response.usage);
                    }
                    _ => {}
                }
            }
            assert!(saw_identity);
            assert!(saw_finished);
            let observations = recorder.0.lock().unwrap();
            let recorded = observations
                .iter()
                .filter_map(|observation| match &observation.state {
                    ObservationState::ToolCallDelta { arguments, .. } => Some(arguments.as_str()),
                    _ => None,
                })
                .collect::<String>();
            assert_eq!(recorded, forwarded);
            assert_eq!(
                serde_json::from_str::<Value>(&forwarded).unwrap()[key],
                source
            );
            assert_eq!(forwarded, json!({key:source}).to_string());
        }
    }
}

#[tokio::test]
async fn provider_diagnostics_are_sanitized_separately_from_raw_audit() {
    for (protocol, response, expected) in [
        (
            Protocol::Google,
            json!({"promptFeedback":{"blockReason":RESPONSE_URL}}),
            "Google Gemini blocked the prompt: [redacted]",
        ),
        (
            Protocol::Google,
            json!({"candidates":[{"finishReason":RESPONSE_URL}]}),
            "Google Gemini finish reason: https://fixture.invalid/image?[redacted]",
        ),
        (
            Protocol::OpenaiResponses,
            json!({"output":[{"type":"function_call","call_id":RESPONSE_URL,"name":"view_image","arguments":INLINE}]}),
            "invalid tool arguments JSON",
        ),
    ] {
        let response = response.to_string();
        let (client, worker) = fixture(
            protocol,
            200,
            "application/json",
            response.clone(),
            Arc::new(Recorder::default()),
        );
        let (audit, mut records) = capture(false);
        let error = client
            .complete(
                &content(true),
                &audit,
                &EmptyVariableView,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        let request = worker.join().unwrap();
        assert_eq!(error.diagnostic_reason(), expected);
        assert!(!format!("{error:?} {error}").contains("response-query-secret"));
        assert_eq!(
            records.try_recv().unwrap().sent_request.as_deref(),
            Some(request.as_str())
        );
        let terminal = records.try_recv().unwrap();
        assert_eq!(terminal.response.as_deref(), Some(response.as_str()));
        assert!(terminal.response_bytes.is_none());
    }
}

#[tokio::test]
async fn provider_failure_body_remains_in_raw_audit_not_in_http_error() {
    for protocol in PROTOCOLS {
        let response = json!({"error":{"message":RESPONSE_URL}}).to_string();
        let (client, worker) = fixture(
            protocol,
            403,
            RESPONSE_URL,
            response.clone(),
            Arc::new(Recorder::default()),
        );
        let (audit, mut records) = capture(false);
        let error = client
            .complete(
                &content(true),
                &audit,
                &EmptyVariableView,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        let request = worker.join().unwrap();
        assert!(matches!(
            error,
            microcrab_transport::Error::ProviderStatus { status: 403 }
        ));
        assert!(!format!("{error:?} {error}").contains("response-query-secret"));
        let prepared = records.try_recv().unwrap();
        let terminal = records.try_recv().unwrap();
        assert_eq!(prepared.sent_request.as_deref(), Some(request.as_str()));
        assert_eq!(terminal.response_status, Some(403));
        assert_eq!(terminal.response_complete, Some(true));
        assert_eq!(
            terminal.response_content_type.as_deref(),
            Some(RESPONSE_URL)
        );
        assert_eq!(terminal.response.as_deref(), Some(response.as_str()));
        assert!(terminal.response_bytes.is_none());
        assert!(records.try_recv().is_err());
    }
}
