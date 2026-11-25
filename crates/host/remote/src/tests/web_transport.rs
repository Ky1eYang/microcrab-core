use super::*;
use microcrab_host_api::{
    OpContext,
    web_transport::{
        ConnectionScope, WEB_MAX_WS_RECEIVE_BYTES, WEB_MAX_WS_SOFT_WAIT_MS,
        WebTransportError as Error, WsPull, WsReceive,
    },
};
use microcrab_host_protocol::{
    MAX_CHUNK_BYTES, MAX_FRAME_BYTES, TransferRef, WebTransportRequest, WireError, decode_frame,
    encode_frame, sha256,
};
use std::time::Duration;

fn conn(session: &str, epoch: u64, id: &str) -> ConnectionScope {
    let BindingScope::Project(binding) = scope() else {
        panic!()
    };
    ConnectionScope {
        binding,
        session_id: session.into(),
        epoch,
        connection_id: id.into(),
    }
}

async fn proven(host: &Arc<RemoteHost>, generation: u64) -> mpsc::Receiver<HostFrame> {
    proven_with_pull(host, generation, false).await
}

async fn proven_with_pull(
    host: &Arc<RemoteHost>,
    generation: u64,
    ws_pull: bool,
) -> mpsc::Receiver<HostFrame> {
    let (outbound, mut receiver) = mpsc::channel(32);
    host.set_scope(scope()).unwrap();
    host.attach_route_with_web_transport_capabilities(
        outbound,
        generation,
        format!("lease-{generation}"),
        "bridge".into(),
        None,
        (false, false),
        (true, ws_pull),
    )
    .unwrap();
    assert!(
        !host.supports_web_transport(),
        "no capability publication while awaiting reconciliation"
    );
    finish_ready(host, generation, &mut receiver).await;
    assert!(host.supports_web_transport());
    receiver
}

async fn request(
    receiver: &mut mpsc::Receiver<HostFrame>,
    generation: u64,
) -> (String, String, WebTransportRequest) {
    let frame = tokio::time::timeout(Duration::from_secs(1), next_request(receiver))
        .await
        .expect("fixture expected a request");
    let HostFrame::Request {
        request_id,
        operation_id,
        lease_generation,
        remaining_budget_ms,
        method: HostRequest::WebTransport(method),
    } = decode_frame(&encode_frame(&frame).unwrap()).unwrap()
    else {
        panic!("expected scoped web transport request")
    };
    assert_eq!(lease_generation, format!("lease-{generation}"));
    assert!(remaining_budget_ms > 0 && remaining_budget_ms <= 10_000);
    (request_id, operation_id, method)
}

fn response(
    host: &RemoteHost,
    generation: u64,
    ids: (String, String),
    result: Result<serde_json::Value, WireError>,
) -> Result<(), ToolError> {
    let frame = HostFrame::Response {
        request_id: ids.0,
        operation_id: ids.1,
        result,
    };
    host.receive(
        generation,
        decode_frame(&encode_frame(&frame).unwrap()).unwrap(),
    )
}

async fn open(
    host: &Arc<RemoteHost>,
    receiver: &mut mpsc::Receiver<HostFrame>,
    generation: u64,
    scope: &ConnectionScope,
) {
    let worker = {
        let (host, scope) = (host.clone(), scope.clone());
        tokio::spawn(async move {
            host.ws_open(
                &OpContext::default(),
                &scope,
                "wss://fixture.invalid:9443/debug",
            )
            .await
        })
    };
    let (id, op, method) = request(receiver, generation).await;
    assert!(
        matches!(method, WebTransportRequest::WsOpen { scope: actual, url } if &actual == scope && url == "wss://fixture.invalid:9443/debug")
    );
    response(host, generation, (id, op), Ok(serde_json::Value::Null)).unwrap();
    assert_eq!(worker.await.unwrap(), Ok(()));
}

fn no_request(receiver: &mut mpsc::Receiver<HostFrame>) {
    while let Ok(frame) = receiver.try_recv() {
        assert!(
            matches!(frame, HostFrame::Ping { .. }),
            "unexpected outbound frame: {frame:?}"
        );
    }
}

#[tokio::test]
async fn route_opt_in_and_exact_binding_are_required_without_changing_descriptor() {
    let descriptor = descriptor();
    let host = RemoteHost::new(descriptor.clone()).unwrap();
    let scope = conn("session-a", 7, "fresh");
    let ctx = OpContext::default();
    let mut old = ready(&host, 1).await;
    assert!(!host.supports_web_transport());
    assert_eq!(
        host.ws_open(&ctx, &scope, "wss://fixture.invalid:9443/debug")
            .await,
        Err(Error::Unsupported)
    );
    no_request(&mut old);
    let mut active = proven(&host, 2).await;
    assert_eq!(host.web_transport_binding(), Some(scope.binding.clone()));
    assert_eq!(
        host.descriptor().descriptor_digest,
        descriptor.descriptor_digest
    );
    let mut wrong = scope.clone();
    wrong.binding.backend_id.push_str("-wrong");
    assert_eq!(
        host.ws_open(&ctx, &wrong, "wss://fixture.invalid:9443/debug")
            .await,
        Err(Error::Denied)
    );
    assert_eq!(
        host.ws_open(&ctx, &scope, "ws://user@fixture.invalid:9")
            .await,
        Err(Error::InvalidRequest)
    );
    assert_eq!(
        host.ws_receive(&ctx, &scope, WEB_MAX_WS_RECEIVE_BYTES + 1)
            .await,
        Err(Error::Limit)
    );
    no_request(&mut active);

    let conversation = RemoteHost::new(descriptor).unwrap();
    conversation
        .set_scope(BindingScope::conversation(
            "b".into(),
            "g".into(),
            "c".into(),
        ))
        .unwrap();
    let (sender, mut receiver) = mpsc::channel(32);
    conversation
        .attach_route_with_capabilities(
            sender,
            1,
            "lease-1".into(),
            "bridge".into(),
            None,
            (false, false),
            true,
        )
        .unwrap();
    finish_ready(&conversation, 1, &mut receiver).await;
    assert!(!conversation.supports_web_transport());
    assert_eq!(
        conversation
            .ws_open(&ctx, &scope, "wss://fixture.invalid:9443/debug")
            .await,
        Err(Error::Unsupported)
    );
    no_request(&mut receiver);
}

#[tokio::test]
async fn v1_without_pull_ack_keeps_legacy_receive_without_fallback() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut peer = proven(&host, 1).await;
    let scope = conn("s", 1, "legacy");
    open(&host, &mut peer, 1, &scope).await;
    assert_eq!(
        host.ws_pull(&OpContext::default(), &scope, 8, 0).await,
        Err(Error::Unsupported)
    );
    no_request(&mut peer);
    let legacy = {
        let host = host.clone();
        tokio::spawn(async move { host.ws_receive(&OpContext::default(), &scope, 8).await })
    };
    let (id, op, method) = request(&mut peer, 1).await;
    assert!(matches!(method, WebTransportRequest::WsReceive { max_bytes: 8, .. }));
    response(&host, 1, (id, op), Ok(json!({"kind":"message", "text":"legacy"}))).unwrap();
    assert_eq!(legacy.await.unwrap(), Ok(WsReceive::Message("legacy".into())));
}

#[tokio::test]
async fn pull_empty_and_proven_precommit_hard_failures_preserve_exact_scope() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut peer = proven_with_pull(&host, 1, true).await;
    let scope = conn("s", 1, "pull");
    open(&host, &mut peer, 1, &scope).await;
    let ctx = OpContext::default();
    ctx.cancel.cancel();
    assert_eq!(host.ws_pull(&ctx, &scope, 8, 0).await, Err(Error::Cancelled));
    no_request(&mut peer);
    let cases = [
        (0, Ok(json!({"kind":"empty"})), Ok(WsPull::Empty)),
        (WEB_MAX_WS_SOFT_WAIT_MS, Err(Error::Timeout.into()), Err(Error::Timeout)),
        (1, Err(Error::Cancelled.into()), Err(Error::Cancelled)),
        (0, Ok(json!({"kind":"message", "text":""})), Ok(WsPull::Message(String::new()))),
        (0, Ok(json!({"kind":"closed"})), Ok(WsPull::Closed)),
    ];
    for (soft_wait_ms, wire, expected) in cases {
        let pull = {
            let (host, scope) = (host.clone(), scope.clone());
            tokio::spawn(async move {
                host.ws_pull(&OpContext::default(), &scope, 8, soft_wait_ms).await
            })
        };
        let (id, op, method) = request(&mut peer, 1).await;
        assert_eq!(method, WebTransportRequest::WsPull { scope: scope.clone(), max_bytes: 8, soft_wait_ms });
        response(&host, 1, (id, op), wire).unwrap();
        assert_eq!(pull.await.unwrap(), expected);
    }
    assert_eq!(host.ws_pull(&OpContext::default(), &scope, 8, 0).await, Err(Error::Stale));
    no_request(&mut peer);
}

#[tokio::test]
async fn lost_admitted_pull_fences_exact_scope_and_ignores_late_reply() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut peer = proven_with_pull(&host, 1, true).await;
    let scope = conn("s", 1, "pull");
    let sibling = conn("s", 1, "sibling");
    open(&host, &mut peer, 1, &scope).await;
    open(&host, &mut peer, 1, &sibling).await;
    let cancel = CancellationToken::new();
    let pull = {
        let (host, scope, cancel) = (host.clone(), scope.clone(), cancel.clone());
        tokio::spawn(async move {
            host.ws_pull(&OpContext { cancel, ..Default::default() }, &scope, 8, 0).await
        })
    };
    let (id, op, method) = request(&mut peer, 1).await;
    assert!(matches!(method, WebTransportRequest::WsPull { .. }));
    cancel.cancel();
    assert_eq!(pull.await.unwrap(), Err(Error::OutcomeUnknown));
    assert!(matches!(next_request(&mut peer).await, HostFrame::Cancel { request_id, .. } if request_id == id));
    response(&host, 1, (id, op), Ok(json!({"kind":"message", "text":"late"}))).unwrap();
    assert_eq!(host.ws_pull(&OpContext::default(), &scope, 8, 0).await, Err(Error::Stale));
    assert_eq!(host.ws_status(&OpContext::default(), &scope).await, Err(Error::Stale));
    let other = {
        let (host, sibling) = (host.clone(), sibling.clone());
        tokio::spawn(async move { host.ws_pull(&OpContext::default(), &sibling, 8, 0).await })
    };
    let (id, op, method) = request(&mut peer, 1).await;
    assert!(matches!(method, WebTransportRequest::WsPull { scope, .. } if scope == sibling));
    response(&host, 1, (id, op), Ok(json!({"kind":"empty"}))).unwrap();
    assert_eq!(other.await.unwrap(), Ok(WsPull::Empty));
    assert!(host.unresolved_operations().is_empty());
    no_request(&mut peer);
}

#[tokio::test]
async fn invalid_pull_reply_fences_scope_without_replay() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut peer = proven_with_pull(&host, 1, true).await;
    for (connection_id, wire, error) in [
        ("legacy-reply", json!({"kind":"timeout"}), Error::Io),
        ("over-limit", json!({"kind":"message", "text":"é"}), Error::Limit),
    ] {
        let scope = conn("s", 1, connection_id);
        open(&host, &mut peer, 1, &scope).await;
        let pull = {
            let (host, scope) = (host.clone(), scope.clone());
            tokio::spawn(async move { host.ws_pull(&OpContext::default(), &scope, 1, 0).await })
        };
        let (id, op, method) = request(&mut peer, 1).await;
        assert!(matches!(method, WebTransportRequest::WsPull { .. }));
        response(&host, 1, (id, op), Ok(wire)).unwrap();
        assert_eq!(pull.await.unwrap(), Err(error));
        assert_eq!(host.ws_pull(&OpContext::default(), &scope, 1, 0).await, Err(Error::Stale));
        no_request(&mut peer);
    }
}

#[tokio::test]
async fn discovery_and_old_closes_cannot_cross_a_route_replacement() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut first = proven(&host, 1).await;
    let scope = conn("session-a", 7, "old-discovery");
    let discovery = {
        let (host, scope) = (host.clone(), scope.clone());
        tokio::spawn(async move {
            host.http_get(
                &OpContext::default(),
                &scope,
                "https://fixture.invalid:9443/json/version",
                1024,
            )
            .await
        })
    };
    let (id, op, method) = request(&mut first, 1).await;
    assert!(
        matches!(method, WebTransportRequest::HttpGet { scope: actual, .. } if actual == scope)
    );
    response(
        &host,
        1,
        (id.clone(), op.clone()),
        Ok(json!({"status": 200, "body": "opaque"})),
    )
    .unwrap();
    assert_eq!(discovery.await.unwrap().unwrap().body, "opaque");
    let mut second = proven(&host, 2).await;
    assert!(
        response(
            &host,
            1,
            (id, op),
            Ok(json!({"status": 200, "body": "late"}))
        )
        .is_err()
    );
    let ctx = OpContext::default();
    assert_eq!(
        host.ws_open(&ctx, &scope, "wss://fixture.invalid:9443/debug")
            .await,
        Err(Error::Stale)
    );
    assert_eq!(host.ws_close(&ctx, &scope).await, Ok(()));
    assert_eq!(
        host.ws_open(&ctx, &scope, "wss://fixture.invalid:9443/debug")
            .await,
        Err(Error::Stale)
    );
    no_request(&mut second);
    open(&host, &mut second, 2, &conn("session-a", 7, "fresh")).await;
    let mut old_peer = ready(&host, 3).await;
    assert!(!host.supports_web_transport());
    assert_eq!(
        host.ws_status(&ctx, &conn("session-a", 7, "fresh")).await,
        Err(Error::Stale)
    );
    no_request(&mut old_peer);
}

#[tokio::test]
async fn exact_owner_cleanup_fences_racing_open_without_touching_other_sessions() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut peer = proven(&host, 1).await;
    let scope = conn("session-a", 7, "same-opaque-id");
    let other = conn("session-b", 7, "same-opaque-id");
    let pending = {
        let (host, scope) = (host.clone(), scope.clone());
        tokio::spawn(async move {
            host.ws_open(
                &OpContext::default(),
                &scope,
                "wss://fixture.invalid:9443/debug",
            )
            .await
        })
    };
    let (open_id, open_op, _) = request(&mut peer, 1).await;
    open(&host, &mut peer, 1, &other).await;
    let release = {
        let (host, owner) = (host.clone(), scope.owner());
        tokio::spawn(async move { host.ws_release_scope(&OpContext::default(), &owner).await })
    };
    let (id, op, method) = request(&mut peer, 1).await;
    assert!(
        matches!(method, WebTransportRequest::ReleaseScope { owner } if owner == scope.owner())
    );
    response(&host, 1, (open_id, open_op), Ok(json!(null))).unwrap();
    assert_eq!(pending.await.unwrap(), Err(Error::Stale));
    response(&host, 1, (id, op), Ok(json!(null))).unwrap();
    assert_eq!(release.await.unwrap(), Ok(()));
    let ctx = OpContext::default();
    assert_eq!(
        host.ws_open(
            &ctx,
            &conn("session-a", 7, "new-id"),
            "wss://fixture.invalid:9443/debug"
        )
        .await,
        Err(Error::Stale)
    );
    no_request(&mut peer);
    let status = {
        let host = host.clone();
        tokio::spawn(async move { host.ws_status(&OpContext::default(), &other).await })
    };
    let (id, op, method) = request(&mut peer, 1).await;
    assert!(
        matches!(method, WebTransportRequest::WsStatus { scope } if scope.session_id == "session-b")
    );
    response(&host, 1, (id, op), Ok(json!(null))).unwrap();
    assert_eq!(status.await.unwrap(), Ok(()));
    open(&host, &mut peer, 1, &conn("session-a", 8, "same-opaque-id")).await;
}

#[tokio::test]
async fn verify_maximal_escaped_receive_over_protobuf_chunks_without_receipt() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut peer = proven(&host, 1).await;
    let scope = conn("s", 0, "large-frame");
    open(&host, &mut peer, 1, &scope).await;
    let result = {
        let host = host.clone();
        tokio::spawn(async move {
            host.ws_receive(&OpContext::default(), &scope, WEB_MAX_WS_RECEIVE_BYTES)
                .await
        })
    };
    let (id, op, method) = request(&mut peer, 1).await;
    let expected = WsReceive::Message("\0".repeat(WEB_MAX_WS_RECEIVE_BYTES as usize));
    send_chunked_reply(&host, (id, op), method, serde_json::to_vec(&expected).unwrap());
    assert_eq!(result.await.unwrap(), Ok(expected));
    assert!(host.is_ready());
    assert!(host.unresolved_operations().is_empty());
    assert!(host.operation_history.lock().unwrap().is_empty());
    no_request(&mut peer);
}

#[tokio::test]
async fn pull_can_deliver_bounded_chunked_message_without_receipt() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut peer = proven_with_pull(&host, 1, true).await;
    let scope = conn("s", 0, "pull-chunks");
    open(&host, &mut peer, 1, &scope).await;
    let result = {
        let host = host.clone();
        tokio::spawn(async move {
            host.ws_pull(&OpContext::default(), &scope, WEB_MAX_WS_RECEIVE_BYTES, 0)
                .await
        })
    };
    let (id, op, method) = request(&mut peer, 1).await;
    assert!(matches!(method, WebTransportRequest::WsPull { .. }));
    let expected = WsPull::Message("\0".repeat(WEB_MAX_WS_RECEIVE_BYTES as usize));
    send_chunked_reply(&host, (id, op), method, serde_json::to_vec(&expected).unwrap());
    assert_eq!(result.await.unwrap(), Ok(expected));
    assert!(host.is_ready());
    assert!(host.unresolved_operations().is_empty());
    assert!(host.operation_history.lock().unwrap().is_empty());
    no_request(&mut peer);
}

fn send_chunked_reply(
    host: &RemoteHost,
    ids: (String, String),
    method: WebTransportRequest,
    json: Vec<u8>,
) {
    assert!(json.len() > MAX_FRAME_BYTES);
    assert!(json.len() <= method.max_reply_json_bytes().unwrap());
    let transfer_id = "synthetic-worst-case".to_owned();
    let reference = TransferRef {
        transfer_id: transfer_id.clone(),
        byte_length: json.len() as u64,
        sha256: sha256(&json),
        kind: "json".into(),
    };
    response(host, 1, ids, Ok(serde_json::to_value(reference).unwrap())).unwrap();
    for (sequence, data) in json.chunks(MAX_CHUNK_BYTES).enumerate() {
        let chunk = HostFrame::TransferChunk {
            transfer_id: transfer_id.clone(),
            sequence: sequence as u32,
            data: data.to_owned(),
            eof: (sequence + 1) * MAX_CHUNK_BYTES >= json.len(),
        };
        host.receive(1, decode_frame(&encode_frame(&chunk).unwrap()).unwrap())
            .unwrap();
    }
}

#[tokio::test]
async fn bad_transfer_declarations_integrity_and_decoded_bounds_are_rejected() {
    for reason in [
        "oversized declaration",
        "bad digest",
        "bad sequence",
        "oversized text",
    ] {
        let host = RemoteHost::new(descriptor()).unwrap();
        let mut peer = proven(&host, 1).await;
        let scope = conn("s", 0, "malformed-transfer");
        open(&host, &mut peer, 1, &scope).await;
        let operation = {
            let host = host.clone();
            tokio::spawn(async move { host.ws_receive(&OpContext::default(), &scope, 32).await })
        };
        let (id, op, method) = request(&mut peer, 1).await;
        let bytes = serde_json::to_vec(&WsReceive::Message("a".repeat(33))).unwrap();
        let declaration = TransferRef {
            transfer_id: reason.into(),
            kind: "json".into(),
            byte_length: if reason == "oversized declaration" {
                method.max_reply_json_bytes().unwrap() as u64 + 1
            } else {
                bytes.len() as u64
            },
            sha256: if reason == "bad digest" {
                sha256(b"fake")
            } else {
                sha256(&bytes)
            },
        };
        response(
            &host,
            1,
            (id, op),
            Ok(serde_json::to_value(declaration).unwrap()),
        )
        .unwrap();
        if reason != "oversized declaration" {
            host.receive(
                1,
                HostFrame::TransferChunk {
                    transfer_id: reason.into(),
                    sequence: u32::from(reason == "bad sequence"),
                    data: bytes,
                    eof: true,
                },
            )
            .unwrap();
        }
        let expected = if reason.starts_with("oversized") {
            Error::Limit
        } else {
            Error::Io
        };
        assert_eq!(operation.await.unwrap(), Err(expected), "{reason}");
        assert!(host.is_ready());
        assert!(host.unresolved_operations().is_empty());
        assert!(host.operation_history.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn cancelled_admitted_read_discards_late_transfer_without_replay_and_keeps_route_ready() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut peer = proven(&host, 1).await;
    let scope = conn("s", 1, "cancelled-read");
    open(&host, &mut peer, 1, &scope).await;
    let cancellation = CancellationToken::new();
    let result = {
        let (host, scope, cancel) = (host.clone(), scope.clone(), cancellation.clone());
        tokio::spawn(async move {
            host.ws_receive(
                &OpContext {
                    cancel,
                    ..Default::default()
                },
                &scope,
                128,
            )
            .await
        })
    };
    let (id, op, _) = request(&mut peer, 1).await;
    cancellation.cancel();
    assert_eq!(result.await.unwrap(), Err(Error::OutcomeUnknown));
    assert!(
        matches!(next_request(&mut peer).await, HostFrame::Cancel { request_id, lease_generation } if request_id == id && lease_generation == "lease-1")
    );
    let sensitive = b"synthetic-late-payload";
    let reference = TransferRef {
        transfer_id: "late".into(),
        byte_length: sensitive.len() as u64,
        sha256: sha256(sensitive),
        kind: "json".into(),
    };
    response(
        &host,
        1,
        (id, op),
        Ok(serde_json::to_value(reference).unwrap()),
    )
    .unwrap();
    host.receive(
        1,
        HostFrame::TransferChunk {
            transfer_id: "late".into(),
            sequence: 0,
            data: sensitive.to_vec(),
            eof: true,
        },
    )
    .unwrap();
    assert_eq!(
        host.ws_receive(&OpContext::default(), &scope, 128).await,
        Err(Error::Stale)
    );
    assert!(host.is_ready());
    assert!(host.unresolved_operations().is_empty());
    assert!(host.operation_history.lock().unwrap().is_empty());
    no_request(&mut peer);
}
