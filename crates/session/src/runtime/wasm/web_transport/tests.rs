use super::*;
use crate::{
    SessionState,
    runtime::wasm::{resources::tests::runtime_state, test_admission},
    state::wasm::SessionBinding,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct FixtureHost {
    events: Mutex<Vec<(&'static str, ConnectionScope)>>,
    closed: tokio::sync::Notify,
    opening: CancellationToken,
    pause_open: AtomicBool,
    fail_close: AtomicBool,
    available: AtomicBool,
    pull_replies: Mutex<std::collections::VecDeque<Result<host::WsPull>>>,
}
impl FixtureHost {
    fn event(&self, name: &'static str, scope: &ConnectionScope) {
        self.events.lock().unwrap().push((name, scope.clone()));
    }
    fn events(&self) -> Vec<(&'static str, ConnectionScope)> {
        self.events.lock().unwrap().clone()
    }
}
fn binding() -> ProjectScope {
    ProjectScope {
        binding_id: "binding".into(),
        backend_id: "backend".into(),
        project_id: "project".into(),
    }
}
impl microcrab_host_api::ProcessApi for FixtureHost {}
impl microcrab_host_api::FsApi for FixtureHost {}
impl microcrab_host_api::NetApi for FixtureHost {}
impl microcrab_host_api::ShellApi for FixtureHost {}
impl microcrab_host_api::HostAPI for FixtureHost {}
#[async_trait::async_trait]
impl Host for FixtureHost {
    fn info(
        &self,
    ) -> std::result::Result<microcrab_host_api::HostInfo, microcrab_host_api::ToolError> {
        unreachable!("these import adapter tests have no catalog")
    }
    fn web_transport_binding(&self) -> Option<ProjectScope> {
        self.available.load(Ordering::Acquire).then(binding)
    }
    async fn http_get(
        &self,
        _: &OpContext,
        scope: &ConnectionScope,
        _: &str,
        _: u32,
    ) -> Result<host::HttpResponse> {
        self.event("get", scope);
        Ok(host::HttpResponse {
            status: 200,
            body: "synthetic".into(),
        })
    }
    async fn ws_open(&self, _: &OpContext, scope: &ConnectionScope, _: &str) -> Result<()> {
        self.event("open", scope);
        self.opening.cancel();
        if self.pause_open.load(Ordering::Acquire) {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    async fn ws_status(&self, _: &OpContext, scope: &ConnectionScope) -> Result<()> {
        self.event("status", scope);
        Ok(())
    }
    async fn ws_send(&self, _: &OpContext, scope: &ConnectionScope, _: &str) -> Result<()> {
        self.event("send", scope);
        Ok(())
    }
    async fn ws_receive(
        &self,
        _: &OpContext,
        scope: &ConnectionScope,
        max: u32,
    ) -> Result<host::WsReceive> {
        self.event("receive", scope);
        assert_eq!(max, host::WEB_MAX_WS_RECEIVE_BYTES);
        Ok(host::WsReceive::Message("x".repeat(max as usize + 1)))
    }
    async fn ws_pull(
        &self,
        _: &OpContext,
        scope: &ConnectionScope,
        _: u32,
        _: u32,
    ) -> Result<host::WsPull> {
        self.event("pull", scope);
        self.pull_replies.lock().unwrap().pop_front().unwrap_or(Ok(host::WsPull::Empty))
    }
    async fn ws_close(&self, context: &OpContext, scope: &ConnectionScope) -> Result<()> {
        assert!(!context.cancel.is_cancelled());
        self.event("close", scope);
        self.closed.notify_one();
        if self.fail_close.load(Ordering::Acquire) {
            return Err(E::OutcomeUnknown);
        }
        Ok(())
    }
}

fn invocation(
    slot: Arc<SlotOwner>,
    host: Arc<FixtureHost>,
    tool: &str,
    endpoint: Option<&str>,
    session: &str,
) -> RuntimeState {
    let mut state = runtime_state(false);
    let mut snapshot = SessionState::new(Default::default());
    snapshot.epoch_id = 42;
    let mut invocation = ToolContext::detached(snapshot, Default::default());
    // Unit tests install the conduit directly. The public dispatch path creates
    // it only on an attached State with a genuine admitted execution.
    invocation.tool = tool.into();
    invocation.call_id = format!("fixture-{tool}");
    invocation.session_id = session.into();
    let connection = slot.lock().active.as_ref().map(|owner| owner.scope.connection_id.clone()).unwrap_or_else(|| "absent".into());
    let arguments = endpoint.map_or_else(|| json!({"socket": connection}), |value| json!({"destination": value}));
    let authority: WebBinding = serde_json::from_value(if endpoint.is_some() {
        json!({"kind":"open","endpoint":"/destination"})
    } else {
        json!({"kind":"existing","connection":"/socket","send":tool != "status"})
    }).unwrap();
    test_admission::admit_arguments(&mut invocation, Some("component".into()), arguments.clone());
    invocation.begin_dispatch(Some("component")).unwrap();
    let mut browser = Invocation::new(
        slot,
        Some(binding()),
        "component".into(),
        &invocation,
        &arguments,
        &authority,
    )
    .unwrap();
    browser.enter();
    state.binding = Some(SessionBinding::from_state(invocation.state.clone()));
    state.tool_context = Some(invocation);
    state.web = Some(browser);
    state.host = Some(host);
    state.runtime = Some(Handle::current());
    state.operation_calls_remaining = 30;
    state.input_bytes_remaining = 64 * 1024;
    state.output_bytes_remaining = 512 * 1024;
    state
}

#[test]
fn endpoint_matching_is_exact_until_same_origin_discovery() {
    let direct = Endpoint::parse("WSS://Browser.Example:443/path?q=a").unwrap();
    assert!(
        direct
            .permits_ws("WSS://Browser.Example:443/path?q=a", false)
            .is_ok()
    );
    assert_eq!(
        direct.permits_ws("wss://browser.example/path?q=a", true),
        Err(E::Denied)
    );
    for (input, exact) in [
        ("127.0.0.1:9222", "http://127.0.0.1:9222/json/version"),
        ("http://127.0.0.1:9222", "http://127.0.0.1:9222/json/version"),
        ("https://[::1]:9222/", "https://[::1]:9222/json/version"),
        ("https://browser.example/?x=1", "https://browser.example/?x=1"),
    ] {
        assert_eq!(Endpoint::parse(input).unwrap().exact, exact);
    }
    let discovered = Endpoint::parse("https://Browser.Example:443/discovery?x=1").unwrap();
    assert_eq!(
        discovered.exact,
        "https://Browser.Example:443/discovery?x=1"
    );
    assert_eq!(
        discovered.permits_ws("wss://browser.example/devtools", false),
        Err(E::Denied)
    );
    assert!(
        discovered
            .permits_ws("wss://browser.example/devtools", true)
            .is_ok()
    );
    for disallowed in [
        "ws://browser.example:443/",
        "wss://127.0.0.1/",
        "wss://browser.example:444/",
    ] {
        assert_eq!(discovered.permits_ws(disallowed, true), Err(E::Denied));
    }
    let bare = Endpoint::parse("[2001:DB8::1]:9222").unwrap();
    assert_eq!(bare.exact, "http://[2001:DB8::1]:9222/json/version");
    assert!(
        bare.permits_ws("ws://[2001:db8:0:0:0::1]:9222/debug", true)
            .is_ok()
    );
    for malformed in [
        "localhost",
        "host:0",
        "host:65536",
        "2001:db8::1:80",
        "https://host:/",
        "https://@host/",
        "https://host/#",
        "https://host/a\\b",
        "https:///host/",
        "https://host/\t",
        "http://{ host}:12/",
        "wss://host/{{secret:example}}",
    ] {
        assert!(
            Endpoint::parse(malformed).is_err(),
            "accepted malformed endpoint {malformed:?}"
        );
    }
    assert!(matches!(
        Endpoint::parse(&format!("ws://host/{}", "x".repeat(4096))),
        Err(E::Limit)
    ));
}

#[test]
fn project_availability_requires_an_exact_negotiated_host_binding() {
    use crate::ComponentCapability::WebTransport;

    let fixture = FixtureHost::default();
    assert!(!WebTransport.available_on_project(&fixture, Some("project")));
    fixture.available.store(true, Ordering::Release);
    assert!(!WebTransport.available_on_project(&fixture, None));
    assert!(!WebTransport.available_on_project(&fixture, Some("other-project")));
    assert!(WebTransport.available_on_project(&fixture, Some("project")));
}

#[tokio::test]
async fn scoped_adapter_refuses_before_dispatch_and_preserves_exact_connection_identity() {
    let fixture = Arc::new(FixtureHost::default());
    let backend = fixture.clone();
    let active = tokio::task::spawn_blocking(move || {
        let slot = Arc::new(SlotOwner::default());
        let mut state = invocation(
            slot.clone(),
            backend.clone(),
            "open_socket",
            Some("example.test:9222"),
            "first",
        );
        let actual = "http://example.test:9222/json/version";
        let ws = "ws://example.test:9222/debug";
        assert!(matches!(
            http_get(&mut state, actual.into(), 64),
            Err(E::Unsupported)
        ));
        state.web.as_mut().unwrap().settle(false);
        drop(state);
        backend.available.store(true, Ordering::Release);
        let mut state = invocation(
            slot.clone(),
            backend.clone(),
            "open_socket",
            Some("example.test:9222"),
            "first",
        );
        assert!(matches!(
            http_get(&mut runtime_state(false), actual.into(), 64),
            Err(E::Denied)
        ));
        // A Host that gains web transport after discovery does not confer
        // authority on a registration that had no admitted Host binding.
        state.web.as_mut().unwrap().binding = None;
        assert!(matches!(
            http_get(&mut state, actual.into(), 64),
            Err(E::Unsupported)
        ));
        state.web.as_mut().unwrap().binding = Some(binding());
        let context = state.tool_context.as_mut().unwrap();
        assert!(
            Invocation::new(
                slot.clone(),
                Some(binding()),
                "component".into(),
                context,
                &json!({"destination":"other.test:9222"}),
                &serde_json::from_value(json!({"kind":"open","endpoint":"/destination"})).unwrap(),
            )
            .is_err()
        );
        context.subject = crate::policy::InvocationSubject::AgentInit {
            component: "component".into(),
            generation: 1,
            attempt: 1,
        };
        assert!(matches!(
            http_get(&mut state, actual.into(), 64),
            Err(E::Denied)
        ));
        state.tool_context.as_mut().unwrap().subject = crate::policy::InvocationSubject::Tool;
        assert!(matches!(
            http_get(&mut state, "http://other.test:9222/json/version".into(), 64),
            Err(E::Denied)
        ));
        assert!(matches!(
            http_get(&mut state, actual.into(), host::WEB_MAX_HTTP_BODY_BYTES + 1),
            Err(E::Limit)
        ));
        assert!(backend.events().is_empty());
        assert_eq!(http_get(&mut state, actual.into(), 64).unwrap().status, 200);
        assert!(matches!(
            http_get(&mut state, actual.into(), 64),
            Err(E::Denied)
        ));
        let id = ws_open(&mut state, ws.into()).unwrap();
        assert!(matches!(ws_open(&mut state, ws.into()), Err(E::Denied)));
        assert_eq!(backend.events()[0].1, backend.events()[1].1);
        assert_eq!(backend.events()[1].1.connection_id, id);
        state.web.as_mut().unwrap().guest_returned(true);
        state.web.as_mut().unwrap().settle(false);
        drop(state);
        let mut observation = invocation(slot.clone(), backend.clone(), "status", None, "first");
        assert!(matches!(
            ws_open(&mut observation, ws.into()),
            Err(E::Denied)
        ));
        assert!(matches!(
            ws_send(&mut observation, id.clone(), "synthetic".into()),
            Err(E::Denied)
        ));
        ws_status(&mut observation, id.clone()).unwrap();
        observation.web.as_mut().unwrap().settle(false);
        let mut foreign = invocation(
            Arc::new(SlotOwner::default()),
            backend.clone(),
            "status",
            None,
            "second",
        );
        assert_eq!(ws_status(&mut foreign, id.clone()), Err(E::Denied));
        let mut snapshot = invocation(slot.clone(), backend.clone(), "snapshot", None, "first");
        assert_eq!(
            ws_send(
                &mut snapshot,
                id.clone(),
                "x".repeat(host::WEB_MAX_WS_SEND_BYTES + 1)
            ),
            Err(E::Limit)
        );
        assert!(matches!(
            ws_receive(&mut snapshot, id.clone(), host::WEB_MAX_WS_RECEIVE_BYTES),
            Err(E::Limit)
        ));
        assert_eq!(ws_status(&mut snapshot, id.clone()), Err(E::Stale));
        snapshot.web.as_mut().unwrap().settle(false);
        slot.retire();
        id
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), fixture.closed.notified())
        .await
        .unwrap();
    let events = fixture.events();
    assert_eq!(
        events.iter().map(|event| event.0).collect::<Vec<_>>(),
        ["get", "open", "status", "receive", "close"]
    );
    assert!(events.iter().all(|(_, scope)| scope.connection_id == active
        && scope.session_id == "first"
        && scope.epoch == 42
        && scope.binding == binding()));
}

#[tokio::test]
async fn transport_uses_one_wall_window_and_excludes_wait_from_guest_budget() {
    let fixture = Arc::new(FixtureHost::default());
    fixture.available.store(true, Ordering::Release);
    tokio::task::spawn_blocking(move || {
        let mut state = invocation(
            Arc::new(SlotOwner::default()),
            fixture,
            "open_socket",
            Some("ws://example.test:9222/debug"),
            "first",
        );
        let initial = state.deadline;
        let (_, first) = begin(&mut state, 0).unwrap();
        let (_, second) = begin(&mut state, 0).unwrap();
        assert_eq!(
            first.deadline, second.deadline,
            "imports cannot renew I/O time"
        );
        let deadline = Instant::now() + Duration::from_millis(25);
        state.web.as_mut().unwrap().transport_deadline = Some(deadline);
        let (_, bounded) = begin(&mut state, 0).unwrap();
        assert_eq!(
            run::<()>(&mut state, &bounded, "ws_receive", std::future::pending()),
            Err(E::OutcomeUnknown)
        );
        assert!(state.deadline > initial);
        assert!(state.deadline <= initial + Duration::from_millis(25));
        assert!(matches!(begin(&mut state, 0), Err(E::Timeout)));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn handed_off_socket_allows_only_scoped_receive_and_acknowledges_native_close() {
    let fixture = Arc::new(FixtureHost::default());
    fixture.available.store(true, Ordering::Release);
    let host = fixture.clone();
    let (continuation, slot, id) = tokio::task::spawn_blocking(move || {
        let slot = Arc::new(SlotOwner::default());
        let endpoint = "ws://example.test:9222/debug";
        let mut state = invocation(slot.clone(), host, "arbitrary_export", Some(endpoint), "first");
        let call = state.web.as_mut().unwrap();
        call.handoff = true;
        call.scoped = Some(ScopedInvocation::default());
        let id = ws_open(&mut state, endpoint.into()).unwrap();
        assert!(state.web.as_mut().unwrap().take_handoff().is_err());
        assert!(matches!(ws_pull(&mut state, id.clone(), 4096, 0), Ok(scoped_wit::WsPullResult::Empty)));
        ws_settle(&mut state, id.clone()).unwrap();
        let grant = state.web.as_mut().unwrap().take_handoff().unwrap();
        state.web.as_mut().unwrap().guest_returned(true);
        finish_invocation(&mut state);
        state.web = Some(grant.step());
        assert!(matches!(ws_send(&mut state, id.clone(), "not authorized".into()), Err(E::Denied)));
        assert!(matches!(ws_open(&mut state, endpoint.into()), Err(E::Denied)));
        assert!(matches!(ws_receive(&mut state, id.clone(), 4096), Err(E::Denied)));
        assert!(matches!(ws_pull(&mut state, id.clone(), 4096, 0), Ok(scoped_wit::WsPullResult::Empty)));
        finish_invocation(&mut state);
        (grant, slot, id)
    }).await.unwrap();
    assert!(slot.active(&id).is_ok());
    continuation.cleanup().await.unwrap();
    continuation.cleanup().await.unwrap();
    assert_eq!(slot.active(&id), Err(E::Stale));
    assert_eq!(fixture.events().iter().filter(|(event, _)| *event == "close").count(), 1);
}

#[tokio::test]
async fn scoped_ack_is_exact_and_preserves_only_a_settled_failure() {
    let fixture = Arc::new(FixtureHost::default());
    fixture.available.store(true, Ordering::Release);
    tokio::task::spawn_blocking(move || {
        for case in ["settled", "legacy", "preflight-only", "post-ack-io", "late", "cancelled", "failed-open", "unknown-delivery"] {
            let slot = Arc::new(SlotOwner::default());
            let make = |tool, endpoint| invocation(slot.clone(), fixture.clone(), tool, endpoint, "same-session");
            let mut connect = make("open_socket", Some("ws://example.test:9222/debug"));
            let id = ws_open(&mut connect, "ws://example.test:9222/debug".into()).unwrap();
            let scope = slot.active(&id).unwrap();
            if case != "failed-open" {
                connect.web.as_mut().unwrap().guest_returned(true);
                finish_invocation(&mut connect);
            }
            let mut tool = if case == "failed-open" { connect } else { make("snapshot", None) };
            if case != "legacy" {
                tool.web.as_mut().unwrap().scoped = Some(ScopedInvocation::default());
            }
            assert_eq!(ws_settle(&mut tool, id.clone()), Err(E::Denied));
            ws_send(&mut tool, id.clone(), "synthetic".into()).unwrap();
            if case != "legacy" {
                let cap = host::WEB_MAX_WS_RECEIVE_BYTES;
                let budget = ws_budget(&mut tool, id.clone()).unwrap();
                let hard = tool.web.as_ref().unwrap().transport_deadline;
                assert!(budget.transport_ms <= host::WEB_MAX_BUDGET_MS);
                assert!(matches!(ws_pull(&mut tool, id.clone(), cap, 1), Ok(scoped_wit::WsPullResult::Empty)));
                assert_eq!(ws_settle(&mut tool, id.clone()), Err(E::Denied), "positive wait is not a drain receipt");
                assert_eq!(tool.web.as_ref().unwrap().transport_deadline, hard);
                assert!(matches!(ws_pull(&mut tool, id.clone(), cap, 0), Ok(scoped_wit::WsPullResult::Empty)));
                assert_eq!(ws_settle(&mut tool, "wrong-generation".into()), Err(E::Denied));
                match case {
                    "preflight-only" => {
                        ws_send(&mut tool, id.clone(), "new command".into()).unwrap();
                        assert_eq!(ws_settle(&mut tool, id.clone()), Err(E::Denied));
                    }
                    "unknown-delivery" => {
                        fixture.pull_replies.lock().unwrap().push_back(Err(E::OutcomeUnknown));
                        assert!(matches!(ws_pull(&mut tool, id.clone(), cap, 0), Err(E::OutcomeUnknown)));
                        assert_eq!(ws_settle(&mut tool, id.clone()), Err(E::Stale));
                    }
                    _ => {
                        ws_settle(&mut tool, id.clone()).unwrap();
                        match case {
                            "post-ack-io" => assert_eq!(ws_send(&mut tool, id.clone(), "denied".into()), Err(E::Denied)),
                            "late" => tool.web.as_mut().unwrap().transport_deadline = Some(Instant::now()),
                            "cancelled" => tool.cancellation.cancel(),
                            _ => {}
                        }
                    }
                }
            }
            // A typed failure reaches normal Runtime finalization with guest_success=false.
            finish_invocation(&mut tool);
            if case == "settled" {
                assert_eq!(slot.active(&id), Ok(scope));
                let mut next = make("snapshot", None);
                next.web.as_mut().unwrap().scoped = Some(ScopedInvocation::default());
                assert_eq!(ws_settle(&mut next, id.clone()), Err(E::Denied));
                ws_send(&mut next, id.clone(), "next original call".into()).unwrap();
                finish_invocation(&mut next);
            } else {
                assert_eq!(slot.active(&id), Err(E::Stale), "{case}");
            }
        }
    }).await.unwrap();
}

#[tokio::test]
async fn wasm_recovery_keeps_exact_close_receipt_after_connection_is_forgotten() {
    let fixture = Arc::new(FixtureHost::default());
    fixture.available.store(true, Ordering::Release);
    fixture.fail_close.store(true, Ordering::Release);
    let slot = Arc::new(SlotOwner::default());
    let id = tokio::task::spawn_blocking({
        let slot = slot.clone();
        let fixture = fixture.clone();
        move || {
            let url = "ws://example.test:9222/debug";
            let mut call = invocation(slot, fixture, "open_socket", Some(url), "first");
            let id = ws_open(&mut call, url.into()).unwrap();
            // Dropping an unsettled invocation fences and schedules exact close
            // before the worker's failure path discovers and retires this slot.
            drop(call);
            id
        }
    }).await.unwrap();
    assert_eq!(slot.active(&id), Err(E::Stale));
    slot.retire();
    assert_eq!(slot.acknowledge_retirement().await, Err(E::OutcomeUnknown));
    fixture.fail_close.store(false, Ordering::Release);
    slot.acknowledge_retirement().await.unwrap();
    slot.acknowledge_retirement().await.unwrap();
    let events = fixture.events();
    assert_eq!(events[0].0, "open");
    assert_eq!(events[0].1.connection_id, id);
    assert!(events[1..].iter().all(|(kind, scope)| *kind == "close" && *scope == events[0].1));
    assert_eq!(slot.active(&id), Err(E::Cancelled));
}

#[tokio::test]
async fn cancellation_or_epoch_retire_fences_a_pending_open() {
    for retire in [false, true] {
        let fixture = Arc::new(FixtureHost::default());
        fixture.available.store(true, Ordering::Release);
        fixture.pause_open.store(true, Ordering::Release);
        let backend = fixture.clone();
        let slot = Arc::new(SlotOwner::default());
        let owner = slot.clone();
        let cancellation = CancellationToken::new();
        let cancel = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let mut state = invocation(
                owner,
                backend,
                "open_socket",
                Some("ws://example.test:9222/debug"),
                "first",
            );
            if retire {
                state.epoch_cancellation = cancel;
            } else {
                state.cancellation = cancel;
            }
            ws_open(&mut state, "ws://example.test:9222/debug".into())
        });
        tokio::time::timeout(Duration::from_secs(2), fixture.opening.cancelled())
            .await
            .unwrap();
        cancellation.cancel();
        if retire {
            slot.retire();
        }
        assert_eq!(worker.await.unwrap(), Err(E::Cancelled));
        tokio::time::timeout(Duration::from_secs(2), fixture.closed.notified())
            .await
            .unwrap();
        let events = fixture.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, "open");
        assert_eq!(events[1], ("close", events[0].1.clone()));
        assert!(slot.active(&events[0].1.connection_id).is_err());
    }
}
