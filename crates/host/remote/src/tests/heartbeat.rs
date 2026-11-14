//! Heartbeat transport pressure is observable without revoking healthy execution.
use super::*;

fn stop_automatic_heartbeat(host: &RemoteHost) -> (mpsc::Sender<HostFrame>, String) {
    let route = host.route.lock().unwrap();
    let route = route.as_ref().unwrap();
    route.stop.cancel();
    (route.sender.clone(), route.lease.clone())
}

#[tokio::test]
async fn recent_inbound_backpressure_keeps_ready_route_and_retry_clears_warning() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let (sender, lease) = stop_automatic_heartbeat(&host);
    // The automatic task observes stop before another tick; remove its optional
    // first Ping, then fill the bounded channel without depending on timing.
    tokio::task::yield_now().await;
    while receiver.try_recv().is_ok() {}
    while sender
        .try_send(HostFrame::Ping {
            lease_generation: lease.clone(),
        })
        .is_ok()
    {}
    let heartbeat = host.heartbeat_once(1, &sender, &lease);
    let inbound = async {
        host.receive(
            1,
            HostFrame::Pong {
                lease_generation: lease.clone(),
            },
        )
        .unwrap();
    };
    let (keep, ()) = tokio::join!(heartbeat, inbound);
    assert!(keep);
    let pressured = host.readiness();
    assert_eq!(pressured["connected"], true);
    assert_eq!(pressured["ready"], true);
    assert_eq!(pressured["cause"], serde_json::Value::Null);
    assert_eq!(
        pressured["transport_warning"],
        json!({"code":"heartbeat_backpressure",
        "message":"Host heartbeat send exceeded 2 second deadline; recent inbound traffic keeps the route connected"})
    );
    assert_eq!(host.diagnostics(1)[0]["reason"], "heartbeat_backpressure");
    assert_eq!(host.diagnostics(1)[0]["connection_generation"], "1");
    while receiver.try_recv().is_ok() {}
    assert!(host.heartbeat_once(1, &sender, &lease).await);
    assert!(matches!(
        receiver.recv().await,
        Some(HostFrame::Ping { .. })
    ));
    let healthy = host.readiness();
    assert_eq!(healthy["transport_warning"], serde_json::Value::Null);
    assert_eq!(healthy["ready"], true);
    host.detach(1);
}

#[tokio::test]
async fn heartbeat_closed_outbound_still_detaches_with_precise_cause() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let receiver = ready(&host, 1).await;
    let (sender, lease) = stop_automatic_heartbeat(&host);
    drop(receiver);
    assert!(!host.heartbeat_once(1, &sender, &lease).await);
    let snapshot = host.readiness();
    assert_eq!(snapshot["connected"], false);
    assert_eq!(
        snapshot["cause"],
        json!({"code":"disconnected", "message":"Host heartbeat outbound route closed"})
    );
    assert_eq!(snapshot["transport_warning"], serde_json::Value::Null);
}

#[tokio::test]
async fn heartbeat_silence_still_detaches_and_old_warning_cannot_affect_new_route() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let _receiver = ready(&host, 1).await;
    let (sender, lease) = stop_automatic_heartbeat(&host);
    host.heartbeat_warning(1, true);
    host.route.lock().unwrap().as_mut().unwrap().last_received =
        Instant::now() - std::time::Duration::from_secs(46);
    assert!(!host.heartbeat_once(1, &sender, &lease).await);
    let snapshot = host.readiness();
    assert_eq!(snapshot["connected"], false);
    assert_eq!(
        snapshot["cause"],
        json!({"code":"timeout", "message":"Host heartbeat received no frame for 45 seconds"})
    );
    assert_eq!(snapshot["transport_warning"], serde_json::Value::Null);
    let (replacement, _receiver) = mpsc::channel(8);
    host.attach(replacement, 2).unwrap();
    let before = host.readiness();
    host.heartbeat_warning(1, true);
    assert_eq!(host.readiness(), before);
    assert!(!host.heartbeat_once(1, &sender, &lease).await);
    assert_eq!(host.readiness(), before);
    host.detach(2);
}
