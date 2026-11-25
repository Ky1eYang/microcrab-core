use super::*;
use microcrab_host_api::{
    GlobIgnoreRuleFile, GlobIgnoreSnapshot, GlobOutput, GlobRequest, GlobScanIncomplete,
    GlobScanReason, OpContext, ResolvedGlobOutput,
};

#[tokio::test]
async fn resolved_paths_keep_their_own_route_ack_and_scan_evidence() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready_with_extensions(&host, 1, scope(), (false, true)).await;
    assert!(matches!(
        host.glob_ignore_snapshot(&OpContext::default()).await,
        Err(ToolError::Unsupported(_))
    ));
    let task = {
        let host = host.clone();
        tokio::spawn(async move {
            host.glob_with_ignored_paths(
                &OpContext::default(),
                GlobRequest {
                    root: ".".into(),
                    pattern: "**".into(),
                    include_hidden: false,
                    include_ignored: false,
                    include_directories: false,
                    max_results: 1,
                },
                vec!["target/".into()],
            )
            .await
        })
    };
    let frame = next_request(&mut receiver).await;
    let frame = microcrab_host_protocol::decode_frame(
        &microcrab_host_protocol::encode_frame(&frame).unwrap(),
    )
    .unwrap();
    let HostFrame::Request {
        method: HostRequest::GlobWithIgnoredPaths { paths, .. },
        request_id,
        operation_id,
        ..
    } = frame
    else {
        panic!("expected resolved-path search");
    };
    assert_eq!(paths, ["target/"]);
    let expected = ResolvedGlobOutput {
        entries: vec![],
        truncated: false,
        scan_incomplete: Some(GlobScanIncomplete {
            reason: GlobScanReason::Entries,
            limit: 16_384,
            observed: 16_385,
        }),
    };
    let reply = HostFrame::Response {
        request_id,
        operation_id,
        result: Ok(serde_json::to_value(&expected).unwrap()),
    };
    host.receive(
        1,
        microcrab_host_protocol::decode_frame(
            &microcrab_host_protocol::encode_frame(&reply).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(task.await.unwrap().unwrap(), expected);
    let _replacement = ready_with_extensions(&host, 2, scope(), (true, false)).await;
    assert!(matches!(
        host.glob_ignored_paths(&OpContext::default()).await,
        Err(ToolError::Unsupported(_))
    ));
    host.detach(2);
}

#[tokio::test]
async fn negotiated_glob_ignore_round_trips_and_does_not_cross_route_replacement() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready_with_extensions(&host, 1, scope(), (true, false)).await;
    assert!(matches!(
        host.glob_ignored_paths(&OpContext::default()).await,
        Err(ToolError::Unsupported(_))
    ));
    let snapshot = GlobIgnoreSnapshot {
        rules: vec![GlobIgnoreRuleFile {
            path: "sub/.gitignore".into(),
            content: "/build/\n".into(),
        }],
        truncated: false,
        pruned: true,
    };
    let task = {
        let host = host.clone();
        tokio::spawn(async move {
            host.glob_ignore_snapshot(&OpContext {
                deadline: Some(Instant::now() + std::time::Duration::from_secs(5)),
                ..Default::default()
            })
            .await
        })
    };
    let frame = next_request(&mut receiver).await;
    let frame = microcrab_host_protocol::decode_frame(
        &microcrab_host_protocol::encode_frame(&frame).unwrap(),
    )
    .unwrap();
    let HostFrame::Request {
        method: HostRequest::GlobIgnoreSnapshot,
        remaining_budget_ms,
        request_id,
        operation_id,
        ..
    } = frame
    else {
        panic!("expected typed snapshot request");
    };
    assert!((1..=5_000).contains(&remaining_budget_ms));
    let response = HostFrame::Response {
        request_id,
        operation_id,
        result: Ok(serde_json::to_value(&snapshot).unwrap()),
    };
    let response = microcrab_host_protocol::decode_frame(
        &microcrab_host_protocol::encode_frame(&response).unwrap(),
    )
    .unwrap();
    host.receive(1, response).unwrap();
    assert_eq!(task.await.unwrap().unwrap(), snapshot);

    let rules = snapshot.rules;
    let task = {
        let host = host.clone();
        let rules = rules.clone();
        tokio::spawn(async move {
            host.glob_with_ignore_rules(
                &OpContext::default(),
                GlobRequest {
                    root: "sub".into(),
                    pattern: "**".into(),
                    include_hidden: false,
                    include_ignored: false,
                    include_directories: false,
                    max_results: 8,
                },
                rules,
            )
            .await
        })
    };
    let frame = next_request(&mut receiver).await;
    let frame = microcrab_host_protocol::decode_frame(
        &microcrab_host_protocol::encode_frame(&frame).unwrap(),
    )
    .unwrap();
    let HostFrame::Request {
        method:
            HostRequest::GlobWithIgnoreRules {
                request,
                rules: actual,
            },
        request_id,
        operation_id,
        ..
    } = frame
    else {
        panic!("expected explicit rules request");
    };
    assert_eq!(request.root, std::path::Path::new("sub"));
    assert_eq!(actual, rules);
    let expected = GlobOutput {
        entries: vec![],
        truncated: false,
    };
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::to_value(&expected).unwrap()),
        },
    )
    .unwrap();
    assert_eq!(task.await.unwrap().unwrap(), expected);
    assert!(host.unresolved.lock().unwrap().is_empty());

    let mut receiver = ready(&host, 2).await;
    assert!(matches!(
        host.glob_ignore_snapshot(&OpContext::default()).await,
        Err(ToolError::Unsupported(_))
    ));
    assert!(host.is_ready());
    while let Ok(frame) = receiver.try_recv() {
        assert!(matches!(frame, HostFrame::Ping { .. }));
    }
    host.detach(2);
}
