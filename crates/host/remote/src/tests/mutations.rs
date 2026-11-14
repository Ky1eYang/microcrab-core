use super::*;
use microcrab_host_api::{DeleteRequest, FileMutation, MutationOutput};

fn mutation_host() -> Arc<RemoteHost> {
    let mut descriptor = descriptor();
    descriptor.tool_capabilities.push("file_mutation".into());
    descriptor.info.capabilities.push("file_mutation".into());
    descriptor.supported_primitives.push("file_mutation".into());
    descriptor.descriptor_digest = descriptor_fingerprint(&descriptor).unwrap();
    RemoteHost::new(descriptor).unwrap()
}
fn deletion() -> FileMutation {
    FileMutation::Delete(DeleteRequest {
        path: "source".into(),
        expected_version: "a".repeat(64),
    })
}

#[tokio::test]
async fn oversized_whole_preflight_and_commit_fail_before_intent_or_send() {
    let host = mutation_host();
    let mut receiver = ready(&host, 1).await;
    assert!(host.supports("file_mutation"));
    let request = FileMutation::Write(WriteRequest {
        path: "destination".into(),
        content: "x".repeat(microcrab_host_protocol::MAX_FRAME_BYTES),
        precondition: WritePrecondition::Missing,
    });
    assert!(matches!(
        Host::preflight_mutations(
            host.as_ref(),
            vec![request.clone()],
            CancellationToken::new()
        )
        .await,
        Err(ToolError::OperationLimit(_))
    ));
    assert!(matches!(
        Host::mutate(host.as_ref(), request, CancellationToken::new()).await,
        Err(ToolError::OperationLimit(_))
    ));
    assert!(host.unresolved_operations().is_empty());
    assert!(host.is_ready());
    while let Ok(frame) = receiver.try_recv() {
        assert!(!matches!(
            frame,
            HostFrame::Request {
                method: HostRequest::Mutate(_) | HostRequest::PreflightMutations { .. },
                ..
            }
        ));
    }
    host.detach(1);
}

#[tokio::test]
async fn mutation_preflight_and_partial_commit_receipts_are_mapped_without_fallback() {
    let host = mutation_host();
    let mut receiver = ready(&host, 1).await;
    let preflight = {
        let host = host.clone();
        tokio::spawn(async move {
            Host::preflight_mutations(host.as_ref(), vec![deletion()], CancellationToken::new())
                .await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected request")
    };
    assert!(matches!(method, HostRequest::PreflightMutations { .. }));
    assert!(host.unresolved_operations().is_empty());
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::to_value(vec![deletion()]).unwrap()),
        },
    )
    .unwrap();
    assert_eq!(preflight.await.unwrap().unwrap().len(), 1);
    let mutation = {
        let host = host.clone();
        tokio::spawn(async move {
            Host::mutate(
                host.as_ref(),
                FileMutation::Move(microcrab_host_api::MoveRequest {
                    path: "source".into(),
                    destination: "destination".into(),
                    expected_version: "a".repeat(64),
                    content: "after".into(),
                }),
                CancellationToken::new(),
            )
            .await
        })
    };
    let HostFrame::Request {
        request_id,
        operation_id,
        method,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected request")
    };
    assert!(matches!(method, HostRequest::Mutate(_)));
    assert_eq!(host.unresolved_operations(), vec![operation_id.clone()]);
    let output = MutationOutput {
        version: Some("b".repeat(64)),
        bytes: 5,
        source_removed: false,
        destination_written: true,
        diagnostic: Some("source removal failed: permission denied".into()),
    };
    host.receive(
        1,
        HostFrame::Response {
            request_id,
            operation_id,
            result: Ok(serde_json::to_value(&output).unwrap()),
        },
    )
    .unwrap();
    assert_eq!(mutation.await.unwrap().unwrap(), output);
    assert!(host.unresolved_operations().is_empty());
    host.detach(1);
}

#[tokio::test]
async fn admitted_mutation_cancellation_fences_unknown_outcome_instead_of_replaying() {
    let host = mutation_host();
    let mut receiver = ready(&host, 1).await;
    let cancel = CancellationToken::new();
    let task = {
        let host = host.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move { Host::mutate(host.as_ref(), deletion(), cancel).await })
    };
    let HostFrame::Request {
        operation_id,
        method,
        ..
    } = next_request(&mut receiver).await
    else {
        panic!("expected request")
    };
    assert!(matches!(method, HostRequest::Mutate(_)));
    cancel.cancel();
    let error = task.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("unknown outcome"));
    assert!(host.unresolved_operations().contains(&operation_id));
    assert!(!host.is_ready());
    // Recovery may send Status/Recovery/Cancel; it must never replay Mutate.
    while let Ok(frame) = receiver.try_recv() {
        assert!(!matches!(
            frame,
            HostFrame::Request {
                method: HostRequest::Mutate(_),
                ..
            }
        ));
    }
    host.detach(1);
}
