//! Consumption receipts remain exact while independent effects and cleanup progress.
use super::*;
use microcrab_host_api::{HandleId, process::ProcessHandle};

async fn control(receiver: &mut mpsc::Receiver<HostFrame>) -> HostFrame {
    loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(2), next_request(receiver))
            .await
            .unwrap();
        if !matches!(frame, HostFrame::Cancel { .. }) {
            return frame;
        }
    }
}

#[tokio::test]
async fn cancelled_consumption_fences_only_its_output_until_exact_terminal_receipt() {
    for raw in [false, true] {
        let host = RemoteHost::new(descriptor()).unwrap();
        let mut receiver = ready(&host, 1).await;
        let handle = ProcessHandle(HandleId(7));
        let method = if raw {
            HostRequest::ProcessStreamRead {
                handle,
                stderr: false,
                max_bytes: 1024,
            }
        } else {
            HostRequest::ShellDrain {
                handle,
                max_bytes: 1024,
            }
        };
        let cancel = CancellationToken::new();
        let task = {
            let host = host.clone();
            let method = method.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { host.call(method, cancel).await })
        };
        let HostFrame::Request { operation_id, .. } = control(&mut receiver).await else {
            panic!("expected consumption")
        };
        cancel.cancel();
        assert!(matches!(task.await.unwrap(), Err(ToolError::Cancelled)));
        assert!(host.is_ready());
        assert_eq!(host.unresolved_operations(), vec![operation_id.clone()]);
        assert!(
            host.call(method.clone(), CancellationToken::new())
                .await
                .is_err(),
            "same output must not advance after an unknown consumption"
        );
        let HostFrame::Status {
            request_id: status,
            operation_id: queried,
            ..
        } = control(&mut receiver).await
        else {
            panic!("only original receipt observation is automatic")
        };
        assert_eq!(queried, operation_id);
        // Hold the receipt observer. Workspace effects and affirmative cleanup
        // still reach the transport while this output remains indeterminate.
        for independent in [
            HostRequest::Stat {
                path: "next".into(),
            },
            HostRequest::Write(WriteRequest {
                path: "next".into(),
                content: "text".into(),
                precondition: WritePrecondition::Missing,
            }),
            HostRequest::ProcessKill {
                handle,
                force: true,
            },
            HostRequest::ProcessRelease { handle },
        ] {
            let expected = independent.method_name();
            let task = {
                let host = host.clone();
                tokio::spawn(async move { host.call(independent, CancellationToken::new()).await })
            };
            let HostFrame::Request {
                request_id,
                operation_id,
                method,
                ..
            } = control(&mut receiver).await
            else {
                panic!("independent effect must progress")
            };
            assert_eq!(method.method_name(), expected);
            host.receive(
                1,
                HostFrame::Response {
                    request_id,
                    operation_id,
                    result: Ok(json!({})),
                },
            )
            .unwrap();
            task.await.unwrap().unwrap();
        }
        host.receive(
            1,
            HostFrame::Receipt {
                request_id: status,
                receipt: Some(OperationReceipt {
                    operation_id: operation_id.clone(),
                    payload_digest: payload_digest(&method).unwrap(),
                    scope: scope(),
                    state: ReceiptState::Completed(Ok(
                        json!({"abandoned_output":"never redelivered or replayed"}),
                    )),
                }),
            },
        )
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !host.unresolved_operations().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(host.is_ready());
        while let Ok(frame) = receiver.try_recv() {
            assert!(!matches!(frame, HostFrame::Request { .. }));
        }
        host.detach(1);
    }
}

#[tokio::test]
async fn missing_output_receipt_retains_scoped_no_replay_fence() {
    let host = RemoteHost::new(descriptor()).unwrap();
    let mut receiver = ready(&host, 1).await;
    let method = HostRequest::ShellDrain {
        handle: ProcessHandle(HandleId(7)),
        max_bytes: 1024,
    };
    let task = {
        let host = host.clone();
        let method = method.clone();
        tokio::spawn(async move { host.call(method, CancellationToken::new()).await })
    };
    let HostFrame::Request { operation_id, .. } = control(&mut receiver).await else {
        panic!("consumption")
    };
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let HostFrame::Status { request_id, .. } = control(&mut receiver).await else {
        panic!("receipt observation")
    };
    host.receive(
        1,
        HostFrame::Receipt {
            request_id,
            receipt: None,
        },
    )
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !host.consumption_recovery.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(host.is_ready());
    assert_eq!(host.unresolved_operations(), vec![operation_id]);
    assert!(host.call(method, CancellationToken::new()).await.is_err());
    while let Ok(frame) = receiver.try_recv() {
        assert!(!matches!(frame, HostFrame::Request { .. }));
    }
    host.detach(1);
}
