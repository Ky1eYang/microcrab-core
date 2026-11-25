use super::*;
use pb::private_file_frame::Body;

fn transfer(max_bytes: usize) -> PrivateFileTransfer {
    PrivateFileTransfer {
        request: pb::PrivateFileRequest {
            operation_id: "exact".into(),
            ..Default::default()
        },
        buffer: PrivateFileBuffer::new(max_bytes, ()),
        declared: None,
        offset: 0,
        next: 0,
        eof: false,
    }
}
fn header(operation: &str, data: &[u8]) -> pb::PrivateFileFrame {
    pb::PrivateFileFrame {
        body: Some(Body::Transfer(pb::PrivateFileTransfer {
            operation_id: operation.into(),
            byte_length: data.len() as u32,
            sha256: Sha256::digest(data).to_vec(),
        })),
    }
}
fn chunk(id: &str, sequence: u32, data: &[u8], eof: bool) -> pb::PrivateFileFrame {
    pb::PrivateFileFrame {
        body: Some(Body::Chunk(pb::TransferChunk {
            transfer_id: id.into(),
            sequence,
            data: data.to_vec(),
            eof,
        })),
    }
}

#[test]
fn private_file_transfer_rejects_inexact_or_incomplete_streams() {
    assert!(transfer(3).receive(header("foreign", b"abc")).is_err());
    assert!(matches!(
        transfer(2).receive(header("exact", b"abc")),
        Err(ToolError::FileLimit)
    ));
    for bad in [
        chunk("wrong", 0, b"abc", true),
        chunk("exact", 1, b"abc", true),
        chunk("exact", 0, b"ab", true),
        chunk("exact", 0, b"abc", false),
        chunk("exact", 0, b"abcd", true),
        chunk("exact", 0, b"", false),
        header("exact", b"abc"),
    ] {
        let mut transfer = transfer(3);
        assert!(transfer.receive(chunk("exact", 0, b"abc", true)).is_err());
        transfer.receive(header("exact", b"abc")).unwrap();
        assert!(transfer.receive(bad).is_err());
        assert!(transfer.finish().is_err());
    }
    let mut corrupt = transfer(3);
    corrupt.receive(header("exact", b"abc")).unwrap();
    corrupt.receive(chunk("exact", 0, b"xyz", true)).unwrap();
    assert!(corrupt.finish().is_err());
    let mut exact = transfer(3);
    exact.receive(header("exact", b"\xff\0\x80")).unwrap();
    exact.receive(chunk("exact", 0, b"\xff\0", false)).unwrap();
    exact.receive(chunk("exact", 1, b"\x80", true)).unwrap();
    assert!(exact.receive(chunk("exact", 2, b"", true)).is_err());
    assert_eq!(exact.finish().unwrap().as_bytes(), b"\xff\0\x80");
}

struct HangingReader(tokio::sync::Notify);
#[async_trait::async_trait]
impl PrivateFileReader for HangingReader {
    async fn read(&self, transfer: PrivateFileTransfer) -> Result<PrivateFileBuffer, ToolError> {
        let req = transfer.request();
        assert_eq!(req.lease_generation, "lease-1");
        assert_eq!(req.scope.as_ref().unwrap().project_id, "project");
        self.0.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn private_file_read_cancellation_and_replacement_drop_only_the_original_read() {
    let host = RemoteHost::new(crate::tests::descriptor()).unwrap();
    host.set_scope(BindingScope::project(
        "backend".into(),
        "binding".into(),
        "project".into(),
    ))
    .unwrap();
    let (tx, _rx) = mpsc::channel(32);
    let reader = Arc::new(HangingReader(tokio::sync::Notify::new()));
    host.attach_route(
        tx.clone(),
        1,
        "lease-1".into(),
        "instance".into(),
        Some(reader.clone()),
    )
    .unwrap();
    let (pass, revision) = host.begin_reconciliation(1).unwrap();
    assert!(host.finish_reconciliation(1, pass, revision, &Ok(())));
    assert!(host.supports_private_file_read());
    for replace in [false, true] {
        let context = OpContext::default();
        let cancel = context.cancel.clone();
        let task = {
            let host = host.clone();
            tokio::spawn(async move {
                host.read_private_file(
                    "/remote/workspace/test".into(),
                    PrivateFileBuffer::new(1024, ()),
                    &context,
                )
                .await
            })
        };
        reader.0.notified().await;
        if replace {
            host.attach(tx.clone(), 2).unwrap();
            assert!(!host.supports_private_file_read());
        } else {
            cancel.cancel();
        }
        let result = task.await.unwrap();
        if replace {
            assert!(matches!(result, Err(error) if error.to_string().contains("route retired")));
        } else {
            assert!(matches!(result, Err(ToolError::Cancelled)));
        }
    }
    let (pass, revision) = host.begin_reconciliation(2).unwrap();
    assert!(host.finish_reconciliation(2, pass, revision, &Ok(())));
    let read = async || {
        host.read_private_file(
            "/remote/workspace/test".into(),
            PrivateFileBuffer::new(1024, ()),
            &OpContext::default(),
        )
        .await
    };
    assert!(matches!(read().await, Err(ToolError::Unsupported(_))));
    host.detach(2);
    assert!(matches!(read().await, Err(error) if error.to_string().contains("disconnected")));
}
