//! Protobuf `HostFrame` shared by the WebSocket relay and the gRPC carrier.
//! Routing fields and chunk bytes are protobuf. Nested contracts that already
//! have one serde schema travel as that JSON, unchanged between carriers.
use crate::{
    BindingScope, BridgeDiagnostic, ErrorCode, HostCause, HostDescriptor, HostRequest,
    MAX_CHUNK_BYTES, MAX_FRAME_BYTES, OperationReceipt, WireError, WireFrame,
};
use microcrab_host_api::{HandleId, process::ProcessHandle};
use prost::Message;

#[allow(clippy::all, clippy::pedantic, clippy::nursery)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/microcrab.host.v1.rs"));
}

#[cfg(feature = "grpc")]
pub use pb::host_bridge_client::HostBridgeClient;
#[cfg(feature = "grpc")]
pub use pb::host_bridge_server::{HostBridge, HostBridgeServer};

fn invalid(message: impl Into<String>) -> WireError {
    WireError::new(ErrorCode::InvalidRequest, message)
}

fn json_bytes(value: &impl serde::Serialize) -> Result<Vec<u8>, WireError> {
    serde_json::to_vec(value).map_err(|_| invalid("cannot encode Host frame JSON"))
}

fn json_from_slice<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, WireError> {
    serde_json::from_slice(bytes).map_err(|error| {
        invalid(format!(
            "Host frame JSON: {}",
            microcrab_protocol::diagnostic::json_reason(&error)
        ))
    })
}

fn code_name(code: &ErrorCode) -> Result<String, WireError> {
    match serde_json::to_value(code).map_err(|_| invalid("cannot encode Host error code"))? {
        serde_json::Value::String(name) => Ok(name),
        _ => Err(invalid("Host error code encoding is not a string")),
    }
}

fn code_from_name(name: &str) -> Result<ErrorCode, WireError> {
    serde_json::from_value(serde_json::Value::String(name.into())).map_err(|_| {
        invalid(format!(
            "unknown Host error code; received name is {} bytes; expected an ErrorCode variant",
            name.len()
        ))
    })
}

fn cause_name(cause: HostCause) -> Result<String, WireError> {
    match serde_json::to_value(cause).map_err(|_| invalid("cannot encode Host cause"))? {
        serde_json::Value::String(name) => Ok(name),
        _ => Err(invalid("Host cause encoding is not a string")),
    }
}

fn cause_from_name(name: &str) -> Result<HostCause, WireError> {
    serde_json::from_value(serde_json::Value::String(name.into())).map_err(|_| {
        invalid(format!(
            "unknown Host cause; received name is {} bytes; expected a HostCause variant",
            name.len()
        ))
    })
}

fn proto_error(error: &WireError) -> Result<pb::WireError, WireError> {
    Ok(pb::WireError {
        code: code_name(&error.code)?,
        message: error.message.clone(),
        cause: error.cause.map(cause_name).transpose()?,
    })
}

fn rust_error(error: pb::WireError) -> Result<WireError, WireError> {
    Ok(WireError {
        code: code_from_name(&error.code)?,
        message: microcrab_protocol::diagnostic::sanitize(&error.message, 4096),
        cause: error.cause.as_deref().map(cause_from_name).transpose()?,
    })
}

fn encode_message(frame: pb::HostFrame) -> Result<Vec<u8>, WireError> {
    let mut bytes = Vec::new();
    frame
        .encode(&mut bytes)
        .map_err(|_| invalid("cannot encode Host frame"))?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(invalid("Host frame exceeds the protocol byte limit"));
    }
    Ok(bytes)
}

fn chunk(
    transfer_id: String,
    sequence: u32,
    data: Vec<u8>,
    eof: bool,
) -> Result<WireFrame, WireError> {
    if transfer_id.is_empty() || transfer_id.len() > 256 || data.len() > MAX_CHUNK_BYTES {
        return Err(invalid(
            "Host transfer chunk identity or payload is outside the protocol bound",
        ));
    }
    Ok(WireFrame::TransferChunk {
        transfer_id,
        sequence,
        data,
        eof,
    })
}

pub fn frame_to_proto(frame: &WireFrame) -> Result<pb::HostFrame, WireError> {
    pb::HostFrame::decode(encode_frame(frame)?.as_slice())
        .map_err(|_| invalid("encoded Host frame is not a protobuf HostFrame"))
}

pub fn frame_from_proto(frame: pb::HostFrame) -> Result<WireFrame, WireError> {
    let mut bytes = Vec::new();
    frame
        .encode(&mut bytes)
        .map_err(|_| invalid("cannot encode Host frame"))?;
    decode_frame(&bytes)
}

/// Encode one native frame. Callers send these bytes as one WebSocket binary
/// message or as one gRPC `HostFrame`.
pub fn encode_frame(frame: &WireFrame) -> Result<Vec<u8>, WireError> {
    use pb::host_frame::Body;
    let body = match frame {
        WireFrame::Inspect { request } => {
            request.validate()?;
            let authentication = match frame_to_proto(&request.handshake())?.body {
                Some(Body::Pair(pair)) => pb::inspect::Authentication::Pair(pair),
                Some(Body::Resume(resume)) => pb::inspect::Authentication::Resume(resume),
                _ => return Err(invalid("inspection requires authentication")),
            };
            Body::Inspect(pb::Inspect {
                request_id: request.request_id.clone(),
                authentication: Some(authentication),
            })
        }
        WireFrame::Inspected { request_id, result } => Body::Inspected(pb::Inspected {
            request_id: request_id.clone(),
            result: Some(match result {
                Ok(descriptor) => pb::inspected::Result::DescriptorJson(json_bytes(descriptor)?),
                Err(error) => pb::inspected::Result::Error(proto_error(error)?),
            }),
        }),
        WireFrame::HandshakeRejected { error } => Body::HandshakeRejected(pb::HandshakeRejected {
            error: Some(proto_error(error)?),
        }),
        WireFrame::Pair {
            token,
            scope,
            directory,
            expected_host_id,
            expected_workspace_id,
            expected_descriptor_digest,
            expected_workspace_root,
        } => Body::Pair(pb::Pair {
            token: token.clone(),
            scope_json: json_bytes(scope)?,
            directory: directory.clone(),
            expected_host_id: expected_host_id.clone(),
            expected_workspace_id: expected_workspace_id.clone(),
            expected_descriptor_digest: expected_descriptor_digest.clone(),
            expected_workspace_root: expected_workspace_root.clone(),
        }),
        WireFrame::Resume {
            credential,
            scope,
            directory,
            expected_host_id,
            expected_workspace_id,
            expected_descriptor_digest,
            expected_workspace_root,
        } => Body::Resume(pb::Resume {
            credential: credential.clone(),
            scope_json: json_bytes(scope)?,
            directory: directory.clone(),
            expected_host_id: expected_host_id.clone(),
            expected_workspace_id: expected_workspace_id.clone(),
            expected_descriptor_digest: expected_descriptor_digest.clone(),
            expected_workspace_root: expected_workspace_root.clone(),
        }),
        WireFrame::WorkspaceBound {
            descriptor,
            project_id,
            backend_id,
        } => Body::WorkspaceBound(pb::WorkspaceBound {
            descriptor_json: json_bytes(descriptor)?,
            project_id: project_id.clone(),
            backend_id: backend_id.clone(),
        }),
        WireFrame::Hello {
            descriptor,
            credential,
            lease_generation,
        } => Body::Hello(pb::Hello {
            descriptor_json: json_bytes(descriptor)?,
            credential: credential.clone(),
            lease_generation: lease_generation.clone(),
        }),
        WireFrame::Request {
            request_id,
            operation_id,
            lease_generation,
            remaining_budget_ms,
            method,
        } => Body::Request(pb::Request {
            request_id: request_id.clone(),
            operation_id: operation_id.clone(),
            lease_generation: lease_generation.clone(),
            remaining_budget_ms: *remaining_budget_ms,
            method_name: method.method_name().into(),
            method_json: json_bytes(method)?,
        }),
        WireFrame::Response {
            request_id,
            operation_id,
            result,
        } => Body::Response(pb::Response {
            request_id: request_id.clone(),
            operation_id: operation_id.clone(),
            result: Some(match result {
                Ok(value) => pb::response::Result::OkJson(json_bytes(value)?),
                Err(error) => pb::response::Result::Error(proto_error(error)?),
            }),
        }),
        WireFrame::Cancel {
            request_id,
            lease_generation,
        } => Body::Cancel(pb::Cancel {
            request_id: request_id.clone(),
            lease_generation: lease_generation.clone(),
        }),
        WireFrame::Status {
            request_id,
            operation_id,
            lease_generation,
        } => Body::Status(pb::Status {
            request_id: request_id.clone(),
            operation_id: operation_id.clone(),
            lease_generation: lease_generation.clone(),
        }),
        WireFrame::Receipt {
            request_id,
            receipt,
        } => Body::Receipt(pb::Receipt {
            request_id: request_id.clone(),
            receipt_json: receipt.as_ref().map(json_bytes).transpose()?,
        }),
        WireFrame::Diagnostic { event } => Body::Diagnostic(pb::Diagnostic {
            event_json: json_bytes(event)?,
        }),
        WireFrame::ProcessChanged {
            lease_generation,
            handle,
            revision,
        } => Body::ProcessChanged(pb::ProcessChanged {
            lease_generation: lease_generation.clone(),
            handle: handle.0.0,
            revision: *revision,
        }),
        WireFrame::Ping { lease_generation } => Body::Ping(pb::Ping {
            lease_generation: lease_generation.clone(),
        }),
        WireFrame::Pong { lease_generation } => Body::Pong(pb::Pong {
            lease_generation: lease_generation.clone(),
        }),
        WireFrame::TransferChunk {
            transfer_id,
            sequence,
            data,
            eof,
        } => {
            chunk(transfer_id.clone(), *sequence, data.clone(), *eof)?;
            Body::TransferChunk(pb::TransferChunk {
                transfer_id: transfer_id.clone(),
                sequence: *sequence,
                data: data.clone(),
                eof: *eof,
            })
        }
        WireFrame::Detach { lease_generation } => Body::Detach(pb::Detach {
            lease_generation: lease_generation.clone(),
        }),
    };
    encode_message(pb::HostFrame { body: Some(body) })
}

/// Decode one native frame produced by [`encode_frame`].
pub fn decode_frame(bytes: &[u8]) -> Result<WireFrame, WireError> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(invalid("Host frame exceeds the protocol byte limit"));
    }
    let frame = pb::HostFrame::decode(bytes)
        .map_err(|error| invalid(format!("Host protobuf decoding failed: {error}")))?;
    let Some(body) = frame.body else {
        return Err(invalid("Host frame has no body"));
    };
    use pb::host_frame::Body;
    Ok(match body {
        Body::Inspect(frame) => {
            let body = match frame.authentication {
                Some(pb::inspect::Authentication::Pair(pair)) => Body::Pair(pair),
                Some(pb::inspect::Authentication::Resume(resume)) => Body::Resume(resume),
                None => return Err(invalid("inspection has no authentication")),
            };
            let handshake = frame_from_proto(pb::HostFrame { body: Some(body) })?;
            WireFrame::Inspect {
                request: crate::InspectRequest::new(frame.request_id, &handshake)?,
            }
        }
        Body::Inspected(frame) => WireFrame::Inspected {
            request_id: frame.request_id,
            result: match frame.result {
                Some(pb::inspected::Result::DescriptorJson(bytes)) => {
                    Ok(json_from_slice::<HostDescriptor>(&bytes)?)
                }
                Some(pb::inspected::Result::Error(error)) => Err(rust_error(error)?),
                None => return Err(invalid("inspection reply has no result")),
            },
        },
        Body::HandshakeRejected(frame) => WireFrame::HandshakeRejected {
            error: rust_error(
                frame
                    .error
                    .ok_or_else(|| invalid("handshake rejection has no error"))?,
            )?,
        },
        Body::Pair(frame) => WireFrame::Pair {
            token: frame.token,
            scope: json_from_slice::<BindingScope>(&frame.scope_json)?,
            directory: frame.directory,
            expected_host_id: frame.expected_host_id,
            expected_workspace_id: frame.expected_workspace_id,
            expected_descriptor_digest: frame.expected_descriptor_digest,
            expected_workspace_root: frame.expected_workspace_root,
        },
        Body::Resume(frame) => WireFrame::Resume {
            credential: frame.credential,
            scope: json_from_slice::<BindingScope>(&frame.scope_json)?,
            directory: frame.directory,
            expected_host_id: frame.expected_host_id,
            expected_workspace_id: frame.expected_workspace_id,
            expected_descriptor_digest: frame.expected_descriptor_digest,
            expected_workspace_root: frame.expected_workspace_root,
        },
        Body::WorkspaceBound(frame) => WireFrame::WorkspaceBound {
            descriptor: json_from_slice::<HostDescriptor>(&frame.descriptor_json)?,
            project_id: frame.project_id,
            backend_id: frame.backend_id,
        },
        Body::Hello(frame) => WireFrame::Hello {
            descriptor: json_from_slice::<HostDescriptor>(&frame.descriptor_json)?,
            credential: frame.credential,
            lease_generation: frame.lease_generation,
        },
        Body::Request(frame) => {
            let method = json_from_slice::<HostRequest>(&frame.method_json)?;
            if method.method_name() != frame.method_name {
                return Err(invalid(
                    "Host request method name does not match its payload",
                ));
            }
            WireFrame::Request {
                request_id: frame.request_id,
                operation_id: frame.operation_id,
                lease_generation: frame.lease_generation,
                remaining_budget_ms: frame.remaining_budget_ms,
                method,
            }
        }
        Body::Response(frame) => WireFrame::Response {
            request_id: frame.request_id,
            operation_id: frame.operation_id,
            result: match frame.result {
                Some(pb::response::Result::OkJson(bytes)) => {
                    Ok(json_from_slice::<serde_json::Value>(&bytes)?)
                }
                Some(pb::response::Result::Error(error)) => Err(rust_error(error)?),
                None => return Err(invalid("Host response has no result")),
            },
        },
        Body::Cancel(frame) => WireFrame::Cancel {
            request_id: frame.request_id,
            lease_generation: frame.lease_generation,
        },
        Body::Status(frame) => WireFrame::Status {
            request_id: frame.request_id,
            operation_id: frame.operation_id,
            lease_generation: frame.lease_generation,
        },
        Body::Receipt(frame) => WireFrame::Receipt {
            request_id: frame.request_id,
            receipt: frame
                .receipt_json
                .as_deref()
                .map(json_from_slice::<OperationReceipt>)
                .transpose()?,
        },
        Body::Diagnostic(frame) => WireFrame::Diagnostic {
            event: json_from_slice::<BridgeDiagnostic>(&frame.event_json)?,
        },
        Body::ProcessChanged(frame) => WireFrame::ProcessChanged {
            lease_generation: frame.lease_generation,
            handle: ProcessHandle(HandleId(frame.handle)),
            revision: frame.revision,
        },
        Body::Ping(frame) => WireFrame::Ping {
            lease_generation: frame.lease_generation,
        },
        Body::Pong(frame) => WireFrame::Pong {
            lease_generation: frame.lease_generation,
        },
        Body::TransferChunk(frame) => {
            chunk(frame.transfer_id, frame.sequence, frame.data, frame.eof)?
        }
        Body::Detach(frame) => WireFrame::Detach {
            lease_generation: frame.lease_generation,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MAX_CHUNK_BYTES;

    #[test]
    fn ping_request_and_chunk_round_trip() {
        let scope = BindingScope::project("backend".into(), "binding".into(), "project".into());
        let frames = [
            WireFrame::Ping {
                lease_generation: "7".into(),
            },
            WireFrame::Request {
                request_id: "request".into(),
                operation_id: "operation".into(),
                lease_generation: "7".into(),
                remaining_budget_ms: 30,
                method: HostRequest::ShellDrain {
                    handle: ProcessHandle(HandleId((1_u64 << 53) - 1)),
                    max_bytes: MAX_CHUNK_BYTES as u32,
                },
            },
            WireFrame::TransferChunk {
                transfer_id: "transfer".into(),
                sequence: 0,
                data: vec![0, 255, 42],
                eof: true,
            },
            WireFrame::Response {
                request_id: "request".into(),
                operation_id: "operation".into(),
                result: Err(WireError::new(ErrorCode::Cancelled, "stopped")
                    .with_cause(Some(HostCause::HostOperationContextDeadlineExceeded))),
            },
            WireFrame::Pair {
                token: String::new(),
                scope,
                directory: Some(".".into()),
                expected_host_id: None,
                expected_workspace_id: None,
                expected_descriptor_digest: None,
                expected_workspace_root: None,
            },
        ];
        for frame in frames {
            let bytes = encode_frame(&frame).unwrap();
            let decoded = decode_frame(&bytes).unwrap();
            let again = encode_frame(&decoded).unwrap();
            assert_eq!(bytes, again);
        }
    }

    #[test]
    fn mutation_method_payloads_round_trip_and_only_commit_is_an_effect() {
        use microcrab_host_api::{FileMutation, MoveRequest};
        let mutation = FileMutation::Move(MoveRequest {
            path: "source".into(),
            destination: "destination".into(),
            expected_version: "a".repeat(64),
            content: "final\r\n".into(),
        });
        let preflight = HostRequest::PreflightMutations {
            requests: vec![mutation.clone()],
        };
        assert!(!preflight.is_mutation());
        assert_eq!(
            serde_json::to_value(&preflight).unwrap()["params"]["requests"][0]["move"]["destination"],
            "destination"
        );
        let commit = HostRequest::Mutate(mutation);
        assert!(commit.is_mutation());
        assert_eq!(
            serde_json::to_value(&commit).unwrap()["params"]["move"]["content"],
            "final\r\n"
        );
        for method in [preflight, commit] {
            let name = method.method_name();
            let frame = WireFrame::Request {
                request_id: "r".into(),
                operation_id: "o".into(),
                lease_generation: "1".into(),
                remaining_budget_ms: 5000,
                method,
            };
            let bytes = encode_frame(&frame).unwrap();
            let decoded = decode_frame(&bytes).unwrap();
            assert!(
                matches!(&decoded, WireFrame::Request { method, .. } if method.method_name() == name)
            );
            assert_eq!(encode_frame(&decoded).unwrap(), bytes);
            let mut proto = frame_to_proto(&frame).unwrap();
            if let Some(pb::host_frame::Body::Request(request)) = &mut proto.body {
                request.method_name = "write".into();
            }
            assert!(
                frame_from_proto(proto)
                    .unwrap_err()
                    .message
                    .contains("does not match")
            );
        }
    }

    #[test]
    fn typed_glob_round_trips_without_changing_the_carrier_envelope() {
        use microcrab_host_api::{GlobEntry, GlobEntryKind, GlobOutput, GlobRequest};
        let request = HostRequest::Glob(GlobRequest {
            root: ".".into(),
            pattern: "**/*".into(),
            include_hidden: false,
            include_ignored: false,
            include_directories: true,
            max_results: 10,
        });
        assert!(!request.is_mutation());
        let frame = WireFrame::Request {
            request_id: "r".into(),
            operation_id: "o".into(),
            lease_generation: "1".into(),
            remaining_budget_ms: 5000,
            method: request,
        };
        let bytes = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&bytes).unwrap();
        assert!(
            matches!(&decoded, WireFrame::Request { method: HostRequest::Glob(r), .. } if r.include_directories)
        );
        assert_eq!(encode_frame(&decoded).unwrap(), bytes);
        let output = GlobOutput {
            entries: vec![
                GlobEntry {
                    path: "file".into(),
                    kind: GlobEntryKind::File,
                },
                GlobEntry {
                    path: "dir".into(),
                    kind: GlobEntryKind::Directory,
                },
                GlobEntry {
                    path: "link".into(),
                    kind: GlobEntryKind::Symlink,
                },
            ],
            truncated: true,
        };
        let response = WireFrame::Response {
            request_id: "r".into(),
            operation_id: "o".into(),
            result: Ok(serde_json::to_value(&output).unwrap()),
        };
        let decoded = decode_frame(&encode_frame(&response).unwrap()).unwrap();
        let WireFrame::Response {
            result: Ok(value), ..
        } = decoded
        else {
            panic!("expected glob success")
        };
        assert!(value.get("paths").is_none());
        assert_eq!(serde_json::from_value::<GlobOutput>(value).unwrap(), output);
        let omitted: GlobRequest = serde_json::from_value(serde_json::json!({
            "root":".","pattern":"*","include_hidden":false,"include_ignored":false,"max_results":10
        }))
        .unwrap();
        assert!(!omitted.include_directories);
    }

    #[test]
    fn inspection_negotiates_separately_and_legacy_pair_fields_stay_optional() {
        // Build the historical protobuf payload directly: new optional fields
        // must decode as absent, while an unknown inspection body cannot be Pair.
        let pair = WireFrame::Pair {
            token: "secret".into(),
            scope: BindingScope::project("backend".into(), "binding".into(), "project".into()),
            directory: None,
            expected_host_id: None,
            expected_workspace_id: None,
            expected_descriptor_digest: None,
            expected_workspace_root: None,
        };
        let value = serde_json::to_value(&pair).unwrap();
        assert!(value.get("expected_descriptor_digest").is_none());
        assert!(value.get("expected_workspace_root").is_none());
        let request = crate::InspectRequest::new("inspect-1".into(), &pair).unwrap();
        assert!(!format!("{:?}", request.authentication).contains("secret"));
        let frames = [
            WireFrame::Inspect { request },
            WireFrame::Inspected {
                request_id: "inspect-1".into(),
                result: Err(WireError::new(ErrorCode::PairingDenied, "pairing denied")),
            },
            WireFrame::HandshakeRejected {
                error: WireError::new(ErrorCode::BindingMismatch, "descriptor changed"),
            },
        ];
        for frame in frames {
            let bytes = encode_frame(&frame).unwrap();
            assert_eq!(bytes, encode_frame(&decode_frame(&bytes).unwrap()).unwrap());
        }
        let mut guarded = pair.clone();
        if let WireFrame::Pair {
            expected_descriptor_digest,
            expected_workspace_root,
            ..
        } = &mut guarded
        {
            *expected_descriptor_digest = Some("a".repeat(64));
            *expected_workspace_root = Some("/workspace".into());
        }
        let bytes = encode_frame(&guarded).unwrap();
        assert_eq!(bytes, encode_frame(&decode_frame(&bytes).unwrap()).unwrap());
        assert!(crate::InspectRequest::new("bad/request".into(), &pair).is_err());
        assert!(
            crate::InspectRequest::new(
                "inspect-2".into(),
                &WireFrame::Ping {
                    lease_generation: "1".into()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn oversized_chunk_is_rejected() {
        let frame = WireFrame::TransferChunk {
            transfer_id: "transfer".into(),
            sequence: 1,
            data: vec![0; MAX_CHUNK_BYTES + 1],
            eof: false,
        };
        assert!(encode_frame(&frame).is_err());
    }
}
