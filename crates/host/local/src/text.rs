//! Streaming text pages and conditional atomic publication. Blocking workers own
//! publication and cleanup even when their awaiting caller is dropped.
use crate::{paths::cancelled, workspace::open_regular};
use microcrab_host_api::{
    ReadOutput, ReadRequest, ToolError, WriteOutput, WritePrecondition, WriteRequest,
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
    path::Path,
};
use tokio_util::sync::CancellationToken;

pub(crate) const HARD_BYTES: usize = 64 * 1024 * 1024;
const PAGE_BYTES: usize = 1024 * 1024;

pub(crate) fn read_text(
    root: &Path,
    protected: &crate::paths::Policy,
    request: ReadRequest,
    token: &CancellationToken,
) -> Result<ReadOutput, ToolError> {
    cancelled(token)?;
    if request.offset_line == 0
        || request.max_lines == 0
        || request.max_lines > 100_000
        || request.max_bytes == 0
        || request.max_bytes > PAGE_BYTES
    {
        return Err(ToolError::invalid_request(
            "read requires a one-based offset and bounded positive line/byte budgets",
        ));
    }
    let path = crate::paths::admit(root, &request.path, protected)?;
    let mut file = open_regular(&path)?;
    if file.metadata()?.len() > HARD_BYTES as u64 {
        return Err(ToolError::FileLimit);
    }
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 8192];
    let mut total = 0usize;
    let mut line = 1usize;
    let mut returned = 0usize;
    let mut content = Vec::new();
    let mut pending = Vec::new();
    let mut next_line = None;
    loop {
        cancelled(token)?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total += count;
        if total > HARD_BYTES {
            return Err(ToolError::FileLimit);
        }
        hash.update(&buffer[..count]);
        for &byte in &buffer[..count] {
            if line >= request.offset_line && next_line.is_none() {
                if returned == request.max_lines {
                    next_line = Some(line);
                } else if content.len() + pending.len() == request.max_bytes {
                    if content.is_empty() {
                        return Err(ToolError::FileLimit);
                    }
                    next_line = Some(line);
                    pending.clear();
                } else {
                    pending.push(byte);
                    if byte == b'\n' {
                        content.append(&mut pending);
                        returned += 1;
                    }
                }
            }
            if byte == b'\n' {
                line += 1;
            }
        }
    }
    if next_line.is_none() {
        content.append(&mut pending);
    }
    let content = String::from_utf8(content)
        .map_err(|_| ToolError::invalid_request("requested file page is not UTF-8 text"))?;
    Ok(ReadOutput {
        content,
        start_line: request.offset_line,
        next_line,
        truncated: next_line.is_some(),
        version: format!("{:x}", hash.finalize()),
    })
}

fn hash_file(path: &Path, token: &CancellationToken) -> Result<String, ToolError> {
    let mut file = open_regular(path)?;
    if file.metadata()?.len() > HARD_BYTES as u64 {
        return Err(ToolError::FileLimit);
    }
    let mut hash = Sha256::new();
    let mut used = 0usize;
    let mut buffer = [0u8; 8192];
    loop {
        cancelled(token)?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        used += count;
        if used > HARD_BYTES {
            return Err(ToolError::FileLimit);
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn check(
    path: &Path,
    expected: &WritePrecondition,
    token: &CancellationToken,
) -> Result<Option<fs::Permissions>, ToolError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => match expected {
            WritePrecondition::Missing => Ok(None),
            _ => Err(ToolError::FileConflict),
        },
        Err(error) => Err(error.into()),
        Ok(metadata) => match expected {
            WritePrecondition::Missing => Err(ToolError::FileConflict),
            WritePrecondition::Version(version) => {
                if hash_file(path, token)? != *version {
                    return Err(ToolError::FileConflict);
                }
                Ok(Some(metadata.permissions()))
            }
        },
    }
}

pub(crate) fn write(
    root: &Path,
    protected: &crate::paths::Policy,
    request: WriteRequest,
    token: &CancellationToken,
) -> Result<WriteOutput, ToolError> {
    cancelled(token)?;
    if request.content.len() > HARD_BYTES {
        return Err(ToolError::FileLimit);
    }
    if let WritePrecondition::Version(version) = &request.precondition
        && (version.len() != 64
            || !version
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
    {
        return Err(ToolError::invalid_request(
            "expected version must be a lowercase SHA-256 digest",
        ));
    }
    let path = crate::paths::admit(root, &request.path, protected)?;
    let permissions = check(&path, &request.precondition, token)?;
    #[cfg(windows)]
    if permissions.as_ref().is_some_and(fs::Permissions::readonly) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "destination is read-only",
        )
        .into());
    }
    let parent = path.parent().ok_or(ToolError::PathOutsideRoot)?;
    // Existing parent required: conflicts never leave newly-created directories.
    let mut staged = tempfile::Builder::new()
        .prefix(".microcrab-write-")
        .tempfile_in(parent)?;
    let mut hash = Sha256::new();
    for chunk in request.content.as_bytes().chunks(8192) {
        cancelled(token)?;
        staged.write_all(chunk)?;
        hash.update(chunk);
    }
    if let Some(permissions) = permissions {
        staged.as_file().set_permissions(permissions)?;
    }
    staged.as_file().sync_all()?;
    // Recheck after staging; per-path serialization covers all clones of this Host.
    if crate::paths::admit(root, &request.path, protected)? != path {
        return Err(ToolError::PathOutsideRoot);
    }
    check(&path, &request.precondition, token)?;
    let output = WriteOutput {
        bytes: request.content.len(),
        version: format!("{:x}", hash.finalize()),
    };
    cancelled(token)?;
    // This is the commit boundary. Never observe cancellation after publication.
    match request.precondition {
        WritePrecondition::Missing => {
            staged.persist_noclobber(&path).map_err(|error| {
                if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                    ToolError::FileConflict
                } else {
                    ToolError::Io(error.error)
                }
            })?;
        }
        WritePrecondition::Version(_) => {
            staged
                .persist(&path)
                .map_err(|error| ToolError::Io(error.error))?;
        }
    }
    Ok(output)
}
