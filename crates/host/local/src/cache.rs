//! Stateless bounded content evidence for one guest-selected controlled-write path.
use microcrab_host_api::{CachePointRequest, FileCachePoint, MAX_CACHE_POINT_BYTES, ToolError};
use sha2::{Digest, Sha256};
use std::{fs, io::Read, path::Path};
use tokio_util::sync::CancellationToken;

pub(crate) fn capture(
    root: &Path,
    protected: &crate::paths::Policy,
    request: CachePointRequest,
    token: &CancellationToken,
) -> Result<FileCachePoint, ToolError> {
    crate::paths::cancelled(token)?;
    if request.max_bytes == 0 || request.max_bytes > MAX_CACHE_POINT_BYTES {
        return Err(ToolError::invalid_request(
            "cache point byte budget is out of bounds",
        ));
    }
    let path = crate::paths::admit(root, &request.path, protected)?;
    let relative = crate::paths::returned(root, &path);
    if relative.as_os_str().is_empty() {
        return Err(ToolError::invalid_request(
            "cache point requires a file path",
        ));
    }
    let normalized = crate::paths::returned_text(root, &path)?;
    if !microcrab_host_api::is_normalized_file_path(&normalized, protected.unrestricted) {
        return Err(ToolError::invalid_request("invalid Host cache point path"));
    }
    let mut file = match crate::workspace::open_regular(&path) {
        Ok(file) => file,
        Err(ToolError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            // Do not infer missing from a failed read or swallow permission errors.
            // Re-admit ancestors before publishing this absence observation.
            crate::paths::admit(root, &request.path, protected)?;
            match fs::symlink_metadata(&path) {
                Ok(_) => return Err(ToolError::FileConflict),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(FileCachePoint {
                path: normalized,
                data: None,
                version: None,
            });
        }
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > request.max_bytes as u64 {
        return Err(ToolError::FileLimit);
    }
    let mut data = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        crate::paths::cancelled(token)?;
        let count = file.read(&mut buffer[..(request.max_bytes + 1 - data.len()).min(8192)])?;
        if count == 0 {
            break;
        }
        data.extend_from_slice(&buffer[..count]);
        if data.len() > request.max_bytes {
            return Err(ToolError::FileLimit);
        }
    }
    crate::paths::cancelled(token)?;
    let version = format!("{:x}", Sha256::digest(&data));
    Ok(FileCachePoint {
        path: normalized,
        data: Some(data),
        version: Some(version),
    })
}
