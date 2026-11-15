//! Compatibility byte read, using the same ordinary-file policy as typed reads.
use microcrab_host_api::ToolError;
use std::{
    io::Read,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

pub(crate) fn read(
    root: &Path,
    protected: &crate::paths::Policy,
    path: PathBuf,
    max_bytes: usize,
    token: &CancellationToken,
) -> Result<Vec<u8>, ToolError> {
    crate::paths::cancelled(token)?;
    if max_bytes == 0 {
        return Err(ToolError::invalid_request(
            "file byte budget must be positive",
        ));
    }
    let path = crate::paths::admit(root, &path, protected)?;
    let mut file = crate::workspace::open_regular(&path)?;
    let limit = max_bytes.min(crate::text::HARD_BYTES);
    if file.metadata()?.len() > limit as u64 {
        return Err(ToolError::FileLimit);
    }
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        crate::paths::cancelled(token)?;
        let count = file.read(&mut buffer[..(limit + 1 - bytes.len()).min(8192)])?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > limit {
            return Err(ToolError::FileLimit);
        }
    }
    Ok(bytes)
}
