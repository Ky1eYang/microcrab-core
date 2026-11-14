//! Ordinary-file path admission shared by typed reads, writes and searches.
//! Like shell admission, this does not confine unrelated external OS writers.
use microcrab_host_api::ToolError;
use std::{
    fs,
    path::{Component, Path, PathBuf},
};

pub(crate) fn reparse(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

/// Reject portable device aliases and Windows ADS even when executing on Unix.
pub(crate) fn resolve(root: &Path, requested: &Path) -> Result<PathBuf, ToolError> {
    let text = requested
        .to_str()
        .ok_or_else(|| ToolError::invalid_request("path must be Unicode"))?;
    if text.is_empty() || text.len() > 32_768 || text.contains('\0') || text.starts_with(r"\\.\") {
        return Err(ToolError::invalid_request("unsupported file path"));
    }
    #[cfg(windows)]
    let ordinary_root = {
        let root_text = root.to_string_lossy();
        if let Some(unc) = root_text.strip_prefix(r"\\?\UNC\") {
            PathBuf::from(format!(r"\\{}", unc))
        } else if let Some(path) = root_text.strip_prefix(r"\\?\") {
            PathBuf::from(path)
        } else {
            root.to_path_buf()
        }
    };
    let relative = if requested.is_absolute() {
        #[cfg(windows)]
        let relative = requested
            .strip_prefix(root)
            .or_else(|_| requested.strip_prefix(&ordinary_root));
        #[cfg(not(windows))]
        let relative = requested.strip_prefix(root);
        relative.map_err(|_| ToolError::PathOutsideRoot)?
    } else {
        requested
    };
    let root_meta = fs::symlink_metadata(root)?;
    if !root_meta.is_dir() {
        return Err(ToolError::invalid_request(
            "workspace root is not a directory",
        ));
    }
    if reparse(&root_meta) {
        return Err(ToolError::invalid_request(
            "workspace root is a disallowed symlink or reparse point",
        ));
    }
    if fs::canonicalize(root)? != root {
        return Err(ToolError::invalid_request(
            "workspace root is not canonical",
        ));
    }
    let mut path = root.to_path_buf();
    for component in relative.components() {
        let name = match component {
            Component::CurDir => continue,
            Component::Normal(name) => name,
            _ => return Err(ToolError::PathOutsideRoot),
        };
        validate_name(name)?;
        path.push(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if reparse(&metadata) {
                    return Err(microcrab_host_api::HostError::known(
                        microcrab_host_api::ErrorCategory::Permission,
                        "target path contains a disallowed symlink or reparse point",
                    )
                    .into());
                }
                let canonical = fs::canonicalize(&path)?;
                if !canonical.starts_with(root) {
                    return Err(ToolError::PathOutsideRoot);
                }
                path = canonical;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(path)
}

fn validate_name(name: &std::ffi::OsStr) -> Result<(), ToolError> {
    let name = name
        .to_str()
        .ok_or_else(|| ToolError::invalid_request("path must be Unicode"))?;
    let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();
    if name.ends_with(['.', ' '])
        || name
            .chars()
            .any(|c| c.is_control() || "<>:\"/\\|?*".contains(c))
        || matches!(
            stem.as_str(),
            "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
        )
        || ((stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(
                &stem[3..],
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            ))
    {
        return Err(ToolError::invalid_request("unsupported file path"));
    }
    Ok(())
}

pub(crate) fn within(path: &Path, prefix: &Path) -> bool {
    #[cfg(windows)]
    {
        Path::new(&path.to_string_lossy().to_lowercase())
            .starts_with(Path::new(&prefix.to_string_lossy().to_lowercase()))
    }
    #[cfg(not(windows))]
    {
        path.starts_with(prefix)
    }
}

pub(crate) fn protected(path: &Path, paths: &std::collections::BTreeSet<PathBuf>) -> bool {
    paths.iter().any(|prefix| within(path, prefix))
}

/// Protection is a Host admission rule. Its caller holds the registration read
/// lock until IO finishes, so policy cannot change under an admitted operation.
pub(crate) fn admit(
    root: &Path,
    requested: &Path,
    paths: &std::collections::BTreeSet<PathBuf>,
) -> Result<PathBuf, ToolError> {
    let path = resolve(root, requested)?;
    if protected(&path, paths) {
        return Err(microcrab_host_api::HostError::known(
            microcrab_host_api::ErrorCategory::Permission,
            "target path is protected by Host private-directory policy",
        )
        .into());
    }
    Ok(path)
}

/// Admit a directory entry for metadata-only link enumeration. Its parent still
/// uses ordinary admission; the final node is never canonicalized or followed.
/// The caller holds the same private-policy read lock as ordinary filesystem IO.
pub(crate) fn admit_link_node(
    root: &Path,
    requested: &Path,
    paths: &std::collections::BTreeSet<PathBuf>,
) -> Result<PathBuf, ToolError> {
    if requested.as_os_str().len() > 32_768 {
        return Err(ToolError::invalid_request("unsupported file path"));
    }
    let parent = requested.parent().ok_or(ToolError::PathOutsideRoot)?;
    let parent = admit(root, parent, paths)?;
    let name = requested.file_name().ok_or(ToolError::PathOutsideRoot)?;
    validate_name(name)?;
    let path = parent.join(name);
    if protected(&path, paths) {
        return Err(microcrab_host_api::HostError::known(
            microcrab_host_api::ErrorCategory::Permission,
            "target path is protected by Host private-directory policy",
        )
        .into());
    }
    Ok(path)
}

/// Only symbolic links and junctions are link nodes. Other Windows reparse
/// points remain unsupported; querying the opened node never opens its target.
pub(crate) fn link_node(path: &Path, metadata: &fs::Metadata) -> Result<bool, ToolError> {
    if !reparse(metadata) {
        return Ok(false);
    }
    #[cfg(not(windows))]
    {
        let _ = path;
        Ok(metadata.file_type().is_symlink())
    }
    #[cfg(windows)]
    {
        use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
        #[repr(C)]
        struct AttributeTag {
            attributes: u32,
            tag: u32,
        }
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetFileInformationByHandleEx(
                handle: *mut std::ffi::c_void,
                class: i32,
                information: *mut std::ffi::c_void,
                size: u32,
            ) -> i32;
        }
        // Zero desired access queries metadata only. OPEN_REPARSE_POINT opens
        // the node itself; BACKUP_SEMANTICS also permits directory junctions.
        let file = fs::OpenOptions::new()
            .access_mode(0)
            .custom_flags(0x0020_0000 | 0x0200_0000)
            .open(path)?;
        let mut information = std::mem::MaybeUninit::<AttributeTag>::uninit();
        // SAFETY: the handle is live, the ABI matches kernel32, class 9 is
        // FileAttributeTagInfo, and the buffer has the exact C structure size.
        if unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                9,
                information.as_mut_ptr().cast(),
                std::mem::size_of::<AttributeTag>() as u32,
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: the successful call initialized the entire AttributeTag buffer.
        let information = unsafe { information.assume_init() };
        Ok(information.attributes & 0x400 != 0
            && matches!(information.tag, 0xA000_000C | 0xA000_0003))
    }
}

pub(crate) fn cancelled(token: &tokio_util::sync::CancellationToken) -> Result<(), ToolError> {
    if token.is_cancelled() {
        Err(ToolError::Cancelled)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_root_protected_path_and_escape_remain_distinct() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        fs::write(
            root.join("not-directory"),
            "body must not enter diagnostics",
        )
        .unwrap();
        let error = resolve(&root.join("not-directory"), Path::new("child")).unwrap_err();
        assert_eq!(
            error.public_reason(),
            Some("workspace root is not a directory")
        );
        assert!(matches!(
            resolve(&root, Path::new("../outside")),
            Err(ToolError::PathOutsideRoot)
        ));
        let protected = [root.join("private")].into_iter().collect();
        let error = admit(&root, Path::new("private/missing"), &protected).unwrap_err();
        assert_eq!(
            error.public_reason(),
            Some("target path is protected by Host private-directory policy")
        );
        assert!(
            !error
                .to_string()
                .contains("body must not enter diagnostics")
        );
    }
}
