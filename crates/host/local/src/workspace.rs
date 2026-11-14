//! Hardened ordinary-file opening shared by bounded file and cache-point operations.
use crate::paths::reparse;
use microcrab_host_api::ToolError;
use std::fs::{self, File, Metadata, OpenOptions};
use std::path::Path;

fn regular(metadata: &Metadata) -> Result<(), ToolError> {
    if !metadata.is_file() || reparse(metadata) {
        return Err(ToolError::invalid_request(
            "workspace entry is not an ordinary file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(ToolError::invalid_request(
                "workspace hard links are unsupported",
            ));
        }
    }
    Ok(())
}

#[cfg(windows)]
fn check_links(file: &File) -> Result<(), ToolError> {
    use std::os::windows::io::AsRawHandle;
    #[repr(C)]
    struct Information {
        attributes: u32,
        creation: [u32; 2],
        access: [u32; 2],
        write: [u32; 2],
        volume: u32,
        size_high: u32,
        size_low: u32,
        links: u32,
        index_high: u32,
        index_low: u32,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileInformationByHandle(handle: *mut std::ffi::c_void, info: *mut Information)
        -> i32;
    }
    let mut info = std::mem::MaybeUninit::<Information>::uninit();
    // SAFETY: the live File owns a valid handle and info has the exact Win32
    // BY_HANDLE_FILE_INFORMATION layout and enough writable storage.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: a successful Win32 call initialized the complete structure.
    if unsafe { info.assume_init() }.links != 1 {
        return Err(ToolError::invalid_request(
            "workspace hard links are unsupported",
        ));
    }
    Ok(())
}

pub(crate) fn open_regular(path: &Path) -> Result<File, ToolError> {
    regular(&fs::symlink_metadata(path)?)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x00200000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    let file = options.open(path)?;
    regular(&file.metadata()?)?;
    #[cfg(windows)]
    check_links(&file)?;
    Ok(file)
}
