//! Bounded in-memory files retained for remote filesystem reads.
use microcrab_host_api::{
    Bytes, ErrorCategory, HandleId, HostError, OpContext, filesystem::FileHandle,
};
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

// Never reuse a handle in another RemoteHost, including after its owner is dropped.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
// Match LocalHost's primitive resource handle budget without adding configuration.
const MAX_HANDLES: usize = 256;

#[derive(Default)]
pub(crate) struct Files {
    entries: Mutex<HashMap<HandleId, File>>,
}
struct File {
    bytes: Bytes,
    owner: OpContext,
}

fn foreign() -> HostError {
    HostError::known(
        ErrorCategory::Permission,
        "file handle belongs to another principal",
    )
}

impl Files {
    pub(crate) fn insert(&self, bytes: Bytes, owner: OpContext) -> Result<FileHandle, HostError> {
        owner.check()?;
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.len() >= MAX_HANDLES {
            return Err(HostError::known(
                ErrorCategory::Io,
                "primitive file handle limit exceeded",
            ));
        }
        let id = NEXT_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| HostError::known(ErrorCategory::Io, "primitive resource IDs exhausted"))?;
        let id = HandleId(id);
        entries.insert(id, File { bytes, owner });
        Ok(FileHandle(id))
    }

    pub(crate) fn read(
        &self,
        ctx: &OpContext,
        handle: FileHandle,
        offset: u64,
        len: usize,
    ) -> Result<Bytes, HostError> {
        ctx.check()?;
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let file = entries
            .get(&handle.0)
            .ok_or_else(|| HostError::known(ErrorCategory::NotFound, "closed file handle"))?;
        if file.owner.principal != ctx.principal {
            return Err(foreign());
        }
        file.owner.check()?;
        let bytes = &file.bytes;
        let Ok(offset) = usize::try_from(offset) else {
            return Ok(Bytes::new());
        };
        if offset >= bytes.len() {
            return Ok(Bytes::new());
        }
        let end = offset + len.min(bytes.len() - offset);
        Ok(bytes.slice(offset..end))
    }

    pub(crate) fn close(&self, ctx: &OpContext, handle: FileHandle) -> Result<(), HostError> {
        // Cancellation fences reads, but the exact owner must still receive its
        // explicit close ACK before execution can release cleanup ownership.
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let file = entries
            .get(&handle.0)
            .ok_or_else(|| HostError::known(ErrorCategory::NotFound, "closed file handle"))?;
        if file.owner.principal != ctx.principal {
            return Err(foreign());
        }
        entries.remove(&handle.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(principal: &str) -> OpContext {
        OpContext {
            principal: principal.into(),
            ..Default::default()
        }
    }

    #[test]
    fn handles_reject_other_principals_and_remote_hosts() {
        let first = Files::default();
        let second = Files::default();
        let owner = context("owner");
        let other = context("other");
        let left = first
            .insert(Bytes::from_static(b"left"), owner.clone())
            .unwrap();
        let right = second
            .insert(Bytes::from_static(b"right"), owner.clone())
            .unwrap();
        assert_ne!(left, right);
        assert_eq!(
            first.read(&other, left, 0, 4).unwrap_err().category,
            ErrorCategory::Permission
        );
        assert_eq!(
            first.close(&other, left).unwrap_err().category,
            ErrorCategory::Permission
        );
        assert_eq!(
            second.read(&owner, left, 0, 4).unwrap_err().category,
            ErrorCategory::NotFound
        );
        assert_eq!(
            second.close(&owner, left).unwrap_err().category,
            ErrorCategory::NotFound
        );
        assert_eq!(&first.read(&owner, left, 0, 4).unwrap()[..], b"left");
        first.close(&owner, left).unwrap();
        second.close(&owner, right).unwrap();
    }

    #[test]
    fn expired_or_cancelled_owners_fence_reads_but_keep_exact_cleanup() {
        let files = Files::default();
        let owner = context("owner");
        let fresh = context("owner");
        let handle = files
            .insert(Bytes::from_static(b"data"), owner.clone())
            .unwrap();
        owner.cancel.cancel();
        assert_eq!(
            files.read(&fresh, handle, 0, 4).unwrap_err().category,
            ErrorCategory::Cancelled
        );
        files.close(&owner, handle).unwrap();
        assert_eq!(
            files.close(&fresh, handle).unwrap_err().category,
            ErrorCategory::NotFound
        );
        assert_eq!(
            files.insert(Bytes::new(), owner).unwrap_err().category,
            ErrorCategory::Cancelled
        );

        let mut expired = context("owner");
        let handle = files
            .insert(Bytes::from_static(b"data"), expired.clone())
            .unwrap();
        // Expire the retained owner's deadline independently of the read caller.
        files
            .entries
            .lock()
            .unwrap()
            .get_mut(&handle.0)
            .unwrap()
            .owner
            .deadline = Some(std::time::Instant::now());
        expired.deadline = Some(std::time::Instant::now());
        assert_eq!(
            files.read(&fresh, handle, 0, 4).unwrap_err().category,
            ErrorCategory::Timeout
        );
        files.close(&expired, handle).unwrap();
        assert_eq!(
            files.insert(Bytes::new(), expired).unwrap_err().category,
            ErrorCategory::Timeout
        );
    }

    #[test]
    fn handle_quota_is_reclaimed_only_by_exact_owned_close() {
        let files = Files::default();
        let owner = context("owner");
        let mut handles = Vec::new();
        for _ in 0..MAX_HANDLES {
            handles.push(files.insert(Bytes::new(), owner.clone()).unwrap());
        }
        assert_eq!(
            files
                .insert(Bytes::new(), owner.clone())
                .unwrap_err()
                .category,
            ErrorCategory::Io
        );
        assert!(files.close(&context("other"), handles[0]).is_err());
        assert!(files.close(&owner, FileHandle(HandleId(0))).is_err());
        assert!(files.insert(Bytes::new(), owner.clone()).is_err());
        files.close(&owner, handles[0]).unwrap();
        let replacement = files.insert(Bytes::new(), owner.clone()).unwrap();
        assert_ne!(replacement, handles[0]);
        assert!(files.insert(Bytes::new(), owner).is_err());
    }

    #[test]
    fn reads_clamp_extreme_ranges_without_overflow_or_offset_truncation() {
        let files = Files::default();
        let ctx = OpContext::default();
        let handle = files
            .insert(Bytes::from_static(b"abcdef"), ctx.clone())
            .unwrap();
        assert_eq!(
            &files.read(&ctx, handle, 2, usize::MAX).unwrap()[..],
            b"cdef"
        );
        for offset in [6, u64::MAX, u64::from(u32::MAX) + 1] {
            assert!(
                files
                    .read(&ctx, handle, offset, usize::MAX)
                    .unwrap()
                    .is_empty()
            );
        }
        assert!(files.read(&ctx, handle, 2, 0).unwrap().is_empty());
        files.close(&ctx, handle).unwrap();
        assert!(files.read(&ctx, handle, 0, 1).is_err());
    }
}
