//! Bounded raster byte acquisition; Runtime remains the image validation authority.
use crate::host::{MediaAttachment, OperationError, OperationErrorCode};

pub(crate) const MAX_IMAGE_BYTES: usize = 20 * 1024 * 1024;
pub(crate) const IMAGE_CHUNK_BYTES: usize = 16 * 1024;

pub(crate) trait ImageFiles {
    type Handle: Copy;
    fn image_stat(&mut self, path: &str) -> Result<(u64, bool), OperationError>;
    fn image_open(&mut self, path: &str) -> Result<Self::Handle, OperationError>;
    fn image_read(
        &mut self,
        handle: Self::Handle,
        offset: u64,
        len: u32,
    ) -> Result<Vec<u8>, OperationError>;
    fn image_close(&mut self, handle: Self::Handle) -> Result<(), OperationError>;
}
fn failure(code: OperationErrorCode, message: &str) -> OperationError {
    OperationError {
        code,
        message: message.into(),
    }
}
pub(crate) fn read(
    files: &mut impl ImageFiles,
    path: &str,
) -> Result<MediaAttachment, OperationError> {
    let (size, ordinary) = files.image_stat(path)?;
    if !ordinary {
        return Err(failure(
            OperationErrorCode::InvalidRequest,
            "image source must be an ordinary file",
        ));
    }
    if size == 0 || size > MAX_IMAGE_BYTES as u64 {
        return Err(failure(
            OperationErrorCode::Limit,
            "image source must be nonempty and at most20MiB",
        ));
    }
    let mut data = Vec::new();
    data.try_reserve_exact(size as usize).map_err(|_| {
        failure(
            OperationErrorCode::Limit,
            "image source exceeds guest allocation capacity",
        )
    })?;
    let handle = files.image_open(path)?;
    let result = (|| {
        while data.len() < size as usize {
            let wanted = IMAGE_CHUNK_BYTES.min(size as usize - data.len());
            let chunk = files.image_read(handle, data.len() as u64, wanted as u32)?;
            if chunk.is_empty() || chunk.len() > wanted {
                return Err(failure(
                    OperationErrorCode::Conflict,
                    "image changed size during bounded read",
                ));
            }
            data.extend_from_slice(&chunk);
        }
        if !files.image_read(handle, size, 1)?.is_empty() {
            return Err(failure(
                OperationErrorCode::Conflict,
                "image grew during bounded read",
            ));
        }
        let media_type = if data.starts_with(b"\x89PNG\r\n\x1a\n") {
            "image/png"
        } else if data.starts_with(&[0xff, 0xd8, 0xff]) {
            "image/jpeg"
        } else if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
            "image/gif"
        } else if data.starts_with(b"RIFF") && data.get(8..12) == Some(b"WEBP") {
            "image/webp"
        } else {
            return Err(failure(
                OperationErrorCode::InvalidRequest,
                "unsupported image signature; expected PNG, JPEG, GIF or WebP",
            ));
        };
        Ok(MediaAttachment {
            media_type: media_type.into(),
            data,
        })
    })();
    // Attempt cleanup even when reading or signature detection failed. Preserve the
    // original failure; Runtime retains failed close ownership for its cleanup ACK.
    let closed = files.image_close(handle);
    match (result, closed) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(image), Ok(())) => Ok(image),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Files {
        bytes: Vec<u8>,
        size: u64,
        reads: usize,
        closed: bool,
        fail: bool,
    }
    impl ImageFiles for Files {
        type Handle = u64;
        fn image_stat(&mut self, _: &str) -> Result<(u64, bool), OperationError> {
            Ok((self.size, true))
        }
        fn image_open(&mut self, _: &str) -> Result<u64, OperationError> {
            Ok(7)
        }
        fn image_read(&mut self, _: u64, offset: u64, len: u32) -> Result<Vec<u8>, OperationError> {
            assert!(len as usize <= IMAGE_CHUNK_BYTES);
            self.reads += 1;
            if self.fail {
                return Err(failure(OperationErrorCode::Cancelled, "read cancelled"));
            }
            let start = (offset as usize).min(self.bytes.len());
            Ok(self.bytes[start..(start + len as usize).min(self.bytes.len())].to_vec())
        }
        fn image_close(&mut self, _: u64) -> Result<(), OperationError> {
            self.closed = true;
            Ok(())
        }
    }
    fn files(bytes: Vec<u8>) -> Files {
        Files {
            size: bytes.len() as u64,
            bytes,
            reads: 0,
            closed: false,
            fail: false,
        }
    }
    #[test]
    fn reads_binary_chunks_and_preserves_exact_source_without_extension_trust() {
        let mut source = b"\x89PNG\r\n\x1a\n".to_vec();
        source.resize(IMAGE_CHUNK_BYTES * 2 + 17, 0xff);
        let mut host = files(source.clone());
        let image = read(&mut host, "no-extension").unwrap();
        assert_eq!(image.media_type, "image/png");
        assert_eq!(image.data, source);
        assert_eq!(host.reads, 4);
        assert!(host.closed);
    }
    #[test]
    fn closes_after_cancellation_bad_signature_and_size_race() {
        for mode in 0..4 {
            let mut host = files(b"not an image".to_vec());
            if mode == 1 {
                host.fail = true;
            }
            if mode == 2 {
                host.size += 1;
            }
            if mode == 3 {
                host.size -= 1;
            }
            assert!(read(&mut host, "fake.png").is_err());
            assert!(host.closed);
        }
    }
    #[test]
    fn rejects_source_ceiling_before_open_or_read() {
        let mut host = files(Vec::new());
        for size in [0, MAX_IMAGE_BYTES as u64 + 1] {
            host.size = size;
            assert_eq!(
                read(&mut host, "huge.png").unwrap_err().code,
                OperationErrorCode::Limit
            );
            assert_eq!(host.reads, 0);
            assert!(!host.closed);
        }
    }
}
