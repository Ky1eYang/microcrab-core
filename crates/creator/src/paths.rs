//! Native creator identities use the existing short-id/creator.json directory layout.
use microcrab_session::Error;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

/// Reserve metadata exclusively. A collision never attaches to an existing child;
/// partial allocation failure leaves an unowned directory rather than deleting
/// bytes that may have become visible to another process.
pub fn reserve_child(parent: &Path) -> Result<(String, PathBuf), Error> {
    let root = parent.join("subsessions");
    std::fs::create_dir_all(&root)?;
    let root = root.canonicalize()?;
    for _ in 0..32 {
        let identity =
            format!("{:x}", Sha256::digest(uuid::Uuid::new_v4().as_bytes()))[..16].to_owned();
        let directory = root.join(&identity);
        match std::fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
        let mut metadata = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("creator.json"))?;
        serde_json::to_writer(&mut metadata, &json!({"version":1,"session_id":identity}))?;
        metadata.flush()?;
        metadata.sync_all()?;
        return Ok((identity, directory));
    }
    Err(Error::config(
        "could not reserve a unique child session directory",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_children_have_exclusive_compatible_metadata() {
        let root = tempfile::tempdir().unwrap();
        let (first, first_path) = reserve_child(root.path()).unwrap();
        let (second, second_path) = reserve_child(root.path()).unwrap();
        assert_ne!(first, second);
        assert_eq!(first.len(), 16);
        assert!(
            first
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
        assert!(first_path.is_absolute());
        assert_ne!(first_path, second_path);
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(first_path.join("creator.json")).unwrap())
                .unwrap();
        assert_eq!(metadata, json!({"version":1,"session_id":first}));
    }
}
