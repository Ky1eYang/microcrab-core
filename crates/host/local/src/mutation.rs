//! Whole-list validation and conditional publication. Locks cover cooperating
//! Host clones; unrelated OS writers remain outside this CAS guarantee.
use crate::{LocalHost, paths::cancelled};
use microcrab_host_api::{
    FileMutation, MAX_FILE_MUTATIONS, MAX_MUTATION_BYTES, MutationOutput, ToolError,
    WritePrecondition,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

fn paths(mutation: &FileMutation) -> Vec<&Path> {
    match mutation {
        FileMutation::Write(request) => vec![&request.path],
        FileMutation::Delete(request) => vec![&request.path],
        FileMutation::Move(request) => vec![&request.path, &request.destination],
    }
}

fn key(path: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        PathBuf::from(path.to_string_lossy().to_lowercase())
    }
    #[cfg(not(windows))]
    {
        path.to_path_buf()
    }
}

fn version(version: &str) -> Result<(), ToolError> {
    if version.len() != 64
        || !version
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ToolError::invalid_request(
            "expected version must be a lowercase SHA-256 digest",
        ));
    }
    Ok(())
}

fn source(
    root: &Path,
    protected: &crate::paths::Policy,
    path: &Path,
    expected: &str,
    token: &CancellationToken,
) -> Result<usize, ToolError> {
    version(expected)?;
    let bytes = match crate::file::read(
        root,
        protected,
        path.to_path_buf(),
        MAX_MUTATION_BYTES,
        token,
    ) {
        Err(ToolError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ToolError::FileConflict);
        }
        result => result?,
    };
    if format!("{:x}", Sha256::digest(&bytes)) != expected {
        return Err(ToolError::FileConflict);
    }
    std::str::from_utf8(&bytes)
        .map_err(|_| ToolError::invalid_request("mutation source is not complete UTF-8 text"))?;
    Ok(bytes.len())
}

fn missing(path: &Path) -> Result<(), ToolError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => Err(ToolError::FileConflict),
    }
}

fn normalize(
    root: &Path,
    protected: &crate::paths::Policy,
    path: &Path,
) -> Result<PathBuf, ToolError> {
    let admitted = crate::paths::admit(root, path, protected)?;
    if admitted == root {
        return Err(ToolError::invalid_request(
            "mutation path must name an ordinary file",
        ));
    }
    let parent = admitted.parent().ok_or(ToolError::PathOutsideRoot)?;
    let metadata = fs::metadata(parent).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ToolError::invalid_request("mutation parent directory is missing; create it explicitly before applying the patch")
        } else { ToolError::Io(error) }
    })?;
    if !metadata.is_dir() {
        return Err(ToolError::invalid_request(
            "mutation parent must be an existing directory",
        ));
    }
    if !admitted.starts_with(root) {
        return Ok(admitted);
    }
    let relative = crate::paths::returned(root, &admitted);
    let relative = relative
        .components()
        .map(|part| part.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    Ok(PathBuf::from(relative))
}

pub(crate) fn preflight(
    root: &Path,
    protected: &crate::paths::Policy,
    mut mutations: Vec<FileMutation>,
    token: &CancellationToken,
) -> Result<Vec<FileMutation>, ToolError> {
    cancelled(token)?;
    if mutations.is_empty() || mutations.len() > MAX_FILE_MUTATIONS {
        return Err(ToolError::invalid_request(
            "mutation list must contain 1..=128 operations",
        ));
    }
    let mut touched = BTreeSet::<PathBuf>::new();
    let mut total = 0usize;
    for mutation in &mut mutations {
        cancelled(token)?;
        let (source_bytes, final_bytes) = match mutation {
            FileMutation::Write(request) => {
                request.path = normalize(root, protected, &request.path)?;
                let bytes = match &request.precondition {
                    WritePrecondition::Missing => {
                        missing(&root.join(&request.path))?;
                        0
                    }
                    WritePrecondition::Version(expected) => {
                        source(root, protected, &request.path, expected, token)?
                    }
                };
                (bytes, request.content.len())
            }
            FileMutation::Delete(request) => {
                request.path = normalize(root, protected, &request.path)?;
                (
                    source(
                        root,
                        protected,
                        &request.path,
                        &request.expected_version,
                        token,
                    )?,
                    0,
                )
            }
            FileMutation::Move(request) => {
                request.path = normalize(root, protected, &request.path)?;
                request.destination = normalize(root, protected, &request.destination)?;
                missing(&root.join(&request.destination))?;
                (
                    source(
                        root,
                        protected,
                        &request.path,
                        &request.expected_version,
                        token,
                    )?,
                    request.content.len(),
                )
            }
        };
        total = total
            .checked_add(source_bytes)
            .and_then(|n| n.checked_add(final_bytes))
            .ok_or(ToolError::FileLimit)?;
        if final_bytes > MAX_MUTATION_BYTES || total > MAX_MUTATION_BYTES {
            return Err(ToolError::FileLimit);
        }
        for path in paths(mutation) {
            let path = key(path);
            if touched
                .iter()
                .any(|other| path.starts_with(other) || other.starts_with(&path))
            {
                return Err(ToolError::invalid_request(
                    "mutation paths alias, overlap, or form a move cycle",
                ));
            }
            touched.insert(path);
        }
    }
    cancelled(token)?;
    Ok(mutations)
}

/// All failure after destination publication is represented in the receipt. This
/// includes cancellation, path revalidation, stale sources and unlink failures.
fn finish_move(
    mut output: MutationOutput,
    remove: impl FnOnce() -> Result<(), ToolError>,
) -> MutationOutput {
    match remove() {
        Ok(()) => output.source_removed = true,
        Err(error) => {
            let reason = match error {
                ToolError::Io(ref source) => format!(
                    "source removal failed: {source}; kind={:?}; os_error={:?}",
                    source.kind(),
                    source.raw_os_error()
                ),
                _ => format!("source removal failed: {error}"),
            };
            // FailureReason excludes control characters and known secret fields;
            // neither content nor a request DTO is included in this diagnostic.
            let reason = microcrab_host_api::FailureReason::from(reason);
            output.diagnostic = reason.public_reason().map(|reason| {
                let mut end = reason.len().min(1024);
                while !reason.is_char_boundary(end) {
                    end -= 1;
                }
                reason[..end].to_owned()
            });
        }
    }
    output
}

fn commit(
    root: &Path,
    protected: &crate::paths::Policy,
    mutation: FileMutation,
    token: &CancellationToken,
) -> Result<MutationOutput, ToolError> {
    match mutation {
        FileMutation::Write(request) => {
            let result = crate::text::write(root, protected, request, token)?;
            Ok(MutationOutput {
                version: Some(result.version),
                bytes: result.bytes,
                source_removed: false,
                destination_written: true,
                diagnostic: None,
            })
        }
        FileMutation::Delete(request) => {
            let path = crate::paths::admit(root, &request.path, protected)?;
            source(
                root,
                protected,
                &request.path,
                &request.expected_version,
                token,
            )?;
            if crate::paths::admit(root, &request.path, protected)? != path {
                return Err(ToolError::PathOutsideRoot);
            }
            cancelled(token)?;
            fs::remove_file(path)?;
            Ok(MutationOutput {
                version: None,
                bytes: 0,
                source_removed: true,
                destination_written: false,
                diagnostic: None,
            })
        }
        FileMutation::Move(request) => {
            let path = crate::paths::admit(root, &request.path, protected)?;
            let destination = crate::paths::admit(root, &request.destination, protected)?;
            let permissions = crate::workspace::open_regular(&path)?
                .metadata()?
                .permissions();
            let mut staged = tempfile::Builder::new()
                .prefix(".microcrab-move-")
                .tempfile_in(destination.parent().ok_or(ToolError::PathOutsideRoot)?)?;
            for chunk in request.content.as_bytes().chunks(8192) {
                cancelled(token)?;
                staged.write_all(chunk)?;
            }
            staged.as_file().sync_all()?;
            source(
                root,
                protected,
                &request.path,
                &request.expected_version,
                token,
            )?;
            if crate::paths::admit(root, &request.path, protected)? != path
                || crate::paths::admit(root, &request.destination, protected)? != destination
            {
                return Err(ToolError::PathOutsideRoot);
            }
            missing(&destination)?;
            let output = MutationOutput {
                version: Some(format!("{:x}", Sha256::digest(request.content.as_bytes()))),
                bytes: request.content.len(),
                source_removed: false,
                destination_written: true,
                diagnostic: None,
            };
            cancelled(token)?;
            // No-clobber publication is the first commit boundary. Do not use
            // rename, which may overwrite an unrelated destination on Unix.
            staged.as_file().set_permissions(permissions)?;
            staged.persist_noclobber(&destination).map_err(|failure| {
                let tempfile::PersistError { error, file } = failure;
                #[cfg(windows)]
                let writable = (|| -> std::io::Result<()> {
                    // The unpublished file is exclusively ours. Clear its
                    // read-only bit so cleanup can remove it on Windows.
                    let mut permissions = file.as_file().metadata()?.permissions();
                    if permissions.readonly() {
                        permissions.set_readonly(false);
                        file.as_file().set_permissions(permissions)?;
                    }
                    Ok(())
                })();
                #[cfg(not(windows))]
                let writable: std::io::Result<()> = Ok(());
                let cleanup = file.close();
                if let Err(cleanup) = writable.and(cleanup) {
                    return ToolError::Host(microcrab_host_api::HostError::new(
                        microcrab_host_api::ErrorCategory::Io,
                        format!("move destination publication failed: {error}; temporary file cleanup failed: {cleanup}"),
                    ));
                }
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    ToolError::FileConflict
                } else {
                    ToolError::Io(error)
                }
            })?;
            Ok(finish_move(output, || {
                cancelled(token)?;
                if crate::paths::admit(root, &request.path, protected)? != path {
                    return Err(ToolError::PathOutsideRoot);
                }
                source(
                    root,
                    protected,
                    &request.path,
                    &request.expected_version,
                    token,
                )?;
                cancelled(token)?;
                fs::remove_file(path)?;
                Ok(())
            }))
        }
    }
}

impl LocalHost {
    /// Acquire every normalized owner path in sorted order, including destination.
    /// Keep permit, workspace and policy guards in the blocking worker through
    /// publication/cleanup even if the awaiting future is dropped.
    pub(crate) async fn file_mutations(
        &self,
        mutations: Vec<FileMutation>,
        commit_one: bool,
        cancellation: CancellationToken,
    ) -> Result<(Vec<FileMutation>, Vec<MutationOutput>), ToolError> {
        let token = cancellation.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        let permit = tokio::select! { biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            result = self.permits.clone().acquire_owned() => result.map_err(|_| ToolError::worker("Host file admission closed"))?,
        };
        if mutations.is_empty()
            || mutations.len() > MAX_FILE_MUTATIONS
            || (commit_one && mutations.len() != 1)
        {
            return Err(ToolError::invalid_request(
                "mutation list must contain 1..=128 operations",
            ));
        }
        let keys = {
            let protected = self
                .protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            let mut keys = BTreeSet::new();
            for mutation in &mutations {
                for path in paths(mutation) {
                    keys.insert(key(&crate::paths::admit(&self.root, path, &protected)?));
                }
            }
            keys
        };
        let locks = {
            let mut writes = self
                .file_writes
                .lock()
                .map_err(|_| ToolError::worker("file admission lock poisoned"))?;
            writes.retain(|_, weak| weak.strong_count() != 0);
            keys.into_iter()
                .map(|path| {
                    let lock = writes
                        .get(&path)
                        .and_then(std::sync::Weak::upgrade)
                        .unwrap_or_else(|| Arc::new(tokio::sync::Mutex::new(())));
                    writes.insert(path, Arc::downgrade(&lock));
                    lock
                })
                .collect::<Vec<_>>()
        };
        let admission = async {
            let workspace = self.workspace.clone().read_owned().await;
            let mut guards = Vec::new();
            for lock in locks {
                guards.push(lock.lock_owned().await);
            }
            (workspace, guards)
        };
        let guards = tokio::select! { biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            guards = admission => guards,
        };
        let root = self.root.clone();
        let protected_paths = self.protected_paths.clone();
        tokio::task::spawn_blocking(move || {
            let (_permit, _guards) = (permit, guards);
            let protected = protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            let mut normalized = preflight(&root, &protected, mutations, &token)?;
            if commit_one {
                let result = commit(&root, &protected, normalized.remove(0), &token)?;
                Ok((Vec::new(), vec![result]))
            } else {
                Ok((normalized, Vec::new()))
            }
        })
        .await
        .map_err(|error| {
            ToolError::Worker(
                microcrab_host_api::FailureReason::from(error.to_string())
                    .with_public_reason("Host mutation worker failed"),
            )
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_move_diagnostic_is_utf8_bounded_without_content() {
        let output = finish_move(
            MutationOutput {
                version: Some("version".into()),
                bytes: 3,
                source_removed: false,
                destination_written: true,
                diagnostic: None,
            },
            || Err(std::io::Error::other("拒".repeat(2000)).into()),
        );
        let diagnostic = output.diagnostic.unwrap();
        assert!(diagnostic.len() <= 1024);
        assert!(diagnostic.starts_with("source removal failed:"));
    }

    #[test]
    fn committed_destination_survives_unlink_error_and_cancellation() {
        for error in [
            ToolError::Cancelled,
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "source is read-only").into(),
        ] {
            let output = finish_move(
                MutationOutput {
                    version: Some("version".into()),
                    bytes: 9,
                    source_removed: false,
                    destination_written: true,
                    diagnostic: None,
                },
                || Err(error),
            );
            assert!(output.destination_written);
            assert!(!output.source_removed);
            assert_eq!(output.bytes, 9);
            assert!(
                output
                    .diagnostic
                    .as_ref()
                    .is_some_and(|reason| !reason.is_empty() && reason.len() <= 1024)
            );
        }
    }
}
