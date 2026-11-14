//! Local Host assembly and file/workspace facade. Process execution has one owner.
//!
//! Canonical cwd validation is an admission boundary, not an OS filesystem sandbox:
//! authorized commands retain the embedding user's filesystem and network rights.

use async_trait::async_trait;
use microcrab_host_api::{Host, HostInfo, OsFamily, ToolError};
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct ShellLimits {
    /// Shared Host-wide process permit cap, independent of per-Session quotas.
    pub max_concurrent: usize,
    pub max_command_bytes: usize,
    /// Maximum shared retained UTF-8 capture budget for one shell process.
    pub max_output_bytes: usize,
}

impl Default for ShellLimits {
    fn default() -> Self {
        Self {
            max_concurrent: 32,
            max_command_bytes: 64 * 1024,
            max_output_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct LocalHost {
    pub(crate) root: PathBuf,
    pub(crate) interpreter: PathBuf,
    pub(crate) limits: ShellLimits,
    pub(crate) permits: Arc<Semaphore>,
    pub(crate) workspace: Arc<tokio::sync::RwLock<()>>,
    pub(crate) jobs: Arc<crate::jobs::ProcessStore>,
    pub(crate) file_writes: Arc<
        std::sync::Mutex<
            std::collections::HashMap<PathBuf, std::sync::Weak<tokio::sync::Mutex<()>>>,
        >,
    >,
    pub(crate) protected_paths: Arc<std::sync::RwLock<std::collections::BTreeSet<PathBuf>>>,
    process_execution: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) resources: Arc<crate::resources::HandleTable>,
}

impl LocalHost {
    pub fn new(root: impl AsRef<Path>, limits: ShellLimits) -> Result<Self, ToolError> {
        Self::assemble(root, limits, true)
    }

    /// Assemble a file-only backend without requiring an installed interpreter.
    /// Process execution remains disabled across every clone.
    pub fn new_filesystem(root: impl AsRef<Path>, limits: ShellLimits) -> Result<Self, ToolError> {
        Self::assemble(root, limits, false)
    }

    fn assemble(
        root: impl AsRef<Path>,
        limits: ShellLimits,
        processes: bool,
    ) -> Result<Self, ToolError> {
        if limits.max_concurrent == 0
            || limits.max_concurrent > 1024
            || limits.max_command_bytes == 0
            || limits.max_output_bytes == 0
        {
            return Err(ToolError::invalid_request(
                "host limits must be positive; concurrency <= 1024",
            ));
        }
        let root = std::fs::canonicalize(root)?;
        if !root.is_dir() {
            return Err(ToolError::invalid_request("host root must be a directory"));
        }
        Ok(Self {
            root,
            interpreter: if processes {
                interpreter()?
            } else {
                PathBuf::new()
            },
            permits: Arc::new(Semaphore::new(limits.max_concurrent)),
            workspace: Arc::new(tokio::sync::RwLock::new(())),
            jobs: Arc::new(crate::jobs::ProcessStore::default()),
            file_writes: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            protected_paths: Arc::new(std::sync::RwLock::new(std::collections::BTreeSet::new())),
            process_execution: Arc::new(std::sync::atomic::AtomicBool::new(processes)),
            resources: Arc::new(crate::resources::HandleTable::default()),
            limits,
        })
    }

    /// Register an existing private file/directory before starting Session work.
    /// Relative paths resolve under the Host root. Clones share protection, and
    /// registration waits for admitted blocking file operations to finish.
    /// This does not constrain external OS processes; disable process execution
    /// as well when untrusted sessions share the server's OS identity.
    pub fn protect_workspace_path(&self, path: PathBuf) -> Result<(), ToolError> {
        let path = self.root.join(path);
        let canonical = std::fs::canonicalize(&path)?;
        let metadata = std::fs::metadata(&canonical)?;
        if !metadata.is_file() && !metadata.is_dir() {
            return Err(ToolError::invalid_request(
                "private path must be an ordinary file or directory",
            ));
        }
        let protected = if self.root.starts_with(&canonical) {
            Some(self.root.clone())
        } else if canonical.starts_with(&self.root) {
            Some(crate::paths::resolve(&self.root, &canonical)?)
        } else {
            None
        };
        if let Some(path) = protected {
            self.protected_paths
                .write()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?
                .insert(path);
        }
        Ok(())
    }

    /// One-way startup policy, shared by clones; not a sandbox and not a means
    /// to terminate processes admitted before this policy was configured.
    pub fn disable_process_execution(&self) {
        self.process_execution.store(false, Ordering::SeqCst);
    }
    pub(crate) fn require_process_execution(
        &self,
        capability: &'static str,
    ) -> Result<(), ToolError> {
        if self.process_execution.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(ToolError::unsupported(capability))
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[async_trait]
impl Host for LocalHost {
    fn tool_capabilities(&self) -> Vec<String> {
        [
            "read",
            "write",
            "file_mutation",
            "glob",
            "grep",
            "read_file",
            "cache_point",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    async fn normalize_resource(
        &self,
        path: PathBuf,
        cancellation: CancellationToken,
    ) -> Result<String, ToolError> {
        let root = self.root.clone();
        let protected_paths = self.protected_paths.clone();
        let token = cancellation.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        let permit = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            permit = self.permits.clone().acquire_owned() => permit.map_err(|error| ToolError::Worker(microcrab_host_api::FailureReason::from(error.to_string()).with_public_reason("Host file admission closed")))?,
        };
        tokio::task::spawn_blocking(move || {
            let protected = protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            let _permit = permit;
            crate::paths::cancelled(&token)?;
            let path = crate::paths::admit(&root, &path, &protected)?;
            let relative = path
                .strip_prefix(&root)
                .map_err(|_| ToolError::PathOutsideRoot)?;
            let resource = relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let resource = if resource.is_empty() {
                ".".into()
            } else {
                resource
            };
            #[cfg(windows)]
            let resource = resource.to_lowercase();
            crate::paths::cancelled(&token)?;
            Ok(resource)
        })
        .await
        .map_err(|error| {
            ToolError::Worker(
                microcrab_host_api::FailureReason::from(error.to_string())
                    .with_public_reason("Host file worker failed"),
            )
        })?
    }

    async fn read_text(
        &self,
        request: microcrab_host_api::ReadRequest,
        cancellation: CancellationToken,
    ) -> Result<microcrab_host_api::ReadOutput, ToolError> {
        let token = cancellation.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        let permit = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            permit = self.permits.clone().acquire_owned() => permit.map_err(|error| ToolError::Worker(microcrab_host_api::FailureReason::from(error.to_string()).with_public_reason("Host file admission closed")))?,
        };
        let guard = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            guard = self.workspace.clone().read_owned() => guard,
        };
        let root = self.root.clone();
        let protected_paths = self.protected_paths.clone();
        tokio::task::spawn_blocking(move || {
            let protected = protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            let _permit = permit;
            let _guard = guard;
            crate::text::read_text(&root, &protected, request, &token)
        })
        .await
        .map_err(|error| {
            ToolError::Worker(
                microcrab_host_api::FailureReason::from(error.to_string())
                    .with_public_reason("Host file worker failed"),
            )
        })?
    }

    async fn write(
        &self,
        request: microcrab_host_api::WriteRequest,
        cancellation: CancellationToken,
    ) -> Result<microcrab_host_api::WriteOutput, ToolError> {
        let token = cancellation.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        let permit = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            permit = self.permits.clone().acquire_owned() => permit.map_err(|error| ToolError::Worker(microcrab_host_api::FailureReason::from(error.to_string()).with_public_reason("Host file admission closed")))?,
        };
        crate::paths::cancelled(&token)?;
        let path = {
            let protected = self
                .protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            crate::paths::admit(&self.root, &request.path, &protected)?
        };
        #[cfg(windows)]
        let path = PathBuf::from(path.to_string_lossy().to_lowercase());
        let lock = {
            let mut writes = self
                .file_writes
                .lock()
                .map_err(|_| ToolError::worker("file admission lock poisoned"))?;
            writes.retain(|_, weak| weak.strong_count() != 0);
            if let Some(lock) = writes.get(&path).and_then(std::sync::Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                writes.insert(path, Arc::downgrade(&lock));
                lock
            }
        };
        let admission = async {
            let workspace = self.workspace.clone().read_owned().await;
            let path = lock.lock_owned().await;
            (workspace, path)
        };
        let (workspace, path) = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            guards = admission => guards,
        };
        let root = self.root.clone();
        let protected_paths = self.protected_paths.clone();
        tokio::task::spawn_blocking(move || {
            let protected = protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            let (_permit, _workspace, _path) = (permit, workspace, path);
            crate::text::write(&root, &protected, request, &token)
        })
        .await
        .map_err(|error| {
            ToolError::Worker(
                microcrab_host_api::FailureReason::from(error.to_string())
                    .with_public_reason("Host file worker failed"),
            )
        })?
    }

    async fn preflight_mutations(
        &self,
        mutations: Vec<microcrab_host_api::FileMutation>,
        cancellation: CancellationToken,
    ) -> Result<Vec<microcrab_host_api::FileMutation>, ToolError> {
        Ok(self.file_mutations(mutations, false, cancellation).await?.0)
    }

    async fn mutate(
        &self,
        mutation: microcrab_host_api::FileMutation,
        cancellation: CancellationToken,
    ) -> Result<microcrab_host_api::MutationOutput, ToolError> {
        let (_, mut outputs) = self
            .file_mutations(vec![mutation], true, cancellation)
            .await?;
        // file_mutations commits exactly one operation when commit_one is true.
        Ok(outputs.remove(0))
    }

    async fn glob(
        &self,
        request: microcrab_host_api::GlobRequest,
        cancellation: CancellationToken,
    ) -> Result<microcrab_host_api::GlobOutput, ToolError> {
        let token = cancellation.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        let permit = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            permit = self.permits.clone().acquire_owned() => permit.map_err(|error| ToolError::Worker(microcrab_host_api::FailureReason::from(error.to_string()).with_public_reason("Host file admission closed")))?,
        };
        let guard = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            guard = self.workspace.clone().read_owned() => guard,
        };
        let root = self.root.clone();
        let protected_paths = self.protected_paths.clone();
        tokio::task::spawn_blocking(move || {
            let protected = protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            let _permit = permit;
            let _guard = guard;
            crate::search::glob_files(&root, &protected, request, &token)
        })
        .await
        .map_err(|error| {
            ToolError::Worker(
                microcrab_host_api::FailureReason::from(error.to_string())
                    .with_public_reason("Host file worker failed"),
            )
        })?
    }

    async fn grep(
        &self,
        request: microcrab_host_api::GrepRequest,
        cancellation: CancellationToken,
    ) -> Result<microcrab_host_api::GrepOutput, ToolError> {
        let token = cancellation.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        let permit = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            permit = self.permits.clone().acquire_owned() => permit.map_err(|error| ToolError::Worker(microcrab_host_api::FailureReason::from(error.to_string()).with_public_reason("Host file admission closed")))?,
        };
        let guard = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            guard = self.workspace.clone().read_owned() => guard,
        };
        let root = self.root.clone();
        let protected_paths = self.protected_paths.clone();
        tokio::task::spawn_blocking(move || {
            let protected = protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            let _permit = permit;
            let _guard = guard;
            crate::search::grep_files(&root, &protected, request, &token)
        })
        .await
        .map_err(|error| {
            ToolError::Worker(
                microcrab_host_api::FailureReason::from(error.to_string())
                    .with_public_reason("Host file worker failed"),
            )
        })?
    }

    fn info(&self) -> Result<HostInfo, ToolError> {
        let os_family = if cfg!(windows) {
            OsFamily::Windows
        } else if cfg!(unix) {
            OsFamily::Unix
        } else {
            OsFamily::Other
        };
        let (path_separator, newline) = match os_family {
            OsFamily::Windows => ("\\", "\r\n"),
            OsFamily::Unix => ("/", "\n"),
            OsFamily::Other => {
                return Err(ToolError::invalid_request(
                    "cannot determine Host path separator and newline for this OS family",
                ));
            }
        };
        Ok(HostInfo {
            os_family,
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            shell: self.interpreter.to_string_lossy().into_owned(),
            path_separator: path_separator.into(),
            newline: newline.into(),
            // The linker overlays only execution imports actually linked to the guest.
            capabilities: Vec::new(),
            resources: None,
            virtual_shell: None,
            description: None,
        })
    }

    async fn cache_point(
        &self,
        request: microcrab_host_api::CachePointRequest,
        cancellation: CancellationToken,
    ) -> Result<microcrab_host_api::FileCachePoint, ToolError> {
        let token = cancellation.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        let permit = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            permit = self.permits.clone().acquire_owned() => permit.map_err(|error| ToolError::Worker(microcrab_host_api::FailureReason::from(error.to_string()).with_public_reason("Host file admission closed")))?,
        };
        let guard = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            guard = self.workspace.clone().read_owned() => guard,
        };
        let root = self.root.clone();
        let protected_paths = self.protected_paths.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _guard = guard;
            let protected = protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            crate::cache::capture(&root, &protected, request, &token)
        })
        .await
        .map_err(|error| {
            ToolError::Worker(
                microcrab_host_api::FailureReason::from(error.to_string())
                    .with_public_reason("Host file worker failed"),
            )
        })?
    }

    async fn read_file(
        &self,
        path: PathBuf,
        max_bytes: usize,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, ToolError> {
        let token = cancellation.child_token();
        let _cancel_on_drop = token.clone().drop_guard();
        let permit = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            result = self.permits.clone().acquire_owned() => result.map_err(|error| ToolError::Worker(microcrab_host_api::FailureReason::from(error.to_string()).with_public_reason("Host file admission closed")))?,
        };
        let workspace = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(ToolError::Cancelled),
            guard = self.workspace.clone().read_owned() => guard,
        };
        let root = self.root.clone();
        let protected_paths = self.protected_paths.clone();
        tokio::task::spawn_blocking(move || {
            let protected = protected_paths
                .read()
                .map_err(|_| ToolError::worker("private path policy lock poisoned"))?;
            let (_permit, _workspace) = (permit, workspace);
            crate::file::read(&root, &protected, path, max_bytes, &token)
        })
        .await
        .map_err(|error| {
            ToolError::Worker(
                microcrab_host_api::FailureReason::from(error.to_string())
                    .with_public_reason("Host file worker failed"),
            )
        })?
    }
}

/// Freeze an absolute interpreter at construction. In particular, Windows must
/// never search an untrusted invocation cwd for a shadow pwsh.exe.
fn interpreter() -> Result<PathBuf, ToolError> {
    #[cfg(windows)]
    {
        if let Some(path) = std::env::var_os("PATH") {
            for directory in std::env::split_paths(&path).filter(|path| path.is_absolute()) {
                let candidate = directory.join("pwsh.exe");
                match std::fs::canonicalize(candidate) {
                    Ok(path) if path.is_file() => return Ok(path),
                    Ok(_) => continue,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Err(ToolError::invalid_request(
            "pwsh.exe must be installed in an absolute PATH directory",
        ))
    }
    #[cfg(unix)]
    {
        if let Some(path) = std::env::var_os("PATH") {
            for directory in std::env::split_paths(&path).filter(|path| path.is_absolute()) {
                let candidate = directory.join("bash");
                match std::fs::canonicalize(candidate) {
                    Ok(path) if path.is_file() => return Ok(path),
                    Ok(_) => continue,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Err(ToolError::invalid_request(
            "bash must be installed in an absolute PATH directory",
        ))
    }
}

/// PowerShell and inherited native console programs must agree with the UTF-8
/// capture decoder. Keep the caller script verbatim after the encoding preamble.
#[cfg(windows)]
pub(crate) fn utf8_script(script: &str) -> String {
    format!(
        "[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false); [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false); $OutputEncoding = [Console]::OutputEncoding;\n{script}"
    )
}
