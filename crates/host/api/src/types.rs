//! Common execution facts and operation state. Policy remains with the caller.
use futures_util::Stream;
use std::{collections::BTreeSet, error::Error, fmt, future::Future, pin::Pin, time::Instant};
pub use tokio_util::sync::CancellationToken as CancelToken;

pub type Result<T> = std::result::Result<T, HostError>;
pub type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct HandleId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    NotFound,
    Permission,
    Timeout,
    Cancelled,
    Conflict,
    Unsupported,
    Io,
}

pub struct HostError {
    pub category: ErrorCategory,
    pub retryable: bool,
    pub message: String,
    /// Bounded sanitized diagnostic description. Producers exclude sensitive bodies.
    pub public_reason: Option<String>,
    pub source: Option<Box<dyn Error + Send + Sync>>,
}
impl HostError {
    pub fn new(category: ErrorCategory, message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            category,
            retryable: false,
            public_reason: Some(microcrab_protocol::diagnostic::sanitize(&message, 2048)),
            message,
            source: None,
        }
    }
    pub fn with_public_reason(mut self, reason: &str) -> Self {
        self.public_reason = Some(microcrab_protocol::diagnostic::sanitize(reason, 2048));
        self
    }
    pub fn known(category: ErrorCategory, reason: &'static str) -> Self {
        Self::new(category, reason)
    }
    pub fn unsupported_known(operation: &'static str) -> Self {
        Self::unsupported(operation).with_public_reason(crate::tool::known_unsupported(operation))
    }
    pub fn unsupported(operation: &str) -> Self {
        Self::new(
            ErrorCategory::Unsupported,
            format!("host operation is unsupported: {operation}"),
        )
        .with_public_reason("host operation is unsupported")
    }
}
impl fmt::Debug for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostError")
            .field("category", &self.category)
            .field("retryable", &self.retryable)
            .field("public_reason", &self.public_reason)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl Error for HostError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn Error + 'static))
    }
}
impl From<std::io::Error> for HostError {
    fn from(source: std::io::Error) -> Self {
        use std::io::ErrorKind;
        let category = match source.kind() {
            ErrorKind::NotFound => ErrorCategory::NotFound,
            ErrorKind::PermissionDenied => ErrorCategory::Permission,
            ErrorKind::TimedOut => ErrorCategory::Timeout,
            ErrorKind::AlreadyExists => ErrorCategory::Conflict,
            ErrorKind::Unsupported => ErrorCategory::Unsupported,
            _ => ErrorCategory::Io,
        };
        Self {
            category,
            retryable: source.kind() == ErrorKind::Interrupted,
            message: source.to_string(),
            public_reason: Some(microcrab_protocol::diagnostic::sanitize(
                &source.to_string(),
                2048,
            )),
            source: Some(Box::new(source)),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpContext {
    pub deadline: Option<Instant>,
    pub cancel: CancelToken,
    pub trace_id: String,
    pub principal: String,
}
impl OpContext {
    pub fn check(&self) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(HostError::known(
                ErrorCategory::Cancelled,
                "host operation cancelled",
            ));
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(HostError::known(
                ErrorCategory::Timeout,
                "host deadline exceeded",
            ));
        }
        Ok(())
    }
    /// Cancellation wins simultaneous completion. Publication operations must check
    /// before commit, then report the known outcome rather than race this wrapper.
    pub async fn run<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        self.check()?;
        let deadline = async {
            match self.deadline {
                Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(HostError::known(ErrorCategory::Cancelled, "host operation cancelled")),
            _ = deadline => Err(HostError::known(ErrorCategory::Timeout, "host deadline exceeded")),
            result = operation => result,
        }
    }
    pub(crate) fn child(&self) -> Self {
        Self {
            cancel: self.cancel.child_token(),
            ..self.clone()
        }
    }
    /// Cleanup must remain possible after the operation deadline or cancellation.
    pub(crate) fn cleanup(&self) -> Self {
        Self {
            deadline: None,
            cancel: CancelToken::new(),
            ..self.clone()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Linux,
    MacOS,
    Windows,
    Other,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Capabilities {
    pub process_pty: bool,
    pub process_kill_tree: bool,
    pub fs_watch: bool,
    pub fs_symlink: bool,
    pub net_listen: bool,
    pub net_udp: bool,
    /// Canonical names such as `process.spawn` and `filesystem.read`; never a permission grant.
    pub supported_operations: BTreeSet<String>,
}
impl Capabilities {
    pub fn supports(&self, operation: &str) -> bool {
        self.supported_operations.contains(operation)
    }
}
