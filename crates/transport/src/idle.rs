//! A provider wait is bounded by silence, never the lifetime of a response.
use crate::Error;
use std::{future::Future, time::Duration};

/// Each invocation starts a fresh idle window. Call only around network waits,
/// never around parsing, observer delivery, audit intervention or a whole call.
/// Caller cancellation is selected first by the owning protocol future.
pub(crate) async fn wait<T>(
    window: Duration,
    waiting: impl Future<Output = T>,
) -> Result<T, Error> {
    tokio::time::timeout(window, waiting).await.map_err(|_| {
        Error::ProviderIdleTimeout(crate::IdleTimeout {
            timeout_ms: u64::try_from(window.as_millis()).unwrap_or(u64::MAX),
            elapsed_ms: 0,
            first_token_ms: None,
            streamed_bytes: 0,
            streamed_chars: 0,
        })
    })
}
