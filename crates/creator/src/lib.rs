//! Pure Rust creator-owned assembly and lifetime ports shared by server and Python.
//! Runtime remains the only tool authority; Journal remains history authority.
pub mod audit;
pub mod children;
pub mod client;
pub mod delegate;
pub mod display;
pub mod naming;
pub mod paths;
pub mod permission;
#[cfg(test)]
mod tests;
pub mod usage;

pub use children::{Children, Completion};
pub use client::{RetryPolicy, RetryingClient};
pub use permission::DeclarativePermission;
pub use usage::{UsageAuditor, UsageLedger};

use microcrab_session::{Error, Session, SessionBuilder, runtime::journal::Recover};

/// Finish the common build/restore -> start -> mode sequence. Failed initialization
/// still closes the created Session so its tasks and journals cannot be abandoned.
pub async fn assemble(
    builder: SessionBuilder,
    recover: Option<Recover>,
    mode: Option<String>,
) -> Result<Session, Error> {
    let session = match recover {
        Some(recover) => Session::restore(builder, recover).await?,
        None => builder.build()?,
    };
    let initialized = async {
        session.start().await?;
        if let Some(mode) = mode {
            session.select_mode(mode).await?;
        }
        Ok::<(), Error>(())
    }
    .await;
    if let Err(error) = initialized {
        if let Err(cleanup) = session.close().await {
            tracing::warn!(error = %cleanup, "failed creator initialization cleanup");
        }
        return Err(error);
    }
    Ok(session)
}
