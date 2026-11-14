//! Native audit recording, bounded nonblocking observation and read-only
//! projections. Storage persists exact canonical records in the selected backend;
//! Creator owns hashing, retention and ancestry. No derived view is written.
mod diff;
mod query;
mod recorder;
pub use query::{
    AuditAttempt, AuditCommit, AuditQuery, AuditQueryResult, AuditStatus, CommitObserver, query,
    query_attempt_metadata, query_internal, query_internal_selected,
    query_internal_selected_with_ancestry, query_selected, query_selected_attempt_metadata,
    query_selected_turn_metadata, query_selected_turn_metadata_with_heads,
    query_selected_with_ancestry,
};
#[cfg(all(test, feature = "storage-file"))]
mod query_tests;
#[cfg(all(test, feature = "storage-file"))]
mod tests;
mod worker;

pub use recorder::{EffectiveRequest, Recorder, RecorderOptions, Retention};
pub use worker::NativeAuditor;

#[cfg(all(test, feature = "storage-sqlite"))]
mod sqlite_tests;
