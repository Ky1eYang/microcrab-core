//! Creator-owned durable records and history backends shared by Python and server.
//! Runtime recovery remains authoritative for Host restoration; history selection
//! is published only after the Journal acknowledgement of successful recovery.
pub mod audit;
pub mod configuration;
pub mod dialogue_reply;
pub mod directory;
pub mod display;
pub mod execution;
pub mod history;
#[cfg(feature = "storage-sqlite")]
mod sqlite;

#[cfg(feature = "storage-file")]
pub use directory::FileDirectory;
#[cfg(feature = "storage-sqlite")]
pub use directory::SqliteDirectory;
pub use directory::{Directory, ModelSelection, OperationRecord, RECORD_VERSION, SessionRecord};

pub use history::{AuditAnchor, CreatorJournal, JournalCursor, Travel};

#[cfg(feature = "storage-sqlite")]
pub mod usage;
