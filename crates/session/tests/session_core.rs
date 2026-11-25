//! Session lifecycle, permissions and recovery cases share one integration binary.
//! Case-local helpers stay private; acceptance runs serialize tests with --test-threads=1.
//! Failures retain descriptive case module and test names in the test harness.

#[path = "support/hooks.rs"]
mod hooks;

#[path = "audit.rs"]
mod audit;
#[path = "backend_object_safety.rs"]
mod backend_object_safety;
#[path = "cache_points.rs"]
mod cache_points;
#[path = "catalog_restore_v018.rs"]
mod catalog_restore_v018;
#[path = "close_cancellation.rs"]
mod close_cancellation;
#[path = "fork.rs"]
mod fork;
#[path = "journal.rs"]
mod journal;
#[path = "lifecycle.rs"]
mod lifecycle;
#[path = "model_admission.rs"]
mod model_admission;
#[path = "permission.rs"]
mod permission;
#[path = "permission_state.rs"]
mod permission_state;
#[path = "permissions_v018.rs"]
mod permissions_v018;
#[path = "recovery_navigation.rs"]
mod recovery_navigation;
#[path = "recovery_session.rs"]
mod recovery_session;
#[path = "recovery_transaction.rs"]
mod recovery_transaction;
#[path = "session.rs"]
mod session;
#[path = "state_lifecycle.rs"]
mod state_lifecycle;
#[path = "transition_failure.rs"]
mod transition_failure;
#[path = "usage.rs"]
mod usage;

#[path = "insertion.rs"]
mod insertion;
#[path = "round_limit.rs"]
mod round_limit;
