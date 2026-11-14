//! Context, reference and template cases share one integration binary.
//! Original source locations preserve template includes; auditors share one suite module.
//! Acceptance runs serialize tests with --test-threads=1 and report individual failures.

#[path = "support/hooks.rs"]
mod hooks;

#[path = "auto_compaction.rs"]
mod auto_compaction;
#[path = "auto_compaction_usage_rewrite.rs"]
mod auto_compaction_usage_rewrite;
#[path = "context_cache_order.rs"]
mod context_cache_order;
#[path = "context_google_replay.rs"]
mod context_google_replay;
#[path = "context_provider.rs"]
mod context_provider;
#[path = "context_streaming.rs"]
mod context_streaming;
#[path = "epoch_turns.rs"]
mod epoch_turns;
#[path = "functional_compact.rs"]
mod functional_compact;
#[path = "functional_definitions.rs"]
mod functional_definitions;
#[path = "input.rs"]
mod input;
#[path = "markdown_references.rs"]
mod markdown_references;
#[path = "pending_epoch.rs"]
mod pending_epoch;
#[path = "provider_recovery.rs"]
mod provider_recovery;
#[path = "reference_cancellation.rs"]
mod reference_cancellation;
#[path = "reference_lifecycle.rs"]
mod reference_lifecycle;
#[path = "system_template.rs"]
mod system_template;
#[path = "variable_protocol.rs"]
mod variable_protocol;
