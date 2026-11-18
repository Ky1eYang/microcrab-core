//! Native tool, process and execution cases share one integration binary.
//! Cases share one execution fixture module; tests own and close their Host resources.
//! Acceptance runs serialize tests with --test-threads=1 and report individual failures.

#[path = "support/execution_fixture.rs"]
mod execution_fixture;

#[path = "tool_media.rs"]
mod tool_media;

#[path = "context_budget.rs"]
mod context_budget;

#[path = "buffered_execution.rs"]
mod buffered_execution;
#[path = "delegate_execution.rs"]
mod delegate_execution;
#[path = "generic_execution.rs"]
mod generic_execution;
#[path = "generic_execution_recovery.rs"]
mod generic_execution_recovery;
#[path = "generic_inherited_permission.rs"]
mod generic_inherited_permission;
#[path = "process_shell.rs"]
mod process_shell;
#[path = "runtime.rs"]
mod runtime;
#[path = "shell_initial_output.rs"]
mod shell_initial_output;
#[path = "tool_argument_rewrite.rs"]
mod tool_argument_rewrite;
#[path = "variable_injection.rs"]
mod variable_injection;
#[path = "tool_job_batches.rs"]
mod tool_job_batches;
#[path = "tool_jobs.rs"]
mod tool_jobs;
#[path = "tool_metadata_v019.rs"]
mod tool_metadata_v019;
#[path = "yield_concurrency_sleep.rs"]
mod yield_concurrency_sleep;
