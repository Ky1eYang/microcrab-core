//! Guest execution and inline component cases share one integration binary.
//! Original feature gates and fixture ownership stay inside each private case module.
//! Acceptance runs serialize tests with --test-threads=1 and report individual failures.

#[cfg(feature = "wasm")]
#[path = "support/typed_agent_fixture.rs"]
mod typed_agent_fixture;
#[cfg(feature = "wasm")]
#[path = "support/guest_observation_fixture.rs"]
mod guest_observation_fixture;

#[path = "wasm_hostless.rs"]
mod wasm_hostless;
#[path = "wasm_init.rs"]
mod wasm_init;

#[path = "generic_guest_execution.rs"]
mod generic_guest_execution;
#[path = "generic_guest_read.rs"]
mod generic_guest_read;
#[path = "tool_diagnostics.rs"]
mod tool_diagnostics;
#[path = "wasm.rs"]
mod wasm;
#[path = "wasm_coding.rs"]
mod wasm_coding;
#[path = "wasm_coding_media.rs"]
mod wasm_coding_media;
#[path = "wasm_communication.rs"]
mod wasm_communication;
#[path = "wasm_epoch.rs"]
mod wasm_epoch;
#[path = "wasm_resources.rs"]
mod wasm_resources;
#[path = "wasm_web_transport.rs"]
mod wasm_web_transport;
