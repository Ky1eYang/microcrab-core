//! The maintained Coding artifact no longer advertises or imports legacy image reading.
#![cfg(feature = "wasm")]
use microcrab_session::runtime::wasm::{WasmAgent, WasmLimits};
use std::sync::Arc;

#[test]
fn actual_coding_omits_legacy_read_image() {
    let artifact = std::fs::read(
        std::env::var_os("MICROCRAB_CODING_WASM")
            .expect("parent gate supplies the current ABI0.9 Coding Component"),
    )
    .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let host = Arc::new(
        microcrab_host_local::LocalHost::new_filesystem(workspace.path(), Default::default())
            .unwrap(),
    );
    let agent = WasmAgent::load(&artifact, WasmLimits::default(), host).unwrap();
    let info = &agent.agents()[0];
    assert!(info.tools.iter().any(|tool| tool.name == "read"));
    assert!(!info.tools.iter().any(|tool| tool.name == "read_image"));
    assert!(info.tool_media_output_bytes.is_empty());
    // Component names, including import names, contain their UTF-8 bytes verbatim.
    let legacy = b"microcrab:host/filesystem@0.1.0";
    assert!(!artifact.windows(legacy.len()).any(|name| name == legacy));
}
