use super::*;
fn agent() -> AgentInfo {
    serde_json::from_value(serde_json::json!({
        "name":"fixture", "modes":[{"name":"work","role":"both","permissions":[]}],
        "tools":[{"name":"opaque_tool","description":"fixture","parameters":{"type":"object"}}],
        "variables":{}
    }))
    .unwrap()
}
#[test]
fn media_output_is_opt_in_and_bound_to_a_declared_tool() {
    let mut info = agent();
    validate_agents(&[info.clone()]).unwrap();
    assert!(info.tool_media_output_bytes.is_empty());
    info.tool_media_output_bytes.insert(
        "opaque_tool".into(),
        crate::admission::image::MAX_IMAGE_BYTES,
    );
    validate_agents(&[info.clone()]).unwrap();
    for bytes in [0, crate::admission::image::MAX_IMAGE_BYTES + 1] {
        info.tool_media_output_bytes
            .insert("opaque_tool".into(), bytes);
        assert!(validate_agents(&[info.clone()]).is_err());
    }
    info.tool_media_output_bytes.clear();
    info.tool_media_output_bytes
        .insert("read_image".into(), 1024);
    assert!(validate_agents(&[info]).is_err());
}
