use crate::{ErrorCode, HostDescriptor, HostRequest, WireError};
use serde::Serialize;
use sha2::{Digest, Sha256};
pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn json_digest(value: &impl Serialize) -> Result<String, WireError> {
    serde_json::to_vec(value)
        .map(|bytes| sha256(&bytes))
        .map_err(|_| WireError::new(ErrorCode::InvalidRequest, "cannot encode payload"))
}
pub fn payload_digest(request: &HostRequest) -> Result<String, WireError> {
    json_digest(request)
}
/// Instance identity is excluded: restart fences resources separately from compatibility.
pub fn descriptor_fingerprint(descriptor: &HostDescriptor) -> Result<String, WireError> {
    let mut value = descriptor.clone();
    value.descriptor_digest.clear();
    value.bridge_instance_id.clear();
    value.tool_capabilities.sort();
    value.supported_primitives.sort();
    json_digest(&value)
}
/// Compare the complete authenticated descriptor with its immutable pin.
/// Capability changes require a new pin; bridge restarts retain the same digest.
pub fn descriptor_matches_pin(
    descriptor: &HostDescriptor,
    pinned_digest: &str,
) -> Result<bool, WireError> {
    let current = descriptor_fingerprint(descriptor)?;
    if descriptor.descriptor_digest != current {
        return Err(WireError::new(
            ErrorCode::InvalidRequest,
            "Host descriptor digest does not match its content",
        ));
    }
    Ok(current == pinned_digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn descriptor() -> HostDescriptor {
        HostDescriptor {
            protocol_version: crate::PROTOCOL_VERSION,
            host_id: "host".into(),
            workspace_id: "workspace".into(),
            workspace_root: "/fixed/root".into(),
            bridge_instance_id: "instance".into(),
            platform: "linux".into(),
            info: microcrab_host_api::HostInfo {
                os_family: microcrab_host_api::OsFamily::Unix,
                os: "linux".into(),
                arch: "x86_64".into(),
                shell: "/bin/bash".into(),
                path_separator: "/".into(),
                newline: "\n".into(),
                capabilities: vec!["read".into(), "write".into()],
                resources: None,
                virtual_shell: None,
                description: None,
            },
            tool_capabilities: vec!["read".into(), "write".into()],
            supported_primitives: vec!["filesystem.stat".into()],
            descriptor_digest: String::new(),
        }
    }
    fn sign(mut descriptor: HostDescriptor) -> HostDescriptor {
        descriptor.descriptor_digest = descriptor_fingerprint(&descriptor).unwrap();
        descriptor
    }
    fn upgraded() -> HostDescriptor {
        let mut descriptor = descriptor();
        descriptor.tool_capabilities.push("cache_point".into());
        descriptor.info.capabilities.push("cache_point".into());
        sign(descriptor)
    }

    #[test]
    fn immutable_pin_requires_exact_capabilities_and_allows_restart() {
        let old = sign(descriptor());
        let current = upgraded();
        assert!(descriptor_matches_pin(&old, &old.descriptor_digest).unwrap());
        assert!(descriptor_matches_pin(&current, &current.descriptor_digest).unwrap());
        assert!(!descriptor_matches_pin(&current, &old.descriptor_digest).unwrap());
        // Capability changes in either direction require an explicit new pin.
        assert!(!descriptor_matches_pin(&old, &current.descriptor_digest).unwrap());
        let mut restarted = current.clone();
        restarted.bridge_instance_id = "fresh-instance".into();
        assert!(descriptor_matches_pin(&restarted, &current.descriptor_digest).unwrap());
        assert_eq!(old.tool_capabilities, vec!["read", "write"]);
        assert_eq!(current.tool_capabilities.last().unwrap(), "cache_point");
    }

    #[test]
    fn compatibility_preserves_every_other_identity_and_environment_field() {
        let old = sign(descriptor());
        let current = upgraded();
        let changes: &[fn(&mut HostDescriptor)] = &[
            |d| d.protocol_version += 1,
            |d| d.host_id.push_str("-other"),
            |d| d.workspace_id.push_str("-other"),
            |d| d.workspace_root.push_str("/other"),
            |d| d.platform = "windows".into(),
            |d| d.info.arch = "aarch64".into(),
            |d| d.info.shell = "/other/shell".into(),
            |d| d.info.capabilities.push("shell".into()),
            |d| d.tool_capabilities.push("shell".into()),
            |d| d.tool_capabilities.retain(|name| name != "write"),
            |d| d.supported_primitives.push("filesystem.write".into()),
        ];
        for change in changes {
            let mut candidate = current.clone();
            change(&mut candidate);
            let candidate = sign(candidate);
            assert!(!descriptor_matches_pin(&candidate, &old.descriptor_digest).unwrap());
        }
        assert!(!descriptor_matches_pin(&current, "unrelated-pin").unwrap());
    }

    #[test]
    fn host_description_is_authenticated_without_changing_older_descriptor_bytes() {
        let old = sign(descriptor());
        let old_bytes = serde_json::to_vec(&old).unwrap();
        let mut updated = old.clone();
        updated.info.description = Some("Host-authored execution facts: λ.".into());
        let updated = sign(updated);
        assert!(!descriptor_matches_pin(&updated, &old.descriptor_digest).unwrap());
        assert!(descriptor_matches_pin(&updated, &updated.descriptor_digest).unwrap());
        let decoded: HostDescriptor = serde_json::from_slice(&old_bytes).unwrap();
        assert_eq!(decoded.info.description, None);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), old_bytes);
        assert!(descriptor_matches_pin(&decoded, &old.descriptor_digest).unwrap());
    }

    #[test]
    fn process_capability_change_requires_a_new_immutable_pin() {
        let old = sign(descriptor());
        let mut processes = old.clone();
        processes.supported_primitives.push("process.spawn".into());
        let processes = sign(processes);
        assert!(!descriptor_matches_pin(&processes, &old.descriptor_digest).unwrap());
        assert!(descriptor_matches_pin(&processes, &processes.descriptor_digest).unwrap());
    }

    #[test]
    fn compatibility_rejects_forged_advertised_digest_before_pin_matching() {
        let old = sign(descriptor());
        let mut current = upgraded();
        current.descriptor_digest = old.descriptor_digest.clone();
        assert_eq!(
            descriptor_matches_pin(&current, &old.descriptor_digest)
                .unwrap_err()
                .code,
            ErrorCode::InvalidRequest
        );
        let mut exact = old.clone();
        exact.workspace_root.push_str("/tampered");
        assert!(descriptor_matches_pin(&exact, &old.descriptor_digest).is_err());
    }
}
