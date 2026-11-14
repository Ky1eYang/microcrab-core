//! Data contracts shared by agent guests, Hosts and runtime discovery.
//! Values remain JSON values throughout injection and template extraction.

use microcrab_protocol::ToolDefinition;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInfo {
    pub name: String,
    pub modes: Vec<AgentMode>,
    pub tools: Vec<ToolDefinition>,
    pub variables: Variable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMode {
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Agent-owned literal instructions; never evaluated as a template.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prompt: String,
    pub role: AgentRole,
    #[serde(default)]
    pub permissions: Vec<microcrab_permission::Permission>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    MainAgent,
    SubAgent,
    Both,
}

/// Independent agent-owned variables; deserialization rejects non-object JSON.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Variable(Map<String, Value>);

impl Variable {
    pub fn new(values: Map<String, Value>) -> Self {
        Self(values)
    }
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }
    pub fn extract<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>, serde_json::Error> {
        self.get(name)
            .cloned()
            .map(serde_json::from_value)
            .transpose()
    }
    pub fn inject(
        &mut self,
        name: impl Into<String>,
        value: impl Serialize,
    ) -> Result<Option<Value>, serde_json::Error> {
        Ok(self.0.insert(name.into(), serde_json::to_value(value)?))
    }
    pub fn values(&self) -> &Map<String, Value> {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hint {
    pub name: String,
    pub description: String,
}

/// Facts about the actual execution Host, never Session variables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostInfo {
    pub os_family: OsFamily,
    pub os: String,
    pub arch: String,
    pub shell: String,
    pub path_separator: String,
    pub newline: String,
    /// Execution imports actually linked for this guest; supplied by the linker.
    pub capabilities: Vec<String>,
    /// Optional projection of native Host metadata, never an authorization grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<HostResources>,
    /// Interpreter facts only, never linked operation availability or a grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_shell: Option<VirtualShellInfo>,
    /// Host-authored implementation facts and limitations, never authorization.
    /// Consumers display this text without inferring capabilities from its content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualShellInfo {
    pub engine: String,
    pub python: Option<PythonInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PythonInfo {
    pub implementation: String,
    pub version: Option<String>,
}

impl HostInfo {
    /// A Host shell name used as an ordinary tool identity must be portable.
    /// This validates naming only; linked capabilities still control availability.
    pub fn shell_tool_name(&self) -> Option<&str> {
        let name = match self.os_family {
            OsFamily::Windows => "pwsh",
            OsFamily::Unix => "bash",
            OsFamily::Other => self.shell.as_str(),
        };
        (!name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')))
        .then_some(name)
    }
}

/// Wire projection only; native Host contracts own the capability definitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostResources {
    pub platform: String,
    pub features: HostFeatures,
}

/// Optional resource refinements, independent of linked operation availability.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostFeatures {
    pub process_pty: bool,
    pub process_kill_tree: bool,
    pub fs_watch: bool,
    pub fs_symlink: bool,
    pub net_listen: bool,
    pub net_udp: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OsFamily {
    Windows,
    Unix,
    Other,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn host_info_requires_extended_metadata_and_preserves_text_fields() {
        for (family, path_separator, newline) in [
            (OsFamily::Windows, "\\", "\r\n"),
            (OsFamily::Unix, "/", "\n"),
        ] {
            let host = HostInfo {
                os_family: family,
                os: "fixture".into(),
                arch: "fixture".into(),
                shell: "fixture".into(),
                path_separator: path_separator.into(),
                newline: newline.into(),
                capabilities: vec!["shell".into()],
                resources: None,
                virtual_shell: None,
                description: None,
            };
            let value = serde_json::to_value(&host).unwrap();
            assert_eq!(value["path_separator"], path_separator);
            assert_eq!(value["newline"], newline);
            assert_eq!(value["capabilities"], json!(["shell"]));
            assert_eq!(
                serde_json::from_value::<HostInfo>(value.clone()).unwrap(),
                host
            );
            for field in ["path_separator", "newline", "capabilities"] {
                let mut incomplete = value.clone();
                incomplete.as_object_mut().unwrap().remove(field);
                assert!(serde_json::from_value::<HostInfo>(incomplete).is_err());
            }
            let mut unexpected = value;
            unexpected["session"] = json!("not a Host fact");
            assert!(serde_json::from_value::<HostInfo>(unexpected).is_err());
        }
    }

    #[test]
    fn host_info_resources_roundtrip_and_remain_optional() {
        let basic = json!({
            "os_family": "unix", "os": "fixture", "arch": "fixture",
            "shell": "fixture", "path_separator": "/", "newline": "\n",
            "capabilities": ["shell"]
        });
        let mut host: HostInfo = serde_json::from_value(basic.clone()).unwrap();
        assert_eq!(host.resources, None);
        assert_eq!(serde_json::to_value(&host).unwrap(), basic);

        host.resources = Some(HostResources {
            platform: "linux".into(),
            features: HostFeatures {
                process_pty: true,
                process_kill_tree: false,
                fs_watch: true,
                fs_symlink: false,
                net_listen: true,
                net_udp: false,
            },
        });
        let value = serde_json::to_value(&host).unwrap();
        assert_eq!(
            value["resources"],
            json!({
                "platform": "linux",
                "features": {
                    "process_pty": true, "process_kill_tree": false,
                    "fs_watch": true, "fs_symlink": false,
                    "net_listen": true, "net_udp": false
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<HostInfo>(value.clone()).unwrap(),
            host
        );
        let mut unexpected = value.clone();
        unexpected["resources"]["permissions"] = json!([]);
        assert!(serde_json::from_value::<HostInfo>(unexpected).is_err());
        let mut unexpected_feature = value.clone();
        unexpected_feature["resources"]["features"]["permission"] = json!(true);
        assert!(serde_json::from_value::<HostInfo>(unexpected_feature).is_err());
        let mut incomplete = value;
        incomplete["resources"]["features"]
            .as_object_mut()
            .unwrap()
            .remove("net_udp");
        assert!(serde_json::from_value::<HostInfo>(incomplete).is_err());
    }

    #[test]
    fn host_descriptions_and_interpreter_facts_are_opaque_and_optional() {
        let value = json!({
            "os_family": "other", "os": "fixture", "arch": "fixture",
            "shell": "custom-shell", "path_separator": "/", "newline": "\n",
            "capabilities": [],
            "description": "Host supplied grammar, Unicode and network facts: 位.",
            "virtual_shell": {
                "engine": "custom-engine",
                "python": { "implementation": "custom-interpreter", "version": "fixture" }
            }
        });
        let host: HostInfo = serde_json::from_value(value.clone()).unwrap();
        assert!(host.capabilities.is_empty());
        assert_eq!(serde_json::to_value(&host).unwrap(), value);
        assert_eq!(host.shell_tool_name(), Some("custom-shell"));
        let mut absent = value.clone();
        absent.as_object_mut().unwrap().remove("description");
        let host: HostInfo = serde_json::from_value(absent.clone()).unwrap();
        assert_eq!(host.description, None);
        assert_eq!(serde_json::to_value(host).unwrap(), absent);
        let mut unexpected = value;
        unexpected["virtual_shell"]["grant"] = json!("process.execute");
        assert!(serde_json::from_value::<HostInfo>(unexpected).is_err());
    }

    #[test]
    fn host_shell_tool_identity_is_portable_without_interpreting_implementation() {
        let mut host: HostInfo = serde_json::from_value(json!({
            "os_family": "other", "os": "fixture", "arch": "fixture",
            "shell": "custom-shell_2", "path_separator": "/", "newline": "\n",
            "capabilities": []
        }))
        .unwrap();
        assert_eq!(host.shell_tool_name(), Some("custom-shell_2"));
        for shell in ["", "/bin/custom", "two words", "位", &"a".repeat(65)] {
            host.shell = shell.into();
            assert_eq!(host.shell_tool_name(), None);
        }
        host.os_family = OsFamily::Windows;
        assert_eq!(host.shell_tool_name(), Some("pwsh"));
        host.os_family = OsFamily::Unix;
        assert_eq!(host.shell_tool_name(), Some("bash"));
    }

    #[test]
    fn variables_preserve_structured_values_and_typed_extraction() {
        let mut variables = Variable::default();
        variables
            .inject(
                "rules",
                vec![Rule {
                    name: "cwd".into(),
                    description: "Use the Host root".into(),
                }],
            )
            .unwrap();
        variables
            .inject("nested", json!({"enabled":true,"limit":7}))
            .unwrap();
        assert_eq!(
            variables.get("nested"),
            Some(&json!({"enabled":true,"limit":7}))
        );
        assert_eq!(
            variables.extract::<Vec<Rule>>("rules").unwrap().unwrap()[0].name,
            "cwd"
        );
        assert!(variables.extract::<String>("nested").is_err());
        assert!(variables.extract::<Value>("absent").unwrap().is_none());
        assert!(serde_json::from_value::<Variable>(json!([])).is_err());
        assert_eq!(
            serde_json::to_value(AgentRole::MainAgent).unwrap(),
            "main_agent"
        );
    }
}

#[cfg(test)]
mod mode_metadata_tests {
    use super::*;
    #[test]
    fn historical_modes_round_trip_without_inventing_empty_metadata() {
        let frozen = serde_json::json!({"name":"historical","role":"both","permissions":[]});
        let mode: AgentMode = serde_json::from_value(frozen.clone()).unwrap();
        assert!(mode.description.is_empty());
        assert!(mode.prompt.is_empty());
        assert_eq!(serde_json::to_value(mode).unwrap(), frozen);
    }
}
