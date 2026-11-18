//! Creation-time prompt configuration, retained independently of mutable defaults.
use super::*;
use serde::{Deserialize, Serialize};

pub const DEFAULT_COMPACT: &str = include_str!("../../../templates/subagents/COMPACT.tmpl");
pub const DEFAULT_AUTO_COMPACT: &str =
    include_str!("../../../templates/subagents/AUTO_COMPACT.tmpl");
pub const DEFAULT_RENAME: &str = include_str!("../../../templates/subagents/RENAME.tmpl");
pub const DEFAULT_ACTION_REVIEW: &str =
    include_str!("../../../templates/subagents/ACTION_REVIEW.tmpl");
pub const MAX_PROMPT_BYTES: usize = 256 * 1024;
pub const MAX_PROMPT_CONFIG_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptConfig {
    pub system_template: String,
    pub compact_template: String,
    pub auto_compact_template: String,
    pub rename_template: String,
    pub action_review_template: String,
    pub client_prompt: String,
}
impl Default for PromptConfig {
    fn default() -> Self {
        Self {
            system_template: DEFAULT_SYSTEM.into(),
            compact_template: DEFAULT_COMPACT.into(),
            auto_compact_template: DEFAULT_AUTO_COMPACT.into(),
            rename_template: DEFAULT_RENAME.into(),
            action_review_template: DEFAULT_ACTION_REVIEW.into(),
            client_prompt: String::new(),
        }
    }
}
impl PromptConfig {
    /// Empty template slots explicitly select the embedded baseline. Client text
    /// stays byte-for-byte intact; empty client text deliberately disables it.
    pub fn resolved(mut self) -> Self {
        for (slot, baseline) in [
            (&mut self.system_template, DEFAULT_SYSTEM),
            (&mut self.compact_template, DEFAULT_COMPACT),
            (&mut self.auto_compact_template, DEFAULT_AUTO_COMPACT),
            (&mut self.rename_template, DEFAULT_RENAME),
            (&mut self.action_review_template, DEFAULT_ACTION_REVIEW),
        ] {
            if slot.is_empty() {
                *slot = baseline.into();
            }
        }
        self
    }
    fn validate_bounds(&self) -> Result<(), Error> {
        let fields = [
            &self.system_template,
            &self.compact_template,
            &self.auto_compact_template,
            &self.rename_template,
            &self.action_review_template,
            &self.client_prompt,
        ];
        if fields.iter().any(|field| field.len() > MAX_PROMPT_BYTES)
            || fields.iter().map(|field| field.len()).sum::<usize>() > MAX_PROMPT_CONFIG_BYTES
        {
            return Err(Error::config("prompt configuration byte limit exceeded"));
        }
        Ok(())
    }
    /// Syntax-only validation is safe before Agent selection or durable effects.
    pub fn validate(&self) -> Result<(), Error> {
        self.validate_bounds()?;
        let config = self.clone().resolved();
        config.validate_bounds()?;
        if config.system_template.trim().is_empty() {
            return Err(Error::config("SYSTEM template must be nonempty"));
        }
        super::syntax::validate(&config.system_template)?;
        let mut engine = tera::Tera::new();
        engine.register_filter("json_encode", super::json_encode);
        engine
            .add_raw_template("SYSTEM", &config.system_template)
            .map_err(|error| {
                Error::config_with_private(
                    "SYSTEM template syntax failed",
                    format!("SYSTEM template syntax failed: {error}"),
                )
            })?;
        for (template, bindings) in [
            (config.compact_template.as_str(), &["user_input"][..]),
            (config.auto_compact_template.as_str(), &[][..]),
            (config.rename_template.as_str(), &["user_input"][..]),
            (
                config.action_review_template.as_str(),
                &["user_input", "tool", "tool_name", "tool_args"][..],
            ),
        ] {
            crate::runtime::subagents::validate_template(
                template,
                bindings,
                MAX_PROMPT_CONFIG_BYTES,
            )?;
        }
        Ok(())
    }
    pub fn render_system(&self, agent: Option<&AgentInfo>) -> Result<String, Error> {
        self.validate()?;
        let config = self.clone().resolved();
        super::render_with_client(&config.system_template, agent, Some(&config.client_prompt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_all_slots_before_agent_render() {
        let mut config = PromptConfig {
            system_template: "{{ agent.required }}".into(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert!(config.render_system(None).is_err());
        config.system_template = "{% if broken".into();
        assert!(config.validate().is_err());
        for slot in ["compact", "auto_compact", "rename", "action_review"] {
            let mut config = PromptConfig::default();
            match slot {
                "compact" => config.compact_template = "{{ unexpected }}".into(),
                "auto_compact" => config.auto_compact_template = "{{ user_input }}".into(),
                "rename" => config.rename_template = "{{ user_input | safe }}".into(),
                _ => config.action_review_template = "{% if tool %}unsafe{% endif %}".into(),
            }
            assert!(config.validate().is_err(), "{slot}");
        }
    }
    #[test]
    fn empty_templates_reset_and_client_bytes_are_literal() {
        let config = PromptConfig {
            system_template: String::new(),
            compact_template: String::new(),
            auto_compact_template: String::new(),
            rename_template: String::new(),
            action_review_template: String::new(),
            client_prompt: "  {{ 1 + 1 }}\n ".into(),
        }
        .resolved();
        config.validate().unwrap();
        assert_eq!(config.compact_template, DEFAULT_COMPACT);
        assert!(
            config
                .render_system(None)
                .unwrap()
                .contains("  {{ 1 + 1 }}\n ")
        );
        let empty = PromptConfig::default().render_system(None).unwrap();
        assert!(!empty.contains("# Client Instructions"));
        assert!(
            config
                .render_system(None)
                .unwrap()
                .contains("# Client Instructions")
        );
    }
    #[test]
    fn configured_review_and_auto_compact_use_original_literal_bindings() {
        use crate::runtime::subagents::{Source, prepare_configured};
        let config = PromptConfig {
            auto_compact_template: "CUSTOM AUTO".into(),
            action_review_template:
                "REVIEW {{ tool }}|{{ tool_name }}|{{ tool_args }}|{{ user_input }}".into(),
            ..Default::default()
        };
        config.validate().unwrap();
        let issuer = crate::context::Context::new("FROZEN".into(), vec![], 65536)
            .unwrap()
            .content(512)
            .unwrap();
        let auto =
            prepare_configured("auto_compact", Source::System, &issuer, &[], &config).unwrap();
        assert_eq!(auto.system, "FROZEN");
        assert!(
            serde_json::to_string(&auto.items)
                .unwrap()
                .contains("CUSTOM AUTO")
        );
        let review = prepare_configured(
            "action_review",
            Source::System,
            &issuer,
            &[
                ("tool", "read"),
                ("tool_name", "read"),
                ("tool_args", "{{ literal }}"),
                ("user_input", "{% literal %}"),
            ],
            &config,
        )
        .unwrap();
        assert_eq!(
            review.system,
            "REVIEW read|read|{{ literal }}|{% literal %}"
        );
        assert!(review.tools.is_empty());
        assert_eq!(issuer.system, "FROZEN");
        assert!(prepare_configured("action_review", Source::User, &issuer, &[], &config).is_err());
    }
    #[test]
    fn rejects_bounds_and_blank_helpers() {
        let mut config = PromptConfig {
            client_prompt: "x".repeat(MAX_PROMPT_BYTES + 1),
            ..Default::default()
        };
        assert!(config.validate().is_err());
        config.client_prompt.clear();
        config.system_template = "  \n\t".into();
        assert!(config.validate().is_err());
        config.system_template = DEFAULT_SYSTEM.into();
        config.rename_template = "  \n".into();
        assert!(config.validate().is_err());
        let mut config = PromptConfig {
            system_template: "{% for i in range(end=100000) %}01234567890123456789{% endfor %}"
                .into(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.render_system(None).is_err());
        config.system_template = "x".repeat(MAX_PROMPT_BYTES);
        config.compact_template = "x".repeat(MAX_PROMPT_BYTES);
        config.auto_compact_template = "x".repeat(MAX_PROMPT_BYTES);
        config.rename_template = "x".repeat(MAX_PROMPT_BYTES);
        config.client_prompt = "x".into();
        assert!(config.validate().is_err());
    }
}
