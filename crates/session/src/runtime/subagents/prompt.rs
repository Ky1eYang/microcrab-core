//! Verbatim strict prompt rendering and frozen context materialization.
use super::{ContextMode, Definition};
use crate::Error;
use crate::context::{Content, ContentItem};
use microcrab_protocol::InputPart;
use microcrab_transport::render_ast;

impl Definition {
    /// Render caller-supplied Markdown with only explicitly declared variables.
    ///
    /// Authored bytes, including headings, are retained. Values are copied once
    /// and never parsed as syntax. The owned request cannot mutate its issuer.
    pub fn prepare_markdown(
        &self,
        markdown: &str,
        issuer: &Content,
        bindings: &[(&str, &str)],
    ) -> Result<Content, Error> {
        if self.name.trim().is_empty() {
            return Err(Error::config("functional subagent name must be nonempty"));
        }
        let rendered = interpolate(markdown, bindings, issuer.max_bytes)?;
        if rendered.trim().is_empty() {
            return Err(Error::config("functional prompt must be nonempty"));
        }
        let mut content = issuer.clone();
        content.subagent = Some(self.name.clone());
        content.turn_id = None;
        content.round = None;
        if self.context_mode != ContextMode::Inherit {
            content.tools.clear();
            content.items.clear();
        }
        let text = if self.context_mode == ContextMode::Clean
            || (self.context_mode == ContextMode::System && content.system.trim().is_empty())
        {
            content.system = rendered;
            "begin".to_owned()
        } else {
            rendered
        };
        content.items.push(ContentItem::User {
            source: None,
            parts: vec![InputPart::Text { text }],
        });
        // Charge the final provider AST, including the trigger and authored headings.
        render_ast(&content)?;
        Ok(content)
    }
}

fn interpolate(template: &str, bindings: &[(&str, &str)], maximum: usize) -> Result<String, Error> {
    for (index, (name, _)) in bindings.iter().enumerate() {
        if name.is_empty()
            || !name.bytes().enumerate().all(|(index, byte)| {
                byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
            })
            || bindings[..index]
                .iter()
                .any(|(previous, _)| previous == name)
        {
            return Err(Error::config(
                "functional prompt bindings require unique variable names",
            ));
        }
    }
    // A text-only upper bound prevents oversized literal substitution from allocating
    // beyond the provider budget. Final render_ast still charges exact JSON bytes.
    let append = |output: &mut String, value: &str| -> Result<(), Error> {
        let required = output.len().checked_add(value.len()).ok_or_else(|| {
            crate::budget::reject(
                crate::LimitKind::Arithmetic,
                "functional prompt byte overflow",
            )
        })?;
        crate::budget::check(crate::LimitKind::ProviderRequest, required, maximum)?;
        output.push_str(value);
        Ok(())
    };
    let mut output = String::new();
    let mut rest = template;
    while let Some(index) = rest.find(['{', '}', '%', '#']) {
        append(&mut output, &rest[..index])?;
        rest = &rest[index..];
        if let Some(expression) = rest.strip_prefix("{{") {
            let end = expression
                .find("}}")
                .ok_or_else(|| Error::config("unclosed functional prompt interpolation"))?;
            let name = expression[..end].trim();
            let value = bindings
                .iter()
                .find_map(|(declared, value)| (*declared == name).then_some(*value))
                .ok_or_else(|| {
                    Error::config("functional prompt references an undeclared variable")
                })?;
            append(&mut output, value)?;
            rest = &expression[end + 2..];
        } else {
            if ["{%", "{#", "}}", "%}", "#}"]
                .iter()
                .any(|token| rest.starts_with(token))
            {
                return Err(Error::config(
                    "unsupported functional prompt template syntax",
                ));
            }
            let character = rest
                .chars()
                .next()
                .expect("rest begins with a matched character");
            append(&mut output, &rest[..character.len_utf8()])?;
            rest = &rest[character.len_utf8()..];
        }
    }
    append(&mut output, rest)?;
    Ok(output)
}
