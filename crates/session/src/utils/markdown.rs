//! Markdown headings disclose disjoint sections, never an overlapping subtree body.
use pulldown_cmark::{Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use serde::Serialize;
use serde_yaml_ng::{Mapping, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error)]
pub enum MarkdownError {
    #[error("Markdown document location must be nonempty")]
    Location,
    #[error("invalid Markdown front matter: {0}")]
    FrontMatter(String),
    #[error("Markdown reference ID collision: {0}")]
    IdentityCollision(String),
}

/// Rich YAML values, including future keys, remain available to the catalog owner.
#[derive(Debug, Clone, Serialize)]
pub struct MarkdownMetadata {
    pub name: Option<String>,
    pub description: Option<String>,
    pub license: Option<String>,
    pub compatibility: Option<String>,
    pub title_depth: u8,
    pub tags: Vec<String>,
    pub metadata: Value,
    pub allowed_tools: Value,
    pub extra: BTreeMap<String, Value>,
}

/// Only this disclosure tree is serializable; body lookup is an explicit action.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MarkdownNode {
    pub id: String,
    pub title: String,
    pub level: u8,
    pub char_length: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub important: Option<String>,
    pub subjects: Vec<MarkdownNode>,
}

#[derive(Debug, Clone)]
pub struct MarkdownDocument {
    pub metadata: MarkdownMetadata,
    pub root: MarkdownNode,
    /// Raw Markdown without YAML. Every body belongs to exactly one disclosed node.
    pub bodies: BTreeMap<String, String>,
}

struct Heading {
    title: String,
    level: u8,
    start: usize,
}

/// Parse a stable catalog location and document into disjoint expandable sections.
///
/// `location` is a caller-owned canonical identity, including any catalog namespace.
/// Only top-level YAML `title_depth` (1..=6, default 2), `tags`/`tag` have special
/// indexing semantics. Unknown fields and nested metadata are retained without aliases.
pub fn parse_markdown(location: &str, markdown: &str) -> Result<MarkdownDocument, MarkdownError> {
    if location.trim().is_empty() {
        return Err(MarkdownError::Location);
    }
    let (metadata, body) = front_matter(markdown)?;
    let headings = headings(body);
    let root_title = metadata
        .name
        .clone()
        .or_else(|| {
            headings
                .iter()
                .find(|heading| heading.level == 1)
                .map(|heading| heading.title.clone())
        })
        .unwrap_or_else(|| {
            location
                .split(['/', '\\'])
                .next_back()
                .unwrap_or(location)
                .to_owned()
        });
    let disclosed = headings
        .into_iter()
        .filter(|heading| heading.level <= metadata.title_depth)
        .collect::<Vec<_>>();
    let root_id = identity(location, "root", &[]);
    let preamble_end = disclosed
        .first()
        .map_or(body.len(), |heading| heading.start);
    let mut bodies = BTreeMap::new();
    let preamble = &body[..preamble_end];
    bodies.insert(root_id.clone(), preamble.to_owned());
    let mut root = MarkdownNode {
        id: root_id,
        title: root_title,
        level: 0,
        char_length: preamble.chars().count(),
        important: important(preamble),
        subjects: vec![],
    };
    // Arena paths keep titles independent of disclosure depth and separate repeated
    // headings within the same hierarchy. No mutable tree references cross pushes.
    let mut hierarchy: Vec<(u8, String, u64)> = vec![];
    let mut sibling_ordinals: BTreeMap<String, u64> = BTreeMap::new();
    let mut arena: Vec<(MarkdownNode, Option<usize>)> = vec![];
    let mut parents: Vec<(u8, usize)> = vec![];
    for (index, heading) in disclosed.iter().enumerate() {
        while hierarchy
            .last()
            .is_some_and(|(level, _, _)| *level >= heading.level)
        {
            hierarchy.pop();
        }
        let parent_key = identity(location, "siblings", &hierarchy);
        let ordinal_key = format!(
            "{}:{}:{}:{}",
            parent_key,
            heading.level,
            heading.title.len(),
            heading.title
        );
        let ordinal = sibling_ordinals.entry(ordinal_key).or_default();
        *ordinal += 1;
        hierarchy.push((heading.level, heading.title.clone(), *ordinal));
        let id = identity(location, "heading", &hierarchy);
        let end = disclosed
            .get(index + 1)
            .map_or(body.len(), |next| next.start);
        let section = &body[heading.start..end];
        if bodies.contains_key(&id) {
            return Err(MarkdownError::IdentityCollision(id));
        }
        bodies.insert(id.clone(), section.to_owned());
        while parents
            .last()
            .is_some_and(|(level, _)| *level >= heading.level)
        {
            parents.pop();
        }
        let parent = parents.last().map(|(_, index)| *index);
        arena.push((
            MarkdownNode {
                id,
                title: heading.title.clone(),
                level: heading.level,
                char_length: section.chars().count(),
                important: important(section),
                subjects: vec![],
            },
            parent,
        ));
        parents.push((heading.level, arena.len() - 1));
    }
    // Reverse assembly preserves sibling order while avoiding recursive parsing.
    let mut children: BTreeMap<Option<usize>, Vec<MarkdownNode>> = BTreeMap::new();
    for (index, (mut node, parent)) in arena.into_iter().enumerate().rev() {
        node.subjects = children.remove(&Some(index)).unwrap_or_default();
        node.subjects.reverse();
        children.entry(parent).or_default().push(node);
    }
    root.subjects = children.remove(&None).unwrap_or_default();
    root.subjects.reverse();
    Ok(MarkdownDocument {
        metadata,
        root,
        bodies,
    })
}

fn identity(location: &str, kind: &str, hierarchy: &[(u8, String, u64)]) -> String {
    let mut digest = Sha256::new();
    for value in ["microcrab-markdown-v1", kind, location] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    for (level, title, ordinal) in hierarchy {
        digest.update([*level]);
        digest.update((title.len() as u64).to_be_bytes());
        digest.update(title.as_bytes());
        digest.update(ordinal.to_be_bytes());
    }
    // Keep the identity inputs stable; catalog and parser insertion reject collisions.
    let digest = format!("{:x}", digest.finalize());
    format!("md-{}", &digest[..24])
}

fn headings(body: &str) -> Vec<Heading> {
    let mut result = vec![];
    let mut current: Option<Heading> = None;
    for (event, range) in Parser::new_ext(body, Options::empty()).into_offset_iter() {
        match event {
            Event::Start(Tag::Heading { level, .. }) => {
                current = Some(Heading {
                    title: String::new(),
                    level: level_number(level),
                    start: range.start,
                });
            }
            Event::End(TagEnd::Heading(_)) => {
                if let Some(mut heading) = current.take() {
                    heading.title = heading.title.trim().to_owned();
                    result.push(heading);
                }
            }
            Event::Text(text) | Event::Code(text) => {
                if let Some(heading) = &mut current {
                    heading.title.push_str(&text);
                }
            }
            Event::SoftBreak | Event::HardBreak => {
                if let Some(heading) = &mut current {
                    heading.title.push(' ');
                }
            }
            _ => {}
        }
    }
    result
}

fn level_number(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

fn important(body: &str) -> Option<String> {
    let mut depth = 0usize;
    let mut quote = String::new();
    let mut notes = vec![];
    for event in Parser::new_ext(body, Options::empty()) {
        match event {
            Event::Start(Tag::BlockQuote(_)) => {
                depth += 1;
            }
            Event::End(TagEnd::BlockQuote(_)) => {
                depth -= 1;
                if depth == 0 {
                    let text = quote.trim();
                    if let Some(note) = text.strip_prefix("[!IMPORTANT]")
                        && (note.is_empty() || note.starts_with(char::is_whitespace))
                    {
                        notes.push(note.trim().to_owned());
                    }
                    quote.clear();
                }
            }
            Event::Text(text) | Event::Code(text) if depth > 0 => quote.push_str(&text),
            Event::SoftBreak | Event::HardBreak if depth > 0 => quote.push('\n'),
            Event::End(TagEnd::Paragraph) if depth > 0 => quote.push_str("\n\n"),
            _ => {}
        }
    }
    if notes.is_empty() {
        None
    } else {
        Some(notes.join("\n\n"))
    }
}

fn front_matter(markdown: &str) -> Result<(MarkdownMetadata, &str), MarkdownError> {
    let markdown = markdown.strip_prefix('\u{feff}').unwrap_or(markdown);
    let mut yaml = Mapping::new();
    let mut body = markdown;
    let first_end = markdown
        .find('\n')
        .map_or(markdown.len(), |index| index + 1);
    if markdown[..first_end].trim_end_matches(['\r', '\n']) == "---" {
        let mut close = None;
        let mut offset = first_end;
        for line in markdown[first_end..].split_inclusive('\n') {
            if matches!(line.trim_end_matches(['\r', '\n']), "---" | "...") {
                close = Some((offset, offset + line.len()));
                break;
            }
            offset += line.len();
        }
        let (yaml_end, body_start) =
            close.ok_or_else(|| MarkdownError::FrontMatter("unclosed YAML block".into()))?;
        let value: Value = serde_yaml_ng::from_str(&markdown[first_end..yaml_end])
            .map_err(|error| MarkdownError::FrontMatter(error.to_string()))?;
        yaml = match value {
            Value::Null => Mapping::new(),
            Value::Mapping(mapping) => mapping,
            _ => return Err(MarkdownError::FrontMatter("expected a YAML mapping".into())),
        };
        body = &markdown[body_start..];
    }
    let name = string(&mut yaml, "name")?;
    let description = string(&mut yaml, "description")?;
    let license = string(&mut yaml, "license")?;
    let compatibility = string(&mut yaml, "compatibility")?;
    let title_depth = match yaml.remove(Value::String("title_depth".into())) {
        None => 2,
        Some(Value::Number(number)) => number
            .as_u64()
            .filter(|value| (1..=6).contains(value))
            .ok_or_else(|| {
                MarkdownError::FrontMatter("title_depth must be an integer from 1 to 6".into())
            })? as u8,
        Some(_) => {
            return Err(MarkdownError::FrontMatter(
                "title_depth must be an integer from 1 to 6".into(),
            ));
        }
    };
    let tags = tags(
        yaml.remove(Value::String("tags".into())),
        yaml.remove(Value::String("tag".into())),
    )?;
    let metadata = yaml
        .remove(Value::String("metadata".into()))
        .unwrap_or(Value::Null);
    let allowed_tools = yaml
        .remove(Value::String("allowed-tools".into()))
        .unwrap_or(Value::Null);
    let mut extra = BTreeMap::new();
    for (key, value) in yaml {
        let Value::String(key) = key else {
            return Err(MarkdownError::FrontMatter(
                "metadata keys must be strings".into(),
            ));
        };
        extra.insert(key, value);
    }
    Ok((
        MarkdownMetadata {
            name,
            description,
            license,
            compatibility,
            title_depth,
            tags,
            metadata,
            allowed_tools,
            extra,
        },
        body,
    ))
}

fn string(mapping: &mut Mapping, key: &str) -> Result<Option<String>, MarkdownError> {
    match mapping.remove(Value::String(key.into())) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(MarkdownError::FrontMatter(format!(
            "{key} must be a string"
        ))),
    }
}

fn tags(tags: Option<Value>, tag: Option<Value>) -> Result<Vec<String>, MarkdownError> {
    let mut result = vec![];
    for value in [tags, tag].into_iter().flatten() {
        match value {
            Value::String(value) => result.push(value),
            Value::Sequence(values) => {
                for value in values {
                    let Value::String(value) = value else {
                        return Err(MarkdownError::FrontMatter(
                            "tags/tag must contain strings".into(),
                        ));
                    };
                    result.push(value);
                }
            }
            _ => {
                return Err(MarkdownError::FrontMatter(
                    "tags/tag must be a string or string sequence".into(),
                ));
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
#[path = "markdown/tests.rs"]
mod tests;
