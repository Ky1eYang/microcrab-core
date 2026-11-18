//! Pre-permission expansion of model-visible custom variables.
//! Inserted values are literal data. This pass never reads Secret plaintext.
use crate::Error;
use crate::state::variables::{InjectionKey, InjectionView};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

const MAX_RESOLVED_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_ARGUMENT_DEPTH: usize = 128;
const MAX_POINTER_BYTES: usize = 512;
const MAX_POINTER_SEGMENTS: usize = 32;
const MAX_PLACEHOLDER_BYTES: usize = 700;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct LiteralMask {
    nodes: BTreeSet<String>,
    ranges: BTreeMap<String, Vec<(usize, usize)>>,
}
impl LiteralMask {
    pub(crate) fn covers(&self, mut pointer: &str) -> bool {
        loop {
            if self.nodes.contains(pointer) {
                return true;
            }
            let Some((parent, _)) = pointer.rsplit_once('/') else {
                return false;
            };
            pointer = parent;
        }
    }
    /// Insertions are ordered and nonoverlapping. Empty substitutions remain
    /// barriers: removing template text must not manufacture a Secret token.
    pub(crate) fn intersects(&self, pointer: &str, start: usize, end: usize) -> bool {
        let ranges = self.ranges(pointer);
        let index = ranges.partition_point(|(_, to)| *to <= start);
        ranges.get(index).is_some_and(|(from, _)| *from < end)
    }
    pub(crate) fn ranges(&self, pointer: &str) -> &[(usize, usize)] {
        self.ranges.get(pointer).map(Vec::as_slice).unwrap_or(&[])
    }
    fn insert_node(&mut self, pointer: &str) {
        self.nodes.insert(pointer.to_owned());
    }
    fn insert_range(&mut self, pointer: &str, range: (usize, usize)) {
        self.ranges
            .entry(pointer.to_owned())
            .or_default()
            .push(range);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Conversion {
    Native,
    String,
    Number,
    Boolean,
}
impl Conversion {
    fn name(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
        }
    }
}

struct Placeholder<'a> {
    end: usize,
    key: &'a str,
    pointer: &'a str,
    conversion: Conversion,
}

/// Expand `{{var:...}}` placeholders once. The returned mask marks inserted data so
/// the later Secret pass cannot treat that data as another template.
pub(crate) fn expand_visible(
    arguments: &Value,
    view: &InjectionView,
    max_context_bytes: usize,
) -> Result<(Value, LiteralMask), Error> {
    if !has_visible_reference(arguments) {
        return Ok((arguments.clone(), LiteralMask::default()));
    }
    let max_bytes = max_context_bytes.min(MAX_RESOLVED_ARGUMENT_BYTES);
    if serde_json::to_vec(arguments)?.len() > max_bytes {
        return Err(budget_error());
    }
    let mut mask = LiteralMask::default();
    let mut remaining = max_bytes;
    let expanded = visit(arguments, view, &mut mask, &mut remaining, max_bytes, "", 0)?;
    if serde_json::to_vec(&expanded)?.len() > max_bytes {
        return Err(budget_error());
    }
    Ok((expanded, mask))
}

fn has_visible_reference(value: &Value) -> bool {
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            Value::String(text) if text.contains("{{var:") => return true,
            Value::Array(items) => pending.extend(items),
            Value::Object(fields) => pending.extend(fields.values()),
            _ => {}
        }
    }
    false
}

fn visit(
    value: &Value,
    view: &InjectionView,
    mask: &mut LiteralMask,
    remaining: &mut usize,
    max_bytes: usize,
    pointer: &str,
    depth: usize,
) -> Result<Value, Error> {
    if depth > MAX_ARGUMENT_DEPTH {
        return Err(budget_error());
    }
    match value {
        Value::String(text) => expand_string(text, view, mask, remaining, max_bytes, pointer),
        Value::Array(items) => {
            let mut expanded = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                expanded.push(visit(
                    item,
                    view,
                    mask,
                    remaining,
                    max_bytes,
                    &join(pointer, &index.to_string()),
                    depth + 1,
                )?);
            }
            Ok(Value::Array(expanded))
        }
        Value::Object(fields) => {
            let mut expanded = Map::new();
            for (key, item) in fields {
                expanded.insert(
                    key.clone(),
                    visit(
                        item,
                        view,
                        mask,
                        remaining,
                        max_bytes,
                        &join(pointer, &escape_pointer(key)),
                        depth + 1,
                    )?,
                );
            }
            Ok(Value::Object(expanded))
        }
        _ => Ok(value.clone()),
    }
}

fn expand_string(
    text: &str,
    view: &InjectionView,
    mask: &mut LiteralMask,
    remaining: &mut usize,
    max_bytes: usize,
    pointer: &str,
) -> Result<Value, Error> {
    let found = placeholders(text)?;
    if found.is_empty() {
        charge(remaining, text.len())?;
        return Ok(Value::String(text.to_owned()));
    }
    if found.len() == 1 && found[0].0 == 0 && found[0].1.end == text.len() {
        let placeholder = &found[0].1;
        let value = converted_value(view, placeholder)?;
        mask.insert_node(pointer);
        charge(remaining, serde_json::to_vec(&value)?.len())?;
        return Ok(value);
    }
    let mut result = String::new();
    let mut consumed = 0;
    for (start, placeholder) in found {
        if placeholder.conversion == Conversion::Native {
            return Err(Error::tool_dynamic(format!(
                "placeholder {} must be the entire value or use an explicit string, number, or boolean conversion",
                display_ref(&placeholder)
            )));
        }
        append(&mut result, &text[consumed..start], max_bytes)?;
        let converted = converted_value(view, &placeholder)?;
        let inserted = embed_text(&converted, &placeholder)?;
        let range = (result.len(), result.len() + inserted.len());
        append(&mut result, &inserted, max_bytes)?;
        mask.insert_range(pointer, range);
        consumed = start + placeholder.end;
    }
    append(&mut result, &text[consumed..], max_bytes)?;
    charge(remaining, result.len())?;
    Ok(Value::String(result))
}

fn placeholders(text: &str) -> Result<Vec<(usize, Placeholder<'_>)>, Error> {
    let mut found = Vec::new();
    let mut offset = 0;
    while let Some(relative) = text[offset..].find("{{var:") {
        let start = offset + relative;
        let placeholder = parse_placeholder(&text[start..]).map_err(|_| invalid_placeholder())?;
        found.push((start, placeholder));
        offset = start + found.last().expect("just pushed").1.end;
    }
    Ok(found)
}

fn parse_placeholder(text: &str) -> Result<Placeholder<'_>, ()> {
    let body = text.strip_prefix("{{var:").ok_or(())?;
    let window = body.len().min(MAX_PLACEHOLDER_BYTES);
    let end = body.as_bytes()[..window]
        .windows(2)
        .position(|pair| pair == b"}}")
        .ok_or(())?;
    let inner = &body[..end];
    let (conversion, rest) = conversion_prefix(inner);
    let key_len = key_prefix_len(rest);
    let key = &rest[..key_len];
    if !microcrab_protocol::valid_secret_key(key) {
        return Err(());
    }
    let pointer = match rest[key_len..].strip_prefix(':') {
        None if rest.len() == key_len => "",
        Some(pointer) if pointer.starts_with('/') && pointer.len() <= MAX_POINTER_BYTES => pointer,
        _ => return Err(()),
    };
    if !pointer.is_empty() {
        validate_pointer_syntax(pointer)?;
    }
    Ok(Placeholder {
        end: "{{var:".len() + end + 2,
        key,
        pointer,
        conversion,
    })
}

fn key_prefix_len(text: &str) -> usize {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return 0;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return 0;
    }
    let mut len = first.len_utf8();
    for ch in chars {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-') {
            len += ch.len_utf8();
        } else {
            break;
        }
    }
    len
}

fn conversion_prefix(text: &str) -> (Conversion, &str) {
    for (name, conversion) in [
        ("string:", Conversion::String),
        ("number:", Conversion::Number),
        ("boolean:", Conversion::Boolean),
    ] {
        if let Some(rest) = text.strip_prefix(name)
            && microcrab_protocol::valid_secret_key(rest.split(':').next().unwrap_or(""))
        {
            return (conversion, rest);
        }
    }
    (Conversion::Native, text)
}

fn validate_pointer_syntax(pointer: &str) -> Result<(), ()> {
    if !pointer.starts_with('/') || pointer.len() > MAX_POINTER_BYTES {
        return Err(());
    }
    let segments = pointer.split('/').skip(1).count();
    if segments == 0 || segments > MAX_POINTER_SEGMENTS {
        return Err(());
    }
    for segment in pointer.split('/').skip(1) {
        unescape(segment)?;
    }
    Ok(())
}

fn converted_value(view: &InjectionView, placeholder: &Placeholder<'_>) -> Result<Value, Error> {
    let value = match view.lookup(placeholder.key) {
        InjectionKey::Visible(value) => value,
        InjectionKey::Invisible => {
            return Err(Error::tool_dynamic(format!(
                "variable {} is not model-visible and cannot be injected",
                placeholder.key
            )));
        }
        InjectionKey::Secret => {
            return Err(Error::tool_dynamic(format!(
                "variable {} is secret; use {{{{secret:{}}}}} and do not read its value",
                placeholder.key, placeholder.key
            )));
        }
        InjectionKey::Missing => {
            return Err(Error::tool_dynamic(format!(
                "unknown variable {}",
                placeholder.key
            )));
        }
    };
    let selected = select_pointer(value, placeholder)?;
    convert(selected, placeholder)
}

fn select_pointer<'a>(value: &'a Value, placeholder: &Placeholder<'_>) -> Result<&'a Value, Error> {
    if placeholder.pointer.is_empty() {
        return Ok(value);
    }
    let mut current = value;
    for (count, segment) in placeholder.pointer.split('/').skip(1).enumerate() {
        if count >= MAX_POINTER_SEGMENTS {
            return Err(invalid_placeholder());
        }
        let key = unescape(segment).map_err(|_| invalid_placeholder())?;
        current = match current {
            Value::Object(fields) => fields.get(&key).ok_or_else(|| absent(placeholder))?,
            Value::Array(items) => {
                let index = array_index(&key, items.len()).map_err(|kind| match kind {
                    IndexError::Absent => absent(placeholder),
                    IndexError::Type => type_error(placeholder, json_type(current)),
                })?;
                &items[index]
            }
            _ => return Err(type_error(placeholder, json_type(current))),
        };
    }
    Ok(current)
}

enum IndexError {
    Absent,
    Type,
}
fn array_index(token: &str, len: usize) -> Result<usize, IndexError> {
    if token.is_empty()
        || !token.bytes().all(|byte| byte.is_ascii_digit())
        || (token.len() > 1 && token.starts_with('0'))
    {
        return Err(IndexError::Type);
    }
    let index: usize = token.parse().map_err(|_| IndexError::Type)?;
    if index >= len {
        Err(IndexError::Absent)
    } else {
        Ok(index)
    }
}

fn convert(value: &Value, placeholder: &Placeholder<'_>) -> Result<Value, Error> {
    match placeholder.conversion {
        Conversion::Native => Ok(value.clone()),
        Conversion::String => Ok(Value::String(string_form(value))),
        Conversion::Number => number_value(value, placeholder).map(Value::Number),
        Conversion::Boolean => boolean_value(value, placeholder).map(Value::Bool),
    }
}

fn embed_text(value: &Value, placeholder: &Placeholder<'_>) -> Result<String, Error> {
    match value {
        Value::String(text) if placeholder.conversion == Conversion::String => Ok(text.clone()),
        Value::Number(number) if placeholder.conversion == Conversion::Number => {
            Ok(number.to_string())
        }
        Value::Bool(value) if placeholder.conversion == Conversion::Boolean => {
            Ok(if *value { "true" } else { "false" }.to_owned())
        }
        _ => Err(Error::tool_dynamic(format!(
            "cannot convert {} from {} to {}",
            display_ref(placeholder),
            json_type(value),
            placeholder.conversion.name()
        ))),
    }
}

fn string_form(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "null".to_owned(),
        Value::Bool(true) => "true".to_owned(),
        Value::Bool(false) => "false".to_owned(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned()),
    }
}

fn number_value(value: &Value, placeholder: &Placeholder<'_>) -> Result<serde_json::Number, Error> {
    match value {
        Value::Number(number) => Ok(number.clone()),
        Value::String(text) if !text.is_empty() && text.trim() == text => {
            serde_json::from_str::<Value>(text)
                .ok()
                .and_then(|parsed| parsed.as_number().cloned())
                .ok_or_else(|| conversion_error(placeholder, value, "number"))
        }
        _ => Err(conversion_error(placeholder, value, "number")),
    }
}

fn boolean_value(value: &Value, placeholder: &Placeholder<'_>) -> Result<bool, Error> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::String(text) if text == "true" => Ok(true),
        Value::String(text) if text == "false" => Ok(false),
        _ => Err(conversion_error(placeholder, value, "boolean")),
    }
}

fn unescape(segment: &str) -> Result<String, ()> {
    let mut out = String::new();
    let mut chars = segment.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '~' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('0') => out.push('~'),
            Some('1') => out.push('/'),
            _ => return Err(()),
        }
    }
    Ok(out)
}

pub(crate) fn escape_pointer(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

fn join(parent: &str, segment: &str) -> String {
    format!("{parent}/{segment}")
}

fn append(result: &mut String, text: &str, max_bytes: usize) -> Result<(), Error> {
    if result
        .len()
        .checked_add(text.len())
        .is_none_or(|bytes| bytes > max_bytes)
    {
        return Err(budget_error());
    }
    result.push_str(text);
    Ok(())
}

fn charge(remaining: &mut usize, bytes: usize) -> Result<(), Error> {
    *remaining = remaining.checked_sub(bytes).ok_or_else(budget_error)?;
    Ok(())
}

fn display_ref(placeholder: &Placeholder<'_>) -> String {
    if placeholder.pointer.is_empty() {
        placeholder.key.to_owned()
    } else {
        format!("{}{}", placeholder.key, placeholder.pointer)
    }
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn absent(placeholder: &Placeholder<'_>) -> Error {
    Error::tool_dynamic(format!(
        "path {} is absent in {}",
        placeholder.pointer, placeholder.key
    ))
}

fn type_error(placeholder: &Placeholder<'_>, actual: &str) -> Error {
    Error::tool_dynamic(format!(
        "path {} in {} expects an object or array, found {}",
        placeholder.pointer, placeholder.key, actual
    ))
}

fn conversion_error(placeholder: &Placeholder<'_>, value: &Value, target: &str) -> Error {
    Error::tool_dynamic(format!(
        "cannot convert {} from {} to {}",
        display_ref(placeholder),
        json_type(value),
        target
    ))
}

fn invalid_placeholder() -> Error {
    Error::tool(
        "variable placeholder is unclosed or invalid; use {{var:KEY}}, {{var:KEY:/pointer}}, or an explicit string, number, or boolean conversion",
    )
}

fn budget_error() -> Error {
    Error::tool("resolved variable arguments exceed the invocation argument byte or depth budget")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;
    use std::sync::atomic::AtomicBool;

    fn view_with(visible: Value, invisible: &[&str], secret: &[&str]) -> InjectionView {
        InjectionView::fixture(
            visible
                .as_object()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect(),
            invisible.iter().map(|key| (*key).to_owned()).collect(),
            secret.iter().map(|key| (*key).to_owned()).collect(),
        )
    }

    #[test]
    fn whole_placeholder_keeps_native_type_and_pointer_extracts_once() {
        let view = view_with(
            serde_json::json!({"profile":{"name":"ada","n":2,"items":["a"]}}),
            &[],
            &[],
        );
        let (value, mask) = expand_visible(
            &serde_json::json!({"who":"{{var:profile:/name}}","all":"{{var:profile}}","item":"{{var:profile:/items/0}}"}),
            &view,
            Limits::default().max_context_bytes,
        )
        .unwrap();
        assert_eq!(value["who"], "ada");
        assert_eq!(value["all"]["n"], 2);
        assert_eq!(value["item"], "a");
        assert!(mask.covers("/who"));
        assert!(mask.covers("/all/name"));
        assert!(!mask.covers("/missing"));
    }

    #[test]
    fn embedded_native_placeholder_and_bad_conversion_fail_closed() {
        let view = view_with(serde_json::json!({"name":"ada","n":2}), &[], &[]);
        let embedded = expand_visible(&serde_json::json!({"text":"id {{var:name}}"}), &view, 1024);
        assert!(embedded.unwrap_err().to_string().contains("entire value"));
        let converted = expand_visible(
            &serde_json::json!({"text":"id {{var:number:name}}"}),
            &view,
            1024,
        );
        assert!(
            converted
                .unwrap_err()
                .to_string()
                .contains("cannot convert")
        );
        let invalid = expand_visible(&serde_json::json!({"text":"{{var:bad key}}"}), &view, 1024);
        assert!(invalid.unwrap_err().to_string().contains("invalid"));
    }

    #[test]
    fn explicit_string_conversion_embeds_without_rescanning_inserted_text() {
        let view = view_with(
            serde_json::json!({"name":"{{secret:token}}","flag":true,"box":{"ok":1}}),
            &["hidden"],
            &["token"],
        );
        let (value, mask) = expand_visible(
            &serde_json::json!({"text":"pre {{var:string:name}} {{secret:token}} {{var:string:box}}"}),
            &view,
            4096,
        )
        .unwrap();
        assert_eq!(
            value["text"],
            "pre {{secret:token}} {{secret:token}} {\"ok\":1}"
        );
        assert_eq!(mask.ranges("/text").len(), 2);
        let hidden = expand_visible(&serde_json::json!({"text":"{{var:hidden}}"}), &view, 1024);
        assert!(
            hidden
                .unwrap_err()
                .to_string()
                .contains("not model-visible")
        );
        let secret = expand_visible(&serde_json::json!({"text":"{{var:token}}"}), &view, 1024)
            .unwrap_err()
            .to_string();
        assert!(secret.contains("secret"));
        assert!(!secret.contains("PRIVATE"));
    }

    #[test]
    fn pointer_array_indices_require_canonical_ascii_digits() {
        let view = view_with(
            serde_json::json!({"a":["first","second"],"o":{"+0":"object-key"}}),
            &[],
            &[],
        );
        for index in ["+0", "-0", "00", "01", "-", "１", "184467440737095516160"] {
            let input = serde_json::json!({"x":format!("{{{{var:a:/{index}}}}}")});
            assert!(expand_visible(&input, &view, 4096).is_err(), "{index}");
        }
        for (reference, expected) in [
            ("{{var:a:/0}}", "first"),
            ("{{var:a:/1}}", "second"),
            ("{{var:o:/+0}}", "object-key"),
        ] {
            assert_eq!(
                expand_visible(&serde_json::json!({"x":reference}), &view, 4096)
                    .unwrap()
                    .0["x"],
                expected
            );
        }
    }

    #[test]
    fn expansion_budget_does_not_lower_unmodified_argument_admission() {
        let view = view_with(
            serde_json::json!({"large":"a".repeat(MAX_RESOLVED_ARGUMENT_BYTES)}),
            &[],
            &[],
        );
        let ordinary = serde_json::json!({"data":"a".repeat(MAX_RESOLVED_ARGUMENT_BYTES + 16),"{{var:missing}}":"literal key"});
        let (unchanged, mask) = expand_visible(&ordinary, &view, 64 * 1024 * 1024).unwrap();
        assert_eq!(unchanged, ordinary);
        assert_eq!(mask, LiteralMask::default());
        assert!(
            expand_visible(
                &serde_json::json!({"data":"{{var:large}}"}),
                &view,
                64 * 1024 * 1024
            )
            .is_err()
        );
        let mut deep = Value::Null;
        for _ in 0..=MAX_ARGUMENT_DEPTH {
            deep = Value::Array(vec![deep]);
        }
        assert_eq!(expand_visible(&deep, &view, 4096).unwrap().0, deep);
    }

    #[test]
    fn literal_mask_queries_exact_ancestors_and_insertion_boundaries() {
        let mut mask = LiteralMask::default();
        mask.insert_node("/a~1b");
        assert!(mask.covers("/a~1b/child"));
        assert!(!mask.covers("/a~1b2"));
        mask.insert_range("/text", (2, 4));
        mask.insert_range("/text", (6, 6));
        mask.insert_range("/text", (8, 10));
        assert!(mask.intersects("/text", 0, 3));
        assert!(mask.intersects("/text", 3, 5));
        assert!(mask.intersects("/text", 5, 7));
        assert!(!mask.intersects("/text", 4, 6));
        assert!(!mask.intersects("/text", 6, 8));
        assert!(mask.intersects("/text", 7, 9));
        assert!(!mask.intersects("/other", 0, 100));
        mask.insert_node("");
        assert!(mask.covers("/any/descendant"));
    }

    #[tokio::test]
    async fn live_state_snapshot_distinguishes_visibility_without_reading_secret_plaintext() {
        let (state, _endpoint) = crate::State::new(Limits::default()).bind_writer(
            std::sync::Arc::new(AtomicBool::new(false)),
            std::sync::Arc::new(AtomicBool::new(false)),
        );
        state
            .inject("shown", serde_json::json!({"ok":true}))
            .await
            .unwrap();
        state
            .prepare_secret_write(std::collections::BTreeMap::from([(
                "token".into(),
                "PRIVATE".into(),
            )]))
            .await
            .unwrap()
            .commit()
            .unwrap();
        let view = state.injection_view();
        assert!(matches!(view.lookup("shown"), InjectionKey::Visible(_)));
        assert!(matches!(view.lookup("token"), InjectionKey::Secret));
        assert!(matches!(view.lookup("missing"), InjectionKey::Missing));
        let rendered = format!("{view:?}");
        assert!(!rendered.contains("PRIVATE"));
    }
}
