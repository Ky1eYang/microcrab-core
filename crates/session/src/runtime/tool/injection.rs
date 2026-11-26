//! Pre-permission expansion of model-visible custom variables and mounted References.
//! Inserted values are literal data. This pass never reads Secret plaintext.
use crate::state::reference::{ReferenceContent, ReferencePayload};
use crate::state::variables::{InjectionKey, InjectionView};
use crate::{Error, LimitKind};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use tokio_util::sync::CancellationToken;

const MAX_RESOLVED_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_REF_ARGUMENT_BYTES: usize = microcrab_permission::MAX_ARGUMENT_BYTES;
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
    Text,
    Base64,
    DataUrl,
    Json,
}
impl Conversion {
    fn name(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Text => "text",
            Self::Base64 => "base64",
            Self::DataUrl => "data_url",
            Self::Json => "json",
        }
    }

    fn embeddable(self) -> bool {
        !matches!(self, Self::Native | Self::Json)
    }
}

enum PlaceholderKind<'a> {
    Var {
        key: &'a str,
    },
    Ref {
        id: &'a str,
    },
}

struct Placeholder<'a> {
    end: usize,
    kind: PlaceholderKind<'a>,
    pointer: &'a str,
    conversion: Conversion,
}

impl<'a> Placeholder<'a> {
    fn key_or_id(&self) -> &'a str {
        match self.kind {
            PlaceholderKind::Var { key } => key,
            PlaceholderKind::Ref { id } => id,
        }
    }
}

#[derive(Clone, Copy)]
struct Budget {
    max_bytes: usize,
    limit_kind: Option<LimitKind>,
}

impl Budget {
    fn error(self, required: usize) -> Error {
        if let Some(kind) = self.limit_kind {
            crate::budget::check(kind, required.max(self.max_bytes.saturating_add(1)), self.max_bytes)
                .unwrap_err()
        } else {
            budget_error()
        }
    }
}

/// Expand `{{var:...}}` placeholders synchronously. Used by unit tests and pure variable callers.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn expand_visible(
    arguments: &Value,
    view: &InjectionView,
    max_context_bytes: usize,
) -> Result<(Value, LiteralMask), Error> {
    if !has_prefix(arguments, "{{var:") {
        return Ok((arguments.clone(), LiteralMask::default()));
    }
    let budget = Budget {
        max_bytes: max_context_bytes.min(MAX_RESOLVED_ARGUMENT_BYTES),
        limit_kind: None,
    };
    let initial_len = serde_json::to_vec(arguments)?.len();
    if initial_len > budget.max_bytes {
        return Err(budget.error(initial_len));
    }
    let mut mask = LiteralMask::default();
    let mut remaining = budget.max_bytes;
    let expanded = visit_sync(arguments, view, &mut mask, &mut remaining, budget, "", 0)?;
    let final_len = serde_json::to_vec(&expanded)?.len();
    if final_len > budget.max_bytes {
        return Err(budget.error(final_len));
    }
    Ok((expanded, mask))
}

/// Expand `{{var:...}}` and `{{ref:...}}` placeholders once before permission/dispatch.
/// Inserted data is marked in `LiteralMask` and never re-scanned for Secret placeholders.
pub(crate) async fn expand_dispatch(
    arguments: &Value,
    state: &crate::State,
    max_context_bytes: usize,
    cancel: &CancellationToken,
) -> Result<(Value, LiteralMask), Error> {
    let has_ref = has_prefix(arguments, "{{ref:");
    let has_var = has_prefix(arguments, "{{var:");
    if !has_ref && !has_var {
        return Ok((arguments.clone(), LiteralMask::default()));
    }
    if !has_ref {
        return expand_visible(arguments, &state.injection_view(), max_context_bytes);
    }
    let budget = Budget {
        max_bytes: max_context_bytes.min(MAX_REF_ARGUMENT_BYTES),
        limit_kind: Some(LimitKind::ToolArguments),
    };
    let initial_len = serde_json::to_vec(arguments)?.len();
    if initial_len > budget.max_bytes {
        return Err(budget.error(initial_len));
    }
    let view = state.injection_view();
    let mut cache: HashMap<String, ReferenceContent> = HashMap::new();
    let mut mask = LiteralMask::default();
    let mut remaining = budget.max_bytes;
    let expanded = visit_async(
        arguments,
        &view,
        state,
        &mut cache,
        cancel,
        &mut mask,
        &mut remaining,
        budget,
        "",
        0,
    )
    .await?;
    let final_len = serde_json::to_vec(&expanded)?.len();
    if final_len > budget.max_bytes {
        return Err(budget.error(final_len));
    }
    Ok((expanded, mask))
}

fn has_prefix(value: &Value, needle: &str) -> bool {
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            Value::String(text) if text.contains(needle) => return true,
            Value::Array(items) => pending.extend(items),
            Value::Object(fields) => pending.extend(fields.values()),
            _ => {}
        }
    }
    false
}

fn visit_sync(
    value: &Value,
    view: &InjectionView,
    mask: &mut LiteralMask,
    remaining: &mut usize,
    budget: Budget,
    pointer: &str,
    depth: usize,
) -> Result<Value, Error> {
    if depth > MAX_ARGUMENT_DEPTH {
        return Err(budget.error(budget.max_bytes.saturating_add(1)));
    }
    match value {
        Value::String(text) => expand_string_sync(text, view, mask, remaining, budget, pointer),
        Value::Array(items) => {
            let mut expanded = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                expanded.push(visit_sync(
                    item,
                    view,
                    mask,
                    remaining,
                    budget,
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
                    visit_sync(
                        item,
                        view,
                        mask,
                        remaining,
                        budget,
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

#[expect(clippy::too_many_arguments)]
fn visit_async<'a>(
    value: &'a Value,
    view: &'a InjectionView,
    state: &'a crate::State,
    cache: &'a mut HashMap<String, ReferenceContent>,
    cancel: &'a CancellationToken,
    mask: &'a mut LiteralMask,
    remaining: &'a mut usize,
    budget: Budget,
    pointer: &'a str,
    depth: usize,
) -> Pin<Box<dyn Future<Output = Result<Value, Error>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_ARGUMENT_DEPTH {
            return Err(budget.error(budget.max_bytes.saturating_add(1)));
        }
        match value {
            Value::String(text) => {
                expand_string_async(text, view, state, cache, cancel, mask, remaining, budget, pointer)
                    .await
            }
            Value::Array(items) => {
                let mut expanded = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    let child_ptr = join(pointer, &index.to_string());
                    expanded.push(
                        visit_async(
                            item,
                            view,
                            state,
                            cache,
                            cancel,
                            mask,
                            remaining,
                            budget,
                            &child_ptr,
                            depth + 1,
                        )
                        .await?,
                    );
                }
                Ok(Value::Array(expanded))
            }
            Value::Object(fields) => {
                let mut expanded = Map::new();
                for (key, item) in fields {
                    let child_ptr = join(pointer, &escape_pointer(key));
                    expanded.insert(
                        key.clone(),
                        visit_async(
                            item,
                            view,
                            state,
                            cache,
                            cancel,
                            mask,
                            remaining,
                            budget,
                            &child_ptr,
                            depth + 1,
                        )
                        .await?,
                    );
                }
                Ok(Value::Object(expanded))
            }
            _ => Ok(value.clone()),
        }
    })
}

fn expand_string_sync(
    text: &str,
    view: &InjectionView,
    mask: &mut LiteralMask,
    remaining: &mut usize,
    budget: Budget,
    pointer: &str,
) -> Result<Value, Error> {
    let found = placeholders(text, false)?;
    if found.is_empty() {
        charge(remaining, text.len(), budget)?;
        return Ok(Value::String(text.to_owned()));
    }
    if found.len() == 1 && found[0].0 == 0 && found[0].1.end == text.len() {
        let placeholder = &found[0].1;
        let value = converted_var_value(view, placeholder)?;
        mask.insert_node(pointer);
        charge(remaining, serde_json::to_vec(&value)?.len(), budget)?;
        return Ok(value);
    }
    let mut result = String::new();
    let mut consumed = 0;
    for (start, placeholder) in found {
        if !placeholder.conversion.embeddable() {
            return Err(Error::tool_dynamic(format!(
                "placeholder {} must be the entire value or use an explicit string, number, or boolean conversion",
                display_ref(&placeholder)
            )));
        }
        append(&mut result, &text[consumed..start], budget)?;
        let converted = converted_var_value(view, &placeholder)?;
        let inserted = embed_text(&converted, &placeholder)?;
        let range = (result.len(), result.len() + inserted.len());
        append(&mut result, &inserted, budget)?;
        mask.insert_range(pointer, range);
        consumed = start + placeholder.end;
    }
    append(&mut result, &text[consumed..], budget)?;
    charge(remaining, result.len(), budget)?;
    Ok(Value::String(result))
}

#[expect(clippy::too_many_arguments)]
async fn expand_string_async(
    text: &str,
    view: &InjectionView,
    state: &crate::State,
    cache: &mut HashMap<String, ReferenceContent>,
    cancel: &CancellationToken,
    mask: &mut LiteralMask,
    remaining: &mut usize,
    budget: Budget,
    pointer: &str,
) -> Result<Value, Error> {
    let found = placeholders(text, true)?;
    if found.is_empty() {
        charge(remaining, text.len(), budget)?;
        return Ok(Value::String(text.to_owned()));
    }
    if found.len() == 1 && found[0].0 == 0 && found[0].1.end == text.len() {
        let placeholder = &found[0].1;
        let value = match placeholder.kind {
            PlaceholderKind::Var { .. } => converted_var_value(view, placeholder)?,
            PlaceholderKind::Ref { id } => {
                converted_ref_value(state, cache, cancel, id, placeholder).await?
            }
        };
        mask.insert_node(pointer);
        charge(remaining, serde_json::to_vec(&value)?.len(), budget)?;
        return Ok(value);
    }
    let mut result = String::new();
    let mut consumed = 0;
    for (start, placeholder) in found {
        if !placeholder.conversion.embeddable() {
            return Err(Error::tool_dynamic(format!(
                "placeholder {} must be the entire value or use an explicit string, number, boolean, text, base64, or data_url conversion",
                display_ref(&placeholder)
            )));
        }
        append(&mut result, &text[consumed..start], budget)?;
        let converted = match placeholder.kind {
            PlaceholderKind::Var { .. } => converted_var_value(view, &placeholder)?,
            PlaceholderKind::Ref { id } => {
                converted_ref_value(state, cache, cancel, id, &placeholder).await?
            }
        };
        let inserted = embed_text(&converted, &placeholder)?;
        let range = (result.len(), result.len() + inserted.len());
        append(&mut result, &inserted, budget)?;
        mask.insert_range(pointer, range);
        consumed = start + placeholder.end;
    }
    append(&mut result, &text[consumed..], budget)?;
    charge(remaining, result.len(), budget)?;
    Ok(Value::String(result))
}

fn placeholders(text: &str, include_refs: bool) -> Result<Vec<(usize, Placeholder<'_>)>, Error> {
    let mut found = Vec::new();
    let mut offset = 0;
    while offset < text.len() {
        let next_var = text[offset..].find("{{var:").map(|rel| (offset + rel, true));
        let next_ref = include_refs
            .then(|| text[offset..].find("{{ref:").map(|rel| (offset + rel, false)))
            .flatten();
        let next = match (next_var, next_ref) {
            (Some(v), Some(r)) => Some(if v.0 <= r.0 { v } else { r }),
            (Some(v), None) => Some(v),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        };
        let Some((start, is_var)) = next else {
            break;
        };
        let placeholder = if is_var {
            parse_var_placeholder(&text[start..]).map_err(|_| invalid_placeholder())?
        } else {
            parse_ref_placeholder(&text[start..]).map_err(|_| invalid_ref_placeholder())?
        };
        let end = start + placeholder.end;
        found.push((start, placeholder));
        offset = end;
    }
    Ok(found)
}

fn parse_var_placeholder(text: &str) -> Result<Placeholder<'_>, ()> {
    let body = text.strip_prefix("{{var:").ok_or(())?;
    let window = body.len().min(MAX_PLACEHOLDER_BYTES);
    let end = body.as_bytes()[..window]
        .windows(2)
        .position(|pair| pair == b"}}")
        .ok_or(())?;
    let inner = &body[..end];
    let (conversion, rest) = var_conversion_prefix(inner);
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
        kind: PlaceholderKind::Var { key },
        pointer,
        conversion,
    })
}

fn parse_ref_placeholder(text: &str) -> Result<Placeholder<'_>, ()> {
    let body = text.strip_prefix("{{ref:").ok_or(())?;
    let window = body.len().min(MAX_PLACEHOLDER_BYTES);
    let end = body.as_bytes()[..window]
        .windows(2)
        .position(|pair| pair == b"}}")
        .ok_or(())?;
    let inner = &body[..end];
    let (conversion, rest) = ref_conversion_prefix(inner);
    let (id, pointer) = if let Some(idx) = rest.rfind(":/") {
        (&rest[..idx], &rest[idx + 1..])
    } else {
        (rest, "")
    };
    if id.is_empty()
        || id.len() > 256
        || id.bytes().any(|b| b.is_ascii_whitespace() || matches!(b, b'{' | b'}' | b'\0'))
    {
        return Err(());
    }
    if !pointer.is_empty() {
        if matches!(conversion, Conversion::Base64 | Conversion::DataUrl) {
            return Err(());
        }
        validate_pointer_syntax(pointer)?;
    }
    Ok(Placeholder {
        end: "{{ref:".len() + end + 2,
        kind: PlaceholderKind::Ref { id },
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

fn var_conversion_prefix(text: &str) -> (Conversion, &str) {
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

fn ref_conversion_prefix(text: &str) -> (Conversion, &str) {
    for (name, conversion) in [
        ("text:", Conversion::Text),
        ("base64:", Conversion::Base64),
        ("data_url:", Conversion::DataUrl),
        ("json:", Conversion::Json),
        ("string:", Conversion::String),
        ("number:", Conversion::Number),
        ("boolean:", Conversion::Boolean),
    ] {
        if let Some(rest) = text.strip_prefix(name)
            && !rest.is_empty()
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

fn converted_var_value(view: &InjectionView, placeholder: &Placeholder<'_>) -> Result<Value, Error> {
    let key = placeholder.key_or_id();
    let value = match view.lookup(key) {
        InjectionKey::Visible(value) => value,
        InjectionKey::Invisible => {
            return Err(Error::tool_dynamic(format!(
                "variable {} is not model-visible and cannot be injected",
                key
            )));
        }
        InjectionKey::Secret => {
            return Err(Error::tool_dynamic(format!(
                "variable {} is secret; use {{{{secret:{}}}}} and do not read its value",
                key, key
            )));
        }
        InjectionKey::Missing => {
            return Err(Error::tool_dynamic(format!("unknown variable {}", key)));
        }
    };
    let selected = select_pointer(value, placeholder)?;
    convert(selected, placeholder)
}

async fn converted_ref_value(
    state: &crate::State,
    cache: &mut HashMap<String, ReferenceContent>,
    cancel: &CancellationToken,
    id: &str,
    placeholder: &Placeholder<'_>,
) -> Result<Value, Error> {
    if !cache.contains_key(id) {
        let content = state.read_reference_async(id, cancel.clone()).await?;
        cache.insert(id.to_owned(), content);
    }
    let content = cache.get(id).expect("just inserted or already present");
    match placeholder.conversion {
        Conversion::Base64 => Ok(Value::String(STANDARD.encode(content.as_bytes()))),
        Conversion::DataUrl => Ok(Value::String(format!(
            "data:{};base64,{}",
            content.media_type,
            STANDARD.encode(content.as_bytes())
        ))),
        Conversion::Text => {
            let ReferencePayload::Text(text) = &content.payload else {
                return Err(Error::tool_dynamic(format!(
                    "reference {} is binary ({}); use {{{{ref:base64:{}}}}} or {{{{ref:data_url:{}}}}}",
                    id, content.media_type, id, id
                )));
            };
            if placeholder.pointer.is_empty() {
                Ok(Value::String(text.clone()))
            } else {
                let parsed: Value = serde_json::from_str(text).map_err(|_| {
                    Error::tool_dynamic(format!(
                        "reference {} must contain valid JSON when using a pointer",
                        id
                    ))
                })?;
                let selected = select_pointer(&parsed, placeholder)?;
                Ok(Value::String(string_form(selected)))
            }
        }
        Conversion::Json => {
            let ReferencePayload::Text(text) = &content.payload else {
                return Err(Error::tool_dynamic(format!(
                    "reference {} is binary ({}); json conversion requires text/JSON content",
                    id, content.media_type
                )));
            };
            let parsed: Value = serde_json::from_str(text).map_err(|_| {
                Error::tool_dynamic(format!("reference {} does not contain valid JSON", id))
            })?;
            Ok(select_pointer(&parsed, placeholder)?.clone())
        }
        Conversion::Native | Conversion::String | Conversion::Number | Conversion::Boolean => {
            let ReferencePayload::Text(text) = &content.payload else {
                return Err(Error::tool_dynamic(format!(
                    "reference {} is binary ({}); use {{{{ref:base64:{}}}}} or {{{{ref:data_url:{}}}}}",
                    id, content.media_type, id, id
                )));
            };
            if !placeholder.pointer.is_empty() || content.media_type == "application/json" {
                let parsed: Value = serde_json::from_str(text).map_err(|_| {
                    Error::tool_dynamic(format!("reference {} does not contain valid JSON", id))
                })?;
                let selected = select_pointer(&parsed, placeholder)?;
                convert(selected, placeholder)
            } else {
                convert(&Value::String(text.clone()), placeholder)
            }
        }
    }
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
        Conversion::Native | Conversion::Json => Ok(value.clone()),
        Conversion::String | Conversion::Text => Ok(Value::String(string_form(value))),
        Conversion::Number => number_value(value, placeholder).map(Value::Number),
        Conversion::Boolean => boolean_value(value, placeholder).map(Value::Bool),
        Conversion::Base64 | Conversion::DataUrl => Ok(value.clone()),
    }
}

fn embed_text(value: &Value, placeholder: &Placeholder<'_>) -> Result<String, Error> {
    match value {
        Value::String(text)
            if matches!(
                placeholder.conversion,
                Conversion::String | Conversion::Text | Conversion::Base64 | Conversion::DataUrl
            ) =>
        {
            Ok(text.clone())
        }
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

fn append(result: &mut String, text: &str, budget: Budget) -> Result<(), Error> {
    let Some(needed) = result.len().checked_add(text.len()) else {
        return Err(budget.error(usize::MAX));
    };
    if needed > budget.max_bytes {
        return Err(budget.error(needed));
    }
    result.push_str(text);
    Ok(())
}

fn charge(remaining: &mut usize, bytes: usize, budget: Budget) -> Result<(), Error> {
    if bytes > *remaining {
        return Err(budget.error(budget.max_bytes.saturating_sub(*remaining).saturating_add(bytes)));
    }
    *remaining -= bytes;
    Ok(())
}

fn display_ref(placeholder: &Placeholder<'_>) -> String {
    if placeholder.pointer.is_empty() {
        placeholder.key_or_id().to_owned()
    } else {
        format!("{}{}", placeholder.key_or_id(), placeholder.pointer)
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
        placeholder.pointer,
        placeholder.key_or_id()
    ))
}

fn type_error(placeholder: &Placeholder<'_>, actual: &str) -> Error {
    Error::tool_dynamic(format!(
        "path {} in {} expects an object or array, found {}",
        placeholder.pointer,
        placeholder.key_or_id(),
        actual
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

fn invalid_ref_placeholder() -> Error {
    Error::tool(
        "reference placeholder is unclosed or invalid; use {{ref:ID}}, {{ref:ID:/pointer}}, {{ref:text:ID}}, {{ref:base64:ID}}, {{ref:data_url:ID}}, {{ref:json:ID}}, or {{ref:string|number|boolean:ID:/pointer}}",
    )
}

fn budget_error() -> Error {
    Error::tool("resolved variable arguments exceed the invocation argument byte or depth budget")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;
    use crate::state::reference::{CreateLibraryObject, LocalBlobOrigin};
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

    #[tokio::test]
    async fn ref_injection_supports_conversions_and_16mb_limit_error() {
        let state = crate::State::new(Limits::default());
        let cancel = CancellationToken::new();
        let library = state.reference_library();
        let json_ref = library
            .add(
                CreateLibraryObject::LocalBlob {
                    id: None,
                    title: "cfg".into(),
                    media_type: "application/json".into(),
                    data: br#"{"name":"ada","count":3,"secret":"{{secret:tok}}"}"#.to_vec(),
                    origin: LocalBlobOrigin::Upload,
                    origin_execution_id: None,
                },
                cancel.clone(),
            )
            .await
            .unwrap();
        let bin_ref = library
            .add(
                CreateLibraryObject::LocalBlob {
                    id: None,
                    title: "blob".into(),
                    media_type: "application/octet-stream".into(),
                    data: vec![1, 2, 3],
                    origin: LocalBlobOrigin::Upload,
                    origin_execution_id: None,
                },
                cancel.clone(),
            )
            .await
            .unwrap();
        let input = serde_json::json!({
            "cfg": format!("{{{{ref:{}}}}}", json_ref.id),
            "name": format!("{{{{ref:{}:/name}}}}", json_ref.id),
            "count": format!("pre {{{{ref:number:{}:/count}}}}", json_ref.id),
            "b64": format!("{{{{ref:base64:{}}}}}", bin_ref.id),
            "url": format!("pre {{{{ref:data_url:{}}}}}", bin_ref.id),
        });
        let (out, mask) = expand_dispatch(&input, &state, 64 * 1024 * 1024, &cancel)
            .await
            .unwrap();
        assert_eq!(out["cfg"]["count"], 3);
        assert_eq!(out["name"], "ada");
        assert_eq!(out["count"], "pre 3");
        assert_eq!(out["b64"], "AQID");
        assert_eq!(out["url"], "pre data:application/octet-stream;base64,AQID");
        assert!(mask.covers("/cfg/secret"));

        let huge_ref = library
            .add(
                CreateLibraryObject::LocalBlob {
                    id: None,
                    title: "huge".into(),
                    media_type: "application/octet-stream".into(),
                    data: vec![7; 13 * 1024 * 1024],
                    origin: LocalBlobOrigin::Upload,
                    origin_execution_id: None,
                },
                cancel.clone(),
            )
            .await
            .unwrap();
        let err = expand_dispatch(
            &serde_json::json!({"payload": format!("{{{{ref:base64:{}}}}}", huge_ref.id)}),
            &state,
            64 * 1024 * 1024,
            &cancel,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            Error::Limit(limit) if limit.kind == LimitKind::ToolArguments
        ));
    }
}
