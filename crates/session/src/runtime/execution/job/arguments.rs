use crate::Error;
use serde_json::{Value, json};
use std::borrow::Cow;
mod encoded;

pub(super) const MAX_IDS: usize = 16;
pub(crate) struct Arguments<'a> {
    pub(super) ids: Vec<Cow<'a, str>>,
    pub(super) batch: bool,
    pub(super) action: &'static str,
    pub(super) args: Value,
    pub(super) redirect: Option<Redirect<'a>>,
}

pub(super) struct Redirect<'a> {
    pub(super) channel: &'a str,
    pub(super) expected_mime: &'a str,
    pub(super) title: Option<&'a str>,
}

pub(crate) fn is_redirect(arguments: &Value) -> bool {
    arguments.get("action").and_then(Value::as_str) == Some(super::REDIRECT)
}

pub(crate) fn canonical(action: &str) -> Option<&'static str> {
    Some(match action {
        "status" => "status",
        "read" => "read",
        "wait" => "wait",
        "write" => "write",
        "cancel" => "cancel",
        "release" => "release",
        super::REDIRECT => super::REDIRECT,
        _ => return None,
    })
}
/// Validate syntax without interpreting the opaque Session namespace or granting
/// access to it. Ownership is checked separately after the full handle is valid.
pub(super) fn parse_handle(id: &str) -> Result<(&str, u64), Error> {
    let parsed = id.split_once('/').and_then(|(session, number)| {
        if session.is_empty()
            || number.is_empty()
            || !number.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        number
            .parse::<u64>()
            .ok()
            .filter(|number| *number > 0)
            .map(|number| (session, number))
    });
    parsed.ok_or_else(invalid_handle)
}
fn invalid_handle() -> Error {
    use crate::observation::diagnostics::{
        DiagnosticCode, DiagnosticLayer, DiagnosticOperation, DiagnosticPhase, EffectKnowledge,
        ToolDiagnostic,
    };
    let mut diagnostic = ToolDiagnostic::new(
        DiagnosticCode::InvalidArgument,
        DiagnosticLayer::Runtime,
        DiagnosticPhase::Validation,
    );
    diagnostic.operation = Some(DiagnosticOperation::Job);
    diagnostic.effects = EffectKnowledge::NotStarted;
    Error::Diagnostic(diagnostic)
}
pub(crate) fn validate(arguments: &Value) -> Result<Arguments<'_>, Error> {
    let (ids, batch) = match arguments.get("id") {
        Some(Value::String(id)) if !id.is_empty() => {
            // Preserve every valid opaque scalar namespace. Decode only a single
            // array layer; member validation below never recursively coerces IDs.
            if parse_handle(id).is_ok() {
                (vec![Cow::Borrowed(id.as_str())], false)
            } else if id.trim_start().starts_with('[') {
                let ids = encoded::parse(id).map_err(|_| invalid_handle())?;
                (ids.into_iter().map(Cow::Owned).collect(), true)
            } else {
                return Err(invalid_handle());
            }
        }
        Some(Value::Array(ids)) if !ids.is_empty() && ids.len() <= MAX_IDS => {
            let ids = ids
                .iter()
                .map(|id| {
                    id.as_str()
                        .filter(|id| !id.is_empty())
                        .map(Cow::Borrowed)
                        .ok_or_else(|| Error::tool("job requires exact execution ids"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            (ids, true)
        }
        _ => {
            return Err(Error::tool("job requires an execution id or 1–16 ids"));
        }
    };
    for id in &ids {
        parse_handle(id)?;
    }
    let action = arguments
        .get("action")
        .and_then(Value::as_str)
        .and_then(canonical)
        .ok_or_else(|| Error::tool("job requires supported action"))?;
    let redirect = if action == super::REDIRECT {
        if batch {
            return Err(Error::tool(
                "job redirect requires one scalar execution handle",
            ));
        }
        let (session, number) = parse_handle(ids[0].as_ref())?;
        if ids[0].as_ref() != format!("{session}/{number}") {
            return Err(invalid_handle());
        }
        let object = arguments
            .get("args")
            .and_then(Value::as_object)
            .ok_or_else(|| Error::tool("job redirect requires channel and expected_mime"))?;
        if object.len() < 2 || object.len() > 3 {
            return Err(Error::tool(
                "job redirect accepts only channel, expected_mime, and optional title",
            ));
        }
        let channel = object
            .get("channel")
            .and_then(Value::as_str)
            .filter(|channel| *channel == "stdout")
            .ok_or_else(|| Error::tool("job redirect requires the exact stdout channel"))?;
        let expected_mime = object
            .get("expected_mime")
            .and_then(Value::as_str)
            .filter(|mime| microcrab_agent_types::is_capture_mime(mime))
            .ok_or_else(|| Error::tool("job redirect requires a canonical concrete MIME type"))?;
        let title = match object.get("title") {
            Some(raw) => {
                let text = raw
                    .as_str()
                    .map(str::trim)
                    .filter(|s| !s.is_empty() && s.chars().count() <= 256 && !s.contains('\0'))
                    .ok_or_else(|| {
                        Error::tool("job redirect title must be 1..=256 non-NUL characters")
                    })?;
                Some(text)
            }
            None => None,
        };
        Some(Redirect {
            channel,
            expected_mime,
            title,
        })
    } else {
        None
    };
    let args = arguments.get("args").cloned().unwrap_or_else(|| json!({}));
    let object = args
        .as_object()
        .ok_or_else(|| Error::tool("job args must be an object"))?;
    let allowed: &[&str] = match action {
        "read" => &["cursor", "max_bytes"],
        "wait" => &["revision"],
        "write" => &["data", "eof"],
        super::REDIRECT => &["channel", "expected_mime", "title"],
        _ => &[],
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(Error::tool("unsupported job args"));
    }
    for key in ["cursor", "revision"] {
        if args.get(key).is_some_and(|value| value.as_u64().is_none()) {
            return Err(Error::tool(match key {
                "cursor" => "job cursor must be unsigned integer",
                _ => "job revision must be unsigned integer",
            }));
        }
    }
    if args.get("max_bytes").is_some_and(|value| {
        !value.as_u64().is_some_and(|bytes| {
            (1..=crate::runtime::execution::buffer::MAX_PAGE_BYTES as u64).contains(&bytes)
        })
    }) {
        return Err(Error::tool(
            "job max_bytes must be an integer between 1 and 16777216",
        ));
    }
    if action == "write" && !args.get("data").is_some_and(Value::is_string) {
        return Err(Error::tool("job write requires data"));
    }
    if args.get("eof").is_some_and(|value| !value.is_boolean()) {
        return Err(Error::tool("job eof must be boolean"));
    }
    Ok(Arguments {
        ids,
        batch,
        action,
        args,
        redirect,
    })
}

#[cfg(test)]
mod tests;
