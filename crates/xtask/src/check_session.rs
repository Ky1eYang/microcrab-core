//! Read-only Session ownership checks, deliberately independent of Cargo metadata.
//! This is a lexical architecture guard, not a Rust resolver or behavior verifier.
use std::{error::Error, fs, path::Path};

// session-direct-imports.md authorizes deleting retired compatibility modules.
// Root entries assemble real owners; they must not restore forwarding files.
const ROOT_MODULES: &[&str] = &[
    "admission",
    "budget",
    "observation",
    "policy",
    "boundary",
    "context",
    "error",
    "inner",
    "runtime",
    "session",
    "state",
    "utils",
];
const RETIRED_MODULES: &[&str] = &[
    "activity.rs",
    "attachment.rs",
    "diagnostics.rs",
    "hooks.rs",
    "inbox.rs",
    "input.rs",
    "limit.rs",
    "round_limit.rs",
    "types.rs",
    "context/audit.rs",
    "context/client.rs",
    "context/content.rs",
    "runtime/permission.rs",
    "runtime/processes.rs",
    "runtime/tool/job.rs",
    "runtime/wasm/evidence.rs",
    "state/limits.rs",
    "state/command.rs",
    "context/template.rs",
];
const GUARD_OWNER: &str = "utils/cancel.rs";
const DEFINITIONS: &[(&str, &str)] = &[
    ("Boundary", "boundary.rs"),
    ("VariableCommand", "state/endpoint.rs"),
    ("EvidenceBoundary", "observation/wasm.rs"),
    ("Limits", "budget/limits.rs"),
    ("LimitKind", "budget/violation.rs"),
    ("LimitError", "budget/violation.rs"),
    ("Exhaustion", "budget/continuation.rs"),
    ("RoundLimitRequest", "budget/continuation.rs"),
    ("RoundLimitDecision", "budget/continuation.rs"),
    ("RoundLimitCallback", "budget/continuation.rs"),
    ("SessionActivity", "observation/activity.rs"),
    ("CompletedTurn", "observation/activity.rs"),
    ("ToolDiagnostic", "observation/diagnostics.rs"),
    ("BoundAuditor", "observation/hooks.rs"),
    ("RecordedCachePoint", "runtime/journal/types.rs"),
    ("RoundCacheReport", "runtime/journal/types.rs"),
    ("ProcessRegistry", "runtime/execution/process/registry.rs"),
];

fn lower_owner(path: &str) -> bool {
    let owner = path
        .split('/')
        .next()
        .unwrap_or(path)
        .trim_end_matches(".rs");
    matches!(
        owner,
        "runtime" | "admission" | "budget" | "observation" | "policy"
    )
}

pub(crate) fn help() {
    println!(
        "cargo xtask check-session\n\nRead-only Session owner/module checks; no builds or source changes.\nNew root owners require a recorded design decision and an explicit baseline update."
    );
}

pub(crate) fn run(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    if let Some(arg) = args.next() {
        if matches!(arg.as_str(), "--help" | "-h") && args.next().is_none() {
            help();
            return Ok(());
        }
        return Err(format!(
            "unknown check-session argument {arg:?}; use cargo xtask check-session --help"
        )
        .into());
    }
    let root = crate::workspace::root()?.join("crates/session/src");
    let mut sources = Vec::new();
    collect(&root, &root, &mut sources)?;
    sources.sort_by(|left, right| left.0.cmp(&right.0));
    let errors = check(&sources);
    if !errors.is_empty() {
        return Err(format!("Session architecture check failed:\n{}", errors.join("\n")).into());
    }
    println!(
        "Session architecture check passed ({} Rust source files).",
        sources.len()
    );
    Ok(())
}

fn collect(
    root: &Path,
    directory: &Path,
    sources: &mut Vec<(String, String)>,
) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = entry.path();
        // Never follow links out of the bounded Session source tree.
        if kind.is_symlink() {
            return Err(format!(
                "{}: source links are unsupported; use ordinary source files",
                path.display()
            )
            .into());
        }
        if kind.is_dir() {
            collect(root, &path, sources)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push((
                path.strip_prefix(root)?
                    .to_string_lossy()
                    .replace('\\', "/"),
                fs::read_to_string(&path)?,
            ));
        }
    }
    Ok(())
}

fn test_source(path: &str) -> bool {
    path.split('/').any(|part| part == "tests")
        || path.ends_with("/tests.rs")
        || path.ends_with("_tests.rs")
}

fn check(sources: &[(String, String)]) -> Vec<String> {
    let mut errors = Vec::new();
    let mut guards = Vec::new();
    for (path, source) in sources {
        if RETIRED_MODULES.contains(&path.as_str()) {
            errors.push(format!(
                "{path}: retired module; import the canonical owner or keep private definitions beside their consumer"
            ));
        }
        let owner = path
            .split('/')
            .next()
            .unwrap_or(path)
            .trim_end_matches(".rs");
        if owner != "lib" && !ROOT_MODULES.contains(&owner) {
            errors.push(format!("{path}: unrecorded root owner {owner:?}; deepen an existing owner or record the design decision and update ROOT_MODULES in xtask/check_session.rs"));
        }
        let tokens = tokens(source);
        let mut depth = 0_usize;
        for (index, token) in tokens.iter().enumerate() {
            let following = tokens.get(index + 1).map(String::as_str);
            if path == "lib.rs"
                && depth == 0
                && token == "mod"
                && let Some(name) = following
                && !ROOT_MODULES.contains(&name)
            {
                errors.push(format!("{path}: unrecorded root module {name:?}; record a design decision before updating ROOT_MODULES"));
            }
            if matches!(token.as_str(), "struct" | "enum" | "type")
                && following == Some("CancelOnDrop")
            {
                guards.push(path.as_str());
            }
            if !test_source(path)
                && matches!(token.as_str(), "struct" | "enum" | "trait" | "type")
                && let Some(name) = following
                && let Some((_, canonical)) = DEFINITIONS
                    .iter()
                    .find(|(definition, _)| *definition == name)
                && path != canonical
            {
                errors.push(format!("{path}: {name} definition belongs to {canonical}; compatibility paths may re-export, not redefine it"));
            }
            if token == "use" && lower_owner(path) {
                let end = tokens[index..]
                    .iter()
                    .position(|part| part == ";")
                    .map_or(tokens.len(), |end| index + end);
                for imported in use_paths(&tokens[index + 1..end]) {
                    if points_to_inner(path, &imported) {
                        errors.push(format!("{path}: runtime imports Session inner; move the shared contract to its existing lower-level owner"));
                    }
                }
            }
            if lower_owner(path)
                && matches!(token.as_str(), "crate" | "super" | "self")
                && (index == 0 || tokens[index - 1] != "::")
            {
                let mut end = index + 1;
                while tokens.get(end).is_some_and(|part| part == "::")
                    && tokens.get(end + 1).is_some()
                {
                    end += 2;
                }
                let qualified: Vec<_> = tokens[index..end]
                    .iter()
                    .filter(|part| part.as_str() != "::")
                    .cloned()
                    .collect();
                if points_to_inner(path, &qualified) {
                    errors.push(format!("{path}: runtime references Session inner; keep Session orchestration above runtime"));
                }
            }
            if !test_source(path) && token == "." {
                let forbidden = if path == "runtime/processes.rs"
                    || path.starts_with("runtime/processes/")
                    || path == "runtime/execution/process.rs"
                    || path.starts_with("runtime/execution/process/")
                {
                    Some("max_execution_buffers")
                } else if path == "runtime/execution.rs" || path.starts_with("runtime/execution/") {
                    Some("max_processes")
                } else {
                    None
                };
                if forbidden.is_some() && following == forbidden {
                    errors.push(format!("{path}: {} belongs to the independent peer owner; preserve separate process and execution admission capacities", forbidden.unwrap_or_default()));
                }
            }
            if !test_source(path)
                && matches!(token.as_str(), "Error" | "Self")
                && tokens.get(index + 1).is_some_and(|token| token == "::")
                && tokens.get(index + 2).is_some_and(|token| token == "Limit")
                && tokens.get(index + 3).is_some_and(|token| token == "(")
                && tokens
                    .get(index + 4)
                    .is_some_and(|token| matches!(token.as_str(), "$literal" | "format"))
            {
                errors.push(format!("{path}: construct typed limit facts through budget::check/reject, not a local string limit"));
            }
            match token.as_str() {
                "{" => depth += 1,
                "}" => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    if guards != [GUARD_OWNER] {
        errors.push(format!("CancelOnDrop must have exactly one definition at {GUARD_OWNER}; found {guards:?}. Reuse crate::utils::cancel::CancelOnDrop."));
    }
    errors.sort();
    errors.dedup();
    errors
}

// Expand grouped imports so `use crate::{inner::Thing, runtime::Other}` has the
// same ownership meaning as a direct path. Aliases do not hide the source owner.
fn use_paths(tokens: &[String]) -> Vec<Vec<String>> {
    fn walk(
        tokens: &[String],
        cursor: &mut usize,
        prefix: &[String],
        output: &mut Vec<Vec<String>>,
    ) {
        let mut path = prefix.to_vec();
        while let Some(token) = tokens.get(*cursor) {
            *cursor += 1;
            match token.as_str() {
                "::" => {}
                "{" => {
                    while *cursor < tokens.len() && tokens[*cursor] != "}" {
                        walk(tokens, cursor, &path, output);
                    }
                    *cursor += usize::from(*cursor < tokens.len());
                }
                "," | "}" => {
                    if token == "}" {
                        *cursor -= 1;
                    }
                    break;
                }
                "as" => {
                    *cursor += usize::from(*cursor < tokens.len());
                }
                _ => path.push(token.clone()),
            }
        }
        if !path.is_empty() {
            output.push(path);
        }
    }
    let mut output = Vec::new();
    walk(tokens, &mut 0, &[], &mut output);
    output
}

fn points_to_inner(file: &str, path: &[String]) -> bool {
    let mut base: Vec<&str> = if file == "runtime.rs" {
        vec!["runtime"]
    } else {
        file.trim_end_matches(".rs").split('/').collect()
    };
    let mut parts = path.iter().map(String::as_str).peekable();
    match parts.peek().copied() {
        Some("crate") => {
            base.clear();
            parts.next();
        }
        Some("super" | "self") => {}
        _ => return false,
    }
    for part in parts {
        match part {
            "super" => {
                base.pop();
            }
            "self" => {}
            _ => base.push(part),
        }
    }
    base.first() == Some(&"inner")
}

// Keep identifiers/punctuation and opaque literal markers, discard literal contents
// and comments. Rust lifetimes remain tokens; raw strings cannot create edges.
fn tokens(source: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut result = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let rest = &bytes[at..];
        if rest.starts_with(b"//") {
            at += rest
                .iter()
                .position(|byte| *byte == b'\n')
                .unwrap_or(rest.len());
        } else if rest.starts_with(b"/*") {
            at += 2;
            let mut depth = 1;
            while at < bytes.len() && depth > 0 {
                if bytes[at..].starts_with(b"/*") {
                    depth += 1;
                    at += 2;
                } else if bytes[at..].starts_with(b"*/") {
                    depth -= 1;
                    at += 2;
                } else {
                    at += 1;
                }
            }
        } else if (rest.starts_with(b"br") || rest.starts_with(b"cr"))
            && rest.get(2).is_some_and(|byte| matches!(byte, b'#' | b'"'))
        {
            // Leave the r-prefix to the raw-string branch on the next iteration.
            at += 1;
        } else if rest.starts_with(b"r#")
            && rest
                .get(2)
                .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
        {
            at += 2; // Normalize raw identifiers such as r#inner.
        } else if bytes[at] == b'r' && {
            let mut end = at + 1;
            while bytes.get(end) == Some(&b'#') {
                end += 1;
            }
            bytes.get(end) == Some(&b'"')
        } {
            let mut start = at + 1;
            while bytes.get(start) == Some(&b'#') {
                start += 1;
            }
            let hashes = start - at - 1;
            result.push("$literal".to_owned());
            at = start + 1;
            while at < bytes.len() {
                if bytes[at] == b'"'
                    && bytes
                        .get(at + 1..at + 1 + hashes)
                        .is_some_and(|tail| tail.iter().all(|byte| *byte == b'#'))
                {
                    at += 1 + hashes;
                    break;
                }
                at += 1;
            }
        } else if bytes[at] == b'"'
            || (bytes[at] == b'\'' && {
                // A lifetime has no closing quote; a char contains one scalar/escape.
                let tail = &source[at + 1..];
                tail.chars().next().is_some_and(|character| {
                    bytes.get(at + 1 + character.len_utf8()) == Some(&b'\'')
                }) || bytes.get(at + 1) == Some(&b'\\')
            })
        {
            let quote = bytes[at];
            result.push("$literal".to_owned());
            at += 1;
            while at < bytes.len() {
                if bytes[at] == b'\\' {
                    at = (at + 2).min(bytes.len());
                } else if bytes[at] == quote {
                    at += 1;
                    break;
                } else {
                    at += 1;
                }
            }
        } else if bytes[at].is_ascii_alphabetic() || bytes[at] == b'_' {
            let start = at;
            at += 1;
            while at < bytes.len() && (bytes[at].is_ascii_alphanumeric() || bytes[at] == b'_') {
                at += 1;
            }
            result.push(source[start..at].to_owned());
        } else if rest.starts_with(b"::") {
            result.push("::".to_owned());
            at += 2;
        } else {
            if !bytes[at].is_ascii_whitespace() {
                result.push(char::from(bytes[at]).to_string());
            }
            at += 1;
        }
    }
    result
}

#[cfg(test)]
mod tests;
