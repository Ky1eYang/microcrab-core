use crate::host::*;
use microcrab_agent_types::HostInfo;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::json;

const MAX_BYTES: u64 = 1_048_576;
const MAX_ARGUMENTS: usize = 4_300_000;
pub(crate) fn invalid(message: &str) -> String {
    encode(&OperationError {
        code: OperationErrorCode::InvalidRequest,
        message: message.into(),
    })
    .unwrap_or_else(|_| message.into())
}
pub(crate) fn encode(value: &impl serde::Serialize) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| e.to_string())
}
pub(crate) fn operation<T>(result: Result<T, OperationError>) -> Result<T, String> {
    result.map_err(|e| encode(&e).unwrap_or_else(|_| "Host operation failed".into()))
}
pub(crate) fn parse<T: DeserializeOwned>(args: &str) -> Result<T, String> {
    if args.len() > MAX_ARGUMENTS {
        return Err(invalid("argument byte limit exceeded"));
    }
    serde_json::from_str(args)
        .map_err(|error| invalid(&microcrab_protocol::diagnostic::json_reason(&error)))
}
pub(crate) fn text(value: &str, max: usize, nonempty: bool) -> Result<(), String> {
    if value.len() > max || (nonempty && value.is_empty()) || value.contains('\0') {
        let reason = if value.len() > max {
            format!("string exceeds maximum {max} bytes")
        } else if value.contains('\0') {
            "string contains NUL".into()
        } else {
            "string must not be empty".into()
        };
        return Err(invalid(&reason));
    }
    Ok(())
}
pub(crate) fn range(value: u64, min: u64, max: u64) -> Result<(), String> {
    if !(min..=max).contains(&value) {
        return Err(invalid(&format!(
            "numeric argument must be between {min} and {max}"
        )));
    }
    Ok(())
}
pub(crate) fn hash(value: &str) -> Result<(), String> {
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(invalid("expected_version must be a SHA-256 hex digest"));
    }
    Ok(())
}
pub(crate) fn one() -> u64 {
    1
}
pub(crate) fn lines() -> u64 {
    200
}
pub(crate) fn bytes() -> u64 {
    65536
}
pub(crate) fn results() -> u64 {
    100
}
pub(crate) fn yes() -> bool {
    true
}
pub(crate) fn yield_ms() -> u64 {
    1000
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    path: String,
    #[serde(default = "one")]
    offset_line: u64,
    #[serde(default = "lines")]
    max_lines: u64,
    #[serde(default = "bytes")]
    max_bytes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArgs {
    path: String,
    content: String,
    #[serde(default)]
    expected_version: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditArgs {
    path: String,
    expected_version: String,
    old_string: String,
    new_string: String,
    #[serde(default)]
    replace_all: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Replacement {
    old_string: String,
    new_string: String,
    #[serde(default)]
    replace_all: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchArgs {
    path: String,
    expected_version: String,
    replacements: Vec<Replacement>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobArgs {
    root: String,
    pattern: String,
    #[serde(default)]
    include_hidden: bool,
    #[serde(default)]
    include_ignored: bool,
    #[serde(default)]
    include_directories: bool,
    #[serde(default = "results")]
    max_results: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrepArgs {
    root: String,
    pattern: String,
    #[serde(default)]
    include_glob: Option<String>,
    #[serde(default = "yes")]
    case_sensitive: bool,
    #[serde(default)]
    literal: bool,
    #[serde(default = "results")]
    max_matches: u64,
    #[serde(default = "bytes")]
    max_bytes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ShellArgs {
    command: String,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default = "yield_ms")]
    yield_ms: u64,
}

/// Validate fresh calls independently of discovery. Host availability is never a grant.
pub fn invoke<H: Host>(
    info: &HostInfo,
    name: &str,
    arguments: &str,
    host: &mut H,
) -> Result<InvocationOutput, String> {
    // The catalog is also the authority for available tools, not for permission decisions.
    if !crate::describe(info)?
        .iter()
        .flat_map(|a| &a.tools)
        .any(|t| t.name == name)
    {
        return Err(invalid("unknown or unavailable tool on this Host"));
    }
    // Option fields must reject explicit null just as their advertised schemas do.
    let value: serde_json::Value = parse(arguments)?;
    let object = value
        .as_object()
        .ok_or_else(|| invalid("arguments must be an object"))?;
    if object.values().any(serde_json::Value::is_null) {
        return Err(invalid("null arguments are not supported"));
    }
    let content = match name {
        "read" => {
            let a: ReadArgs = parse(arguments)?;
            text(&a.path, 4096, true)?;
            range(a.offset_line, 1, u64::MAX)?;
            range(a.max_lines, 1, 10000)?;
            range(a.max_bytes, 1, MAX_BYTES)?;
            let output = operation(host.read(ReadRequest {
                path: a.path,
                offset_line: a.offset_line,
                max_lines: a.max_lines,
                max_bytes: a.max_bytes,
            }))?;
            let numbered = output
                .content
                .split_inclusive('\n')
                .enumerate()
                .map(|(i, line)| {
                    format!("{}\t{}", output.start_line.saturating_add(i as u64), line)
                })
                .collect::<String>();
            encode(
                &json!({"content":numbered,"start_line":output.start_line,"next_line":output.next_line,"truncated":output.truncated,"version":output.version}),
            )
        }
        "write" => {
            let a: WriteArgs = parse(arguments)?;
            text(&a.path, 4096, true)?;
            text(&a.content, MAX_BYTES as usize, false)?;
            let precondition = match a.expected_version {
                Some(v) => {
                    hash(&v)?;
                    WritePrecondition::Version(v.to_ascii_lowercase())
                }
                None => WritePrecondition::Missing,
            };
            encode(&controlled_write(
                host,
                WriteRequest {
                    path: a.path,
                    content: a.content,
                    precondition,
                },
            )?)
        }
        "edit" => {
            let a: EditArgs = parse(arguments)?;
            replace(
                host,
                a.path,
                a.expected_version,
                vec![Replacement {
                    old_string: a.old_string,
                    new_string: a.new_string,
                    replace_all: a.replace_all,
                }],
            )
        }
        "patch" => {
            let a: PatchArgs = parse(arguments)?;
            replace(host, a.path, a.expected_version, a.replacements)
        }
        "apply_patch" => crate::apply_patch::invoke(arguments, host),
        "glob" => {
            let a: GlobArgs = parse(arguments)?;
            text(&a.root, 4096, true)?;
            text(&a.pattern, 1024, true)?;
            range(a.max_results, 1, 4096)?;
            let output = operation(host.glob(GlobRequest {
                root: a.root,
                pattern: a.pattern,
                include_hidden: a.include_hidden,
                include_ignored: a.include_ignored,
                include_directories: a.include_directories,
                max_results: a.max_results,
            }))?;
            // Host entries retain per-node facts. Model presentation groups kind
            // once, preserving each kind's Host order and every path verbatim.
            let mut groups = Vec::new();
            for kind in [
                GlobEntryKind::File,
                GlobEntryKind::Directory,
                GlobEntryKind::Symlink,
            ] {
                let paths = output
                    .entries
                    .iter()
                    .filter(|entry| entry.kind == kind)
                    .map(|entry| &entry.path)
                    .collect::<Vec<_>>();
                if !paths.is_empty() {
                    groups.push(json!({"kind":kind,"path":paths}));
                }
            }
            encode(&json!({"groups":groups,"truncated":output.truncated}))
        }
        "grep" => {
            let a: GrepArgs = parse(arguments)?;
            text(&a.root, 4096, true)?;
            text(&a.pattern, 1024, true)?;
            if let Some(g) = &a.include_glob {
                text(g, 1024, true)?;
            }
            range(a.max_matches, 1, 4096)?;
            range(a.max_bytes, 1, MAX_BYTES)?;
            let output = operation(host.grep(GrepRequest {
                root: a.root,
                pattern: a.pattern,
                include_glob: a.include_glob,
                case_sensitive: a.case_sensitive,
                literal: a.literal,
                max_matches: a.max_matches,
                max_bytes: a.max_bytes,
            }))?;
            let rendered = output
                .matches
                .iter()
                .map(|m| format!("{}:{}:{}", m.path, m.line, m.text))
                .collect::<Vec<_>>()
                .join("\n");
            encode(
                &json!({"content":rendered,"matches":output.matches,"truncated":output.truncated}),
            )
        }
        name if crate::catalog::shell_name(info) == Some(name) => {
            let a: ShellArgs = parse(arguments)?;
            text(&a.command, 65536, true)?;
            if let Some(cwd) = &a.cwd {
                text(cwd, 4096, true)?;
            }
            range(a.yield_ms, 0, 60000)?;
            let handle = operation(host.shell_launch(ShellRequest {
                command: a.command,
                cwd: a.cwd,
            }))?;
            let state = operation(host.process_wait_for_state(handle, a.yield_ms))?;
            // Initial output is a nonconsuming observation. Keep the same handle even
            // after exit so explicit Session job reads can recover oversized output.
            let output = operation(host.shell_read(handle, 65536))?;
            let (state_name, running) = match &state {
                ProcessState::Running => ("running", true),
                ProcessState::Exited { .. } => ("exited", false),
                ProcessState::Failed(_) => ("failed", false),
            };
            let mut value = json!({
                "job_id": handle.id, "pid": handle.pid, "state": state_name,
                "run_in_background": running, "stdout": output.stdout,
                "stderr": output.stderr, "truncated": output.truncated,
            });
            match state {
                ProcessState::Exited { code: Some(code) } => value["exit_code"] = json!(code),
                ProcessState::Failed(reason) => value["failure"] = json!(reason),
                _ => {}
            }
            encode(&value)
        }
        _ => Err(invalid("unknown tool")),
    }?;
    Ok(InvocationOutput { content })
}

fn replace<H: Host>(
    host: &mut H,
    path: String,
    expected: String,
    replacements: Vec<Replacement>,
) -> Result<String, String> {
    text(&path, 4096, true)?;
    hash(&expected)?;
    if replacements.is_empty() || replacements.len() > 64 {
        return Err(invalid("patch requires 1..=64 replacements"));
    }
    for r in &replacements {
        text(&r.old_string, MAX_BYTES as usize, true)?;
        text(&r.new_string, MAX_BYTES as usize, false)?;
    }
    let original = operation(host.read(ReadRequest {
        path: path.clone(),
        offset_line: 1,
        max_lines: 10000,
        max_bytes: MAX_BYTES,
    }))?;
    if original.truncated || original.next_line.is_some() || original.start_line != 1 {
        return Err(invalid(
            "edit requires a complete bounded read; file exceeds edit limits",
        ));
    }
    if !original.version.eq_ignore_ascii_case(&expected) {
        return Err(encode(&OperationError {
            code: OperationErrorCode::Conflict,
            message: "file version differs from expected_version".into(),
        })?);
    }
    let mut content = original.content.clone();
    let mut counts = Vec::with_capacity(replacements.len());
    let (mut lines_added, mut lines_removed) = (0, 0);
    for (index, r) in replacements.into_iter().enumerate() {
        let (next, count) = literal_replace(&content, &r.old_string, &r.new_string, r.replace_all)
            .map_err(|reason| invalid(&format!("replacement step {}: {reason}", index + 1)))?;
        content = next;
        counts.push(count);
        if r.old_string != r.new_string {
            lines_added += r.new_string.lines().count() * count;
            lines_removed += r.old_string.lines().count() * count;
        }
        if content.len() > MAX_BYTES as usize {
            return Err(invalid(&format!(
                "replacement step {} exceeds the file byte limit",
                index + 1
            )));
        }
    }
    if content == original.content {
        return encode(
            &json!({"version":original.version,"bytes":content.len(),"changed":false,"replacement_counts":counts}),
        );
    }
    let output = controlled_write(
        host,
        WriteRequest {
            path,
            content,
            precondition: WritePrecondition::Version(original.version),
        },
    )?;
    encode(
        &json!({"version":output.version,"bytes":output.bytes,"changed":true,"replacement_counts":counts,"lines_added":lines_added,"lines_removed":lines_removed}),
    )
}

/// Count overlapping occurrences for diagnostics, then replace nonoverlapping
/// matches only. replace_all never silently chooses an overlapping interpretation.
pub(crate) fn literal_replace(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Result<(String, usize), String> {
    if old.is_empty() {
        return Err("old_string must not be empty".into());
    }
    let positions: Vec<usize> = content.match_indices(old).map(|(at, _)| at).collect();
    if positions.is_empty() {
        return Err("old_string was not found (zero literal matches)".into());
    }
    let step = old
        .chars()
        .next()
        .map(char::len_utf8)
        .ok_or_else(|| "old_string must not be empty".to_owned())?;
    if positions.iter().any(|at| {
        content[at + step..]
            .find(old)
            .is_some_and(|relative| relative < old.len() - step)
    }) {
        return Err("old_string has overlapping matches; provide larger exact context".into());
    }
    if !replace_all && positions.len() != 1 {
        return Err(format!(
            "old_string is ambiguous: {} matches; provide unique context or set replace_all=true",
            positions.len()
        ));
    }
    let count = if replace_all { positions.len() } else { 1 };
    let bytes = content
        .len()
        .checked_sub(old.len() * count)
        .and_then(|size| {
            new.len()
                .checked_mul(count)
                .and_then(|add| size.checked_add(add))
        })
        .ok_or_else(|| "replacement byte size overflow".to_owned())?;
    if bytes > MAX_BYTES as usize {
        return Err("replacement exceeds the file byte limit".into());
    }
    Ok((content.replacen(old, new, count), count))
}

/// A failed write keeps its before point. A successful write whose after capture
/// fails must report its side effect so callers do not blindly repeat a mutation.
pub(crate) fn controlled_write<H: Host>(
    host: &mut H,
    request: WriteRequest,
) -> Result<WriteOutput, String> {
    let path = request.path.clone();
    operation(host.capture(&path, MAX_BYTES))?;
    let output = operation(host.write(request))?;
    host.capture(&path, MAX_BYTES).map_err(|mut error| {
        error.message = format!(
            "write succeeded; after cache-point capture failed: {}",
            error.message
        );
        encode(&error)
            .unwrap_or_else(|_| "write succeeded; after cache-point capture failed".into())
    })?;
    Ok(output)
}
