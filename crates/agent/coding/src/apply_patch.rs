//! Deterministic patch grammar and sequential receipts. Parsing and all bounded
//! reads finish before Host preflight and before any evidence or mutation effects.
use crate::host::*;
use crate::tools::{encode, hash, invalid, operation, parse, text};
use serde::Deserialize;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

const FILE_BYTES: usize = 1_048_576;
const TOTAL_BYTES: usize = 4 * FILE_BYTES;
const OPERATIONS: usize = 32;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    patch: String,
    preconditions: Vec<Precondition>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Precondition {
    path: String,
    expected: Expected,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Expected {
    Missing,
    Version(String),
}
#[derive(Debug)]
enum Change {
    Add(String),
    Update {
        destination: Option<String>,
        hunks: Vec<(String, String)>,
    },
    Delete,
}
#[derive(Debug)]
struct Entry {
    path: String,
    change: Change,
}

/// Headers use LF; payload preserves literal bytes (including CR). The optional
/// no-newline marker removes only the previous payload LF. No fuzzy matching,
/// line-number guessing, Unicode folding or implicit newline conversion occurs.
fn grammar(patch: &str) -> Result<Vec<Entry>, String> {
    text(patch, TOTAL_BYTES, true)?;
    let lines: Vec<&str> = patch.split_inclusive('\n').collect();
    fn header(line: &str) -> &str {
        line.strip_suffix('\n').unwrap_or(line)
    }
    if lines.first().map(|s| header(s)) != Some("*** Begin Patch") {
        return Err(invalid(
            "patch must start with *** Begin Patch on its own LF-delimited line",
        ));
    }
    let mut at = 1;
    let mut entries = Vec::new();
    while at < lines.len() && header(lines[at]) != "*** End Patch" {
        if entries.len() == OPERATIONS {
            return Err(invalid("patch exceeds 32 operations"));
        }
        let line = header(lines[at]);
        at += 1;
        let (path, kind) = if let Some(path) = line.strip_prefix("*** Add File: ") {
            (path, 0)
        } else if let Some(path) = line.strip_prefix("*** Update File: ") {
            (path, 1)
        } else if let Some(path) = line.strip_prefix("*** Delete File: ") {
            (path, 2)
        } else {
            return Err(invalid(&format!("unsupported patch header at line {at}")));
        };
        text(path, 4096, true)?;
        let change = match kind {
            0 => {
                let mut content = String::new();
                let mut payload_before_marker = false;
                while at < lines.len() && !lines[at].starts_with("*** ") {
                    if header(lines[at]) == "\\ No newline at end of file" {
                        if !payload_before_marker || !content.ends_with('\n') {
                            return Err(invalid(
                                "no-newline marker requires a preceding payload line",
                            ));
                        }
                        content.pop();
                        payload_before_marker = false;
                    } else {
                        let payload = lines[at]
                            .strip_prefix('+')
                            .ok_or_else(|| invalid("add file payload lines must start with +"))?;
                        content.push_str(payload);
                        payload_before_marker = true;
                    }
                    at += 1;
                }
                Change::Add(content)
            }
            1 => {
                let destination = if at < lines.len() {
                    header(lines[at])
                        .strip_prefix("*** Move to: ")
                        .map(str::to_owned)
                } else {
                    None
                };
                if let Some(destination) = &destination {
                    text(destination, 4096, true)?;
                    at += 1;
                }
                let mut hunks = Vec::new();
                while at < lines.len() && !lines[at].starts_with("*** ") {
                    if header(lines[at]) != "@@" {
                        return Err(invalid(
                            "update hunks must begin with an exact @@ line; numbered/fuzzy hunks are unsupported",
                        ));
                    }
                    at += 1;
                    let (mut old, mut new) = (String::new(), String::new());
                    let mut previous = None;
                    while at < lines.len()
                        && header(lines[at]) != "@@"
                        && !lines[at].starts_with("*** ")
                    {
                        let line = lines[at];
                        if header(line) == "\\ No newline at end of file" {
                            match previous.take() {
                                Some(' ') => {
                                    if !old.ends_with('\n') || !new.ends_with('\n') {
                                        return Err(invalid("invalid no-newline context marker"));
                                    }
                                    old.pop();
                                    new.pop();
                                }
                                Some('-') => {
                                    if old.pop() != Some('\n') {
                                        return Err(invalid("invalid no-newline deletion marker"));
                                    }
                                }
                                Some('+') => {
                                    if new.pop() != Some('\n') {
                                        return Err(invalid("invalid no-newline addition marker"));
                                    }
                                }
                                _ => {
                                    return Err(invalid(
                                        "no-newline marker requires a preceding payload line",
                                    ));
                                }
                            }
                        } else {
                            let prefix = match line.as_bytes().first() {
                                Some(b' ' | b'-' | b'+') => line.as_bytes()[0] as char,
                                _ => {
                                    return Err(invalid(
                                        "hunk payload requires an ASCII space, - or + prefix",
                                    ));
                                }
                            };
                            let payload = &line[1..];
                            match prefix {
                                ' ' => {
                                    old.push_str(payload);
                                    new.push_str(payload);
                                }
                                '-' => old.push_str(payload),
                                '+' => new.push_str(payload),
                                _ => {
                                    return Err(invalid(
                                        "hunk payload requires a space, - or + prefix",
                                    ));
                                }
                            }
                            previous = Some(prefix);
                        }
                        at += 1;
                    }
                    if old.is_empty() {
                        return Err(invalid(
                            "update hunks require nonempty exact source context",
                        ));
                    }
                    hunks.push((old, new));
                    if hunks.len() > 64 {
                        return Err(invalid("update exceeds 64 hunks"));
                    }
                }
                if hunks.is_empty() && destination.is_none() {
                    return Err(invalid("update requires hunks or Move to"));
                }
                Change::Update { destination, hunks }
            }
            _ => Change::Delete,
        };
        entries.push(Entry {
            path: path.to_owned(),
            change,
        });
    }
    if entries.is_empty() || at + 1 != lines.len() || header(lines[at]) != "*** End Patch" {
        return Err(invalid(
            "patch requires 1..=32 operations and one final *** End Patch line without trailing material",
        ));
    }
    Ok(entries)
}

fn existing<H: Host>(
    host: &mut H,
    path: &str,
    expected: Expected,
    total: &mut usize,
) -> Result<(String, String), String> {
    let Expected::Version(version) = expected else {
        return Err(invalid(&format!(
            "existing source {path} requires a version precondition"
        )));
    };
    hash(&version)?;
    let original = operation(host.read(ReadRequest {
        path: path.into(),
        offset_line: 1,
        max_lines: 10000,
        max_bytes: FILE_BYTES as u64,
    }))?;
    if original.truncated || original.next_line.is_some() || original.start_line != 1 {
        return Err(invalid(&format!(
            "{path}: source requires a complete read within 10000 lines / 1048576 bytes"
        )));
    }
    if !original.version.eq_ignore_ascii_case(&version) {
        return Err(encode(&OperationError {
            code: OperationErrorCode::Conflict,
            message: format!("{path}: stale source version; reread before applying patch"),
        })?);
    }
    *total += original.content.len();
    if *total > TOTAL_BYTES {
        return Err(invalid("aggregate source bytes exceed 4194304"));
    }
    Ok((original.content, version.to_ascii_lowercase()))
}

fn bounded_error(mut error: OperationError) -> OperationError {
    error.message = error.message.chars().take(512).collect();
    error
}

pub(crate) fn invoke<H: Host>(arguments: &str, host: &mut H) -> Result<String, String> {
    let args: Arguments = parse(arguments)?;
    let entries = grammar(&args.patch)?;
    if args.preconditions.is_empty() || args.preconditions.len() > 64 {
        return Err(invalid("preconditions requires 1..=64 exact path entries"));
    }
    let mut table = BTreeMap::new();
    for p in args.preconditions {
        text(&p.path, 4096, true)?;
        if let Expected::Version(v) = &p.expected {
            hash(v)?;
        }
        if table.insert(p.path, p.expected).is_some() {
            return Err(invalid("duplicate precondition path"));
        }
    }
    let mut used = BTreeSet::new();
    let (mut source_bytes, mut result_bytes) = (0, 0);
    let (mut requests, mut unchanged) = (Vec::new(), Vec::new());
    let mut line_counts = Vec::new();
    for (index, entry) in entries.into_iter().enumerate() {
        if !used.insert(entry.path.clone()) {
            return Err(invalid("duplicate patch source path"));
        }
        let expected = table
            .remove(&entry.path)
            .ok_or_else(|| invalid(&format!("missing precondition for {}", entry.path)))?;
        let mut noop = false;
        let (mut before_lines, mut after_lines) = (0, 0);
        let request = match entry.change {
            Change::Add(content) => {
                if !matches!(expected, Expected::Missing) {
                    return Err(invalid("add destination must require missing"));
                }
                text(&content, FILE_BYTES, false)?;
                result_bytes += content.len();
                after_lines = content.lines().count();
                FileMutation::Write(WriteRequest {
                    path: entry.path,
                    content,
                    precondition: WritePrecondition::Missing,
                })
            }
            Change::Delete => {
                let (original, version) = existing(host, &entry.path, expected, &mut source_bytes)?;
                before_lines = original.lines().count();
                FileMutation::Delete(DeleteRequest {
                    path: entry.path,
                    expected_version: version,
                })
            }
            Change::Update { destination, hunks } => {
                let (original, version) = existing(host, &entry.path, expected, &mut source_bytes)?;
                let mut content = original.clone();
                for (step, (old, new)) in hunks.into_iter().enumerate() {
                    content = crate::tools::literal_replace(&content, &old, &new, false)
                        .map_err(|reason| {
                            invalid(&format!(
                                "operation {}, hunk {} ({}): {reason}",
                                index + 1,
                                step + 1,
                                entry.path
                            ))
                        })?
                        .0;
                    if content.len() > FILE_BYTES {
                        return Err(invalid("patch result exceeds per-file 1048576 byte limit"));
                    }
                }
                text(&content, FILE_BYTES, false)?;
                before_lines = original.lines().count();
                after_lines = content.lines().count();
                result_bytes += content.len();
                if let Some(destination) = destination {
                    if !used.insert(destination.clone()) {
                        return Err(invalid("move destination collides with another patch path"));
                    }
                    if !matches!(table.remove(&destination), Some(Expected::Missing)) {
                        return Err(invalid(
                            "move destination requires its own missing precondition",
                        ));
                    }
                    FileMutation::Move(MoveRequest {
                        path: entry.path,
                        destination,
                        expected_version: version,
                        content,
                    })
                } else {
                    noop = content == original;
                    FileMutation::Write(WriteRequest {
                        path: entry.path,
                        content,
                        precondition: WritePrecondition::Version(version),
                    })
                }
            }
        };
        if result_bytes > TOTAL_BYTES {
            return Err(invalid("aggregate result bytes exceed 4194304"));
        }
        requests.push(request);
        unchanged.push(noop);
        line_counts.push(json!({"before_lines":before_lines,"after_lines":after_lines}));
    }
    if !table.is_empty() {
        return Err(invalid("unused precondition entries are not allowed"));
    }
    // Host preflight resolves platform aliases, versions, missing destinations and
    // reserves Runtime evidence/output for the entire call before effects begin.
    let requests = operation(host.preflight_mutations(requests))?;
    if requests.len() != unchanged.len() {
        return Err(invalid("Host preflight changed operation count"));
    }
    for (request, noop) in requests.iter().zip(&unchanged) {
        if !noop {
            for path in request.paths() {
                operation(host.capture(path, FILE_BYTES as u64))?;
            }
        }
    }
    let mut receipts = Vec::with_capacity(requests.len());
    let mut stopped = false;
    for (index, (request, noop)) in requests.into_iter().zip(unchanged).enumerate() {
        let paths: Vec<String> = request.paths().into_iter().map(str::to_owned).collect();
        if stopped {
            receipts.push(json!({"operation":index+1,"paths":paths,"status":"not_started"}));
            continue;
        }
        if noop {
            receipts.push(json!({"operation":index+1,"paths":paths,"status":"unchanged"}));
            continue;
        }
        match host.mutate(request) {
            Ok(output) => {
                let mut evidence = Vec::new();
                for path in &paths {
                    if let Err(error) = host.capture(path, FILE_BYTES as u64) {
                        evidence.push(bounded_error(error));
                    }
                }
                stopped = output.diagnostic.is_some() || !evidence.is_empty();
                receipts.push(json!({"operation":index+1,"paths":paths,"status":"committed","line_counts":line_counts[index],"outcome":output,"evidence_errors":evidence}));
            }
            Err(error) => {
                let error = bounded_error(error);
                // A transport/IO/timeout/cancel error cannot prove that a remote
                // admitted operation had no effect. Never replay this invocation.
                let status = match error.code {
                    OperationErrorCode::Conflict
                    | OperationErrorCode::InvalidRequest
                    | OperationErrorCode::Unsupported
                    | OperationErrorCode::Denied
                    | OperationErrorCode::Limit => "failed",
                    _ => "uncertain",
                };
                receipts
                    .push(json!({"operation":index+1,"paths":paths,"status":status,"error":error}));
                stopped = true;
            }
        }
    }
    encode(&json!({"complete":!stopped,"receipts":receipts,"replay_safe":false}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_unicode_prefix_and_repeated_eof_marker_are_errors() {
        for prefix in ["é", "中文", "🦀"] {
            let patch = format!(
                "*** Begin Patch\n*** Update File: a\n@@\n{prefix}malformed\n*** End Patch\n"
            );
            assert!(grammar(&patch).unwrap_err().contains("ASCII"));
        }
        assert!(grammar("*** Begin Patch\n*** Add File: a\n+first\n+\n\\ No newline at end of file\n\\ No newline at end of file\n*** End Patch\n").is_err());
    }
    #[test]
    fn grammar_preserves_crlf_and_explicit_eof() {
        let entries=grammar("*** Begin Patch\n*** Update File: a\n@@\n-old\r\n+new\r\n\\ No newline at end of file\n*** End Patch\n").unwrap();
        let Change::Update { hunks, .. } = &entries[0].change else {
            panic!()
        };
        assert_eq!(hunks[0], ("old\r\n".into(), "new\r".into()));
    }
    #[test]
    fn grammar_rejects_unknown_hunks_trailing_and_unbounded_ops() {
        for patch in [
            "*** Begin Patch\n*** Update File: a\n@@ -1 +1 @@\n-a\n+b\n*** End Patch\n",
            "*** Begin Patch\n*** Delete File: a\n*** End Patch\ntrailing",
            "*** Begin Patch\n*** Update File: a\n@@\n+only\n*** End Patch\n",
        ] {
            assert!(grammar(patch).is_err());
        }
        let patch = format!(
            "*** Begin Patch\n{}*** End Patch\n",
            "*** Delete File: a\n".repeat(33)
        );
        assert!(grammar(&patch).is_err());
    }
}
