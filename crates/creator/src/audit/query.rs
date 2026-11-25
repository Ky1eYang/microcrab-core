//! Read-only projections of the verified durable audit chain.
use super::recorder::{FORMAT, canonical, error, hash, json_error, valid_hash};
use microcrab_auditor::AuditorError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Read},
    sync::Arc,
};

/// All matching attempts are returned. Request identity disambiguates travel.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuditQuery {
    pub epoch: u64,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub request_hash: Option<String>,
    pub history_head: Option<String>,
    pub subagent: Option<String>,
    pub call_id: Option<String>,
    /// A conversation tool call contained in a sent request (for next-request deltas).
    pub tool_call_id: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditStatus {
    Missing,
    Pending,
    Omitted,
    Unchanged,
    Available,
}
#[derive(Clone, Debug, Serialize)]
pub struct AuditAttempt {
    pub request_hash: String,
    pub sequence: u64,
    /// First actual transport publication, distinct from effective preparation.
    #[serde(skip_serializing)]
    pub transmitted_sequence: Option<u64>,
    pub call_id: String,
    /// Optional exact issued Context budget fingerprint; legacy records retain None.
    pub context_fingerprint: Option<String>,
    pub context_target: Option<String>,
    pub epoch: u64,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub history_head: Option<String>,
    pub parent_request: Option<String>,
    pub base_request: Option<String>,
    pub subagent: Option<String>,
    pub status: AuditStatus,
    pub source: Option<String>,
    pub sent_request: Option<String>,
    pub request_document: Option<String>,
    /// Retained compatible causal parent document, or empty for an initial diff.
    pub baseline: Option<String>,
    /// Unified diff text only; identities, status and outcome stay in sibling fields.
    pub delta: Option<String>,
    /// Historical provider-shaped advertised definitions, never a current catalog.
    pub tools: Option<Value>,
    /// Canonical frozen definitions, not provider-shaped wire evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frozen_tools: Option<Value>,
    /// Identity-only normalized output from a protected model invocation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protected_tool_call_ids: Option<Value>,
    pub outcome: Option<Value>,
    /// Persisted normalized model response, not inferred from sent evidence.
    pub response: Option<Value>,
    /// Exact retained UTF-8 HTTP response body in ordinary request detail only.
    /// May be a partial prefix; missing or non-UTF-8 bodies are not reconstructed.
    pub response_raw: Option<String>,
    /// Persisted transport facts, independent of normalization or Session outcome.
    pub response_complete: Option<bool>,
    pub response_status: Option<u16>,
    pub response_content_type: Option<String>,
    pub response_omitted: bool,
    pub completion: String,
    /// Generic transport cancellation only; concrete Session causes belong to receipts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<&'static str>,
    /// Actual transport wall-clock start; absent when no timestamp was retained.
    pub started_at_ms: Option<u64>,
    /// Terminal start plus measured elapsed milliseconds, checked for overflow.
    pub ended_at_ms: Option<u64>,
    /// Validated normalized-success sample (or checked legacy terminal duration).
    /// Not proof that this attempt produced a particular selected Assistant row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_completed_at_ms: Option<u64>,
    /// Request start to first actual provider stream content (not a token count).
    pub first_token_ms: Option<u64>,
    /// First actual content to stream EOF; historical/nonstreamed values stay absent.
    pub generation_ms: Option<u64>,
    pub timing_source: Option<String>,
    pub usage: Option<Value>,
    pub baseline_usage: Option<Value>,
    pub token_delta: Value,
}
#[derive(Clone, Debug, Serialize)]
pub struct AuditTimeline {
    /// True only after verified selected Journal ancestry filtering.
    pub branch_selected: bool,
    pub request_hash: String,
    pub sequence: u64,
    pub call_id: String,
    pub epoch: u64,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub started_at_ms: Option<u64>,
    pub ended_at_ms: Option<u64>,
    pub completion: String,
    /// Generic transport cancellation only; concrete Session causes belong to receipts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<&'static str>,
    pub history_head: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct AuditStage {
    pub kind: String,
    pub sequence: u64,
    pub epoch: u64,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub tool_call_id: Option<String>,
    pub name: Option<String>,
    pub subagent: Option<String>,
    pub history_head: Option<String>,
    pub started_at_ms: Option<u64>,
    pub ended_at_ms: Option<u64>,
    pub completion: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct AuditQueryResult {
    pub status: AuditStatus,
    pub committed_sequence: u64,
    pub incomplete_tail: bool,
    /// Internal projection evidence; ordinary audit wire responses stay unchanged.
    #[serde(skip_serializing)]
    pub selected_ancestry_complete: bool,
    #[serde(skip_serializing)]
    pub selected_identities_complete: bool,
    pub attempts: Vec<AuditAttempt>,
    pub selected_request_hash: Option<String>,
    /// Main request timing only, populated by internal assistant listing queries.
    pub timeline: Vec<AuditTimeline>,
    /// Actual correlated foreground tool invocations, never inferred from model gaps.
    pub stages: Vec<AuditStage>,
    /// Exact selected ToolCall's safe authored image arguments, never provider text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authored_tool_arguments: Option<Value>,
    /// Legacy exact-call lifecycle facts without a linked request or branch proof.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legacy_unlinked_attempt: Option<LegacyUnlinkedAttempt>,
}
#[derive(Clone, Debug, Serialize)]
pub struct LegacyUnlinkedAttempt {
    pub call_id: String,
    pub epoch: u64,
    pub turn: u64,
    pub round: u64,
    pub subagent: Option<String>,
    pub started_at_ms: Option<u64>,
    pub ended_at_ms: Option<u64>,
    pub completion: &'static str,
}
/// Metadata emitted after append and sync. No bodies cross this boundary.
#[derive(Clone, Debug, Serialize)]
pub struct AuditCommit {
    pub sequence: u64,
    pub request_hash: Option<String>,
    pub call_id: Option<String>,
    pub epoch: Option<u64>,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub subagent: Option<String>,
    pub kind: String,
}
/// Observers must remain synchronous, short and nonblocking.
pub type CommitObserver = Arc<dyn Fn(AuditCommit) + Send + Sync>;
/// Only explicit transport stream metadata supplies these clocks. Old records,
/// JSON previews and unrecognized sources cannot be reconstructed from bodies.
fn persisted_stream_timing(record: Option<&Value>) -> (Option<u64>, Option<u64>, Option<String>) {
    let Some(record) = record.filter(|record| record["timing_source"] == "transport_stream") else {
        return (None, None, None);
    };
    let elapsed = record["elapsed_ms"].as_u64();
    let first = record["first_token_ms"]
        .as_u64()
        .filter(|first| elapsed.is_none_or(|elapsed| *first <= elapsed));
    let generation = first
        .zip(record["generation_ms"].as_u64())
        .filter(|(first, generation)| {
            first
                .checked_add(*generation)
                .is_some_and(|end| elapsed.is_none_or(|elapsed| end <= elapsed))
        })
        .map(|(_, generation)| generation);
    (
        first,
        generation,
        first.map(|_| "transport_stream".to_owned()),
    )
}

fn text(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}
/// Canonical readable document: two-space indentation, Python-compatible floats,
/// LF terminated. Byte parity with the Python implementation is required because
/// both describe the same retained request.
fn pretty(value: &Value) -> Result<String, AuditorError> {
    fn render(value: &Value, depth: usize, out: &mut String) -> Result<(), AuditorError> {
        match value {
            Value::Object(map) if !map.is_empty() => {
                out.push_str("{\n");
                for (index, (key, value)) in map.iter().enumerate() {
                    if index > 0 {
                        out.push_str(",\n");
                    }
                    out.push_str(&"  ".repeat(depth + 1));
                    out.push_str(&serde_json::to_string(key).map_err(json_error)?);
                    out.push_str(": ");
                    render(value, depth + 1, out)?;
                }
                out.push('\n');
                out.push_str(&"  ".repeat(depth));
                out.push('}');
            }
            Value::Array(items) if !items.is_empty() => {
                out.push_str("[\n");
                for (index, value) in items.iter().enumerate() {
                    if index > 0 {
                        out.push_str(",\n");
                    }
                    out.push_str(&"  ".repeat(depth + 1));
                    render(value, depth + 1, out)?;
                }
                out.push('\n');
                out.push_str(&"  ".repeat(depth));
                out.push(']');
            }
            Value::Number(number) if number.is_f64() => {
                out.push_str(&super::recorder::python_float(number)?)
            }
            _ => out.push_str(&serde_json::to_string(value).map_err(json_error)?),
        }
        Ok(())
    }
    let mut out = String::new();
    render(value, 0, &mut out)?;
    out.push('\n');
    Ok(out)
}
fn body(value: &Value) -> &Value {
    value.get("body").unwrap_or(value)
}
fn document(wire: &str) -> Result<String, AuditorError> {
    match serde_json::from_str::<Value>(wire) {
        Ok(value) => pretty(&value),
        Err(_) => Ok(format!(
            "{wire}{}",
            if wire.ends_with('\n') { "" } else { "\n" }
        )),
    }
}
/// Inspect only transport-owned message envelopes, never arbitrary user data.
/// The neutral AST uses role=tool/call_id; the four maintained HTTP adapters
/// preserve the same identity through their respective result representations.
fn contains_result(wire: &str, call: &str) -> bool {
    let Ok(request) = serde_json::from_str::<Value>(wire) else {
        return false;
    };
    let messages = request.get("messages").and_then(Value::as_array);
    let chat_or_anthropic = messages.is_some_and(|messages| {
        messages.iter().any(|message| {
            (message["role"] == "tool"
                && (message["tool_call_id"].as_str() == Some(call)
                    || message["call_id"].as_str() == Some(call)))
                || (message["role"] == "user"
                    && message["content"].as_array().is_some_and(|parts| {
                        parts.iter().any(|part| {
                            part["type"] == "tool_result"
                                && part["tool_use_id"].as_str() == Some(call)
                        })
                    }))
        })
    });
    let responses = request
        .get("input")
        .and_then(Value::as_array)
        .is_some_and(|input| {
            input.iter().any(|item| {
                item["type"] == "function_call_output" && item["call_id"].as_str() == Some(call)
            })
        });
    let google = request
        .get("contents")
        .and_then(Value::as_array)
        .is_some_and(|contents| {
            contents.iter().any(|message| {
                message["role"] == "user"
                    && message["parts"].as_array().is_some_and(|parts| {
                        parts.iter().any(|part| {
                            part.get("functionResponse")
                                .is_some_and(|result| result["id"].as_str() == Some(call))
                                || part.get("function_response").is_some_and(|result| {
                                    result["id"].as_str() == Some(call)
                                        || (result.get("id").is_none()
                                            && result["name"].as_str() == Some(call))
                                })
                        })
                    })
            })
        });
    chat_or_anthropic || responses || google
}

fn request_contains_result(
    id: &str,
    call: &str,
    sent: &HashMap<String, String>,
    effective: &HashMap<&str, &Value>,
) -> bool {
    // An actual retained sent request takes precedence over prepared membership.
    // Prepared content is association metadata only; evidence fields stay absent.
    if let Some(wire) = sent.get(id) {
        return contains_result(wire, call);
    }
    effective.get(id).is_some_and(|data| {
        if let Some(ids) = data["protected"]["tool_result_ids"].as_array() {
            ids.iter().any(|id| id.as_str() == Some(call))
        } else {
            data["request_body"]
                .as_str()
                .is_some_and(|wire| contains_result(wire, call))
        }
    })
}
/// Read a stable byte-length prefix; never opens a writer or derives views.
/// Incomplete concurrent final appends are excluded explicitly.
pub fn query(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    selector: &AuditQuery,
) -> Result<AuditQueryResult, AuditorError> {
    query_mode(&directory.into(), selector, false, false, None)
}
/// Selected-branch ordinary evidence with the established checkpoint fences.
pub fn query_selected(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    selector: &AuditQuery,
    request_head: Option<&str>,
    history_head: Option<&str>,
) -> Result<AuditQueryResult, AuditorError> {
    query_mode(
        &directory.into(),
        selector,
        false,
        false,
        Some(SelectedBranch {
            request_head,
            history_head,
            history_ancestry: None,
            display_positions: None,
            display_turns: None,
            request_heads: None,
        }),
    )
}

/// Selected evidence with storage-verified immutable parents, selected head first.
pub fn query_selected_with_ancestry(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    selector: &AuditQuery,
    request_head: Option<&str>,
    history_head: Option<&str>,
    history_ancestry: &[String],
) -> Result<AuditQueryResult, AuditorError> {
    query_mode(
        &directory.into(),
        selector,
        false,
        false,
        Some(SelectedBranch {
            request_head,
            history_head,
            history_ancestry: Some(history_ancestry),
            display_positions: None,
            display_turns: None,
            request_heads: None,
        }),
    )
}
/// Bounded internal assistant summaries; immutable selectors retain their fences.
pub fn query_internal(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    selector: &AuditQuery,
) -> Result<AuditQueryResult, AuditorError> {
    query_mode(&directory.into(), selector, true, false, None)
}
/// Selected summaries with the established conservative checkpoint fences.
pub fn query_internal_selected(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    selector: &AuditQuery,
    request_head: Option<&str>,
    history_head: Option<&str>,
) -> Result<AuditQueryResult, AuditorError> {
    query_mode(
        &directory.into(),
        selector,
        true,
        false,
        Some(SelectedBranch {
            request_head,
            history_head,
            history_ancestry: None,
            display_positions: None,
            display_turns: None,
            request_heads: None,
        }),
    )
}

/// Server-supplied Journal authority limits branch-sensitive trajectory evidence.
/// `history_ancestry` must be verified by storage and start at `history_head`.
/// An absent selected request is an empty main branch, never latest-append fallback.
pub fn query_internal_selected_with_ancestry(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    selector: &AuditQuery,
    request_head: Option<&str>,
    history_head: Option<&str>,
    history_ancestry: &[String],
) -> Result<AuditQueryResult, AuditorError> {
    query_mode(
        &directory.into(),
        selector,
        true,
        false,
        Some(SelectedBranch {
            request_head,
            history_head,
            history_ancestry: Some(history_ancestry),
            display_positions: None,
            display_turns: None,
            request_heads: None,
        }),
    )
}
/// Verified immutable-attempt facts without provider bodies, diffs or timelines.
/// Intended for cached statistics; the durable chain is checked by the same reader.
pub fn query_attempt_metadata(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    selector: &AuditQuery,
) -> Result<AuditQueryResult, AuditorError> {
    if selector.request_hash.is_none() {
        return Err(error("statistics require an immutable request selector"));
    }
    query_mode(&directory.into(), selector, false, true, None)
}
/// Main-attempt metadata from the immutable selected request ancestry only.
/// The ordinary scan, ancestry and response limits apply; no provider bodies escape.
pub fn query_selected_attempt_metadata(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    request_head: &str,
    positions: &[(u64, u64, u64)],
) -> Result<AuditQueryResult, AuditorError> {
    if positions.len() > 128 {
        return Err(error("display usage position budget exceeded"));
    }
    let selector = AuditQuery {
        epoch: 0,
        turn: None,
        round: None,
        request_hash: Some(request_head.to_owned()),
        history_head: None,
        subagent: None,
        call_id: None,
        tool_call_id: None,
    };
    query_mode(
        &directory.into(),
        &selector,
        false,
        true,
        Some(SelectedBranch {
            request_head: Some(request_head),
            history_head: None,
            history_ancestry: None,
            display_positions: Some(positions),
            display_turns: None,
            request_heads: None,
        }),
    )
}
/// Whole requested-Turn metadata, including failed and pending main invocations.
/// Bounds apply to Turns and the complete ancestry, never to a page of rounds.
/// Callers must reconcile every accepted position against canonical display history.
pub fn query_selected_turn_metadata(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    request_head: &str,
    turns: &[(u64, u64)],
) -> Result<AuditQueryResult, AuditorError> {
    query_selected_turn_metadata_with_heads(directory, &[request_head.to_owned()], turns)
}
/// Selected current and historical display anchors, supplied only by Journal.
/// Their verified ancestry union shares one scan and one total ancestry budget.
pub fn query_selected_turn_metadata_with_heads(
    directory: impl Into<microcrab_storage::audit::AuditSource>,
    request_heads: &[String],
    turns: &[(u64, u64)],
) -> Result<AuditQueryResult, AuditorError> {
    if request_heads.is_empty() || request_heads.len() > 1025 {
        return Err(error("display audit anchor budget exceeded"));
    }
    let request_head = &request_heads[0];
    if turns.len() > 129 {
        return Err(error("display usage Turn budget exceeded"));
    }
    let selector = AuditQuery {
        epoch: 0,
        turn: None,
        round: None,
        request_hash: Some(request_head.to_owned()),
        history_head: None,
        subagent: None,
        call_id: None,
        tool_call_id: None,
    };
    query_mode(
        &directory.into(),
        &selector,
        false,
        true,
        Some(SelectedBranch {
            request_head: Some(request_head),
            history_head: None,
            history_ancestry: None,
            display_positions: None,
            display_turns: Some(turns),
            request_heads: Some(request_heads),
        }),
    )
}
#[derive(Clone, Copy)]
struct SelectedBranch<'a> {
    request_head: Option<&'a str>,
    history_head: Option<&'a str>,
    history_ancestry: Option<&'a [String]>,
    display_positions: Option<&'a [(u64, u64, u64)]>,
    display_turns: Option<&'a [(u64, u64)]>,
    request_heads: Option<&'a [String]>,
}

fn query_mode(
    directory: &microcrab_storage::audit::AuditSource,
    selector: &AuditQuery,
    internal: bool,
    metadata_only: bool,
    selected_branch: Option<SelectedBranch<'_>>,
) -> Result<AuditQueryResult, AuditorError> {
    let display_metadata = metadata_only && selected_branch.is_some();
    let authored_selector = selector
        .turn
        .zip(selector.round)
        .zip(selector.tool_call_id.as_deref())
        .filter(|_| {
            !internal
                && !metadata_only
                && selector.subagent.is_none()
                && selector.call_id.is_none()
                && selector.request_hash.is_none()
        })
        .filter(|_| selected_branch.is_some_and(|branch| branch.history_ancestry.is_some()));
    for id in [&selector.request_hash, &selector.history_head]
        .into_iter()
        .flatten()
    {
        valid_hash(id)?;
    }
    for value in [
        &selector.call_id,
        &selector.tool_call_id,
        &selector.subagent,
    ]
    .into_iter()
    .flatten()
    {
        if value.len() > 1024 {
            return Err(error("audit selector budget exceeded"));
        }
    }
    if !internal
        && selector.turn.is_none()
        && selector.request_hash.is_none()
        && selector.call_id.is_none()
        && selector.tool_call_id.is_none()
    {
        return Err(error(
            "audit query requires a position or immutable call/request selector",
        ));
    }
    let legacy_call = selector.call_id.as_deref().filter(|call| {
        !call.is_empty()
            && !internal
            && !metadata_only
            && selector.request_hash.is_none()
            && selector.history_head.is_none()
            && selector.tool_call_id.is_none()
    });
    let file = directory.reader().map_err(|err| error(err.to_string()))?;
    let Some(file) = file else {
        return Ok(AuditQueryResult {
            status: AuditStatus::Missing,
            committed_sequence: 0,
            incomplete_tail: false,
            selected_ancestry_complete: false,
            selected_identities_complete: false,
            attempts: vec![],
            selected_request_hash: None,
            timeline: vec![],
            stages: vec![],
            authored_tool_arguments: None,
            legacy_unlinked_attempt: None,
        });
    };
    let mut reader = BufReader::new(file);
    let mut previous: Option<String> = None;
    let mut sequence = 0u64;
    let mut incomplete_tail = false;
    let mut effective: Vec<(String, u64, Value)> = vec![];
    let mut sent: HashMap<String, String> = HashMap::new();
    let mut transmitted = HashSet::new();
    let mut transmitted_sequences = HashMap::new();
    let mut outcomes: HashMap<String, Value> = HashMap::new();
    let mut completed: HashMap<String, Value> = HashMap::new();
    let mut ambiguous_completed = HashSet::new();
    // Keep completion evidence separate from the existing trajectory clocks and
    // usage projection. Once timing disagrees, no later replay may restore it.
    let mut response_completion = HashMap::new();
    let mut unbound_terminals = HashMap::new();
    let mut timing: HashMap<String, (Option<u64>, Option<u64>)> = HashMap::new();
    let mut stages = Vec::new();
    let mut stage_index = HashMap::new();
    let mut authored_candidates = Vec::new();
    let mut legacy = LegacyLifecycle::default();
    loop {
        let mut bytes = vec![];
        reader
            .by_ref()
            .take(64 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes)
            .map_err(|_| error("audit read failed"))?;
        if bytes.is_empty() {
            break;
        }
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(error("audit record budget exceeded"));
        }
        if !bytes.ends_with(b"\n") {
            incomplete_tail = true;
            break;
        }
        let mut row: Value =
            serde_json::from_slice(&bytes).map_err(|_| error("invalid audit JSON"))?;
        let digest = row
            .as_object_mut()
            .and_then(|row| row.remove("sha256"))
            .and_then(|v| v.as_str().map(str::to_owned))
            .ok_or_else(|| error("missing audit digest"))?;
        valid_hash(&digest)?;
        if row["sequence"].as_u64() != sequence.checked_add(1)
            || row["previous"] != serde_json::json!(previous)
            || hash(canonical(&row)?) != digest
        {
            return Err(error("audit query integrity mismatch"));
        }
        let data = &row["data"];
        if sequence == 0 && (data["kind"] != "format" || data["format"] != FORMAT) {
            return Err(error("unsupported audit format"));
        }
        sequence += 1;
        previous = Some(digest.clone());
        match data["kind"].as_str() {
            Some("effective") => {
                if let Some(call) = legacy_call
                    && data["call_id"].as_str() == Some(call)
                {
                    legacy.different_authority = true;
                }
                let mut effective_data = data.clone();
                if display_metadata && let Some(data) = effective_data.as_object_mut() {
                    let _ = data.remove("request_body");
                }
                effective.push((digest, sequence, effective_data));
            }
            Some("published") => {
                if internal {
                    project_stage(
                        body(&data["record"]),
                        sequence,
                        selector,
                        &mut stages,
                        &mut stage_index,
                    )?;
                }
                let record = body(&data["record"]);
                if let Some(call) = legacy_call
                    && record["call_id"].as_str() == Some(call)
                {
                    legacy.observe(sequence, data, record, selector);
                }
                if let Some(((turn, round), tool_call_id)) = authored_selector
                    && record["event"] == "tool_call"
                    && record["subagent"].is_null()
                    && record["epoch"].as_u64() == Some(selector.epoch)
                    && record["turn_id"].as_u64() == Some(turn)
                    && record["round"].as_u64() == Some(round)
                    && record["tool_call_id"].as_str() == Some(tool_call_id)
                    && let Some(head) = text(record, "history_head")
                    && selector
                        .history_head
                        .as_ref()
                        .is_none_or(|selected| *selected == head)
                {
                    let arguments = if record["authored_image_arguments"] == true
                        && record["payload"]["type"] == "tool_call"
                        && record["payload"]["name"] == "view_image"
                        && record["tool_execution_id"]
                            .as_u64()
                            .is_some_and(|id| id > 0)
                    {
                        record["payload"]["arguments"]
                            .as_object()
                            .map(|_| record["payload"]["arguments"].clone())
                    } else {
                        None
                    };
                    if authored_candidates.len() >= 256 {
                        return Err(error("authored arguments occurrence budget exceeded"));
                    }
                    authored_candidates.push((
                        head,
                        text(record, "producer_request_head"),
                        arguments,
                    ));
                }
                if let Some(id) = text(data, "request_hash") {
                    if record["event"] == "request_prepared" || record["sent_request"].is_string() {
                        transmitted.insert(id.clone());
                        transmitted_sequences.entry(id.clone()).or_insert(sequence);
                        if let Some(started) = record["started_at_unix_ms"].as_u64() {
                            timing.entry(id.clone()).or_default().0 = Some(started);
                        }
                    }
                    if !display_metadata && let Some(wire) = text(record, "sent_request") {
                        sent.insert(id.clone(), wire);
                    }
                    if record["event"] == "round_completed" || !record["outcome"].is_null() {
                        let entry = timing.entry(id.clone()).or_default();
                        // Terminal transport records normally retain the same start.
                        // A pending preparation start is usable when terminal start
                        // is absent; an end requires an actual terminal duration.
                        entry.0 = record["started_at_unix_ms"].as_u64().or(entry.0);
                        entry.1 = entry
                            .0
                            .zip(record["elapsed_ms"].as_u64())
                            .and_then(|(started, elapsed)| started.checked_add(elapsed));
                        let sample = persisted_response_completion(record);
                        retain_response_completion(&mut response_completion, id.clone(), sample);
                        let terminal = if display_metadata {
                            // Completion and usage remain canonical; provider text
                            // is never needed for this projection.
                            serde_json::json!({
                                "event": record["event"],
                                "outcome": record["outcome"],
                                "usage": record.get("usage").unwrap_or(&record["payload"]["usage"]),
                            })
                        } else {
                            record.clone()
                        };
                        if let Some(previous) = completed.insert(id.clone(), terminal.clone())
                            && display_metadata
                            && previous != terminal
                        {
                            ambiguous_completed.insert(id.clone());
                        }
                    }
                    if !record["outcome"].is_null() {
                        outcomes.insert(id, record["outcome"].clone());
                    }
                } else if (record["event"] == "round_completed" || !record["outcome"].is_null())
                    && let Some(call) = text(record, "call_id")
                {
                    // The recorder forgets the in-flight association after a
                    // terminal. Detect late conflicting terminal publications
                    // without using them as evidence or moving them to a branch.
                    let sample = persisted_response_completion(record);
                    retain_response_completion(&mut unbound_terminals, call, sample);
                }
            }
            _ => {}
        }
    }
    if sequence == 0 && !incomplete_tail {
        return Err(error("empty audit authority"));
    }
    let index: HashMap<_, _> = effective
        .iter()
        .map(|(id, _, data)| (id.as_str(), data))
        .collect();
    let mut selected_ancestry_complete = selected_branch.is_some();
    let mut selected_identities_complete = selected_branch.is_some();
    let mut branch_requests = HashSet::new();
    let mut branch_histories = HashSet::new();
    if let Some(SelectedBranch {
        request_head,
        history_head,
        history_ancestry,
        ..
    }) = selected_branch
    {
        if let Some(ancestry) = history_ancestry {
            if ancestry.len() > 4096 || ancestry.first().map(String::as_str) != history_head {
                return Err(error("invalid selected Journal ancestry"));
            }
            for head in ancestry {
                valid_hash(head)?;
                if !branch_histories.insert(head.clone()) {
                    return Err(error("duplicate selected Journal ancestor"));
                }
            }
        } else if let Some(head) = history_head {
            valid_hash(head)?;
            branch_histories.insert(head.to_owned());
        }
        let roots: Vec<_> = selected_branch
            .and_then(|branch| branch.request_heads)
            .map(|heads| heads.iter().map(String::as_str).collect())
            .unwrap_or_else(|| request_head.into_iter().collect());
        for root in roots {
            let mut next = Some(root.to_owned());
            let mut visiting = HashSet::new();
            while let Some(id) = next {
                valid_hash(&id)?;
                if !visiting.insert(id.clone()) {
                    return Err(error("selected audit ancestry cycle"));
                }
                // Overlapping selected anchors share the same immutable suffix.
                // A duplicate root is not an ambiguous provider invocation.
                if !branch_requests.insert(id.clone()) {
                    break;
                }
                if branch_requests.len() > 4096 {
                    return Err(error("selected audit ancestry budget exceeded"));
                }
                let Some(data) = index.get(id.as_str()) else {
                    selected_ancestry_complete = false;
                    break;
                };
                if history_ancestry.is_none()
                    && let Some(head) = text(data, "history_head")
                {
                    branch_histories.insert(head);
                }
                if data.get("parent_request").is_none()
                    || (!data["parent_request"].is_null() && !data["parent_request"].is_string())
                {
                    selected_ancestry_complete = false;
                }
                next = text(data, "parent_request");
            }
        }
        // Helpers/stages carry admission history, not a main request parent.
        // If a checkpoint admitted a competing main branch, that history alone
        // cannot prove their branch. Keep those durations unknown rather than
        // attaching an off-branch helper/tool invocation to selected history.
        for (id, _, data) in &effective {
            if data["subagent"].is_null()
                && !branch_requests.contains(id)
                && let Some(head) = text(data, "history_head")
            {
                branch_histories.remove(&head);
            }
        }
        stages.retain(|stage| {
            stage
                .history_head
                .as_ref()
                .is_some_and(|head| branch_histories.contains(head))
        });
    }
    let response_completion_conflicts = |id: &str, data: &Value| {
        let bound = response_completion.get(id);
        bound.is_some_and(Result::is_err)
            || data["call_id"]
                .as_str()
                .and_then(|call| unbound_terminals.get(call))
                .is_some_and(|late| late.is_err() || bound != Some(late))
    };
    // Statistics retain the selected pending attempt plus its closest measured
    // causal ancestor, not an unrelated latest append or a whole timeline.
    let mut metadata_requests = HashSet::new();
    if metadata_only && selected_branch.is_some_and(|branch| branch.display_turns.is_some()) {
        let wanted: HashSet<_> = selected_branch
            .and_then(|branch| branch.display_turns)
            .unwrap_or_default()
            .iter()
            .copied()
            .collect();
        let mut calls = HashSet::new();
        for (id, _, data) in &effective {
            if !branch_requests.contains(id) || !data["subagent"].is_null() {
                continue;
            }
            if ambiguous_completed.contains(id)
                || (outcomes.get(id).and_then(Value::as_str) == Some("accepted")
                    && !transmitted.contains(id))
            {
                selected_identities_complete = false;
            }
            if data["call_id"]
                .as_str()
                .is_none_or(|call| call.is_empty() || !calls.insert(call))
            {
                selected_identities_complete = false;
            }
            let scope = data["epoch"].as_u64().zip(data["turn"].as_u64());
            if scope.is_none() || data["round"].as_u64().is_none() {
                selected_identities_complete = false;
            }
            if scope.is_some_and(|scope| wanted.contains(&scope)) {
                metadata_requests.insert(id.clone());
            }
        }
    } else if metadata_only && selected_branch.is_some() {
        let wanted: HashSet<_> = selected_branch
            .and_then(|branch| branch.display_positions)
            .unwrap_or_default()
            .iter()
            .copied()
            .collect();
        let scope = |data: &Value| {
            Some((
                data["epoch"].as_u64()?,
                data["turn"].as_u64()?,
                data["round"].as_u64()?,
            ))
        };
        let mut requests = HashMap::new();
        let mut calls = HashMap::new();
        let mut accepted_positions = HashMap::new();
        // Count the entire selected main chain before filtering to the page.
        // Off-page reused identities must invalidate their on-page counterpart.
        for (id, _, data) in &effective {
            if !branch_requests.contains(id) || !data["subagent"].is_null() {
                continue;
            }
            *requests.entry(id.as_str()).or_insert(0usize) += 1;
            if let Some(call) = data["call_id"].as_str().filter(|call| !call.is_empty()) {
                *calls.entry(call).or_insert(0usize) += 1;
            } else {
                selected_identities_complete = false;
            }
            if scope(data).is_none()
                || ambiguous_completed.contains(id)
                || response_completion_conflicts(id, data)
                || (outcomes.get(id).and_then(Value::as_str) == Some("accepted")
                    && !transmitted.contains(id))
            {
                selected_identities_complete = false;
            }
            if outcomes.get(id).and_then(Value::as_str) == Some("accepted")
                && completion(completed.get(id)) == "succeeded"
                && let Some(position) = scope(data)
            {
                *accepted_positions.entry(position).or_insert(0usize) += 1;
            }
        }
        if requests
            .values()
            .chain(calls.values())
            .any(|count| *count != 1)
        {
            selected_identities_complete = false;
        }
        for (id, _, data) in &effective {
            let Some(position) = scope(data) else {
                continue;
            };
            if branch_requests.contains(id)
                && data["subagent"].is_null()
                && wanted.contains(&position)
                && accepted_positions.get(&position) == Some(&1)
                && requests.get(id.as_str()) == Some(&1)
                && data["call_id"]
                    .as_str()
                    .is_some_and(|call| calls.get(call) == Some(&1))
                && outcomes.get(id).and_then(Value::as_str) == Some("accepted")
                && completion(completed.get(id)) == "succeeded"
            {
                metadata_requests.insert(id.clone());
            }
        }
    } else if metadata_only {
        let mut next = selector.request_hash.clone();
        let mut visited = HashSet::new();
        while let Some(id) = next {
            if !visited.insert(id.clone()) || visited.len() > 4096 {
                return Err(error("audit ancestry budget exceeded"));
            }
            let measured = completed
                .get(&id)
                .and_then(persisted_usage)
                .is_some_and(|usage| usage["input_tokens"].is_u64());
            if selector.request_hash.as_ref() == Some(&id) || measured {
                metadata_requests.insert(id.clone());
            }
            if measured {
                break;
            }
            next = index
                .get(id.as_str())
                .and_then(|data| text(data, "parent_request"));
        }
    }
    let mut attempts = vec![];
    let mut timeline = vec![];
    let mut response_bytes = serde_json::to_vec(&stages)
        .map_err(|_| error("audit stages serialization failed"))?
        .len();
    if internal {
        for (id, number, data) in &effective {
            if (selected_branch.is_some() && !branch_requests.contains(id))
                || !data["subagent"].is_null()
                || data["epoch"].as_u64() != Some(selector.epoch)
                || selector
                    .history_head
                    .as_ref()
                    .is_some_and(|head| data["history_head"].as_str() != Some(head.as_str()))
            {
                continue;
            }
            if timeline.len() >= 256 {
                return Err(error(
                    "audit timeline budget exceeded; select an exact history head",
                ));
            }
            let (started_at_ms, ended_at_ms) = timing.get(id).copied().unwrap_or_default();
            let row = AuditTimeline {
                branch_selected: selected_branch.is_some(),
                request_hash: id.clone(),
                sequence: *number,
                call_id: text(data, "call_id")
                    .ok_or_else(|| error("missing audit call identity"))?,
                epoch: selector.epoch,
                turn: data["turn"].as_u64(),
                round: data["round"].as_u64(),
                started_at_ms,
                ended_at_ms,
                completion: completion(completed.get(id)).into(),
                cancel_reason: (completion(completed.get(id)) == "cancelled")
                    .then_some("cancelled"),
                history_head: text(data, "history_head"),
            };
            response_bytes += serde_json::to_vec(&row)
                .map_err(|_| error("audit timeline serialization failed"))?
                .len();
            timeline.push(row);
        }
    }
    if response_bytes > 32 * 1024 * 1024 {
        return Err(error("audit response budget exceeded"));
    }
    for (id, number, data) in &effective {
        if (selected_branch.is_some()
            && if data["subagent"].is_null() {
                !branch_requests.contains(id)
            } else {
                !text(data, "history_head").is_some_and(|head| branch_histories.contains(&head))
            })
            || (internal && data["subagent"].is_null())
            || (!(metadata_only && selector.request_hash.is_some())
                && data["epoch"].as_u64() != Some(selector.epoch))
            || (!internal
                && selector
                    .turn
                    .is_some_and(|v| data["turn"].as_u64() != Some(v)))
            || (!internal
                && selector
                    .round
                    .is_some_and(|v| data["round"].as_u64() != Some(v)))
            || (!internal && text(data, "subagent") != selector.subagent)
            || (metadata_only && !metadata_requests.contains(id))
            || (!metadata_only && selector.request_hash.as_ref().is_some_and(|v| v != id))
            || selector
                .history_head
                .as_ref()
                .is_some_and(|v| data["history_head"].as_str() != Some(v.as_str()))
            || selector
                .call_id
                .as_ref()
                .is_some_and(|v| data["call_id"].as_str() != Some(v.as_str()))
            || selector
                .tool_call_id
                .as_ref()
                .is_some_and(|v| !request_contains_result(id, v, &sent, &index))
        {
            continue;
        }
        let parent = text(data, "parent_request");
        let base = parent.as_ref().filter(|id| {
            sent.contains_key(*id)
                && index.get(id.as_str()).is_some_and(|old| {
                    old["epoch"] == data["epoch"] && old["subagent"] == data["subagent"]
                })
        });
        // A result remains in later history. Same-position retries are separate
        // attempts and must remain visible; only later positions are suppressed.
        if selector.tool_call_id.as_ref().is_some_and(|call| {
            let mut ancestor = parent.as_deref();
            for _ in 0..index.len() {
                let Some(identity) = ancestor else {
                    break;
                };
                let Some(old) = index.get(identity) else {
                    break;
                };
                if old["epoch"] != data["epoch"]
                    || old["subagent"] != data["subagent"]
                    || !request_contains_result(identity, call, &sent, &index)
                {
                    break;
                }
                if old["turn"] != data["turn"] || old["round"] != data["round"] {
                    return true;
                }
                ancestor = old["parent_request"].as_str();
            }
            false
        }) {
            continue;
        }
        let retained_wire = sent.get(id);
        let wire = if internal || metadata_only {
            None
        } else {
            retained_wire.cloned()
        };
        let current = wire.as_deref().map(document).transpose()?;
        let baseline = if current.is_some() {
            Some(
                base.and_then(|id| sent.get(id))
                    .map(|wire| document(wire))
                    .transpose()?
                    .unwrap_or_default(),
            )
        } else {
            None
        };
        let delta = current
            .as_ref()
            .zip(baseline.as_ref())
            .map(|(current, baseline)| {
                super::diff::patch(baseline, current, base.map_or("empty", String::as_str), id)
            });
        let status = if delta.as_ref().is_some_and(String::is_empty) {
            AuditStatus::Unchanged
        } else if retained_wire.is_some() {
            AuditStatus::Available
        } else if transmitted.contains(id) {
            AuditStatus::Omitted
        } else if outcomes.contains_key(id) {
            AuditStatus::Missing
        } else {
            AuditStatus::Pending
        };
        let tools = wire
            .as_ref()
            .and_then(|wire| serde_json::from_str::<Value>(wire).ok())
            .and_then(|value| value.get("tools").cloned());
        let frozen_tools = (!internal && !metadata_only)
            .then(|| data["protected"].get("frozen_tools"))
            .flatten()
            .filter(|tools| tools.is_array())
            .cloned();
        let protected_tool_call_ids = (!metadata_only && data["protected"].is_object())
            .then(|| completed.get(id)?.get("protected_tool_call_ids"))
            .flatten()
            .filter(|ids| ids.is_array())
            .cloned();
        response_bytes += wire.as_ref().map_or(0, String::len)
            + current.as_ref().map_or(0, String::len)
            + baseline.as_ref().map_or(0, String::len)
            + delta.as_ref().map_or(0, String::len)
            + tools.as_ref().map_or(0, |v| v.to_string().len())
            + frozen_tools.as_ref().map_or(0, |v| v.to_string().len())
            + protected_tool_call_ids
                .as_ref()
                .map_or(0, |v| v.to_string().len());
        if response_bytes > 32 * 1024 * 1024
            || attempts.len()
                >= if selected_branch.is_some_and(|branch| branch.display_turns.is_some()) {
                    4096
                } else {
                    256
                }
        {
            return Err(error(
                "audit response budget exceeded; select immutable request identity",
            ));
        }
        let terminal = completed.get(id);
        // Detail uses the terminal already bound to this attempt and filtered by
        // the same selectors and ancestry as the sent request. No summaries or
        // protected invocations expose provider wire evidence.
        let wire_terminal =
            terminal.filter(|_| !internal && !metadata_only && !data["protected"].is_object());
        let response_raw = wire_terminal.and_then(|record| record["response"].as_str());
        let response_complete =
            wire_terminal.and_then(|record| record["response_complete"].as_bool());
        let response_status = wire_terminal
            .and_then(|record| record["response_status"].as_u64())
            .and_then(|status| u16::try_from(status).ok());
        let response_content_type =
            wire_terminal.and_then(|record| record["response_content_type"].as_str());
        let mut response = if metadata_only {
            None
        } else {
            terminal.and_then(model_response)
        };
        let response_omitted = internal
            && response.as_ref().is_some_and(|value| {
                serde_json::to_vec(value).map_or(true, |bytes| bytes.len() > 8192)
            });
        if response_omitted {
            response = None;
        }
        let completion = completion(terminal);
        let (first_token_ms, generation_ms, timing_source) = persisted_stream_timing(terminal);
        let usage = terminal.and_then(persisted_usage);
        let baseline_usage = base.and_then(|id| completed.get(id)).and_then(persisted_usage)
            .or_else(|| if parent.is_none() { Some(serde_json::json!({"input_tokens":0,"cache_read_tokens":0,"cache_write_tokens":0,"output_tokens":0,"total_tokens":0})) } else { None });
        let token_delta = token_delta(usage.as_ref(), baseline_usage.as_ref());
        // Charge the JSON-escaped response evidence to the existing detail cap.
        response_bytes += serde_json::to_vec(&(
            &response,
            response_raw,
            response_complete,
            response_status,
            response_content_type,
            &usage,
            &baseline_usage,
            &token_delta,
        ))
        .map_err(|_| error("audit response evidence serialization failed"))?
        .len();
        if response_bytes > 32 * 1024 * 1024 {
            return Err(error("audit response budget exceeded"));
        }
        attempts.push(AuditAttempt {
            request_hash: id.clone(),
            sequence: *number,
            transmitted_sequence: transmitted_sequences.get(id).copied(),
            call_id: text(data, "call_id").ok_or_else(|| error("missing audit call identity"))?,
            context_fingerprint: terminal.and_then(|record| text(record, "context_fingerprint")),
            context_target: terminal.and_then(|record| text(record, "context_target")),
            epoch: data["epoch"]
                .as_u64()
                .ok_or_else(|| error("missing audit epoch"))?,
            turn: data["turn"].as_u64(),
            round: data["round"].as_u64(),
            history_head: text(data, "history_head"),
            parent_request: parent.clone(),
            base_request: base.cloned(),
            subagent: text(data, "subagent"),
            status,
            source: retained_wire.map(|_| "sent_request".into()),
            sent_request: wire,
            request_document: current,
            baseline,
            delta,
            tools,
            frozen_tools,
            protected_tool_call_ids,
            outcome: outcomes.get(id).cloned(),
            response,
            response_raw: response_raw.map(str::to_owned),
            response_complete,
            response_status,
            response_content_type: response_content_type.map(str::to_owned),
            response_omitted,
            completion: completion.into(),
            cancel_reason: (completion == "cancelled").then_some("cancelled"),
            started_at_ms: timing.get(id).and_then(|entry| entry.0),
            ended_at_ms: timing.get(id).and_then(|entry| entry.1),
            response_completed_at_ms: response_completion
                .get(id)
                .and_then(|evidence| evidence.as_ref().ok().copied().flatten())
                .filter(|_| !response_completion_conflicts(id, data)),
            first_token_ms,
            generation_ms,
            timing_source,
            usage,
            baseline_usage,
            token_delta,
        });
    }
    let status = if attempts.is_empty() {
        AuditStatus::Missing
    } else if attempts.iter().any(|a| a.status == AuditStatus::Available) {
        AuditStatus::Available
    } else if attempts.iter().any(|a| a.status == AuditStatus::Pending) {
        AuditStatus::Pending
    } else if attempts.iter().any(|a| a.status == AuditStatus::Omitted) {
        AuditStatus::Omitted
    } else if attempts.iter().all(|a| a.status == AuditStatus::Unchanged) {
        AuditStatus::Unchanged
    } else {
        AuditStatus::Missing
    };
    let position = attempts.first().map(|a| (a.turn, a.round));
    let candidates: Vec<_> = attempts
        .iter()
        .filter(|a| Some((a.turn, a.round)) == position)
        .collect();
    let selected_request_hash = ["succeeded", "cancelled", "failed", "pending"]
        .into_iter()
        .find_map(|state| candidates.iter().rev().find(|a| a.completion == state))
        .map(|a| a.request_hash.clone());
    // A reused ID or duplicate publication on the same selected position
    // cannot prove which authored call the visible tool represents.
    let mut selected_authored = authored_candidates
        .into_iter()
        .filter(|(head, producer, _)| {
            let Some(producer) = producer else {
                return branch_histories.contains(head);
            };
            selected_branch
                .and_then(|branch| branch.history_ancestry)
                .is_some_and(|ancestry| ancestry.contains(head))
                && branch_requests.contains(producer)
        });
    let authored_tool_arguments = selected_authored
        .next()
        .and_then(|(_, producer, arguments)| {
            let producer = producer?;
            let ((turn, round), call) = authored_selector?;
            let effective = index.get(producer.as_str())?;
            let terminal = completed.get(&producer)?;
            (transmitted.contains(&producer)
                && effective["epoch"].as_u64() == Some(selector.epoch)
                && effective["turn"].as_u64() == Some(turn)
                && effective["round"].as_u64() == Some(round)
                && effective["subagent"].is_null()
                && terminal["outcome"] == "accepted"
                && terminal_contains_authored_image_call(effective, terminal, call))
            .then_some(arguments)
            .flatten()
        })
        .filter(|_| selected_authored.next().is_none());
    Ok(AuditQueryResult {
        status,
        committed_sequence: sequence,
        incomplete_tail,
        selected_ancestry_complete,
        selected_identities_complete,
        attempts,
        selected_request_hash,
        timeline,
        stages,
        authored_tool_arguments,
        legacy_unlinked_attempt: legacy.project(legacy_call, selector),
    })
}

#[derive(Default)]
struct LegacyLifecycle {
    different_authority: bool,
    prepared: Option<(u64, u64, u64, Option<u64>)>,
    terminal: Option<(u64, u64, u64, Option<u64>, Option<u64>, &'static str)>,
}
impl LegacyLifecycle {
    fn observe(&mut self, sequence: u64, data: &Value, record: &Value, selector: &AuditQuery) {
        let event = record["event"].as_str().unwrap_or_default();
        if !matches!(event, "request_prepared" | "round_completed") {
            return;
        }
        let scope = record["turn_id"].as_u64().zip(record["round"].as_u64());
        let Some((turn, round)) = scope.filter(|(turn, round)| {
            record["epoch"].as_u64() == Some(selector.epoch)
                && text(record, "subagent") == selector.subagent
                && selector.turn.is_none_or(|expected| expected == *turn)
                && selector.round.is_none_or(|expected| expected == *round)
                && data["request_hash"].is_null()
                && record["sent_request"].is_null()
                && record["response"].is_null()
                && record["response_bytes"].is_null()
                && record["payload"]["response"].is_null()
        }) else {
            self.different_authority = true;
            return;
        };
        let start = record["started_at_unix_ms"].as_u64();
        if event == "request_prepared" {
            self.different_authority |= self
                .prepared
                .replace((sequence, turn, round, start))
                .is_some();
        } else {
            let state = completion(Some(record));
            if !record["outcome"].is_string() {
                self.different_authority = true;
                return;
            }
            self.different_authority |= self
                .terminal
                .replace((
                    sequence,
                    turn,
                    round,
                    start,
                    record["elapsed_ms"].as_u64(),
                    state,
                ))
                .is_some();
        }
    }
    fn project(self, call: Option<&str>, selector: &AuditQuery) -> Option<LegacyUnlinkedAttempt> {
        let (start_sequence, turn, round, start) = self.prepared?;
        let (terminal_sequence, end_turn, end_round, terminal_start, elapsed, completion) =
            self.terminal?;
        if self.different_authority
            || start_sequence >= terminal_sequence
            || (turn, round) != (end_turn, end_round)
            || start
                .zip(terminal_start)
                .is_some_and(|(start, other)| start != other)
        {
            return None;
        }
        let started_at_ms = start.or(terminal_start);
        Some(LegacyUnlinkedAttempt {
            call_id: call?.to_owned(),
            epoch: selector.epoch,
            turn,
            round,
            subagent: selector.subagent.clone(),
            started_at_ms,
            ended_at_ms: started_at_ms
                .zip(elapsed)
                .and_then(|(start, elapsed)| start.checked_add(elapsed)),
            completion,
        })
    }
}

fn terminal_contains_authored_image_call(effective: &Value, terminal: &Value, call: &str) -> bool {
    if effective["protected"].is_object() {
        return terminal["protected_tool_call_ids"]
            .as_array()
            .is_some_and(|ids| ids.iter().filter(|id| id.as_str() == Some(call)).count() == 1);
    }
    // Raw audit also retains the transport's normalized response. Reuse its
    // exact IDs (which may be synthesized by an adapter) rather than trying to
    // decode provider JSON/SSE again or treating strings in that body as IDs.
    terminal["payload"]["response"]["tool_calls"]
        .as_array()
        .is_some_and(|calls| {
            let mut matching = calls
                .iter()
                .filter(|item| item["id"].as_str() == Some(call));
            matching
                .next()
                .is_some_and(|item| item["name"] == "view_image")
                && matching.next().is_none()
        })
}

fn model_response(record: &Value) -> Option<Value> {
    let mut response = record
        .get("payload")
        .and_then(|p| p.get("response"))
        .filter(|v| !v.is_null())
        .cloned()
        // Legacy normalized objects lived at the top level. A transport body
        // string at the same key is not normalized output (notably on errors).
        .or_else(|| record.get("response").filter(|v| v.is_object()).cloned())?;
    if let Some(calls) = response.get_mut("tool_calls").and_then(Value::as_array_mut) {
        for call in calls {
            if let Some(call) = call.as_object_mut() {
                call.remove("provider_metadata");
            }
        }
    }
    Some(response)
}

fn retain_response_completion(
    index: &mut HashMap<String, Result<Option<u64>, ()>>,
    key: String,
    sample: Option<u64>,
) {
    index
        .entry(key)
        .and_modify(|previous| {
            if *previous != Ok(sample) {
                *previous = Err(());
            }
        })
        .or_insert(Ok(sample));
}

fn persisted_response_completion(record: &Value) -> Option<u64> {
    if record["event"] != "round_completed" || record["outcome"] != "accepted" {
        return None;
    }
    // A legacy terminal record must itself contain both clock witnesses.
    let clocks = record["started_at_unix_ms"]
        .as_u64()
        .zip(record["elapsed_ms"].as_u64());
    let unix_ms = match record.get("response_completed_at_ms") {
        Some(value) => {
            let explicit = value.as_u64()?;
            if clocks.is_some_and(|(start, elapsed)| start.checked_add(elapsed) != Some(explicit)) {
                return None;
            }
            explicit
        }
        None => {
            let (start, elapsed) = clocks?;
            start.checked_add(elapsed)?
        }
    };
    microcrab_transport::ResponseCompletedAt::new(unix_ms)
        .map(microcrab_transport::ResponseCompletedAt::get)
}

fn completion(record: Option<&Value>) -> &'static str {
    let Some(record) = record else {
        return "pending";
    };
    if let Some(outcome) = record["outcome"].as_str() {
        return match outcome {
            "accepted" => "succeeded",
            "cancelled" => "cancelled",
            _ => "failed",
        };
    }
    let status = record["payload"]["status"]
        .as_u64()
        .or_else(|| record["response_status"].as_u64());
    let normalized = &record["payload"]["response"];
    let valid_response = normalized.is_object()
        && normalized.get("error").is_none()
        && (normalized.get("content").is_some() || normalized.get("tool_calls").is_some());
    if status.is_some_and(|s| (200..300).contains(&s)) && valid_response {
        "succeeded"
    } else if status.is_some() || record["event"] == "round_completed" {
        "failed"
    } else {
        "pending"
    }
}
fn persisted_usage(record: &Value) -> Option<Value> {
    if completion(Some(record)) == "pending" {
        return None;
    }
    record
        .get("usage")
        .filter(|v| v.is_object())
        .cloned()
        .or_else(|| {
            record["payload"]
                .get("usage")
                .filter(|v| v.is_object())
                .cloned()
        })
}
pub(super) fn token_delta(current: Option<&Value>, baseline: Option<&Value>) -> Value {
    let mut values = serde_json::Map::new();
    for key in [
        "input_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "output_tokens",
        "total_tokens",
    ] {
        let difference = current
            .and_then(|v| v[key].as_u64())
            .zip(baseline.and_then(|v| v[key].as_u64()))
            .and_then(|(a, b)| i64::try_from(i128::from(a) - i128::from(b)).ok());
        values.insert(
            key.into(),
            difference.map(Value::from).unwrap_or(Value::Null),
        );
    }
    Value::Object(values)
}

/// Tool call IDs have a separate namespace from provider calls. Pair only the
/// same retained position and branch, including overlapping or reused IDs.
fn project_stage(
    record: &Value,
    sequence: u64,
    selector: &AuditQuery,
    stages: &mut Vec<AuditStage>,
    index: &mut HashMap<String, usize>,
) -> Result<(), AuditorError> {
    let event = record["event"].as_str().unwrap_or("");
    if !matches!(event, "tool_call" | "tool_result")
        || record["epoch"].as_u64() != Some(selector.epoch)
        || !record["subagent"].is_null()
        || selector
            .history_head
            .as_ref()
            .is_some_and(|head| record["history_head"].as_str() != Some(head.as_str()))
    {
        return Ok(());
    }
    let Some(tool) = text(record, "tool_call_id") else {
        return Ok(());
    };
    let turn = record["turn_id"].as_u64();
    let round = record["round"].as_u64();
    let history_head = text(record, "history_head");
    let key = serde_json::json!([
        selector.epoch,
        turn,
        round,
        tool,
        history_head,
        record["started_at_unix_ms"]
    ])
    .to_string();
    let slot = if let Some(slot) = index.get(&key) {
        *slot
    } else {
        if stages.len() >= 256 {
            return Err(error(
                "audit stage budget exceeded; select an exact history head",
            ));
        }
        let slot = stages.len();
        stages.push(AuditStage {
            kind: "tool".into(),
            sequence,
            epoch: selector.epoch,
            turn,
            round,
            tool_call_id: Some(tool),
            name: None,
            subagent: None,
            history_head,
            started_at_ms: None,
            ended_at_ms: None,
            completion: "pending".into(),
        });
        index.insert(key, slot);
        slot
    };
    let stage = &mut stages[slot];
    stage.started_at_ms = record["started_at_unix_ms"]
        .as_u64()
        .or(stage.started_at_ms);
    if event == "tool_call" {
        stage.name = text(&record["payload"], "name").or(stage.name.take());
    } else {
        stage.ended_at_ms = stage
            .started_at_ms
            .zip(record["elapsed_ms"].as_u64())
            .and_then(|(start, elapsed)| start.checked_add(elapsed));
        stage.completion = if record["payload"]["type"] != "tool_result" {
            "pending"
        } else if record["payload"]["error"].is_null() {
            "succeeded"
        } else {
            "failed"
        }
        .into();
    }
    Ok(())
}

#[cfg(test)]
mod provider_evidence_tests {
    use super::*;

    #[test]
    fn normalized_response_hides_provider_evidence_without_filtering_tool_arguments() {
        let response = serde_json::json!({"content":"","tool_calls":[{
            "id":"call","name":"lookup","arguments":{"provider_metadata":"ordinary user data"},
            "provider_metadata":{"protocol":"google","thought_signature":"b3BhcXVlLWF1ZGl0"}
        }]});
        for record in [
            serde_json::json!({"payload":{"response":response}}),
            serde_json::json!({"response":response}),
        ] {
            let projected = model_response(&record).unwrap();
            assert!(
                projected["tool_calls"][0]
                    .get("provider_metadata")
                    .is_none()
            );
            assert_eq!(
                projected["tool_calls"][0]["arguments"]["provider_metadata"],
                "ordinary user data"
            );
            assert!(!projected.to_string().contains("b3BhcXVlLWF1ZGl0"));
            assert!(
                record.to_string().contains("b3BhcXVlLWF1ZGl0"),
                "projection must not mutate durable source evidence"
            );
        }
    }
}
