use microcrab_auditor::AuditorError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    path::Path,
};

pub(crate) const FORMAT: &str = "microcrab-audit-v1";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    Effective,
    Sent,
    #[default]
    Both,
    None,
}
impl Retention {
    fn effective(self) -> bool {
        matches!(self, Self::Effective | Self::Both)
    }
    fn sent(self) -> bool {
        matches!(self, Self::Sent | Self::Both)
    }
}

#[derive(Clone, Debug)]
pub struct RecorderOptions {
    pub retention: Retention,
    pub max_record_bytes: usize,
    pub pending_records: usize,
}
impl Default for RecorderOptions {
    fn default() -> Self {
        Self {
            retention: Retention::Both,
            max_record_bytes: 16 * 1024 * 1024,
            pending_records: 16,
        }
    }
}
impl RecorderOptions {
    pub(crate) fn validate(&self) -> Result<(), AuditorError> {
        if !(1024..=64 * 1024 * 1024).contains(&self.max_record_bytes)
            || !(1..=4096).contains(&self.pending_records)
            || self
                .max_record_bytes
                .checked_mul(self.pending_records)
                .is_none_or(|bytes| bytes > 256 * 1024 * 1024)
        {
            return Err(error("invalid audit record or queue budget"));
        }
        Ok(())
    }
}

/// Prepared effective body after any application rewrite; never confused with sent bytes.
#[derive(Clone, Debug)]
pub struct EffectiveRequest {
    pub call_id: String,
    pub epoch: u64,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub parent_request: Option<String>,
    pub history_head: Option<String>,
    pub subagent: Option<String>,
    pub request_body: String,
}

/// Owned by one blocking operation at a time. The lock guards one authoritative writer,
/// including reopen; an async waiter dropping does not release publication ownership.
pub struct Recorder {
    #[cfg(test)]
    source: microcrab_storage::audit::AuditSource,
    store: Box<dyn microcrab_storage::audit::AuditStore>,
    options: RecorderOptions,
    sequence: u64,
    previous: Option<String>,
    requests: HashMap<String, String>,
    /// Digest identity to `(epoch, subagent)` for every retained effective request,
    /// so the auditor rebuilds its branch map without a second traversal.
    scopes: HashMap<String, (u64, Option<String>)>,
    fault: Option<AuditorError>,
    pub(super) observer: Option<super::CommitObserver>,
}
pub(crate) fn error(message: impl Into<String>) -> AuditorError {
    AuditorError(microcrab_protocol::diagnostic::sanitize(
        &message.into(),
        2048,
    ))
}
fn storage_error(error: microcrab_session::Error) -> AuditorError {
    super::recorder::error(error.diagnostic_reason())
}
pub(crate) fn io_error(source: std::io::Error) -> AuditorError {
    error(format!(
        "audit filesystem operation failed: {}; kind={:?}; os_error={:?}",
        microcrab_protocol::diagnostic::describe(&source, 1800),
        source.kind(),
        source.raw_os_error()
    ))
}
pub(crate) fn json_error(source: serde_json::Error) -> AuditorError {
    error(format!(
        "invalid audit JSON: {}",
        microcrab_protocol::diagnostic::json_reason(&source)
    ))
}
pub(crate) fn hash(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

/// Python v3 uses recursively sorted compact UTF8 JSON. Map ordering is explicit,
/// independent of serde_json's preserve_order feature selected by an embedder.
pub(crate) fn canonical(value: &Value) -> Result<Vec<u8>, AuditorError> {
    fn encode(value: &Value, out: &mut Vec<u8>) -> Result<(), AuditorError> {
        match value {
            Value::Object(map) => {
                out.push(b'{');
                let mut keys = map.keys().collect::<Vec<_>>();
                keys.sort();
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    out.extend(serde_json::to_vec(key).map_err(json_error)?);
                    out.push(b':');
                    encode(&map[key], out)?;
                }
                out.push(b'}');
            }
            Value::Array(items) => {
                out.push(b'[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    encode(item, out)?;
                }
                out.push(b']');
            }
            Value::Number(number) if number.is_f64() => {
                out.extend(python_float(number)?.as_bytes())
            }
            _ => out.extend(serde_json::to_vec(value).map_err(json_error)?),
        }
        Ok(())
    }
    let mut bytes = vec![];
    encode(value, &mut bytes)?;
    Ok(bytes)
}
pub(crate) fn python_float(number: &serde_json::Number) -> Result<String, AuditorError> {
    let value = number
        .as_f64()
        .ok_or_else(|| error("invalid audit float"))?;
    let text = number.to_string();
    let magnitude = value.abs();
    if magnitude == 0.0 {
        return Ok(if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .into());
    }
    let scientific = !(0.0001..1e16).contains(&magnitude);
    let (negative, unsigned) = if let Some(text) = text.strip_prefix('-') {
        ("-", text)
    } else {
        ("", text.as_str())
    };
    let (mut digits, mut exponent) = if let Some((mantissa, exponent)) = unsigned.split_once('e') {
        let exponent: i32 = exponent.parse().map_err(|_| error("invalid audit float"))?;
        (mantissa.replace('.', ""), exponent)
    } else {
        let decimal = unsigned.find('.').unwrap_or(unsigned.len());
        let all = unsigned.replace('.', "");
        let first = all
            .find(|c: char| c != '0')
            .ok_or_else(|| error("invalid audit float"))?;
        (all[first..].into(), decimal as i32 - first as i32 - 1)
    };
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }
    if scientific {
        let mantissa = if digits.len() == 1 {
            digits
        } else {
            format!("{}.{}", &digits[..1], &digits[1..])
        };
        Ok(format!(
            "{negative}{mantissa}e{}{:02}",
            if exponent < 0 { "-" } else { "+" },
            exponent.abs()
        ))
    } else {
        exponent += 1;
        let rendered = if exponent <= 0 {
            format!("0.{}{}", "0".repeat((-exponent) as usize), digits)
        } else if exponent as usize >= digits.len() {
            format!(
                "{}{}.0",
                digits,
                "0".repeat(exponent as usize - digits.len())
            )
        } else {
            format!(
                "{}.{}",
                &digits[..exponent as usize],
                &digits[exponent as usize..]
            )
        };
        Ok(format!("{negative}{rendered}"))
    }
}

pub(crate) fn valid_hash(value: &str) -> Result<(), AuditorError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(error("invalid audit identity"));
    }
    Ok(())
}
fn body(value: &Value) -> &Value {
    value.get("body").unwrap_or(value)
}

impl Recorder {
    pub fn open(
        directory: impl AsRef<Path>,
        options: RecorderOptions,
    ) -> Result<Self, AuditorError> {
        options.validate()?;
        let source = microcrab_storage::audit::AuditSource::File(directory.as_ref().to_owned());
        Self::open_source(source, options)
    }
    pub fn open_source(
        source: microcrab_storage::audit::AuditSource,
        options: RecorderOptions,
    ) -> Result<Self, AuditorError> {
        options.validate()?;
        let store = source.open().map_err(storage_error)?;
        let mut recorder = Self {
            #[cfg(test)]
            source,
            store,
            options,
            sequence: 0,
            previous: None,
            requests: HashMap::new(),
            scopes: HashMap::new(),
            fault: None,
            observer: None,
        };
        if !recorder.replay()? {
            recorder.append(
                json!({"kind":"format","format":FORMAT,"retention":recorder.options.retention}),
            )?;
        }
        Ok(recorder)
    }
    /// Verify the durable chain and rebuild only the indexes later records need, in
    /// one pass over raw line bytes. Record bodies are never materialized here.
    fn replay(&mut self) -> Result<bool, AuditorError> {
        let file = self.store.reader().map_err(storage_error)?;
        let mut reader = BufReader::new(file);
        loop {
            let mut line = vec![];
            use std::io::Read;
            reader
                .by_ref()
                .take(self.options.max_record_bytes as u64 + 1)
                .read_until(b'\n', &mut line)
                .map_err(io_error)?;
            if line.is_empty() {
                break;
            }
            if line.len() > self.options.max_record_bytes || !line.ends_with(b"\n") {
                return Err(error("oversized record or incomplete audit tail"));
            }
            line.pop();
            let row: ReplayRow = serde_json::from_slice(&line).map_err(json_error)?;
            verify_line(&line, &row.sha256)?;
            if row.sequence != self.sequence + 1 || row.previous != self.previous {
                return Err(error("audit sequence/ancestry mismatch"));
            }
            if self.sequence == 0
                && (row.data.format.as_deref() != Some(FORMAT)
                    || row.data.retention != Some(self.options.retention))
            {
                return Err(error("audit format or retention mismatch"));
            }
            self.sequence = row.sequence;
            self.previous = Some(row.sha256.clone());
            self.index(&row);
        }
        Ok(self.sequence != 0)
    }
    /// Digest identity to `(epoch, subagent)` for every retained effective request.
    pub(crate) fn effective_scopes(&self) -> &HashMap<String, (u64, Option<String>)> {
        &self.scopes
    }
    fn index(&mut self, row: &ReplayRow) {
        match row.data.kind.as_str() {
            "effective" => {
                if let Some(call) = &row.data.call_id {
                    self.requests.insert(call.clone(), row.sha256.clone());
                    self.scopes.insert(
                        row.sha256.clone(),
                        (row.data.epoch.unwrap_or(0), row.data.subagent.clone()),
                    );
                }
            }
            "published" => {
                let record = row.data.record.as_ref();
                if record.is_some_and(|record| record.outcome.is_some())
                    && let Some(call) = record.and_then(|record| record.call_id.as_ref())
                {
                    self.requests.remove(call);
                }
            }
            _ => {}
        }
    }
    pub fn request_head(&self, call_id: &str) -> Option<String> {
        self.requests.get(call_id).cloned()
    }
    pub fn effective(&mut self, request: EffectiveRequest) -> Result<String, AuditorError> {
        if request.call_id.is_empty() || request.call_id.len() > 1024 {
            return Err(error("invalid audit call identity"));
        }
        for identity in [&request.parent_request, &request.history_head]
            .into_iter()
            .flatten()
        {
            valid_hash(identity)?;
        }
        let epoch = request.epoch;
        let subagent = request.subagent.clone();
        let mut data = json!({"kind":"effective","call_id":request.call_id,"epoch":request.epoch,"turn":request.turn,"round":request.round,"parent_request":request.parent_request,"history_head":request.history_head,"subagent":request.subagent});
        if self.options.retention.effective() {
            data["request_body"] = json!(request.request_body);
        }
        let digest = self.append(data)?;
        self.requests.insert(request.call_id, digest.clone());
        self.scopes.insert(digest.clone(), (epoch, subagent));
        Ok(digest)
    }
    pub fn publish(&mut self, mut record: Value) -> Result<(), AuditorError> {
        let body = if record.get("body").is_some() {
            record.get_mut("body").expect("checked body")
        } else {
            &mut record
        };
        let body = body
            .as_object_mut()
            .ok_or_else(|| error("audit publication must be an object"))?;
        let sent = body.remove("sent_request").filter(|value| !value.is_null());
        body.remove("request_diff");
        if self.options.retention.sent()
            && let Some(sent) = &sent
        {
            body.insert("sent_request".into(), sent.clone());
        }
        let call = body
            .get("call_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let complete = body.get("outcome").is_some_and(|value| !value.is_null());
        let identity = call
            .as_ref()
            .and_then(|call| self.requests.get(call))
            .cloned();
        let mut data = json!({"kind":"published","record":record});
        if let Some(identity) = &identity {
            data["effective_call_id"] = json!(call);
            data["request_hash"] = json!(identity);
        }
        let result = self.append(data).map(|_| ());
        if complete && let Some(call) = call {
            self.requests.remove(&call);
        }
        result
    }
    /// Durability barrier: every record already reached disk in `append`, so this
    /// surfaces sticky writer faults. Readable projections are not rewritten;
    /// the selected store remains the authority.
    pub fn flush(&mut self) -> Result<(), AuditorError> {
        self.ensure_healthy()?;
        if let Err(failure) = self.store.flush().map_err(storage_error) {
            self.fault = Some(failure.clone());
            return Err(failure);
        }
        Ok(())
    }
    fn ensure_healthy(&self) -> Result<(), AuditorError> {
        self.fault.clone().map_or(Ok(()), Err)
    }
    fn append(&mut self, data: Value) -> Result<String, AuditorError> {
        self.ensure_healthy()?;
        let mut row = json!({"sequence":self.sequence.checked_add(1).ok_or_else(||error("audit sequence overflow"))?,"previous":self.previous,"data":data});
        let digest = hash(canonical(&row)?);
        row["sha256"] = json!(digest);
        let mut bytes = canonical(&row)?;
        bytes.push(b'\n');
        let result = (|| {
            if bytes.len() > self.options.max_record_bytes {
                return Err(error("audit record byte budget exceeded"));
            }
            self.store
                .append(
                    self.sequence,
                    self.previous.as_deref(),
                    self.sequence + 1,
                    &digest,
                    &bytes,
                )
                .map_err(storage_error)?;
            Ok(())
        })();
        if let Err(error) = result {
            // The sticky fault stops later audit writes. Record it once here so the
            // condition is not silent; no record body or credential is logged.
            tracing::warn!(reason = %microcrab_protocol::diagnostic::describe(&error,2048), "audit writer faulted and stops accepting records");
            self.fault = Some(error.clone());
            return Err(error);
        }
        self.sequence += 1;
        self.previous = Some(digest.clone());
        if let Some(observer) = &self.observer {
            let data = &row["data"];
            let effective = data["kind"] == "effective";
            let record = if effective {
                data
            } else {
                body(&data["record"])
            };
            let request_hash = if effective {
                Some(digest.clone())
            } else {
                data["request_hash"].as_str().map(str::to_owned)
            };
            if request_hash.is_some() {
                observer(super::AuditCommit {
                    sequence: self.sequence,
                    request_hash,
                    call_id: record["call_id"].as_str().map(str::to_owned),
                    epoch: record["epoch"].as_u64(),
                    turn: record["turn"]
                        .as_u64()
                        .or_else(|| record["turn_id"].as_u64()),
                    round: record["round"].as_u64(),
                    subagent: record["subagent"].as_str().map(str::to_owned),
                    kind: if effective {
                        "effective".into()
                    } else if record["sent_request"].is_string()
                        || record["event"] == "request_prepared"
                    {
                        "sent".into()
                    } else {
                        "published".into()
                    },
                });
            }
        }
        Ok(digest)
    }
    /// Full record materialization for assertions; the runtime path uses `replay`,
    /// which never allocates record bodies.
    #[cfg(test)]
    pub(crate) fn rows(&self) -> Result<Vec<Value>, AuditorError> {
        let file = self
            .source
            .reader()
            .map_err(storage_error)?
            .ok_or_else(|| error("audit authority disappeared"))?;
        let mut reader = BufReader::new(file);
        let mut rows = vec![];
        loop {
            let mut line = vec![];
            use std::io::Read;
            reader
                .by_ref()
                .take(self.options.max_record_bytes as u64 + 1)
                .read_until(b'\n', &mut line)
                .map_err(io_error)?;
            if line.is_empty() {
                break;
            }
            if line.len() > self.options.max_record_bytes || !line.ends_with(b"\n") {
                return Err(error("oversized record or incomplete audit tail"));
            }
            rows.push(serde_json::from_slice(&line).map_err(json_error)?);
        }
        Ok(rows)
    }
}

/// Narrow replay target: only the fields later records need, so large bodies are
/// scanned without being allocated during open.
#[derive(Deserialize)]
struct ReplayRow {
    sequence: u64,
    #[serde(default)]
    previous: Option<String>,
    sha256: String,
    data: ReplayData,
}
#[derive(Deserialize)]
struct ReplayData {
    kind: String,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    retention: Option<Retention>,
    #[serde(default)]
    call_id: Option<String>,
    #[serde(default)]
    epoch: Option<u64>,
    #[serde(default)]
    subagent: Option<String>,
    #[serde(default)]
    record: Option<ReplayRecord>,
}
#[derive(Deserialize)]
struct ReplayRecord {
    #[serde(default)]
    call_id: Option<String>,
    #[serde(default)]
    outcome: Option<serde::de::IgnoredAny>,
}
const DIGEST_KEY: &[u8] = b",\"sha256\":\"";

/// `sha256` sorts last among `data`/`previous`/`sequence`/`sha256`, so the stored
/// line is the canonical object with its closing brace replaced by the digest
/// member. The digest input is therefore this prefix plus that one brace.
pub(super) fn digest_prefix(line: &[u8]) -> Option<(&[u8], &str)> {
    let body = line.strip_suffix(&b"\"}"[..])?;
    let at = body.len().checked_sub(64)?;
    let digest = std::str::from_utf8(&body[at..]).ok()?;
    if !digest
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    body[..at]
        .strip_suffix(DIGEST_KEY)
        .map(|payload| (payload, digest))
}

/// Digest of one record over the stored prefix plus the object's closing brace.
pub(super) fn hash_record(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hasher.update(b"}");
    format!("{:x}", hasher.finalize())
}

/// Prefer raw-byte verification; fall back to the canonical form for bytes this
/// writer would not have produced, so verification never silently weakens.
fn verify_line(line: &[u8], claimed: &str) -> Result<(), AuditorError> {
    if let Some((payload, digest)) = digest_prefix(line) {
        if digest != claimed || hash_record(payload) != claimed {
            return Err(error("audit integrity mismatch"));
        }
        return Ok(());
    }
    let value: Value = serde_json::from_slice(line).map_err(json_error)?;
    let mut checked = value
        .as_object()
        .cloned()
        .ok_or_else(|| error("invalid audit record"))?;
    let digest = checked
        .remove("sha256")
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| error("missing audit integrity hash"))?;
    if digest != claimed || hash(canonical(&Value::Object(checked))?) != claimed {
        return Err(error("audit integrity mismatch"));
    }
    Ok(())
}
