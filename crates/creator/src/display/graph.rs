//! Restore stream byte counters from immutable records; bodies are paged and dropped.
use super::*;
pub(super) fn offsets(
    source: &DisplaySource,
    high: u64,
) -> Result<HashMap<(String, String), u64>, Error> {
    let mut offsets = HashMap::new();
    let mut after = 0;
    loop {
        let page = source
            .query(after, Some(high), 128)?
            .ok_or_else(|| Error::audit("display archive disappeared while restoring offsets"))?;
        for record in page.records {
            let stream = match record.event["phase"].as_str() {
                Some("reasoning_delta") => Some(("reasoning".into(), "text")),
                Some("text_delta") => Some(("text".into(), "text")),
                Some("tool_call_delta") => {
                    Some((format!("tool:{}", record.event["index"]), "arguments"))
                }
                _ => None,
            };
            if let Some((kind, field)) = stream {
                let key = (record.attempt_id.clone(), kind);
                let offset = offsets.get(&key).copied().unwrap_or(0u64);
                if record.event["stream_offset"].as_u64() != Some(offset) {
                    return Err(Error::audit("display archived stream offset mismatch"));
                }
                let bytes = record.event[field]
                    .as_str()
                    .ok_or_else(|| Error::audit("display archived stream content is not text"))?
                    .len() as u64;
                offsets.insert(
                    key,
                    offset
                        .checked_add(bytes)
                        .ok_or_else(|| Error::audit("display archived stream offset exhausted"))?,
                );
            }
            if matches!(
                record.event["phase"].as_str(),
                Some("validated" | "invalidated")
            ) {
                offsets.retain(|(attempt, _), _| attempt != &record.attempt_id);
            }
        }
        match page.next_cursor {
            Some(next) => after = next,
            None => return Ok(offsets),
        }
    }
}
