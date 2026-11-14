//! Shared strict replay of new keyed metadata patches and historical top-level patches.
use crate::StateFrame;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

fn identity(row: &Value, id_field: &str) -> Result<(String, u64), &'static str> {
    let session = row
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("metadata delta requires session_id")?;
    let id = row
        .get(id_field)
        .and_then(Value::as_u64)
        .filter(|id| *id > 0)
        .ok_or("metadata delta requires positive row id")?;
    Ok((session.to_owned(), id))
}

/// Build changes for metadata rows only. Unchanged siblings and fields are omitted.
/// None means the caller should use a normal property replacement (non-array input).
pub fn keyed_state_delta(
    before: Option<&Value>,
    after: &Value,
    id_field: &str,
) -> Result<Option<Value>, &'static str> {
    if let Some(after_rows) = after.as_object() {
        let before_rows = match before {
            Some(Value::Object(rows)) => rows.clone(),
            None => Map::new(),
            _ => return Ok(None),
        };
        let old = Value::Array(before_rows.values().cloned().collect());
        let new = Value::Array(after_rows.values().cloned().collect());
        let mut keys = BTreeMap::new();
        for (key, row) in before_rows.iter().chain(after_rows.iter()) {
            keys.insert(identity(row, id_field)?, key.clone());
        }
        let mut delta =
            keyed_state_delta(Some(&old), &new, id_field)?.ok_or("invalid job collection delta")?;
        for kind in ["upsert", "remove"] {
            for row in delta[kind].as_array_mut().ok_or("invalid job delta")? {
                let key = keys
                    .get(&identity(row, id_field)?)
                    .ok_or("missing job metadata key")?
                    .clone();
                row.as_object_mut()
                    .ok_or("invalid job delta row")?
                    .insert("key".into(), Value::String(key));
            }
        }
        return Ok(Some(delta));
    }
    let Some(after) = after.as_array() else {
        return Ok(None);
    };
    let empty = Vec::new();
    let before = match before {
        None => &empty,
        Some(Value::Array(rows)) => rows,
        _ => return Ok(None),
    };
    let mut old = BTreeMap::new();
    for row in before {
        if old.insert(identity(row, id_field)?, row).is_some() {
            return Err("duplicate metadata row");
        }
    }
    let mut seen = BTreeSet::new();
    let mut upsert = Vec::new();
    for row in after {
        let key = identity(row, id_field)?;
        if !seen.insert(key.clone()) {
            return Err("duplicate metadata row");
        }
        let current = row.as_object().ok_or("metadata row must be an object")?;
        let previous = old.get(&key).and_then(|row| row.as_object());
        let set = current
            .iter()
            .filter(|(name, value)| {
                name.as_str() != "session_id"
                    && name.as_str() != id_field
                    && previous.and_then(|row| row.get(*name)) != Some(*value)
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Map<_, _>>();
        let remove = previous
            .into_iter()
            .flat_map(|row| row.keys())
            .filter(|name| {
                name.as_str() != "session_id"
                    && name.as_str() != id_field
                    && !current.contains_key(*name)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !set.is_empty() || !remove.is_empty() || !old.contains_key(&key) {
            let mut change = Map::new();
            change.insert("session_id".into(), Value::String(key.0));
            change.insert(id_field.into(), Value::from(key.1));
            change.insert("set".into(), Value::Object(set));
            change.insert("remove".into(), serde_json::json!(remove));
            upsert.push(Value::Object(change));
        }
    }
    let remove = old
        .keys()
        .filter(|key| !seen.contains(*key))
        .map(|key| {
            let mut row = Map::new();
            row.insert("session_id".into(), Value::String(key.0.clone()));
            row.insert(id_field.into(), Value::from(key.1));
            Value::Object(row)
        })
        .collect::<Vec<_>>();
    Ok(Some(serde_json::json!({"upsert":upsert,"remove":remove})))
}

fn apply_rows(
    state: &mut Map<String, Value>,
    field: &str,
    id_field: &str,
    delta: &Value,
) -> Result<(), &'static str> {
    if field == "jobs" {
        let original = state
            .get(field)
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new()));
        let rows = original
            .as_object()
            .ok_or("job delta target must be keyed object")?;
        let mut keys = BTreeMap::new();
        let mut used = BTreeSet::new();
        for (key, row) in rows {
            keys.insert(identity(row, id_field)?, key.clone());
            used.insert(key.clone());
        }
        let mut normalized = delta.clone();
        for kind in ["remove", "upsert"] {
            for row in normalized[kind]
                .as_array_mut()
                .ok_or("invalid job delta array")?
            {
                let id = identity(row, id_field)?;
                let key = row
                    .get("key")
                    .and_then(Value::as_str)
                    .filter(|key| !key.is_empty())
                    .ok_or("job delta requires key")?
                    .to_owned();
                if let Some(existing) = keys.get(&id) {
                    if existing != &key {
                        return Err("job delta changed row key");
                    }
                } else if used.contains(&key) {
                    return Err("job delta key collision");
                }
                if kind == "upsert" {
                    keys.insert(id, key.clone());
                    used.insert(key);
                }
                row.as_object_mut()
                    .ok_or("invalid job delta row")?
                    .remove("key");
            }
        }
        let mut temporary = Map::new();
        temporary.insert(
            "tool_tasks".into(),
            Value::Array(rows.values().cloned().collect()),
        );
        apply_rows(&mut temporary, "tool_tasks", id_field, &normalized)?;
        let mut result = Map::new();
        for row in temporary["tool_tasks"]
            .as_array()
            .ok_or("invalid normalized jobs")?
        {
            result.insert(
                keys.get(&identity(row, id_field)?)
                    .ok_or("missing job row key")?
                    .clone(),
                row.clone(),
            );
        }
        state.insert(field.into(), Value::Object(result));
        return Ok(());
    }
    let object = delta.as_object().ok_or("metadata delta must be object")?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "upsert" | "remove"))
    {
        return Err("unknown metadata delta property");
    }
    let upsert = object
        .get("upsert")
        .and_then(Value::as_array)
        .ok_or("metadata delta requires upsert array")?;
    let remove = object
        .get("remove")
        .and_then(Value::as_array)
        .ok_or("metadata delta requires remove array")?;
    let rows = state
        .entry(field)
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or("metadata delta target is not an array")?;
    let mut keyed = BTreeMap::new();
    for row in rows.iter() {
        if keyed
            .insert(identity(row, id_field)?, row.clone())
            .is_some()
        {
            return Err("duplicate existing metadata row");
        }
    }
    let mut touched = BTreeSet::new();
    for row in remove {
        let key = identity(row, id_field)?;
        if !touched.insert(key.clone()) {
            return Err("duplicate metadata delta identity");
        }
        if row.as_object().is_none_or(|row| row.len() != 2) {
            return Err("invalid metadata removal");
        }
        keyed.remove(&key);
    }
    for change in upsert {
        let key = identity(change, id_field)?;
        if !touched.insert(key.clone()) {
            return Err("overlapping metadata delta identity");
        }
        let object = change.as_object().ok_or("metadata change must be object")?;
        if object.len() != 4 {
            return Err("invalid metadata change properties");
        }
        let set = object
            .get("set")
            .and_then(Value::as_object)
            .ok_or("metadata change requires set")?;
        let remove = object
            .get("remove")
            .and_then(Value::as_array)
            .ok_or("metadata change requires remove")?;
        let row = keyed
            .entry(key.clone())
            .or_insert_with(|| {
                let mut row = Map::new();
                row.insert("session_id".into(), Value::String(key.0));
                row.insert(id_field.into(), Value::from(key.1));
                Value::Object(row)
            })
            .as_object_mut()
            .ok_or("metadata row must be object")?;
        let mut fields = BTreeSet::new();
        for name in set
            .keys()
            .map(String::as_str)
            .chain(remove.iter().map(|value| value.as_str().unwrap_or("")))
        {
            if name.is_empty() || name == "session_id" || name == id_field || !fields.insert(name) {
                return Err("invalid overlapping metadata fields");
            }
        }
        for name in remove {
            row.remove(name.as_str().ok_or("metadata removal must be string")?);
        }
        row.extend(set.clone());
    }
    *rows = keyed.into_values().collect();
    Ok(())
}

/// Atomic replay for server history, Context recovery, and embedders. Historical
/// frames retain their original serde bytes; only new frames contain keyed deltas.
pub fn apply_state_patch(state: &mut Value, frame: &StateFrame) -> Result<(), &'static str> {
    let mut candidate = state.clone();
    match frame {
        StateFrame::StateSnapshot { state: next } => {
            if !next.is_object() {
                return Err("state snapshot must be object");
            }
            candidate = next.clone();
        }
        StateFrame::StatePatch {
            set,
            remove,
            execution_delta,
            task_delta,
            job_delta,
        } => {
            let object = candidate
                .as_object_mut()
                .ok_or("state patch precedes snapshot")?;
            let mut keys = BTreeSet::new();
            for name in set.keys().chain(remove.iter()) {
                if !keys.insert(name) {
                    return Err("overlapping state patch");
                }
            }
            for (field, delta) in [
                ("executions", execution_delta),
                ("tool_tasks", task_delta),
                ("jobs", job_delta),
            ] {
                if delta.is_some()
                    && (set.contains_key(field) || remove.iter().any(|name| name == field))
                {
                    return Err("overlapping metadata collection patch");
                }
            }
            for name in remove {
                object.remove(name);
            }
            object.extend(set.clone());
            if let Some(delta) = execution_delta {
                apply_rows(object, "executions", "execution_id", delta)?;
            }
            if let Some(delta) = task_delta {
                apply_rows(object, "tool_tasks", "task_id", delta)?;
            }
            if let Some(delta) = job_delta {
                apply_rows(object, "jobs", "job_id", delta)?;
            }
        }
    }
    *state = candidate;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn sibling_and_unchanged_fields_are_not_resent_and_replay_is_atomic() {
        let before = json!({"tool_tasks":[{"session_id":"s","task_id":1,"status":"succeeded","output":{"bytes":42}},{"session_id":"s","task_id":2,"status":"running","tool":"long"}]});
        let after = json!([{"session_id":"s","task_id":1,"status":"succeeded","output":{"bytes":42}},{"session_id":"s","task_id":2,"status":"failed","tool":"long"}]);
        let delta = keyed_state_delta(before.get("tool_tasks"), &after, "task_id")
            .unwrap()
            .unwrap();
        assert_eq!(
            delta,
            json!({"upsert":[{"session_id":"s","task_id":2,"set":{"status":"failed"},"remove":[]}],"remove":[]})
        );
        let frame = StateFrame::StatePatch {
            set: Map::new(),
            remove: vec![],
            execution_delta: None,
            task_delta: Some(delta),
            job_delta: None,
        };
        let mut projected = before.clone();
        apply_state_patch(&mut projected, &frame).unwrap();
        assert_eq!(projected["tool_tasks"], after);
        let invalid = StateFrame::StatePatch {
            set: Map::new(),
            remove: vec![],
            execution_delta: None,
            task_delta: Some(
                json!({"upsert":[{"session_id":"s","task_id":2,"set":{"task_id":8},"remove":[]}],"remove":[]}),
            ),
            job_delta: None,
        };
        assert!(apply_state_patch(&mut projected, &invalid).is_err());
        assert_eq!(projected["tool_tasks"], after);
    }
    #[test]
    fn execution_delta_updates_only_changed_metadata_and_preserves_legacy_bytes() {
        let before = json!({"executions":[{"session_id":"s","execution_id":1,"status":"running","tool":"custom","output":{"start_cursor":0,"end_cursor":12}}]});
        let after = json!([{"session_id":"s","execution_id":1,"status":"succeeded","tool":"custom","output":{"start_cursor":0,"end_cursor":12}}]);
        let delta = keyed_state_delta(before.get("executions"), &after, "execution_id")
            .unwrap()
            .unwrap();
        assert_eq!(
            delta,
            json!({"upsert":[{"session_id":"s","execution_id":1,"set":{"status":"succeeded"},"remove":[]}],"remove":[]})
        );
        let frame = StateFrame::StatePatch {
            set: Map::new(),
            remove: vec![],
            execution_delta: Some(delta),
            task_delta: None,
            job_delta: None,
        };
        let mut projected = before;
        apply_state_patch(&mut projected, &frame).unwrap();
        assert_eq!(projected["executions"], after);
        let legacy =
            r#"{"type":"state_patch","set":{},"remove":[],"task_delta":{"upsert":[],"remove":[]}}"#;
        let frame: StateFrame = serde_json::from_str(legacy).unwrap();
        assert_eq!(serde_json::to_string(&frame).unwrap(), legacy);
    }

    #[test]
    fn historical_frame_bytes_and_row_removal_are_preserved() {
        let old = r#"{"type":"state_patch","set":{"x":1},"remove":[]}"#;
        let frame: StateFrame = serde_json::from_str(old).unwrap();
        assert_eq!(serde_json::to_string(&frame).unwrap(), old);
        let before = json!([{"session_id":"s","task_id":1,"status":"running"}]);
        let delta = keyed_state_delta(Some(&before), &json!([]), "task_id")
            .unwrap()
            .unwrap();
        let mut state = json!({"tool_tasks":before});
        apply_state_patch(
            &mut state,
            &StateFrame::StatePatch {
                set: Map::new(),
                remove: vec![],
                execution_delta: None,
                task_delta: Some(delta),
                job_delta: None,
            },
        )
        .unwrap();
        assert_eq!(state, json!({"tool_tasks":[]}));
    }
}
