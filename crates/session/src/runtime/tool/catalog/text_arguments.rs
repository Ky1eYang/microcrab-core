//! Explicit schema-directed textual conversion on the dispatch copy only.
//! Never infer a string, select a schema branch, or expand a container from text.
use serde_json::Value;

const MAX_DEPTH: usize = 32;
const MAX_CHANGES: usize = 32;
const MAX_VISITS: usize = 4096;

pub(super) fn convert(schema: &Value, arguments: &mut Value) -> usize {
    let mut remaining = MAX_VISITS;
    let mut changes = MAX_CHANGES;
    walk(schema, arguments, 0, &mut remaining, &mut changes)
}

fn walk(
    schema: &Value,
    value: &mut Value,
    depth: usize,
    remaining: &mut usize,
    changes: &mut usize,
) -> usize {
    if depth > MAX_DEPTH || *remaining == 0 || *changes == 0 {
        return 0;
    }
    *remaining -= 1;
    if [
        "$ref",
        "anyOf",
        "oneOf",
        "allOf",
        "if",
        "not",
        "patternProperties",
        "prefixItems",
    ]
    .iter()
    .any(|key| schema.get(*key).is_some())
    {
        return 0;
    }
    // A registered tool must explicitly authorize coercion, on the leaf. Rules
    // and permission predicates then see the same converted JSON value.
    if let Some(text) = value.as_str()
        && schema.get("x-microcrab-text-input") == Some(&Value::Bool(true))
        && text.len() <= 64
    {
        let converted = match schema.get("type").and_then(Value::as_str) {
            Some("boolean") => match text {
                "true" => Some(Value::Bool(true)),
                "false" => Some(Value::Bool(false)),
                _ => None,
            },
            Some("integer") => serde_json::from_str::<Value>(text).ok().filter(|v| {
                (v.as_i64().is_some() || v.as_u64().is_some()) && v.to_string() == text
            }),
            _ => None,
        };
        if let Some(converted) = converted {
            *value = converted;
            *changes -= 1;
            return 1;
        }
    }
    let mut changed = 0;
    match value {
        Value::Object(fields) => {
            if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
                for (name, value) in fields {
                    if let Some(schema) = properties.get(name) {
                        changed += walk(schema, value, depth + 1, remaining, changes);
                        if *changes == 0 || *remaining == 0 {
                            break;
                        }
                    }
                }
            }
        }
        Value::Array(items) => {
            if let Some(schema) = schema.get("items").filter(|schema| schema.is_object()) {
                for value in items {
                    changed += walk(schema, value, depth + 1, remaining, changes);
                    if *changes == 0 || *remaining == 0 {
                        break;
                    }
                }
            }
        }
        _ => {}
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_explicit_canonical_integer_and_boolean_lexemes_convert() {
        let int = json!({"type":"integer","x-microcrab-text-input":true});
        let schema = json!({"properties":{
            "count":int,"leading":int,"scientific":int,"quote":int,
            "flag":{"type":"boolean","x-microcrab-text-input":true},
            "ref":{"type":"string","x-microcrab-text-input":true},
            "unmarked":{"type":"integer"}
        }});
        let mut args = json!({"count":"42","leading":"042","scientific":"4e1","quote":" 42","flag":"false","ref":9.42e90,"unmarked":"5"});
        assert_eq!(convert(&schema, &mut args), 2);
        assert_eq!(
            args,
            json!({"count":42,"leading":"042","scientific":"4e1","quote":" 42","flag":false,"ref":9.42e90,"unmarked":"5"})
        );
    }

    #[test]
    fn arrays_and_objects_are_walked_only_through_explicit_unambiguous_schema_paths() {
        let schema = json!({"properties":{
            "items":{"items":{"properties":{"flag":{"type":"boolean","x-microcrab-text-input":true}}}},
            "union":{"oneOf":[{"type":"string"},{"type":"integer","x-microcrab-text-input":true}]}
        }});
        let mut args = json!({"items":[{"flag":"true"},{"flag":"false"}],"union":"1"});
        assert_eq!(convert(&schema, &mut args), 2);
        assert_eq!(
            args,
            json!({"items":[{"flag":true},{"flag":false}],"union":"1"})
        );
    }
}
