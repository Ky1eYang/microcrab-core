//! Safe corrective guidance for type-invalid null values, without instance disclosure.
use super::schema_reason;
use serde_json::{Value, json};

#[test]
fn forbidden_null_gets_actionable_guidance_without_instance_values() {
    let schema = json!({
        "type": "object",
        "required": ["artifact"],
        "properties": {"artifact": {"type": "string"}}
    });
    let validator = jsonschema::validator_for(&schema).unwrap();
    let arguments = json!({"artifact": null, "private": "PRIVATE_SCHEMA_SENTINEL"});
    let before = arguments.clone();
    let reason = schema_reason(&validator.validate(&arguments).unwrap_err());
    assert!(reason.contains("expected type"));
    assert!(reason.contains("explicit null is invalid"));
    assert!(reason.contains("required non-null value"));
    assert!(reason.contains("omit an unused optional field"));
    assert!(reason.contains("/properties/artifact/type"));
    assert!(!reason.contains("PRIVATE_SCHEMA_SENTINEL"));
    assert_eq!(arguments, before);
}

#[test]
fn nullable_values_and_missing_or_wrong_type_fields_keep_distinct_semantics() {
    let schema = json!({
        "type": "object",
        "required": ["artifact"],
        "properties": {
            "artifact": {"type": "string"},
            "optional": {"type": "string"},
            "nullable": {"type": ["null", "string"]},
            "arguments": {"type": "object"}
        }
    });
    let validator = jsonschema::validator_for(&schema).unwrap();
    assert!(
        validator
            .validate(&json!({
                "artifact": "exact", "nullable": null, "arguments": {"business": null}
            }))
            .is_ok()
    );
    for value in [json!({}), json!({"artifact": 42}), json!({"artifact": []})] {
        let reason = schema_reason(&validator.validate(&value).unwrap_err());
        assert!(!reason.contains("explicit null"), "{reason}");
    }
    let optional_null = json!({"artifact": "exact", "optional": null});
    let reason = schema_reason(&validator.validate(&optional_null).unwrap_err());
    assert!(reason.contains("explicit null"));
    assert!(reason.contains("/properties/optional/type"));
    assert_eq!(optional_null["optional"], Value::Null);
}
