//! Checked native usage primitives. Each adapter selects its own schema first.
use crate::Error;
use serde_json::Value;

pub(crate) fn count(value: &Value, key: &str) -> Result<Option<u64>, Error> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| Error::Protocol(format!("invalid native usage count: {key}").into())),
    }
}
pub(crate) fn details<'a>(value: &'a Value, key: &str) -> Result<&'a Value, Error> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(&Value::Null),
        Some(v) if v.is_object() => Ok(v),
        _ => Err(Error::Protocol(
            format!("invalid native usage details: {key}").into(),
        )),
    }
}
pub(crate) fn add(left: Option<u64>, right: Option<u64>) -> Result<Option<u64>, Error> {
    left.zip(right)
        .map(|(a, b)| {
            a.checked_add(b)
                .ok_or_else(|| Error::protocol("native usage count overflow"))
        })
        .transpose()
}
pub(crate) fn subtract(left: Option<u64>, right: Option<u64>) -> Result<Option<u64>, Error> {
    left.zip(right)
        .map(|(a, b)| {
            a.checked_sub(b)
                .ok_or_else(|| Error::protocol("native usage component exceeds total"))
        })
        .transpose()
}
