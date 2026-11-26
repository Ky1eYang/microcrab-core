//! Tool-declared bindings to exact, admitted values, independent of tool names.
use super::{ToolContext, admission_error};
use crate::Error;
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Deserialize)]
#[serde(transparent)]
pub(super) struct ArgumentPointer(String);

impl ArgumentPointer {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if !self.0.starts_with('/') || self.0.len() > 256 {
            return Err(Error::wasm("invalid capability argument pointer"));
        }
        let mut bytes = self.0.bytes();
        while let Some(byte) = bytes.next() {
            if byte == b'~' && !matches!(bytes.next(), Some(b'0' | b'1')) {
                return Err(Error::wasm("invalid capability argument pointer escape"));
            }
        }
        Ok(())
    }

    pub(super) fn bind<'a>(
        &self,
        context: &'a ToolContext,
        dispatched: &Value,
    ) -> Result<&'a str, Error> {
        self.bind_values(
            context.recorded_arguments().map_err(admission_error)?,
            dispatched,
        )
    }

    fn bind_values<'a>(&self, recorded: &'a Value, dispatched: &Value) -> Result<&'a str, Error> {
        recorded
            .pointer(&self.0)
            .and_then(Value::as_str)
            .filter(|value| dispatched.pointer(&self.0).and_then(Value::as_str) == Some(*value))
            .ok_or_else(|| Error::denied("capability argument must match the exact admitted string"))
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct ToolBindings {
    pub(super) web: Option<WebBinding>,
    pub(super) exact_arguments: Vec<ArgumentPointer>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum WebBinding {
    Open {
        endpoint: ArgumentPointer,
        #[serde(default)]
        handoff: bool,
    },
    Existing {
        connection: ArgumentPointer,
        #[serde(default)]
        send: bool,
    },
}

impl ToolBindings {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.exact_arguments.len() > 16 {
            return Err(Error::wasm("too many exact capability argument bindings"));
        }
        for pointer in &self.exact_arguments {
            pointer.validate()?;
        }
        if let Some(web) = &self.web {
            match web {
                WebBinding::Open { endpoint, .. } => endpoint.validate()?,
                WebBinding::Existing { connection, .. } => connection.validate()?,
            }
        }
        Ok(())
    }

    pub(super) fn bind_exact(
        &self,
        context: &ToolContext,
        dispatched: &Value,
    ) -> Result<(), Error> {
        if self.exact_arguments.is_empty() {
            return Ok(());
        }
        let recorded = context.recorded_arguments().map_err(admission_error)?;
        for pointer in &self.exact_arguments {
            if recorded.pointer(&pointer.0).is_some() || dispatched.pointer(&pointer.0).is_some() {
                pointer.bind_values(recorded, dispatched)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bindings_require_a_well_formed_pointer_and_exact_dispatch_value() {
        for pointer in ["", "endpoint", "/invalid~", "/invalid~3"] {
            assert!(ArgumentPointer(pointer.into()).validate().is_err());
        }
        let pointer = ArgumentPointer("/destinations/a~1b~0c".into());
        pointer.validate().unwrap();
        let recorded = json!({"destinations":{"a/b~c":"original"},"unrelated":"other"});
        assert_eq!(pointer.bind_values(&recorded, &recorded).unwrap(), "original");
        let changed = json!({"destinations":{"a/b~c":"other"},"unrelated":"original"});
        assert!(pointer.bind_values(&recorded, &changed).is_err());
    }
}
