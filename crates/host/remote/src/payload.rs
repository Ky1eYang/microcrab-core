//! Structural response validation without promoting serde's value-bearing Display.
//! The adapter lets trusted Deserialize implementations report closed missing-field
//! and expected-type facts. Nested maps/sequences retain this error type as well.
use microcrab_host_api::ToolError;
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
use std::fmt;

#[derive(Debug)]
pub(crate) struct PayloadError(&'static str);
impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for PayloadError {}
impl de::Error for PayloadError {
    fn custom<T: fmt::Display>(_: T) -> Self {
        Self("Host result does not match the declared response schema")
    }
    fn invalid_type(_: de::Unexpected<'_>, expected: &dyn de::Expected) -> Self {
        Self(match expected.to_string().as_str() {
            "a string" => "Host result expected a string",
            "a boolean" => "Host result expected a boolean",
            "u8" => "Host result expected an unsigned 8-bit integer",
            "u32" => "Host result expected an unsigned 32-bit integer",
            "u64" => "Host result expected an unsigned 64-bit integer",
            "usize" => "Host result expected an unsigned integer",
            "i32" => "Host result expected a signed 32-bit integer",
            "a sequence" => "Host result expected an array",
            "a map" => "Host result expected an object",
            "unit" => "Host result expected null",
            _ => "Host result has an unexpected JSON type for its declared schema",
        })
    }
    fn invalid_value(_: de::Unexpected<'_>, _: &dyn de::Expected) -> Self {
        Self("Host result value is outside its declared schema bounds")
    }
    fn invalid_length(_: usize, _: &dyn de::Expected) -> Self {
        Self("Host result array length does not match its declared schema")
    }
    fn missing_field(field: &'static str) -> Self {
        macro_rules! missing { ($($field:literal),+ $(,)?) => { match field { $($field => concat!("Host result is missing required field: ", $field),)+ _ => "Host result is missing a required schema field" } }; }
        Self(missing!(
            "content",
            "start_line",
            "next_line",
            "truncated",
            "version",
            "bytes",
            "source_removed",
            "destination_written",
            "diagnostic",
            "expected_version",
            "destination",
            "precondition",
            "paths",
            "matches",
            "path",
            "line",
            "text",
            "data",
            "handle",
            "pid",
            "revision",
            "state",
            "elapsed_s",
            "code",
            "signal",
            "stdout",
            "stderr",
            "closed",
            "cursor",
            "stdout_bytes",
            "stderr_bytes",
            "next",
            "column",
            "through_line",
            "eof",
            "size",
            "kind"
        ))
    }
    fn unknown_field(_: &str, _: &'static [&'static str]) -> Self {
        Self("Host result contains an unknown schema field")
    }
    fn unknown_variant(_: &str, _: &'static [&'static str]) -> Self {
        Self("Host result contains an unknown enum variant")
    }
    fn duplicate_field(_: &'static str) -> Self {
        Self("Host result contains a duplicate schema field")
    }
}

pub(crate) fn decode<T: DeserializeOwned>(value: serde_json::Value) -> Result<T, ToolError> {
    match serde_json::from_value(value.clone()) {
        Ok(result) => Ok(result),
        Err(private) => {
            // Preserve serde_json as the acceptance authority. The second pass
            // only extracts closed facts from a response already rejected by it.
            let reason = match T::deserialize(Decoder(value)) {
                Err(error) => error.0,
                Ok(_) => "Host result does not match the declared response schema",
            };
            Err(ToolError::Worker(
                microcrab_host_api::FailureReason::from(private.to_string())
                    .with_public_reason(reason),
            ))
        }
    }
}
struct Decoder(serde_json::Value);
impl<'de> de::Deserializer<'de> for Decoder {
    type Error = PayloadError;
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.0 {
            serde_json::Value::Null => visitor.visit_unit(),
            serde_json::Value::Bool(value) => visitor.visit_bool(value),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_u64() {
                    visitor.visit_u64(value)
                } else if let Some(value) = value.as_i64() {
                    visitor.visit_i64(value)
                } else if let Some(value) = value.as_f64() {
                    visitor.visit_f64(value)
                } else {
                    Err(PayloadError(
                        "Host result number is outside supported JSON bounds",
                    ))
                }
            }
            serde_json::Value::String(value) => visitor.visit_string(value),
            serde_json::Value::Array(value) => visitor.visit_seq(Sequence(value.into_iter())),
            serde_json::Value::Object(value) => visitor.visit_map(Object {
                entries: value.into_iter(),
                value: None,
            }),
        }
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        if self.0.is_null() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        match self.0 {
            serde_json::Value::String(name) => {
                visitor.visit_enum(Enumeration { name, value: None })
            }
            serde_json::Value::Object(map) if map.len() == 1 => {
                let (name, value) = map.into_iter().next().expect("one-entry enum object");
                visitor.visit_enum(Enumeration {
                    name,
                    value: Some(value),
                })
            }
            _ => Err(PayloadError(
                "Host result enum must be a string or a single-key object",
            )),
        }
    }
    serde::forward_to_deserialize_any! { bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf unit unit_struct seq tuple tuple_struct map struct identifier ignored_any }
}
struct Sequence(std::vec::IntoIter<serde_json::Value>);
impl<'de> SeqAccess<'de> for Sequence {
    type Error = PayloadError;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        self.0
            .next()
            .map(|value| seed.deserialize(Decoder(value)))
            .transpose()
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }
}
struct Object {
    entries: serde_json::map::IntoIter,
    value: Option<serde_json::Value>,
}
impl<'de> MapAccess<'de> for Object {
    type Error = PayloadError;
    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        let Some((key, value)) = self.entries.next() else {
            return Ok(None);
        };
        self.value = Some(value);
        seed.deserialize(Decoder(serde_json::Value::String(key)))
            .map(Some)
    }
    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        seed.deserialize(Decoder(
            self.value
                .take()
                .expect("serde requests a value only after a key"),
        ))
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}
struct Enumeration {
    name: String,
    value: Option<serde_json::Value>,
}
impl<'de> EnumAccess<'de> for Enumeration {
    type Error = PayloadError;
    type Variant = Self;
    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self), Self::Error> {
        seed.deserialize(Decoder(serde_json::Value::String(self.name.clone())))
            .map(|name| (name, self))
    }
}
impl<'de> VariantAccess<'de> for Enumeration {
    type Error = PayloadError;
    fn unit_variant(self) -> Result<(), Self::Error> {
        match self.value {
            None | Some(serde_json::Value::Null) => Ok(()),
            _ => Err(PayloadError(
                "Host result unit enum variant must not carry a value",
            )),
        }
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, Self::Error> {
        seed.deserialize(Decoder(self.value.ok_or(PayloadError(
            "Host result enum variant is missing its value",
        ))?))
    }
    fn tuple_variant<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value, Self::Error> {
        de::Deserializer::deserialize_any(
            Decoder(self.value.ok_or(PayloadError(
                "Host result enum variant is missing its array",
            ))?),
            visitor,
        )
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        de::Deserializer::deserialize_any(
            Decoder(self.value.ok_or(PayloadError(
                "Host result enum variant is missing its object",
            ))?),
            visitor,
        )
    }
}
