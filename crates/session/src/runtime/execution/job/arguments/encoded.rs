//! Decode the current encoded-array input form once with the native array target cap.
use super::MAX_IDS;
use serde::{
    Deserialize, Deserializer,
    de::{Error, SeqAccess, Visitor},
};
use std::fmt;

pub(super) fn parse(text: &str) -> Result<Vec<String>, serde_json::Error> {
    struct Handles(Vec<String>);
    impl<'de> Deserialize<'de> for Handles {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct HandleVisitor;
            impl<'de> Visitor<'de> for HandleVisitor {
                type Value = Handles;
                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("an array of 1–16 execution handles")
                }
                fn visit_seq<A: SeqAccess<'de>>(
                    self,
                    mut sequence: A,
                ) -> Result<Handles, A::Error> {
                    let mut handles = Vec::new();
                    while let Some(handle) = sequence.next_element::<String>()? {
                        if handles.len() == MAX_IDS {
                            return Err(A::Error::custom("too many job handles"));
                        }
                        handles.push(handle);
                    }
                    if handles.is_empty() {
                        return Err(A::Error::custom("job handles must not be empty"));
                    }
                    Ok(Handles(handles))
                }
            }
            deserializer.deserialize_seq(HandleVisitor)
        }
    }
    serde_json::from_str::<Handles>(text).map(|handles| handles.0)
}
