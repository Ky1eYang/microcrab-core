//! Bounded serde measurement shared across content owners, without a payload copy.
use crate::{Error, LimitKind};
use serde::Serialize;

pub(super) fn serialized_size<T: Serialize + ?Sized>(
    value: &T,
    kind: LimitKind,
    budget: usize,
) -> Result<usize, Error> {
    struct Writer {
        used: usize,
        budget: usize,
        rejected: Option<Error>,
        kind: LimitKind,
    }
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let Some(required) = self.used.checked_add(bytes.len()) else {
                self.rejected = Some(crate::budget::reject(
                    LimitKind::Arithmetic,
                    "content byte count overflow",
                ));
                return Err(std::io::Error::other("content byte count overflow"));
            };
            if let Err(error) = crate::budget::check(self.kind, required, self.budget) {
                self.rejected = Some(error);
                return Err(std::io::Error::other("content byte budget"));
            }
            self.used = required;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        used: 0,
        budget,
        rejected: None,
        kind,
    };
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        return Err(writer.rejected.unwrap_or(Error::Json(error)));
    }
    Ok(writer.used)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct Counted<'a>(&'a Cell<usize>);
    impl Serialize for Counted<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeSeq;
            let mut sequence = serializer.serialize_seq(Some(1000))?;
            for _ in 0..1000 {
                self.0.set(self.0.get() + 1);
                sequence.serialize_element("content")?;
            }
            sequence.end()
        }
    }

    #[test]
    fn bounded_measurement_stops_at_rejection_without_claiming_pre_effects() {
        let visited = Cell::new(0);
        let error = serialized_size(&Counted(&visited), LimitKind::InboxBytes, 32).unwrap_err();
        assert!(visited.get() < 1000);
        let Error::Limit(limit) = error else {
            panic!("expected typed limit")
        };
        assert_eq!(limit.kind, LimitKind::InboxBytes);
        assert!(!limit.is_pre_effect());
        assert!(limit.required_bytes.unwrap() > 32);
        assert_eq!(limit.budget_bytes, Some(32));
        let value = serde_json::json!({"text": "你好"});
        let exact = serde_json::to_vec(&value).unwrap().len();
        assert_eq!(
            serialized_size(&value, LimitKind::InputBytes, exact).unwrap(),
            exact
        );
        assert!(serialized_size(&value, LimitKind::InputBytes, exact - 1).is_err());
    }
}
