//! Content admission owns bounded measurement and policy; callers retain locks,
//! identities and commit authority. A decision never asserts absence of effects.
pub mod image;
pub(crate) mod inbox;
pub(crate) mod input;
mod measurement;
pub(crate) mod variables;

use crate::{Error, LimitKind};

/// The independently owned content boundary requesting a bounded decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionSource {
    Input,
    HistoricalInput,
    Image,
    Inbox,
    StateDelivery,
    CustomVariables,
}

/// Encoded payload, text metadata and retained storage are different dimensions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Measurement {
    pub text_metadata_bytes: usize,
    /// None when this boundary did not measure retained storage.
    pub retained_bytes: Option<usize>,
    pub image_bytes: usize,
    pub image_count: usize,
}

/// A content decision carries no Session identity, locks or mutation capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Admission {
    pub source: AdmissionSource,
    pub measurement: Measurement,
}

impl Admission {
    /// Validate resolved input using the same policy as direct Context admission.
    /// Paths and attachment identities must first be resolved by their injected
    /// owners. Remote HTTP images remain provider-owned and count zero known bytes.
    pub fn input(input: &crate::SessionInput, max_text_bytes: usize) -> Result<Self, Error> {
        input::admit(input, max_text_bytes, true)
    }

    pub(crate) fn check(
        source: AdmissionSource,
        kind: LimitKind,
        required: usize,
        budget: usize,
    ) -> Result<Self, Error> {
        crate::budget::check(kind, required, budget)?;
        Ok(Self::measured(source, required))
    }

    fn measured(source: AdmissionSource, required: usize) -> Self {
        let mut measurement = Measurement::default();
        match source {
            AdmissionSource::Input | AdmissionSource::HistoricalInput => {
                measurement.text_metadata_bytes = required;
            }
            AdmissionSource::Image => measurement.image_bytes = required,
            _ => measurement.retained_bytes = Some(required),
        }
        Self {
            source,
            measurement,
        }
    }

    /// Count encoded content without allocating its serialization. On rejection,
    /// required bytes describe the first observed boundary crossing, not a claim
    /// to have traversed the unbounded remainder of the value.
    pub(crate) fn serialized<T: serde::Serialize + ?Sized>(
        source: AdmissionSource,
        kind: LimitKind,
        value: &T,
        budget: usize,
    ) -> Result<Self, Error> {
        let required = measurement::serialized_size(value, kind, budget)?;
        Ok(Self::measured(source, required))
    }
}
