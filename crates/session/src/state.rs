//! Session runtime state and validated execution budgets.
pub(crate) mod access;
pub mod communication;
pub(crate) mod endpoint;
pub mod reference;
mod session;
pub mod variables;
pub(crate) mod wasm;

pub use crate::budget::Limits;
pub use access::{
    PreparedSecretWrite, State, VariableCommit, VariableDescriptor, VariableInspection,
    VariableWitness,
};
pub use reference::{
    CompositeReference, DynReference, ListedReference, Reference, ReferenceCapabilities,
    ReferenceContent, ReferenceEntry, ReferenceError, ReferenceErrorKind, ReferenceInfo,
    ReferenceNodeKind, ReferencePatch, ReferencePayload, ReferenceProjection, ReferenceQuota,
    ReferenceWrite,
};
pub use session::SessionState;
pub use variables::Variables;
