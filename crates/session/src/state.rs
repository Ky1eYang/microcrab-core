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
    DynReference, ListedReference, Reference, ReferenceContent, ReferenceInfo, ReferenceLibrary,
    ReferencePayload,
};
pub use session::SessionState;
pub use variables::Variables;
