//! Coding guest: schemas and presentation belong here; authorization belongs to Runtime.
mod apply_patch;
mod catalog;
#[cfg(target_arch = "wasm32")]
mod component;
mod host;
#[cfg(test)]
mod tests;
mod tools;

pub use catalog::describe;
pub use host::*;
pub use tools::invoke;
