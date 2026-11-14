//! Local implementation of injected Host shell and file capabilities.
//!
//! File reads and cwd admission are bounded to the configured root; commands retain
//! the embedding user's OS permissions. Process lifetime is owned by the Host.

mod cache;
mod file;
mod filesystem;
mod jobs;
mod mutation;
mod paths;
mod primitive;
mod process;
mod resources;
mod search;
mod shell;
mod text;
mod workspace;

pub use shell::{LocalHost, ShellLimits};
