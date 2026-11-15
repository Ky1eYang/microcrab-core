//! LocalHost acceptance contracts share one public-API test binary. Each module
//! retains its own fixtures; adding a leaf here avoids another native link target.
mod cache;
mod file;
mod filesystem_scope;
mod info;
mod jobs;
mod mutations;
mod primitives;
mod private_paths;
mod shell;
mod typed_files;
