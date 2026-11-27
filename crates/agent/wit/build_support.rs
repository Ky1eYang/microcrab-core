//! Deterministic staging of owner-maintained WIT sources for build scripts.
//!
//! This module has no Cargo dependencies. Include it from a build script with
//! `#[path = "..."] mod wit_support;`; generated dependencies live only in OUT_DIR.
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Stage an owned world and its canonical WIT dependencies into OUT_DIR.
/// The Session runtime world shares the root package with the variables interface.
pub fn stage(manifest: &Path, world: &str, runtime: bool) -> io::Result<PathBuf> {
    // Crates may have their own AGENTS.md; instruction scope is not a source
    // root marker. Locate the two canonical packages needed by every world.
    let root = manifest
        .ancestors()
        .find(|path| {
            path.join("crates/agent/wit/wit/agent.wit").is_file()
                && path.join("crates/host/wit/host.wit").is_file()
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "canonical WIT source root"))?;
    let out = PathBuf::from(
        std::env::var_os("OUT_DIR")
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "OUT_DIR"))?,
    )
    // Use a fresh staging layout so reused OUT_DIRs cannot carry forward
    // removed dependency packages that WIT tooling would still discover.
    .join("wit-session-abi");
    copy(&manifest.join("wit").join(world), &out.join("world.wit"))?;
    copy(
        &root.join("crates/agent/wit/wit/agent.wit"),
        &out.join("deps/agent/agent.wit"),
    )?;
    copy(
        &root.join("crates/host/wit/host.wit"),
        &out.join("deps/host/host.wit"),
    )?;
    copy(
        &root.join("crates/session/wit/session.wit"),
        &out.join("deps/session-api/session.wit"),
    )?;
    if runtime {
        copy(
            &root.join("crates/session/wit/file-versions.wit"),
            &out.join("file-versions.wit"),
        )?;
        copy(
            &root.join("crates/session/wit/observation.wit"),
            &out.join("deps/session-observation/observation.wit"),
        )?;
    }
    Ok(out)
}

fn copy(source: &Path, destination: &Path) -> io::Result<()> {
    println!("cargo:rerun-if-changed={}", source.display());
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "WIT destination parent"))?;
    fs::create_dir_all(parent)?;
    let content = fs::read(source)?;
    // Avoid touching staged files when their canonical contents are unchanged.
    if fs::read(destination).ok().as_deref() != Some(content.as_slice()) {
        fs::write(destination, content)?;
    }
    Ok(())
}
