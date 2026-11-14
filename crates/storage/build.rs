//! Platform linkage only; SQLite connection and persistence policy live in src/sqlite.rs.
#[path = "build/windows_sqlite.rs"]
mod windows_sqlite;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=build/windows_sqlite.rs");
    if std::env::var_os("CARGO_FEATURE_STORAGE_SQLITE").is_some()
        && std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows")
    {
        windows_sqlite::configure()?;
    }
    Ok(())
}
