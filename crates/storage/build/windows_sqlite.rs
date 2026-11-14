//! Adapt libsqlite3-sys's fixed sqlite3 link name to the Windows SDK import library.
//! Each Cargo build owns its OUT_DIR alias; no system file or toolchain is changed.
use std::{env, error::Error, fs, io, path::PathBuf};

pub(super) fn configure() -> Result<(), Box<dyn Error>> {
    for name in [
        "WindowsSdkDir",
        "WindowsSDKVersion",
        "ProgramFiles(x86)",
        "ProgramFiles",
        "SQLITE3_LIB_DIR",
        "SQLITE3_STATIC",
        "LIBSQLITE3_SYS_USE_PKG_CONFIG",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    if env::var("CARGO_CFG_TARGET_ENV").as_deref() != Ok("msvc") {
        return Err("system SQLite on Windows requires an MSVC target and its Windows SDK winsqlite3.lib import library".into());
    }
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH")?;
    let sdk_arch = match target_arch.as_str() {
        "x86_64" => "x64",
        "x86" => "x86",
        "aarch64" => "arm64",
        _ => return Err(format!(
            "Windows SDK SQLite import library is unsupported for target architecture {target_arch}"
        )
        .into()),
    };
    // Upstream resolves these settings before our build script runs. Refuse a
    // competing library authority instead of accidentally linking another SQLite.
    if env::var_os("SQLITE3_LIB_DIR").is_some() {
        return Err("SQLITE3_LIB_DIR conflicts with Windows system SQLite linkage; remove it so the Windows SDK winsqlite3.lib is authoritative".into());
    }
    for name in ["SQLITE3_STATIC", "LIBSQLITE3_SYS_USE_PKG_CONFIG"] {
        if env::var_os(name).is_some_and(|value| value != "0") {
            return Err(format!("{name} conflicts with dynamic Windows system SQLite linkage; unset it or set it to 0").into());
        }
    }
    let source = find_import_library(sdk_arch)?;
    println!("cargo:rerun-if-changed={}", source.display());
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or("Cargo did not provide OUT_DIR")?);
    let alias = out_dir.join("sqlite3.lib");
    fs::copy(&source, &alias).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "copy Windows SDK import library {} to {}: {error}",
                source.display(),
                alias.display()
            ),
        )
    })?;
    // COFF import records retain winsqlite3.dll even though the filename matches
    // libsqlite3-sys's sqlite3 request. Cargo propagates this search path to final
    // consumers; the dependency rlib carries the native dylib link request.
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!(
        "cargo:warning=System SQLite: SDK import library {} -> {}; runtime DLL is Windows winsqlite3.dll (not sqlite3.dll)",
        source.display(),
        alias.display()
    );
    Ok(())
}

fn find_import_library(architecture: &str) -> Result<PathBuf, Box<dyn Error>> {
    let roots = if let Some(root) = env::var_os("WindowsSdkDir") {
        vec![PathBuf::from(root)]
    } else {
        let mut roots = Vec::new();
        for name in ["ProgramFiles(x86)", "ProgramFiles"] {
            if let Some(directory) = env::var_os(name) {
                let root = PathBuf::from(directory).join("Windows Kits").join("10");
                if !roots.contains(&root) {
                    roots.push(root);
                }
            }
        }
        roots
    };
    let version = env::var("WindowsSDKVersion")
        .ok()
        .map(|value| value.trim_end_matches(['\\', '/']).to_owned())
        .filter(|value| !value.is_empty());
    let mut attempted = Vec::new();
    for root in roots {
        let lib = root.join("Lib");
        println!("cargo:rerun-if-changed={}", lib.display());
        let versions = if let Some(version) = &version {
            vec![lib.join(version)]
        } else {
            let entries = match fs::read_dir(&lib) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    attempted.push(format!("{} (directory not found)", lib.display()));
                    continue;
                }
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!(
                            "discover Windows SDK libraries in {}: {error}",
                            lib.display()
                        ),
                    )
                    .into());
                }
            };
            let mut versions = Vec::new();
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name();
                if let Some(parts) = name.to_str().and_then(sdk_version) {
                    versions.push((parts, entry.path()));
                }
            }
            versions.sort_by(|left, right| right.0.cmp(&left.0));
            if versions.is_empty() {
                attempted.push(format!("{} (no versioned SDK directories)", lib.display()));
            }
            versions.into_iter().map(|(_, path)| path).collect()
        };
        for version in versions {
            let candidate = version.join("um").join(architecture).join("winsqlite3.lib");
            match fs::metadata(&candidate) {
                Ok(metadata) if metadata.is_file() => return Ok(candidate),
                Ok(_) => attempted.push(format!("{} (not a file)", candidate.display())),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    attempted.push(format!("{} (not found)", candidate.display()))
                }
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!(
                            "inspect Windows SDK SQLite import library {}: {error}",
                            candidate.display()
                        ),
                    )
                    .into());
                }
            }
        }
    }
    Err(format!("Windows system SQLite requires Windows SDK winsqlite3.lib for target {architecture}; install that SDK architecture or set WindowsSdkDir and WindowsSDKVersion to an installed SDK. Candidates: {}", if attempted.is_empty() { "none (WindowsSdkDir and ProgramFiles environment roots unavailable)".into() } else { attempted.join("; ") }).into())
}

fn sdk_version(name: &str) -> Option<Vec<u32>> {
    let parts = name
        .split('.')
        .map(str::parse)
        .collect::<std::result::Result<Vec<u32>, _>>()
        .ok()?;
    (parts.len() == 4).then_some(parts)
}
