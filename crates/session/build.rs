#[path = "../agent/wit/build_support.rs"]
mod wit_support;

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=../agent/wit/build_support.rs");
    let manifest =
        std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "CARGO_MANIFEST_DIR")
        })?);
    let staged = wit_support::stage(&manifest, "runtime.wit", true)?;
    let scoped_source = manifest.join("../host/wit/scoped-web-transport.wit");
    println!("cargo:rerun-if-changed={}", scoped_source.display());
    let scoped_dir = staged.join("deps/host-scoped");
    std::fs::create_dir_all(&scoped_dir)?;
    std::fs::copy(&scoped_source, scoped_dir.join("scoped-web-transport.wit"))?;
    let observation_source = manifest.join("wit/browser-observation.wit");
    println!("cargo:rerun-if-changed={}", observation_source.display());
    let observation_dir = staged.join("deps/session-browser-observation");
    std::fs::create_dir_all(&observation_dir)?;
    std::fs::copy(
        &observation_source,
        observation_dir.join("browser-observation.wit"),
    )?;
    let out = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR")
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "OUT_DIR"))?,
    );
    println!("cargo:rustc-env=MICROCRAB_WIT_DIR={}", staged.display());
    std::fs::write(
        out.join("runtime-bindings.rs"),
        format!(
            "wasmtime::component::bindgen!({{ path: {:?}, world: \"runtime\", with: {{ \"microcrab:agent/execution@0.9.0.buffer\": crate::runtime::wasm::output::BufferResource }} }});\n",
            staged
        ),
    )?;
    Ok(())
}
