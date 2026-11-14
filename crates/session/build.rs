#[path = "../agent/wit/build_support.rs"]
mod wit_support;

fn main() -> std::io::Result<()> {
    let manifest =
        std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "CARGO_MANIFEST_DIR")
        })?);
    let staged = wit_support::stage(&manifest, "runtime.wit", true)?;
    let out = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR")
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "OUT_DIR"))?,
    );
    println!("cargo:rustc-env=MICROCRAB_WIT_DIR={}", staged.display());
    std::fs::write(
        out.join("runtime-bindings.rs"),
        format!(
            "wasmtime::component::bindgen!({{ path: {:?}, world: \"runtime\", with: {{ \"microcrab:agent/execution@0.7.0.buffer\": crate::runtime::wasm::output::BufferResource }} }});\n",
            staged
        ),
    )?;
    Ok(())
}
