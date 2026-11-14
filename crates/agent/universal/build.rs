#[path = "../wit/build_support.rs"]
mod wit_support;

fn main() -> std::io::Result<()> {
    let manifest =
        std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "CARGO_MANIFEST_DIR")
        })?);
    let staged = wit_support::stage(&manifest, "world.wit", false)?;
    let out = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR")
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "OUT_DIR"))?,
    );
    std::fs::write(
        out.join("guest-bindings.rs"),
        format!(
            "wit_bindgen::generate!({{ path: {:?}, world: \"universal\", generate_all }});\n",
            staged
        ),
    )?;
    Ok(())
}
