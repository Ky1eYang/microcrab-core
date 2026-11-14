//! Build selected guest packages in one Cargo graph, then publish components.
//! Failed conversion/validation leaves the last published component untouched.
use serde_json::Value;
use std::{
    collections::BTreeSet,
    error::Error,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

const TARGET: &str = "wasm32-unknown-unknown";
type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub(crate) fn help() {
    println!(
        "cargo xtask build-wasm [--release | --debug] [--only PACKAGE[,PACKAGE] ...]\n\
              \x20                       [--validate] [--install | --install-dir PATH]\n\n\
              Build workspace agent cdylibs (debug by default), create actual components\n\
              under target/<name>.wasm, and optionally install them to .microcrab/agents.\n\
              --only accepts exact workspace package names and may be repeated.\n\
              --install-dir is relative to the caller's cwd; installation always validates.\n\
              Requires the installed wasm32-unknown-unknown target and wasm-tools on PATH.\n\
              Cargo uses --locked --offline; no toolchain or dependency downloads occur."
    );
}

#[derive(Default, Debug, PartialEq, Eq)]
struct Options {
    release: bool,
    only: BTreeSet<String>,
    validate: bool,
    install: Option<PathBuf>,
}
fn parse(args: impl Iterator<Item = String>) -> Result<Option<Options>> {
    let mut options = Options::default();
    let mut profile = None;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(None),
            "--release" | "--debug" => {
                let release = arg == "--release";
                if profile.is_some_and(|previous| previous != release) {
                    return Err("--release and --debug cannot be combined".into());
                }
                profile = Some(release);
                options.release = release;
            }
            "--validate" => options.validate = true,
            "--install" => {
                options
                    .install
                    .get_or_insert_with(|| ".microcrab/agents".into());
            }
            "--install-dir" => {
                let path = argument(&mut args, "--install-dir")?;
                options.install = Some(path.into());
            }
            "--only" => {
                let packages = argument(&mut args, "--only")?;
                for package in packages.split(',') {
                    if package.is_empty() {
                        return Err("--only contains an empty package name".into());
                    }
                    options.only.insert(package.to_owned());
                }
            }
            _ => return Err(format!("unknown build-wasm argument {arg:?}").into()),
        }
    }
    Ok(Some(options))
}
fn argument(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    let value = args
        .next()
        .ok_or_else(|| format!("{flag} requires a value"))?;
    if value.is_empty() || value.starts_with('-') {
        return Err(format!("{flag} requires a nonempty value").into());
    }
    Ok(value)
}

#[derive(Debug)]
struct Guest {
    package: String,
    module: String,
    component: String,
}
fn discover(metadata: &Value, selected: &BTreeSet<String>) -> Result<Vec<Guest>> {
    let members: BTreeSet<_> = metadata["workspace_members"]
        .as_array()
        .ok_or("cargo metadata missing workspace_members")?
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let mut guests = Vec::new();
    let mut names = BTreeSet::new();
    for package in metadata["packages"]
        .as_array()
        .ok_or("cargo metadata missing packages")?
    {
        if !package["id"]
            .as_str()
            .is_some_and(|id| members.contains(id))
        {
            continue;
        }
        let name = package["name"].as_str().ok_or("package name missing")?;
        let targets = package["targets"]
            .as_array()
            .ok_or("package targets missing")?;
        for target in targets {
            if !target["crate_types"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == "cdylib"))
            {
                continue;
            }
            if crate::artifact_kind::classify(package)? != crate::artifact_kind::Kind::Agent {
                continue;
            }
            let component = package["metadata"]["microcrab"]["component-name"]
                .as_str()
                .ok_or_else(|| {
                    format!("cdylib package {name} needs package.metadata.microcrab.component-name")
                })?;
            if component.is_empty()
                || !component
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
            {
                return Err(format!("invalid component name for {name}").into());
            }
            if !names.insert(component) {
                return Err(format!("duplicate component name {component}").into());
            }
            guests.push(Guest {
                package: name.to_owned(),
                module: target["name"]
                    .as_str()
                    .ok_or("cdylib target name missing")?
                    .to_owned(),
                component: component.to_owned(),
            });
        }
    }
    for name in selected {
        if !guests.iter().any(|guest| &guest.package == name) {
            return Err(format!("--only package {name:?} is not a workspace agent cdylib").into());
        }
    }
    guests.retain(|guest| selected.is_empty() || selected.contains(&guest.package));
    guests.sort_by(|a, b| a.package.cmp(&b.package));
    if guests.is_empty() {
        return Err("no workspace agent cdylibs found".into());
    }
    Ok(guests)
}

pub(crate) fn run(args: impl Iterator<Item = String>) -> Result<()> {
    let Some(options) = parse(args)? else {
        help();
        return Ok(());
    };
    let caller = std::env::current_dir()?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or("xtask is not under crates/xtask")?;
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(&cargo)
        .current_dir(root)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--locked",
            "--offline",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let metadata: Value = serde_json::from_slice(&output.stdout)?;
    let guests = discover(&metadata, &options.only)?;
    let target_dir = PathBuf::from(
        metadata["target_directory"]
            .as_str()
            .ok_or("Cargo target_directory missing")?,
    );
    let installed = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()?;
    if !installed.status.success()
        || !String::from_utf8_lossy(&installed.stdout)
            .lines()
            .any(|line| line.trim() == TARGET)
    {
        return Err(format!(
            "missing Rust target {TARGET}; install it explicitly with rustup target add {TARGET}"
        )
        .into());
    }
    let mut probe = Command::new("wasm-tools");
    probe.arg("--version");
    execute(
        &mut probe,
        "wasm-tools prerequisite (install wasm-tools explicitly if absent)",
    )?;
    let mut build = Command::new(&cargo);
    build
        .current_dir(root)
        .args(["build", "--locked", "--offline", "--target", TARGET]);
    if options.release {
        build.arg("--release");
    }
    for guest in &guests {
        build.arg("-p").arg(&guest.package);
    }
    execute(&mut build, "guest Cargo build")?;

    let profile = if options.release { "release" } else { "debug" };
    let mut components = Vec::new();
    // Stage every selected output before publishing any. Conversion or validation
    // failure cannot install a partly built selection or clobber a working file.
    for guest in &guests {
        let module = target_dir
            .join(TARGET)
            .join(profile)
            .join(format!("{}.wasm", guest.module));
        let destination = target_dir.join(format!("{}.wasm", guest.component));
        let stage = Staged::new(&destination)?;
        let mut convert = Command::new("wasm-tools");
        convert
            .args(["component", "new"])
            .arg(&module)
            .arg("-o")
            .arg(&stage.path);
        execute(&mut convert, "component conversion")?;
        if options.validate || options.install.is_some() {
            let mut validate = Command::new("wasm-tools");
            validate.arg("validate").arg(&stage.path);
            execute(&mut validate, "component validation")?;
        }
        components.push((stage, destination));
    }
    for (stage, destination) in &components {
        stage.publish(destination)?;
        println!("component: {}", destination.display());
    }
    if let Some(install) = options.install {
        let directory = caller.join(install);
        let mut staged = Vec::new();
        for (_, component) in &components {
            let destination =
                directory.join(component.file_name().ok_or("component filename missing")?);
            let stage = Staged::new(&destination)?;
            fs::copy(component, &stage.path)?;
            staged.push((stage, destination));
        }
        for (stage, destination) in &staged {
            stage.publish(destination)?;
            println!("installed: {}", destination.display());
        }
    }
    Ok(())
}
pub(super) fn execute(command: &mut Command, stage: &str) -> Result<()> {
    // Inherit stdio: normal diagnostics remain visible and no child output pipe
    // is needed for build/conversion. Metadata is the sole structured capture.
    let status = command
        .status()
        .map_err(|error| format!("{stage}: {error}"))?;
    if !status.success() {
        return Err(format!("{stage} failed ({status})").into());
    }
    Ok(())
}

pub(super) struct Staged {
    pub(super) path: PathBuf,
}
impl Staged {
    pub(super) fn new(destination: &Path) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let parent = destination
            .parent()
            .ok_or("output needs a parent directory")?;
        fs::create_dir_all(parent)?;
        let name = destination
            .file_name()
            .ok_or("output needs a filename")?
            .to_string_lossy();
        let path = parent.join(format!(
            ".{name}.xtask-{}-{}.tmp",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        // Only files exclusively created by this invocation are cleaned on drop.
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        Ok(Self { path })
    }
    pub(super) fn publish(&self, destination: &Path) -> Result<()> {
        // Same-directory rename replaces one complete file. A multi-component
        // install is not a transaction if the filesystem fails during publication.
        fs::rename(&self.path, destination)?;
        Ok(())
    }
}
impl Drop for Staged {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests;
