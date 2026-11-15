//! Source selection is runtime data: cached executables may have been built in another worktree.
use std::{
    error::Error,
    path::{Path, PathBuf},
};

fn valid(root: &Path) -> bool {
    root.join("Cargo.toml").is_file()
        && root.join("crates/xtask/Cargo.toml").is_file()
        && root.join("crates/session/src").is_dir()
}

fn resolve(explicit: Option<&Path>, caller: &Path) -> Result<PathBuf, Box<dyn Error>> {
    if let Some(path) = explicit {
        let root = path.canonicalize()?;
        if !valid(&root) {
            return Err("MICROCRAB_WORKSPACE_ROOT must select a Core workspace".into());
        }
        return Ok(root);
    }
    for candidate in caller.ancestors() {
        if valid(candidate) {
            return Ok(candidate.canonicalize()?);
        }
    }
    Err("run from the Core workspace or set MICROCRAB_WORKSPACE_ROOT explicitly".into())
}

pub(crate) fn root() -> Result<PathBuf, Box<dyn Error>> {
    let explicit = std::env::var_os("MICROCRAB_WORKSPACE_ROOT").map(PathBuf::from);
    resolve(explicit.as_deref(), &std::env::current_dir()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_runtime_workspace_wins_and_invalid_override_never_falls_back() {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fixture = std::env::temp_dir().join(format!(
            "microcrab-core-roots-{}-{stamp}",
            std::process::id()
        ));
        std::fs::create_dir(&fixture).unwrap();
        let first = fixture.join("first");
        let second = fixture.join("second");
        for root in [&first, &second] {
            std::fs::create_dir_all(root.join("crates/xtask")).unwrap();
            std::fs::create_dir_all(root.join("crates/session/src")).unwrap();
            std::fs::write(root.join("Cargo.toml"), "[workspace]\n").unwrap();
            std::fs::write(
                root.join("crates/xtask/Cargo.toml"),
                "[package]\nname='fixture'\n",
            )
            .unwrap();
        }
        assert_eq!(
            resolve(Some(&second), &first).unwrap(),
            second.canonicalize().unwrap()
        );
        assert_eq!(
            resolve(None, &first.join("crates/xtask")).unwrap(),
            first.canonicalize().unwrap()
        );
        assert!(resolve(Some(&fixture), &first).is_err());
        // This directory was created exclusively above; never remove a caller-selected root.
        std::fs::remove_dir_all(&fixture).unwrap();
    }
}
