use super::*;
use serde_json::json;

fn metadata() -> Value {
    json!({
        "workspace_members": ["coding-id", "universal-id", "native-id"],
        "packages": [
            {"id":"coding-id", "name":"coding-package", "metadata":{"microcrab":{"component-name":"coding"}},
             "targets":[{"name":"coding_module", "crate_types":["cdylib","rlib"]}]},
            {"id":"universal-id", "name":"universal-package", "metadata":{"microcrab":{"component-name":"universal"}},
             "targets":[{"name":"universal_module", "crate_types":["cdylib","rlib"]}]},
            {"id":"native-id", "name":"native", "targets":[{"name":"native", "crate_types":["rlib"]}]},
            {"id":"external-id", "name":"external", "targets":[{"name":"external", "crate_types":["cdylib"]}]}
        ]
    })
}
#[test]
fn package_selection_rejects_any_unmatched_name_and_duplicate_output() {
    let metadata = metadata();
    assert_eq!(discover(&metadata, &BTreeSet::new()).unwrap().len(), 2);
    let selected = BTreeSet::from(["coding-package".to_owned()]);
    let guests = discover(&metadata, &selected).unwrap();
    assert_eq!(guests[0].module, "coding_module");
    let invalid = BTreeSet::from(["coding-package".to_owned(), "typo".to_owned()]);
    assert!(discover(&metadata, &invalid).is_err());
    let mut duplicate = metadata.clone();
    duplicate["packages"][1]["metadata"]["microcrab"]["component-name"] = "coding".into();
    assert!(discover(&duplicate, &BTreeSet::new()).is_err());
    duplicate["packages"][1]["metadata"]["microcrab"]["component-name"] = "../escape".into();
    assert!(discover(&duplicate, &BTreeSet::new()).is_err());
}
#[test]
fn non_agent_artifacts_never_enter_component_selection() {
    let mut m = metadata();
    m["workspace_members"].as_array_mut().unwrap().extend([
        json!("browser-id"),
        json!("ffi-id"),
        json!("plugin-id"),
    ]);
    m["packages"].as_array_mut().unwrap().extend([
        json!({"id":"browser-id", "name":"browser-package", "metadata":{"microcrab":{"artifact-kind":"frontend-host"}}, "targets":[{"name":"browser", "crate_types":["cdylib"]}]}),
        json!({"id":"ffi-id", "name":"ffi-package", "metadata":{"microcrab":{"artifact-kind":"native-library"}}, "targets":[{"name":"ffi", "crate_types":["cdylib"]}]}),
        json!({"id":"plugin-id", "name":"plugin-package", "metadata":{"microcrab":{"artifact-kind":"client-plugin", "component-name":"not-an-agent"}}, "targets":[{"name":"plugin", "crate_types":["cdylib"]}]})
    ]);
    assert_eq!(discover(&m, &BTreeSet::new()).unwrap().len(), 2);
    assert!(discover(&m, &BTreeSet::from(["browser-package".to_owned()])).is_err());
    assert!(discover(&m, &BTreeSet::from(["plugin-package".to_owned()])).is_err());
    m["packages"][4]["metadata"]["microcrab"]["artifact-kind"] = "unknown".into();
    assert!(discover(&m, &BTreeSet::new()).is_err());
}
#[test]
fn conflicting_profiles_and_missing_values_do_not_admit_a_build() {
    for args in [
        vec!["--release", "--debug"],
        vec!["--only"],
        vec!["--install-dir", "--release"],
        vec!["--only", "coding,,universal"],
        vec!["--typo"],
    ] {
        assert!(parse(args.into_iter().map(str::to_owned)).is_err());
    }
    let options = parse(
        [
            "--only",
            "coding-package,universal-package",
            "--only",
            "coding-package",
            "--install-dir",
            "other agents",
            "--release",
        ]
        .into_iter()
        .map(str::to_owned),
    )
    .unwrap()
    .unwrap();
    assert_eq!(options.only.len(), 2);
    assert_eq!(options.install, Some(PathBuf::from("other agents")));
    assert!(options.release);
}
#[test]
fn failed_staging_preserves_existing_output_and_success_replaces_it() {
    let executable = std::env::current_exe().unwrap();
    let target = executable
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temporary = target.join("tmp");
    fs::create_dir_all(&temporary).unwrap();
    let root = temporary.join(format!("microcrab-xtask-test-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let destination = root.join("coding.wasm");
    fs::write(&destination, b"previous component").unwrap();
    let staged_path;
    {
        let stage = Staged::new(&destination).unwrap();
        staged_path = stage.path.clone();
        fs::write(&stage.path, b"invalid candidate").unwrap();
    }
    assert!(!staged_path.exists());
    assert_eq!(fs::read(&destination).unwrap(), b"previous component");
    {
        let stage = Staged::new(&destination).unwrap();
        fs::write(&stage.path, b"validated component").unwrap();
        stage.publish(&destination).unwrap();
    }
    assert_eq!(fs::read(&destination).unwrap(), b"validated component");
    // This exact directory is created exclusively for this process by this test.
    fs::remove_file(&destination).unwrap();
    fs::remove_dir(&root).unwrap();
}
