use super::*;

fn fixture(files: &[(&str, &str)]) -> Vec<String> {
    let mut sources = vec![(
        GUARD_OWNER.to_owned(),
        "pub(crate) struct CancelOnDrop;".to_owned(),
    )];
    sources.extend(
        files
            .iter()
            .map(|(path, source)| (path.to_string(), source.to_string())),
    );
    check(&sources)
}

#[test]
fn accepts_baseline_and_independent_capacity_owners() {
    assert!(
        fixture(&[
            ("lib.rs", "pub mod runtime; mod inner; pub mod utils;"),
            (
                "runtime/execution/registry.rs",
                "let capacity = limits.max_execution_buffers;"
            ),
            (
                "runtime/processes/registry.rs",
                "let capacity = limits.max_processes;"
            ),
            (
                "runtime/execution/tests.rs",
                "let fixture = limits.max_processes;"
            ),
            (
                "runtime/processes/quota_tests.rs",
                "let fixture = limits.max_execution_buffers;"
            ),
        ])
        .is_empty()
    );
}

#[test]
fn rejects_direct_grouped_aliased_and_relative_inner_paths() {
    for source in [
        "use crate::inner::SessionInner;",
        "use crate::{runtime::Thing, inner::{SessionInner as Alias}};",
        "use super::super::{inner::SessionInner};",
        "fn bypass() { crate::inner::start(); }",
        "fn bypass() { super::super::inner::start(); }",
        "use crate::r#inner::SessionInner;",
    ] {
        let errors = fixture(&[("runtime/handler.rs", source)]);
        assert!(
            errors.iter().any(|error| error.contains("inner")),
            "{source}: {errors:?}"
        );
    }
}

#[test]
fn allows_unrelated_inner_names_and_reverse_or_sibling_dependencies() {
    assert!(fixture(&[
        ("runtime/handler.rs", "use crate::{context::inner::Thing, runtime::tool::Thing as inner}; fn local() { let inner = 1; }"),
        ("inner/commands.rs", "use crate::runtime::handler::Runtime;"),
        ("runtime/processes/source.rs", "use crate::runtime::execution::ExecutionController;"),
    ]).is_empty());
}

#[test]
fn rejects_new_root_file_directory_and_inline_module() {
    for (path, source) in [
        ("new_feature.rs", ""),
        ("new_feature/leaf.rs", ""),
        ("lib.rs", "pub(crate) mod new_feature {}"),
    ] {
        assert!(
            fixture(&[(path, source)])
                .iter()
                .any(|error| error.contains("unrecorded root"))
        );
    }
    // New leaves deepen existing owners; no line or file-count ceiling applies.
    assert!(
        fixture(&[
            ("runtime/execution/new_leaf.rs", "struct Producer;"),
            ("utils/other.rs", "")
        ])
        .is_empty()
    );
}

#[test]
fn requires_one_guard_in_its_shared_owner() {
    assert!(
        check(&[])
            .iter()
            .any(|error| error.contains("exactly one definition"))
    );
    for declaration in [
        "struct CancelOnDrop;",
        "enum CancelOnDrop {}",
        "type CancelOnDrop = Other;",
    ] {
        assert!(
            fixture(&[("runtime/wasm.rs", declaration)])
                .iter()
                .any(|error| error.contains("exactly one definition"))
        );
    }
    assert!(
        check(&[(
            "runtime/processes.rs".to_owned(),
            "struct CancelOnDrop;".to_owned()
        )])
        .iter()
        .any(|error| error.contains(GUARD_OWNER))
    );
}

#[test]
fn rejects_cross_owner_capacity_member_access_only() {
    for (path, source) in [
        (
            "runtime/execution/registry.rs",
            "if limits.max_processes == 0 {}",
        ),
        (
            "runtime/processes/registry.rs",
            "let cap = limits . max_execution_buffers;",
        ),
    ] {
        assert!(
            fixture(&[(path, source)])
                .iter()
                .any(|error| error.contains("independent peer owner"))
        );
    }
    assert!(
        fixture(&[(
            "runtime/execution/registry.rs",
            "fn max_processes() {} let max_processes = 1;"
        )])
        .is_empty()
    );
}

#[test]
fn rejects_retired_forwarding_files_but_accepts_direct_owner_exports() {
    for path in RETIRED_MODULES {
        assert!(
            fixture(&[(path, "pub use crate::budget::Limits;")])
                .iter()
                .any(|error| error.contains("retired module")),
            "{path}"
        );
    }
    assert!(
        fixture(&[
            ("lib.rs", "pub use crate::budget::Limits;"),
            ("state.rs", "pub use crate::budget::Limits;")
        ])
        .is_empty()
    );
}

#[test]
fn approved_trunks_keep_one_definition_authority() {
    assert!(
        fixture(&[
            (
                "lib.rs",
                "pub mod admission; pub mod budget; pub mod observation; pub mod policy;"
            ),
            ("budget/limits.rs", "pub struct Limits;"),
            (
                "budget/violation.rs",
                "pub enum LimitKind {} pub struct LimitError;"
            ),
            ("observation/activity.rs", "pub struct SessionActivity;"),
            (
                "runtime/execution/process/registry.rs",
                "pub struct ProcessRegistry; let capacity = limits.max_processes;"
            ),
            ("state.rs", "pub use crate::budget::Limits;"),
            ("state/endpoint.rs", "enum VariableCommand {}"),
            ("boundary.rs", "enum Boundary {}"),
            ("observation/wasm.rs", "struct EvidenceBoundary;"),
        ])
        .is_empty()
    );
    for (path, source) in [
        ("state/access.rs", "enum VariableCommand {}"),
        ("inner/commands.rs", "enum Boundary {}"),
        ("runtime/wasm.rs", "struct EvidenceBoundary;"),
        ("state/limits.rs", "pub struct Limits;"),
        ("limit.rs", "pub enum LimitKind {}"),
        ("runtime/cache.rs", "pub struct RoundCacheReport;"),
        (
            "runtime/processes/registry.rs",
            "pub struct ProcessRegistry;",
        ),
    ] {
        assert!(
            fixture(&[(path, source)])
                .iter()
                .any(|error| error.contains("definition belongs")),
            "{path}"
        );
    }
    assert!(
        fixture(&[(
            "runtime/execution/process/registry.rs",
            "let cap = limits.max_execution_buffers;"
        )])
        .iter()
        .any(|error| error.contains("independent peer owner"))
    );
    for owner in ["budget", "admission", "observation", "policy"] {
        let path = format!("{owner}/leaf.rs");
        assert!(
            fixture(&[(&path, "use crate::inner::SessionInner;")])
                .iter()
                .any(|error| error.contains("inner"))
        );
    }
}

#[test]
fn requires_shared_budget_construction_without_reading_literal_contents() {
    for source in [
        r#"Err(Error::Limit("local budget".into()))"#,
        "Err(Error::Limit(\n r#\"local budget\"#.into()))",
        r#"Err(Self::Limit(format!("local {value}").into()))"#,
    ] {
        assert!(
            fixture(&[("runtime/wasm/leaf.rs", source)])
                .iter()
                .any(|error| error.contains("local string limit")),
            "{source}"
        );
    }
    assert!(
        fixture(&[(
            "runtime/wasm/leaf.rs",
            r#"
        let documentation = "Error::Limit(unsafe text)";
        // Error::Limit("local budget".into())
        let typed = Error::Limit(limit.with_label("legacy label"));
        crate::budget::check(crate::LimitKind::WasmBytes, required, budget)?;
    "#
        )])
        .is_empty()
    );
}

#[test]
fn ignores_comments_strings_raw_byte_strings_and_chars() {
    let source = r####"
        // use crate::inner::Thing; struct CancelOnDrop;
        /* outer /* nested */ crate::inner::Thing */
        let ordinary = "crate::inner::Thing \" struct CancelOnDrop;";
        let raw = r###"crate::inner::Thing " struct CancelOnDrop;"###;
        let bytes = br##"crate::inner::Thing " struct CancelOnDrop;"##;
        let c_string = cr#"crate::inner::Thing " struct CancelOnDrop;"#;
        let escaped = '\''; let unicode = '界'; let slash = b'\\';
        fn borrow<'a>(value: &'a str) -> &'a str { value }
        let description = "limits.max_processes";
    "####;
    assert!(fixture(&[("runtime/execution/registry.rs", source)]).is_empty());
}
