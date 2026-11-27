use microcrab_host_api::{
    HostMarkdownReference, HostSkillsReference, Reference, ReferenceNodeKind,
};
use microcrab_host_local::{LocalHost, ShellLimits};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

fn modern_web_guidance_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .nth(3)
        .expect("workspace root")
        .join("thirdparty/skills/modern-web-guidance")
}

#[tokio::test]
async fn local_host_loads_thirdparty_modern_web_guidance_skills_and_filters_clutter() {
    let skill_root = modern_web_guidance_root();
    if !skill_root.is_dir() {
        return;
    }
    let host = Arc::new(LocalHost::new_filesystem(&skill_root, ShellLimits::default()).unwrap());
    let skills = host
        .skills_reference("skills", vec![PathBuf::from(".")])
        .unwrap();
    let cancel = CancellationToken::new();
    skills.load(cancel.clone()).await.unwrap();

    let entries = skills.list().unwrap();
    // Both skills in modern-web-guidance must be discovered.
    let dirs: Vec<_> = entries
        .iter()
        .filter(|e| e.node_kind == ReferenceNodeKind::Directory)
        .collect();
    assert!(dirs.iter().any(|d| d.title == "chrome-extensions"));
    assert!(dirs.iter().any(|d| d.title == "modern-web-guidance"));

    // Root clutter (package.json, plugin.json, LICENSE, README.md) must be filtered out.
    assert!(entries.iter().all(|e| {
        let loc = e.location.as_deref().unwrap_or("");
        !loc.ends_with("package.json")
            && !loc.ends_with("plugin.json")
            && !loc.ends_with("LICENSE")
            && !loc.ends_with("CONTRIBUTING.md")
    }));

    // Directory expand returns JSON child listing.
    let chrome_dir = dirs
        .iter()
        .find(|d| d.title == "chrome-extensions")
        .unwrap();
    let dir_content = skills
        .expand(&chrome_dir.id, cancel.clone())
        .await
        .unwrap()
        .unwrap();
    let dir_text = dir_content.as_text().unwrap();
    assert!(dir_text.contains("\"SKILL.md\""));

    // SKILL.md sections expand cleanly and hit memory cache on subsequent section reads.
    let sections: Vec<_> = entries
        .iter()
        .filter(|e| e.node_kind == ReferenceNodeKind::Section)
        .collect();
    assert!(sections.len() >= 2);
    for section in sections.iter().take(3) {
        let body = skills
            .expand(&section.id, cancel.clone())
            .await
            .unwrap()
            .unwrap();
        assert!(!body.as_text().unwrap().is_empty());
    }

    // Accompanying guide/reference files are discoverable and expandable.
    let resource_file = entries
        .iter()
        .find(|e| {
            e.node_kind == ReferenceNodeKind::File
                && e.location
                    .as_deref()
                    .is_some_and(|p| p.contains("/guides/") || p.contains("/references/"))
        })
        .expect("expected resource file under guides/ or references/");
    let file_body = skills
        .expand(&resource_file.id, cancel.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(!file_body.as_text().unwrap().is_empty());

    // State projection exposes skill entries with resources.
    let projection = skills.projections().unwrap();
    let rows = projection.exact.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn host_markdown_and_skills_support_memory_fallback_when_fs_unavailable() {
    let mut docs = BTreeMap::new();
    docs.insert(
        "AGENTS.md".into(),
        "# Rules\nBe concise.\n## Style\nKeep functions small.\n".into(),
    );
    let md_ref = HostMarkdownReference::from_memory("references", docs);
    let cancel = CancellationToken::new();
    md_ref.load(cancel.clone()).await.unwrap();
    let md_entries = md_ref.list().unwrap();
    let sec = md_entries
        .iter()
        .find(|e| e.node_kind == ReferenceNodeKind::Section && e.title == "Style")
        .unwrap();
    let content = md_ref.expand(&sec.id, cancel.clone()).await.unwrap().unwrap();
    assert!(content.as_text().unwrap().contains("Keep functions small."));

    let mut skill_files = BTreeMap::new();
    skill_files.insert(
        "demo-skill/SKILL.md".into(),
        b"---\nname: demo-skill\ndescription: Demo\n---\n# Intro\nHello\n## Usage\nRun it\n"
            .to_vec(),
    );
    skill_files.insert(
        "demo-skill/examples/sample.txt".into(),
        b"example content".to_vec(),
    );
    skill_files.insert("noise/package.json".into(), b"{}".to_vec());
    let mem_skills = HostSkillsReference::from_memory("skills", skill_files);
    mem_skills.load(cancel.clone()).await.unwrap();
    let listed = mem_skills.list().unwrap();
    assert!(
        listed
            .iter()
            .all(|e| !e.location.as_deref().unwrap_or("").contains("package.json"))
    );
    let ex = listed
        .iter()
        .find(|e| e.location.as_deref() == Some("demo-skill/examples/sample.txt"))
        .unwrap();
    assert_eq!(
        mem_skills
            .expand(&ex.id, cancel)
            .await
            .unwrap()
            .unwrap()
            .as_text(),
        Some("example content")
    );
}
