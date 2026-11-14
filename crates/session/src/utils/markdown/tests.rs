use super::*;

fn flat(node: &MarkdownNode) -> Vec<&MarkdownNode> {
    let mut result = vec![node];
    for child in &node.subjects {
        result.extend(flat(child));
    }
    result
}

#[test]
fn rich_yaml_and_unicode_sections_form_an_exact_disjoint_partition() {
    let body = "前言🌊\n\n# 第一章\n正文é\n\n## 子节\n> [!IMPORTANT]\n> 多行提示。\n> Keep this line.\n\n### Hidden detail\n详情\n\n# Last\nDone.\n";
    let source = format!(
        "---\nname: 海洋知识\ndescription: |\n  First line.\n  Second line.\nlicense: MIT\ncompatibility: Python 3.11+\ntitle_depth: 2\ntags: [marine, 科学]\ntag: shared\nallowed-tools:\n  - read\n  - grep\nmetadata:\n  author: owner\n  reference:\n    title_depth: 6\nfuture:\n  nested: [one, 2, true]\n---\n{body}"
    );
    let doc = parse_markdown("catalog/ocean.md", &source).unwrap();
    assert_eq!(doc.metadata.name.as_deref(), Some("海洋知识"));
    assert_eq!(
        doc.metadata.description.as_deref(),
        Some("First line.\nSecond line.\n")
    );
    assert_eq!(doc.metadata.license.as_deref(), Some("MIT"));
    assert_eq!(doc.metadata.compatibility.as_deref(), Some("Python 3.11+"));
    assert_eq!(doc.metadata.tags, ["marine", "科学", "shared"]);
    assert_eq!(doc.metadata.title_depth, 2);
    assert_eq!(
        doc.metadata.metadata["reference"]["title_depth"].as_u64(),
        Some(6)
    );
    assert_eq!(doc.metadata.allowed_tools.as_sequence().unwrap().len(), 2);
    assert!(doc.metadata.extra["future"].as_mapping().is_some());
    assert_eq!(doc.root.title, "海洋知识");
    assert_eq!(doc.root.level, 0);
    assert_eq!(doc.root.subjects.len(), 2);
    assert_eq!(doc.root.subjects[0].subjects.len(), 1);
    let nodes = flat(&doc.root);
    let recovered: String = nodes
        .iter()
        .map(|node| doc.bodies[&node.id].as_str())
        .collect();
    assert_eq!(recovered, body);
    assert_eq!(nodes.len(), doc.bodies.len());
    for node in &nodes {
        assert_eq!(node.char_length, doc.bodies[&node.id].chars().count());
    }
    assert_eq!(doc.bodies[&doc.root.id], "前言🌊\n\n");
    assert_eq!(doc.root.char_length, 5);
    let child = &doc.root.subjects[0].subjects[0];
    assert!(doc.bodies[&child.id].contains("### Hidden detail"));
    assert_eq!(
        child.important.as_deref(),
        Some("多行提示。\nKeep this line.")
    );
    assert!(doc.root.subjects[0].important.is_none());
    assert!(
        serde_json::to_value(&doc.root.subjects[1])
            .unwrap()
            .get("important")
            .is_none()
    );
}

#[test]
fn markdown_events_include_setext_and_ignore_fenced_fake_headings_or_alerts() {
    let source = "---\ntitle_depth: 2\n---\nSetext *One*\n=====\n\n```markdown\n# Fake\n> [!IMPORTANT]\n> Fake warning\n```\n\nSecond `code`\n-----\n\n> [!IMPORTANT]\n> First paragraph.\n>\n> Second **paragraph**.\n\n> Ordinary blockquote\n\n# Final\n";
    let doc = parse_markdown("setext.md", source).unwrap();
    assert_eq!(doc.root.title, "Setext One");
    assert_eq!(
        doc.root
            .subjects
            .iter()
            .map(|node| node.title.as_str())
            .collect::<Vec<_>>(),
        ["Setext One", "Final"]
    );
    let first = &doc.root.subjects[0];
    assert!(first.important.is_none());
    assert_eq!(first.subjects.len(), 1);
    assert_eq!(first.subjects[0].title, "Second code");
    assert_eq!(
        first.subjects[0].important.as_deref(),
        Some("First paragraph.\n\nSecond paragraph.")
    );
    assert!(doc.bodies[&first.id].contains("# Fake"));
}

#[test]
fn stable_ids_separate_repeated_titles_hierarchies_locations_and_root() {
    let source = "---\ntitle_depth: 3\n---\n# A\n## Same\none\n## Same\ntwo\n# B\n## Same\nthree\n";
    let doc = parse_markdown("ns/doc.md", source).unwrap();
    let again = parse_markdown("ns/doc.md", source).unwrap();
    let elsewhere = parse_markdown("other/doc.md", source).unwrap();
    assert_eq!(doc.root, again.root);
    let ids = flat(&doc.root)
        .iter()
        .map(|node| node.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 6);
    assert!(ids.iter().all(|id| {
        id.len() == 27
            && id.starts_with("md-")
            && id[3..].bytes().all(|byte| byte.is_ascii_hexdigit())
    }));
    assert!(
        flat(&elsewhere.root)
            .iter()
            .all(|node| !ids.contains(&node.id))
    );
    assert_ne!(
        doc.root.subjects[0].subjects[0].id,
        doc.root.subjects[0].subjects[1].id
    );
    assert_ne!(
        doc.root.subjects[0].subjects[0].id,
        doc.root.subjects[1].subjects[0].id
    );
    // Content edits do not change a heading identity or its duplicate ordinal.
    let edited = parse_markdown("ns/doc.md", &source.replace("one", "changed text")).unwrap();
    assert_eq!(
        flat(&doc.root)
            .iter()
            .map(|node| &node.id)
            .collect::<Vec<_>>(),
        flat(&edited.root)
            .iter()
            .map(|node| &node.id)
            .collect::<Vec<_>>()
    );
}

#[test]
fn default_depth_discloses_h2_and_keeps_h3_in_disjoint_bodies() {
    let source = "Intro\n# Top\nOverview\n## Work\nText\n### Hidden\nDetail\n## Last\nDone\n";
    let doc = parse_markdown("guides/example.md", source).unwrap();
    assert_eq!(doc.metadata.title_depth, 2);
    assert_eq!(doc.root.title, "Top");
    assert_eq!(doc.root.subjects.len(), 1);
    let top = &doc.root.subjects[0];
    assert_eq!(top.subjects.len(), 2);
    assert_eq!(doc.bodies[&doc.root.id], "Intro\n");
    assert_eq!(doc.bodies[&top.id], "# Top\nOverview\n");
    assert_eq!(top.subjects[0].title, "Work");
    assert!(top.subjects[0].subjects.is_empty());
    assert_eq!(
        doc.bodies[&top.subjects[0].id],
        "## Work\nText\n### Hidden\nDetail\n"
    );
    let recovered: String = flat(&doc.root)
        .iter()
        .map(|node| doc.bodies[&node.id].as_str())
        .collect();
    assert_eq!(recovered, source);
    for node in flat(&doc.root) {
        assert_eq!(node.char_length, doc.bodies[&node.id].chars().count());
    }
}

#[test]
fn explicit_depth_one_hides_h2_and_preserves_matching_ids() {
    let body = "Intro\n# Top\n## Hidden\nText\n";
    let doc = parse_markdown(
        "guides/example.md",
        &format!("---\ntitle_depth: 1\n---\n{body}"),
    )
    .unwrap();
    let default = parse_markdown("guides/example.md", body).unwrap();
    assert_eq!(doc.metadata.title_depth, 1);
    assert_eq!(doc.root.subjects.len(), 1);
    assert!(doc.root.subjects[0].subjects.is_empty());
    assert_eq!(doc.root.id, default.root.id);
    assert_eq!(doc.root.subjects[0].id, default.root.subjects[0].id);
    assert_eq!(
        doc.bodies[&doc.root.subjects[0].id],
        "# Top\n## Hidden\nText\n"
    );
}

#[test]
fn root_without_disclosures_expands_all() {
    for source in ["No headings é🌊\n", "### Hidden without H1\nBody\n", ""] {
        let doc = parse_markdown("guides/example.md", source).unwrap();
        assert_eq!(doc.root.title, "example.md");
        assert!(doc.root.subjects.is_empty());
        assert_eq!(doc.bodies[&doc.root.id], source);
        assert_eq!(doc.root.char_length, source.chars().count());
    }
}

#[test]
fn yaml_validation_rejects_ambiguous_depth_and_bad_tags_without_losing_unknowns() {
    for value in ["0", "7", "-1", "1.5", "true", "null", "'2'"] {
        assert!(
            parse_markdown("x.md", &format!("---\ntitle_depth: {value}\n---\n# X\n")).is_err(),
            "accepted {value}"
        );
    }
    for yaml in [
        "tags: [ok, 3]",
        "tag: false",
        "name: [wrong]",
        "1: non-string",
        "[not, a, mapping]",
        "tags: [unterminated",
    ] {
        assert!(
            parse_markdown("x.md", &format!("---\n{yaml}\n---\n")).is_err(),
            "accepted {yaml}"
        );
    }
    assert!(parse_markdown("x.md", "---\nname: never closed\n").is_err());
    assert!(parse_markdown("", "text").is_err());
    let source = "\u{feff}---\r\nname: Windows\r\ntag: solo\r\nallowed-tools: Read Bash(git:*)\r\n...\r\n# X\r\n";
    let doc = parse_markdown("windows.md", source).unwrap();
    assert_eq!(doc.metadata.tags, ["solo"]);
    assert_eq!(
        doc.metadata.allowed_tools.as_str(),
        Some("Read Bash(git:*)")
    );
    assert_eq!(doc.bodies[&doc.root.subjects[0].id], "# X\r\n");
}
