use super::*;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Server {
    origin: String,
    routes: Arc<Mutex<BTreeMap<String, (u16, String)>>>,
    requests: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let routes = Arc::new(Mutex::new(BTreeMap::<String, (u16, String)>::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let responses = routes.clone();
        let observed = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let mut buffer = [0u8; 1024];
                    let count = stream.read(&mut buffer).await.unwrap();
                    if count == 0 || request.len() + count > 8192 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..count]);
                }
                let request = String::from_utf8(request).unwrap();
                let path = request.split_whitespace().nth(1).unwrap().to_owned();
                observed.lock().unwrap().push(path.clone());
                let (status, body) = responses
                    .lock()
                    .unwrap()
                    .get(&path)
                    .cloned()
                    .unwrap_or((404, "missing".into()));
                let headers = if status == 302 {
                    "Location: /outside.md\r\n"
                } else {
                    ""
                };
                let response = format!(
                    "HTTP/1.1 {status} Fixture\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Self {
            origin,
            routes,
            requests,
            task,
        }
    }
    fn set(&self, path: &str, status: u16, body: &str) {
        self.routes
            .lock()
            .unwrap()
            .insert(path.into(), (status, body.into()));
    }
    fn paths(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[test]
fn compact_projection_preserves_authored_values_and_rejects_duplicate_ids() {
    let mut catalog = Catalog::default();
    let source = "---\nname: Named\ndescription: Useful\nmetadata:\n  enabled: false\n  count: 0\n  positions: [null, '', [], {}, false, 0]\n---\nIntro\n# H1\n## H2\nBody\n";
    let doc = parse_markdown("skills/doc.md", source).unwrap();
    let root_id = doc.root.id.clone();
    add_document(
        &mut catalog,
        "skills/doc.md".into(),
        doc.clone(),
        "digest".into(),
    )
    .unwrap();
    let projection = &catalog.projection[0];
    assert_eq!(projection["id"], root_id);
    assert!(projection.get("level").is_none());
    assert_eq!(projection["metadata"]["metadata"]["enabled"], false);
    assert_eq!(projection["metadata"]["metadata"]["count"], 0);
    assert_eq!(
        projection["metadata"]["metadata"]["positions"],
        json!([null, "", [], {}, false, 0])
    );
    for key in [
        "license",
        "compatibility",
        "allowed_tools",
        "tags",
        "extra",
        "title_depth",
    ] {
        assert!(projection["metadata"].get(key).is_none(), "{key}");
    }
    assert!(
        add_document(&mut catalog, "collision.md".into(), doc, "other".into())
            .unwrap_err()
            .to_string()
            .contains("duplicate Markdown reference ID")
    );
}

#[tokio::test]
async fn local_paths_and_glob_deduplicate_parse_expand_and_refresh_atomically() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("docs")).unwrap();
    // File-only reference globs must not attempt to read matching directory nodes.
    std::fs::create_dir(root.path().join("docs/directory.md")).unwrap();
    let path = root.path().join("docs/技能.md");
    let source = "---\nname: 技能\ntitle_depth: 2\ntags: [example]\n---\n前言🌊\n# Main\n正文é\n## Detail\n> [!IMPORTANT]\n> Remember.\n";
    std::fs::write(&path, source).unwrap();
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap());
    let sources = vec![
        ReferenceSource::HostPath {
            path: "docs/技能.md".into(),
        },
        ReferenceSource::HostPath {
            path: "./docs/技能.md".into(),
        },
        ReferenceSource::HostGlob {
            root: "docs".into(),
            pattern: "*.md".into(),
        },
    ];
    let catalog = MarkdownReferences::new("skills", sources.clone(), Some(host.clone())).limits(
        ReferenceLimits {
            max_documents: 1,
            ..Default::default()
        },
    );
    catalog.load().await.unwrap();
    let projection = catalog.change().unwrap().unwrap();
    assert_eq!(projection.as_array().unwrap().len(), 1);
    assert_eq!(projection[0]["title"], "技能");
    assert_eq!(projection[0]["metadata"]["tags"], json!(["example"]));
    assert_eq!(
        projection[0]["subjects"][0]["subjects"][0]["important"],
        "Remember."
    );
    let listed = catalog.list().unwrap();
    assert_eq!(listed.len(), 3);
    let preamble = catalog.read(&listed[0].id).unwrap().unwrap();
    assert_eq!(preamble.body, "前言🌊\n");
    assert_eq!(projection[0]["char_length"], preamble.body.chars().count());
    assert!(catalog.read("unknown").unwrap().is_none());
    assert!(
        catalog
            .read(&listed[1].id)
            .unwrap()
            .unwrap()
            .body
            .starts_with("# Main")
    );
    assert!(
        catalog
            .read(&listed[2].id)
            .unwrap()
            .unwrap()
            .body
            .starts_with("## Detail")
    );
    let other = MarkdownReferences::new("memory", sources, Some(host));
    other.load().await.unwrap();
    assert_ne!(other.list().unwrap()[0].id, listed[0].id);
    // Pure reads neither enumerate nor touch a deleted/invalid backing source.
    std::fs::write(&path, "---\ntitle_depth: broken\n---\n# Wrong\n").unwrap();
    assert!(catalog.load().await.is_err());
    assert_eq!(catalog.change().unwrap().unwrap(), projection);
    assert_eq!(catalog.read(&listed[0].id).unwrap().unwrap(), preamble);
    std::fs::remove_file(&path).unwrap();
    assert_eq!(catalog.list().unwrap(), listed);
    assert_eq!(catalog.change().unwrap().unwrap(), projection);
    assert!(catalog.load().await.is_err());
    assert_eq!(catalog.change().unwrap().unwrap(), projection);
    std::fs::write(&path, source).unwrap();
    std::fs::write(root.path().join("docs/b.md"), "# B\n").unwrap();
    std::fs::write(root.path().join("docs/c.md"), "# C\n").unwrap();
    assert!(catalog.load().await.is_err());
    assert_eq!(catalog.change().unwrap().unwrap(), projection);
}

#[tokio::test]
async fn host_paging_respects_shipped_line_caps_and_rejects_document_overflow() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("large.md");
    let body = format!("# Long\n{}", "x\n".repeat(100_005));
    std::fs::write(&path, &body).unwrap();
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap());
    let catalog = MarkdownReferences::new(
        "skills",
        vec![ReferenceSource::HostPath {
            path: "large.md".into(),
        }],
        Some(host),
    );
    catalog.load().await.unwrap();
    let listed = catalog.list().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(catalog.read(&listed[0].id).unwrap().unwrap().body, body);
    let before = catalog.change().unwrap();
    std::fs::write(&path, format!("# Too big\n{}", "x\n".repeat(600_000))).unwrap();
    assert!(catalog.load().await.is_err());
    assert_eq!(catalog.change().unwrap(), before);
}

#[tokio::test]
async fn directory_url_loads_only_quoted_same_origin_direct_markdown_children() {
    let server = Server::new().await;
    server.set(
        "/docs/",
        200,
        r#"<a title="x > y" href="one.md">one</a>
<A HREF='two.MARKDOWN'>two</A><a href="one.md">duplicate</a>
<a data-href="fake.md" href="one.md">data</a>
<a title="href='fake.md'" href="one.md">quoted attribute</a>
<!-- <a href="comment.md">fake</a> -->
<script>const x='<a href="script.md">fake</a>';</script>
<a href="../outside.md">parent</a><a href="nested/deep.md">nested</a>
<a href="https://example.invalid/remote.md">remote</a>
<a href="one.md?query=1">query</a><a href="one.md#fragment">fragment</a>
<a href=unquoted.md>unquoted</a><a href="nested%2Fdeep.md">encoded</a>
<a href="not.txt">other</a>"#,
    );
    server.set(
        "/docs/one.md",
        200,
        "---\nname: Remote one\n---\n# One\nHello é🌊\n",
    );
    server.set("/docs/two.MARKDOWN", 200, "# Two\nBody\n");
    let catalog = MarkdownReferences::new(
        "skills",
        vec![
            ReferenceSource::DirectoryUrl {
                url: format!("{}/docs/", server.origin),
            },
            ReferenceSource::Url {
                url: format!("{}/docs/one.md#ignored", server.origin),
            },
        ],
        None,
    );
    catalog.load().await.unwrap();
    assert_eq!(
        server.paths(),
        ["/docs/", "/docs/one.md", "/docs/two.MARKDOWN"]
    );
    let listed = catalog.list().unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().any(|entry| entry.title == "One"));
    assert_eq!(
        catalog.change().unwrap().unwrap()[0]["metadata"]["name"],
        "Remote one"
    );
    let body = listed
        .iter()
        .find_map(|entry| {
            catalog
                .read(&entry.id)
                .unwrap()
                .filter(|content| content.body.contains("Hello"))
        })
        .unwrap();
    assert!(body.body.contains("é🌊"));
    let snapshot = catalog.change().unwrap();
    server.set("/docs/one.md", 500, "failed");
    assert!(catalog.load().await.is_err());
    assert_eq!(catalog.change().unwrap(), snapshot);
    assert_eq!(catalog.read(&body.id).unwrap().unwrap(), body);
    let count = server.paths().len();
    catalog.list().unwrap();
    catalog.read(&body.id).unwrap();
    catalog.change().unwrap();
    assert_eq!(server.paths().len(), count);
}

#[tokio::test]
async fn directory_count_listing_total_and_redirects_fail_before_publication() {
    let server = Server::new().await;
    let listing = "<a href='one.md'>one</a><a href='two.md'>two</a>";
    server.set("/docs/", 200, listing);
    server.set("/docs/one.md", 200, "# One\n");
    server.set("/docs/two.md", 200, "# Two\n");
    let source = vec![ReferenceSource::DirectoryUrl {
        url: format!("{}/docs/", server.origin),
    }];
    let catalog = MarkdownReferences::new("skills", source.clone(), None).limits(ReferenceLimits {
        max_documents: 1,
        ..Default::default()
    });
    assert!(catalog.load().await.is_err());
    assert_eq!(server.paths(), ["/docs/"]);
    assert!(catalog.list().unwrap().is_empty());
    let catalog = MarkdownReferences::new("skills", source, None).limits(ReferenceLimits {
        max_documents: 2,
        max_document_bytes: 128,
        max_total_bytes: listing.len(),
    });
    assert!(catalog.load().await.is_err());
    assert_eq!(server.paths(), ["/docs/", "/docs/"]);
    let direct = MarkdownReferences::new(
        "skills",
        vec![ReferenceSource::Url {
            url: format!("{}/redirect", server.origin),
        }],
        None,
    );
    server.set("/redirect", 302, "");
    assert!(direct.load().await.is_err());
    assert_eq!(server.paths().last().unwrap(), "/redirect");
    assert!(!server.paths().iter().any(|path| path == "/outside.md"));
    assert!(direct.list().unwrap().is_empty());
}
