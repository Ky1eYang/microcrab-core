//! Concrete Markdown references reach State and batch expand through real Session admission.
use microcrab_protocol::{Response, ToolCall, Usage};
use microcrab_session::{
    Auditor, Client, Session,
    context::{Content, ContentItem},
    runtime::{
        Runtime,
        tool::{DenyAll, builtin_tools},
    },
    state::reference::ReferenceSource,
};
use microcrab_transport::StateFrame;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
#[derive(Default)]
struct Model {
    calls: Mutex<Option<Value>>,
    requests: Mutex<Vec<Content>>,
}
#[async_trait::async_trait]
impl Client for Model {
    async fn complete(
        &self,
        request: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_session::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.lock().unwrap().push(request.clone());
        let tool_calls = self
            .calls
            .lock()
            .unwrap()
            .take()
            .map(|arguments| {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "expand-batch".into(),
                    name: "expand".into(),
                    arguments,
                }]
            })
            .unwrap_or_default();
        Ok(Response {
            provider_metadata: None,
            content: if tool_calls.is_empty() {
                "done".into()
            } else {
                String::new()
            },
            tool_calls,
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
async fn configured_host_sources_disclose_expand_and_patch_after_explicit_refresh() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("SKILL.md");
    std::fs::write(&path,"---\nname: sample\ndescription: Demo\ntitle_depth: 2\ntags: [rust]\n---\nIntro 中文\n# Guide\nRoot text\n## Work\n> [!IMPORTANT]\n> Keep it\n### Hidden\nDeep text\n").unwrap();
    let model = Arc::new(Model::default());
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap());
    let session = Session::builder_arc(
        model.clone(),
        Runtime::new(builtin_tools().into(), Arc::new(DenyAll)).unwrap(),
    )
    .markdown_references(
        "references",
        vec![ReferenceSource::HostPath {
            path: "SKILL.md".into(),
        }],
    )
    .host(host)
    .build()
    .unwrap();
    session.start().await.unwrap();
    let listed = session.state_handle().list_references().unwrap();
    assert_eq!(listed.len(), 3);
    let ids = listed
        .iter()
        .map(|entry| entry.info.id.clone())
        .collect::<Vec<_>>();
    *model.calls.lock().unwrap() = Some(json!({"ids":ids}));
    session
        .submit("Expand all disclosed sections")
        .await
        .unwrap();
    let output = session
        .messages()
        .into_iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content, .. } if call_id == "expand-batch" => Some(content),
            _ => None,
        })
        .unwrap();
    let expanded: Vec<Value> = serde_json::from_str(&output).unwrap();
    assert_eq!(expanded.len(), 3);
    let joined = expanded
        .iter()
        .map(|row| row["content"].as_str().unwrap())
        .collect::<String>();
    assert!(joined.starts_with("Intro 中文\n# Guide"));
    assert_eq!(joined.matches("Deep text").count(), 1);
    for row in &expanded {
        assert_eq!(
            row["char_length"].as_u64().unwrap() as usize,
            row["content"].as_str().unwrap().chars().count()
        );
    }
    assert!(session.messages().iter().any(|item| match item {
        ContentItem::State(StateFrame::StateSnapshot { state }) =>
            state["references"][0]["subjects"][0]["subjects"][0]["important"] == "Keep it",
        _ => false,
    }));
    let prefix = session.messages();
    std::fs::write(&path, "# Changed\nNew body\n").unwrap();
    session
        .submit("Ordinary snapshots do not scan")
        .await
        .unwrap();
    assert_eq!(session.state_handle().list_references().unwrap(), listed);
    session.refresh_references().await.unwrap();
    assert_eq!(
        serde_json::to_value(&session.messages()[..prefix.len()]).unwrap(),
        serde_json::to_value(&prefix).unwrap()
    );
    session.submit("Read refreshed catalog").await.unwrap();
    assert!(session.messages().iter().any(|item| {
        match item {
            ContentItem::State(StateFrame::StatePatch { set, .. }) => set
                .get("references")
                .is_some_and(|rows| rows[0]["subjects"][0]["title"] == "Changed"),
            _ => false,
        }
    }));
    session.close().await.unwrap();
}

#[tokio::test]
async fn default_h2_compact_catalog_uses_short_ids_and_expands_exact_sections() {
    let root = tempfile::tempdir().unwrap();
    let source = "# Project\nOverview\n## Workflow\nDo the work\n### Details\nKeep exact text\n## Checks\nRun checks\n";
    std::fs::write(root.path().join("AGENTS.md"), source).unwrap();
    let model = Arc::new(Model::default());
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap());
    let session = Session::builder_arc(
        model.clone(),
        Runtime::new(builtin_tools().into(), Arc::new(DenyAll)).unwrap(),
    )
    .host(host)
    .markdown_references(
        "references",
        vec![ReferenceSource::HostPath {
            path: "AGENTS.md".into(),
        }],
    )
    .build()
    .unwrap();
    session.start().await.unwrap();
    let view = session.state_view().unwrap();
    let document = &view["references"][0];
    assert!(document.get("id").is_none());
    assert!(document.get("title").is_none());
    assert!(document.get("char_length").is_none());
    assert!(document.get("metadata").is_none());
    let top = &document["subjects"][0];
    assert_eq!(top["title"], "Project");
    assert!(top.get("level").is_none());
    assert_eq!(top["subjects"].as_array().unwrap().len(), 2);
    assert_eq!(top["subjects"][0]["title"], "Workflow");
    assert!(top["subjects"][0].get("subjects").is_none());
    let listed = session.state_handle().list_references().unwrap();
    assert_eq!(listed.len(), 3);
    for entry in &listed {
        assert!(entry.info.id.starts_with("md-"));
        assert_eq!(entry.info.id.len(), 27);
    }
    *model.calls.lock().unwrap() =
        Some(json!({"ids":listed.iter().map(|row| &row.info.id).collect::<Vec<_>>()}));
    session
        .submit("Expand the disclosed catalog")
        .await
        .unwrap();
    let output = session
        .messages()
        .into_iter()
        .find_map(|item| match item {
            ContentItem::Tool { call_id, content, .. } if call_id == "expand-batch" => Some(content),
            _ => None,
        })
        .unwrap();
    let sections: Vec<Value> = serde_json::from_str(&output).unwrap();
    assert_eq!(
        sections
            .iter()
            .map(|row| row["content"].as_str().unwrap())
            .collect::<String>(),
        source
    );
    assert!(
        sections[1]["content"]
            .as_str()
            .unwrap()
            .contains("### Details")
    );
    assert!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.items.iter().any(|item| match item {
                ContentItem::State(StateFrame::StateSnapshot { state }) =>
                    state["references"][0] == *document,
                _ => false,
            }))
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn same_length_body_refresh_changes_digest_and_patch_without_rewriting_frozen_context() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("guide.md");
    let initial = "# Guide\nAlpha\n";
    let changed = "# Guide\nBravo\n";
    assert_eq!(initial.len(), changed.len());
    std::fs::write(&path, initial).unwrap();
    let model = Arc::new(Model::default());
    let host =
        Arc::new(microcrab_host_local::LocalHost::new(root.path(), Default::default()).unwrap());
    let session = Session::builder_arc(
        model.clone(),
        Runtime::new(builtin_tools().into(), Arc::new(DenyAll)).unwrap(),
    )
    .host(host)
    .markdown_references(
        "references",
        vec![ReferenceSource::HostPath {
            path: "guide.md".into(),
        }],
    )
    .build()
    .unwrap();
    session.start().await.unwrap();
    session.submit("Read initial catalog").await.unwrap();
    let before = session.state_view().unwrap()["references"].clone();
    let listed = session.state_handle().list_references().unwrap();
    let prefix = session.messages();
    let request = model.requests.lock().unwrap()[0].clone();
    std::fs::write(&path, changed).unwrap();
    session.refresh_references().await.unwrap();
    let after = session.state_view().unwrap()["references"].clone();
    assert_eq!(session.state_handle().list_references().unwrap(), listed);
    assert_eq!(listed.len(), 1, "empty preamble is not an expansion choice");
    assert!(before[0].get("id").is_none());
    assert!(after[0].get("id").is_none());
    assert_eq!(before[0]["subjects"], after[0]["subjects"]);
    assert_ne!(before[0]["content_digest"], after[0]["content_digest"]);
    assert_eq!(after[0]["content_digest"].as_str().unwrap().len(), 64);
    assert_eq!(
        session
            .state_handle()
            .read_reference(&listed[0].info.id)
            .unwrap()
            .body,
        changed
    );
    assert_eq!(
        serde_json::to_value(session.messages()).unwrap(),
        serde_json::to_value(&prefix).unwrap()
    );
    session.submit("Observe refreshed content").await.unwrap();
    assert!(session.messages().iter().any(|item| match item {
        ContentItem::State(StateFrame::StatePatch { set, .. }) =>
            set.get("references") == Some(&after),
        _ => false,
    }));
    assert_eq!(
        serde_json::to_value(&session.messages()[..prefix.len()]).unwrap(),
        serde_json::to_value(&prefix).unwrap()
    );
    {
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.last().unwrap().system, request.system);
        assert_eq!(requests.last().unwrap().tools, request.tools);
    }
    session.close().await.unwrap();
}
