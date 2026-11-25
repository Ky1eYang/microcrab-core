//! A maintained coding read returns large nonprocess results without Final-budget loss.
#![cfg(feature = "wasm")]
use async_trait::async_trait;
use microcrab_host_api::ProcessApi;
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_protocol::{Response, ToolCall};
use microcrab_session::{
    Auditor, Client, Limits, Session, VariableView,
    context::{Content, ContentItem},
    runtime::{
        Runtime, ToolRegistration,
        execution::{ExecutionId, ExecutionStatus, OutputPage},
        tool::DenyAll,
        wasm::{WasmAgent, WasmLimits},
    },
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
struct Once(Mutex<bool>);
#[async_trait]
impl Client for Once {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut first = self.0.lock().unwrap();
        let calls = if *first {
            *first = false;
            vec![ToolCall {
                provider_metadata: None,
                id: "large-native-read".into(),
                name: "read".into(),
                arguments: json!({"path":"large.txt","offset_line":1,"max_lines":2000,"max_bytes":65536}),
            }]
        } else {
            vec![]
        };
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: calls,
            usage: Default::default(),
        })
    }
}
struct GlobTurns(Mutex<u64>);
#[async_trait]
impl Client for GlobTurns {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        variables: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut step = self.0.lock().unwrap();
        let call = if *step % 2 == 0 {
            assert!(
                variables.get("glob_ignored").is_some(),
                "init must publish rules before the first provider request"
            );
            vec![ToolCall {
                id: format!("state-glob-{}", *step),
                name: "glob".into(),
                arguments: json!({"root":".","pattern":"**/*.png","include_ignored":*step == 4}),
                provider_metadata: None,
            }]
        } else {
            vec![]
        };
        *step += 1;
        Ok(Response {
            content: "done".into(),
            tool_calls: call,
            provider_metadata: None,
            usage: Default::default(),
        })
    }
}

#[tokio::test]
#[ignore = "set MICROCRAB_CODING_WASM to the v0.2.6 Coding Component"]
async fn maintained_coding_glob_consumes_live_state_without_rereading_ignore_files() {
    let root = tempfile::tempdir().unwrap();
    for dir in ["sub", "target"] {
        std::fs::create_dir(root.path().join(dir)).unwrap();
    }
    for name in ["root.png", "sub/nested.png", "target/hello.png"] {
        std::fs::write(root.path().join(name), b"fixture").unwrap();
    }
    std::fs::write(root.path().join(".gitignore"), "target/\n").unwrap();
    std::fs::write(root.path().join("sub/.ignore"), "nested.png\n").unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let bytes = std::fs::read(std::env::var_os("MICROCRAB_CODING_WASM").unwrap()).unwrap();
    let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(DenyAll),
    )
    .unwrap();
    let session = Session::builder(GlobTurns(Mutex::new(0)), runtime)
        .session_id("coding-live-ignore")
        .host(host)
        .agent(agent.agents()[0].clone())
        .permissions(serde_json::from_value(json!({"initialization":"allow"})).unwrap())
        .build()
        .unwrap();
    session.select_mode("plan").await.unwrap();
    session.submit("initial rules").await.unwrap();
    let state = session.state_handle();
    let initial = state.extract("glob_ignored").unwrap();
    assert_eq!(
        initial,
        json!([{"path":".gitignore","content":"target/\n"},{"path":"sub/.ignore","content":"nested.png\n"}])
    );
    std::fs::write(root.path().join(".gitignore"), "*.png\n").unwrap();
    state.inject("glob_ignored", json!([])).await.unwrap();
    session
        .submit("use the edited empty list, not the files")
        .await
        .unwrap();
    state
        .inject("glob_ignored", json!({"status":"incomplete"}))
        .await
        .unwrap();
    session.submit("explicit ignore bypass").await.unwrap();
    session.submit("unavailable ignore list").await.unwrap();
    let outputs: Vec<_> = session
        .messages()
        .into_iter()
        .filter_map(|item| match item {
            ContentItem::Tool {
                call_id, content, ..
            } if call_id.starts_with("state-glob-") => Some(content),
            _ => None,
        })
        .collect();
    assert_eq!(outputs.len(), 4);
    assert!(
        outputs[0].contains("root.png")
            && !outputs[0].contains("hello.png")
            && !outputs[0].contains("nested.png")
    );
    for index in [1, 2] {
        assert!(
            outputs[index].contains("root.png")
                && outputs[index].contains("hello.png")
                && outputs[index].contains("nested.png")
        );
    }
    assert!(outputs[3].contains("glob_ignored is unavailable or invalid"));
    session.close().await.unwrap();
}

struct ResolvedGlobTurns(Mutex<usize>);
#[async_trait]
impl Client for ResolvedGlobTurns {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        variables: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut step = self.0.lock().unwrap();
        if *step == 0 {
            assert_eq!(
                variables.get("glob_ignored"),
                Some(json!(["sub/nested.png", "target/"]))
            );
        }
        let mut arguments = json!({"root":".", "pattern":"target/hello.png"});
        if *step == 4 {
            arguments["ignored_list"] = json!(["target/"]);
        }
        if *step == 6 {
            arguments["ignored_list"] = json!([]);
        }
        if *step == 8 {
            arguments["ignored_list"] = Value::Null;
        }
        let tool_calls = if *step % 2 == 0 {
            vec![ToolCall {
                id: format!("resolved-glob-{}", *step),
                name: "glob".into(),
                arguments,
                provider_metadata: None,
            }]
        } else {
            vec![]
        };
        *step += 1;
        Ok(Response {
            content: "done".into(),
            tool_calls,
            provider_metadata: None,
            usage: Default::default(),
        })
    }
}

#[tokio::test]
#[ignore = "set MICROCRAB_RESOLVED_GLOB_WASM to the new Coding Component"]
async fn maintained_coding_glob_injects_concrete_live_paths_only_on_the_dispatch_copy() {
    let root = tempfile::tempdir().unwrap();
    for dir in ["sub", "target"] {
        std::fs::create_dir(root.path().join(dir)).unwrap();
    }
    for name in ["sub/nested.png", "target/hello.png"] {
        std::fs::write(root.path().join(name), b"fixture").unwrap();
    }
    std::fs::write(root.path().join(".gitignore"), "target/\n").unwrap();
    std::fs::write(root.path().join("sub/.ignore"), "*.png\n").unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let bytes = std::fs::read(std::env::var_os("MICROCRAB_RESOLVED_GLOB_WASM").unwrap()).unwrap();
    let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(DenyAll),
    )
    .unwrap();
    let session = Session::builder(ResolvedGlobTurns(Mutex::new(0)), runtime)
        .session_id("coding-resolved-glob")
        .host(host)
        .agent(agent.agents()[0].clone())
        .permissions(serde_json::from_value(json!({"initialization":"allow"})).unwrap())
        .build()
        .unwrap();
    session.select_mode("plan").await.unwrap();
    session.submit("initial exclusions").await.unwrap();
    let state = session.state_handle();
    std::fs::write(root.path().join(".gitignore"), "*.png\n").unwrap();
    state.inject("glob_ignored", json!([])).await.unwrap();
    session.submit("edited exclusions").await.unwrap();
    session.submit("per-call override").await.unwrap();
    state
        .inject("glob_ignored", json!({"status":"unavailable"}))
        .await
        .unwrap();
    session
        .submit("explicit empty list despite unavailable state")
        .await
        .unwrap();
    session.submit("explicit null is rejected").await.unwrap();
    session
        .submit("unavailable default is rejected")
        .await
        .unwrap();
    let items = session.messages();
    let calls: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            ContentItem::Assistant { tool_calls, .. } => Some(tool_calls),
            _ => None,
        })
        .flatten()
        .filter(|call| call.id.starts_with("resolved-glob-"))
        .collect();
    assert_eq!(calls.len(), 6);
    for index in [0, 1, 5] {
        assert!(calls[index].arguments.get("ignored_list").is_none());
    }
    assert_eq!(calls[2].arguments["ignored_list"], json!(["target/"]));
    assert_eq!(calls[3].arguments["ignored_list"], json!([]));
    assert_eq!(calls[4].arguments["ignored_list"], Value::Null);
    let output: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            ContentItem::Tool {
                call_id, content, ..
            } if call_id.starts_with("resolved-glob-") => Some(content),
            _ => None,
        })
        .collect();
    assert_eq!(output.len(), 6);
    for index in [0, 1, 2, 3] {
        assert_eq!(
            output[index].contains("hello.png"),
            matches!(index, 1 | 3),
            "{}",
            output[index]
        );
    }
    for index in [4, 5] {
        assert!(output[index].contains("Single(Array)"), "{}", output[index]);
    }
    session.close().await.unwrap();
}

fn append_page(body: &mut String, page: &OutputPage) {
    assert!(page.closed);
    assert_eq!(page.start_cursor, 0);
    for chunk in &page.chunks {
        assert_eq!(
            chunk.channel.as_deref(),
            Some("result"),
            "nonprocess output uses the guest's declared result channel"
        );
        body.push_str(&chunk.content);
    }
}
#[tokio::test]
async fn maintained_coding_large_read_preserves_escaped_unicode_through_closed_buffer_pages_and_explicit_release()
 {
    let root = tempfile::tempdir().unwrap();
    let mut content = String::new();
    let mut line = 0;
    while content.len() < 32 * 1024 {
        line += 1;
        content.push_str(&format!("line-{line:04}\t\"quoted\"\\path 界🙂\u{0001}\n"));
    }
    assert!(line < 2000 && content.len() < 65536);
    std::fs::write(root.path().join("large.txt"), &content).unwrap();
    let numbered = content
        .split_inclusive('\n')
        .enumerate()
        .map(|(index, text)| format!("{}\t{}", index + 1, text))
        .collect::<String>();
    let bytes = std::fs::read(
        std::env::var_os("MICROCRAB_CODING_WASM")
            .expect("parent gate supplies the freshly built coding component"),
    )
    .unwrap();
    let host = Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap());
    let agent = WasmAgent::load(&bytes, WasmLimits::default(), host.clone()).unwrap();
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(DenyAll),
    )
    .unwrap();
    let session = Session::builder(Once(Mutex::new(true)), runtime)
        .session_id("coding-buffered-read")
        .host(host.clone())
        .agent(agent.agents()[0].clone())
        .permissions(serde_json::from_value(json!({"initialization":"allow"})).unwrap())
        .build()
        .unwrap();
    session.select_mode("plan").await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(20),
        session.submit("read the complete bounded file"),
    )
    .await
    .expect("closed nonprocess output must settle the tool turn")
    .unwrap();
    let receipt_text = session
        .messages()
        .iter()
        .find_map(|item| match item {
            ContentItem::Tool {
                call_id, content, ..
            } if call_id == "large-native-read" => Some(content.clone()),
            _ => None,
        })
        .expect("actual maintained coding read returns a tool result");
    assert!(
        receipt_text.len() <= Limits::default().max_tool_output_bytes,
        "escaped initial receipt stays within the model budget"
    );
    let receipt: Value = serde_json::from_str(&receipt_text).unwrap();
    assert_eq!(receipt["session_id"], "coding-buffered-read");
    assert_eq!(receipt["tool"], "read");
    assert_eq!(receipt["status"], "succeeded");
    assert!(receipt.get("pid").is_none() && receipt.get("job_id").is_none());
    assert!(
        !ProcessApi::has_running_processes(host.as_ref()),
        "a nonprocess read does not acquire process ownership"
    );
    let id = ExecutionId(
        receipt["execution_id"]
            .as_u64()
            .expect("large read has an actual generic execution identity"),
    );
    let row = session.executions().get(id).unwrap();
    assert_eq!(row.status, ExecutionStatus::Succeeded);
    assert!(row.failure.is_none());
    let metadata = row
        .output
        .expect("large read uses an actual retained Runtime buffer");
    assert!(metadata.closed && metadata.start_cursor == 0);
    assert!(metadata.bytes > Limits::default().max_tool_output_bytes as u64);
    assert_eq!(metadata.bytes, metadata.end_cursor - metadata.start_cursor);
    let initial: OutputPage = serde_json::from_value(receipt["initial"].clone()).unwrap();
    assert_eq!(initial.cursor, 0);
    assert!(
        initial.next_cursor < initial.end_cursor,
        "the initial receipt cannot contain the entire result"
    );
    let mut body = String::new();
    append_page(&mut body, &initial);
    let mut cursor = initial.next_cursor;
    for _ in 0..256 {
        if cursor == initial.end_cursor {
            break;
        }
        let page = session.read_execution_output(id, cursor, 2048).unwrap();
        let again = session.read_execution_output(id, cursor, 2048).unwrap();
        assert_eq!(
            serde_json::to_value(&page).unwrap(),
            serde_json::to_value(again).unwrap(),
            "UI reads at the same cursor are nonconsuming"
        );
        assert_eq!(page.cursor, cursor);
        assert!(page.next_cursor > cursor);
        append_page(&mut body, &page);
        cursor = page.next_cursor;
    }
    assert_eq!(cursor, initial.end_cursor);
    assert_eq!(body.len() as u64, metadata.bytes);
    let decoded: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(decoded["content"], numbered);
    assert_eq!(decoded["start_line"], 1);
    assert_eq!(decoded["truncated"], false);
    assert!(decoded["next_line"].is_null());
    let version = decoded["version"].as_str().unwrap();
    let digest = version.strip_prefix("fv_").expect("current Coding returns a scoped file-version reference");
    assert!(matches!(digest.len(), 16 | 20 | 24));
    assert!(digest.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
    let mut model_read = String::new();
    let mut model_cursor = 0;
    for _ in 0..256 {
        let page = session.executions().read(id, model_cursor, 2048).unwrap();
        assert_eq!(page.cursor, model_cursor);
        assert_eq!(page.start_cursor, 0);
        append_page(&mut model_read, &page);
        model_cursor = page.next_cursor;
        if model_cursor == page.end_cursor {
            break;
        }
    }
    assert_eq!(model_cursor, metadata.end_cursor);
    assert_eq!(
        model_read, body,
        "initial and UI reads preserved every byte for later model reads"
    );
    session
        .executions()
        .release(id, CancellationToken::new())
        .await
        .unwrap();
    let reread = session.read_execution_output(id, 0, 256 * 1024).unwrap();
    assert!(reread.chunks.is_empty() && reread.closed);
    assert_eq!(reread.start_cursor, metadata.end_cursor);
    assert_eq!(reread.cursor, metadata.end_cursor);
    let released = session.executions().get(id).unwrap().output.unwrap();
    assert_eq!(released.start_cursor, released.end_cursor);
    session.close().await.unwrap();
    assert!(!ProcessApi::has_running_processes(host.as_ref()));
}
