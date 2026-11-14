//! Batch job calls exercise the real Runtime and its Host-backed execution owner.
use crate::execution_fixture::{Fixture, channel, id, waiting};
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_session::{
    Error, Limits,
    runtime::execution::{ExecutionStatus, OutputAvailability, OutputPage},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tempfile::TempDir;

fn fixture(root: &TempDir, owner: &str, limits: Limits) -> Fixture {
    Fixture::new(
        owner,
        Arc::new(LocalHost::new(root.path(), ShellLimits::default()).unwrap()),
        limits,
    )
}

async fn job(f: &Fixture, ids: Value, action: &str, args: Value) -> Value {
    serde_json::from_str(
        &f.invoke("job", json!({"id":ids,"action":action,"args":args}))
            .await
            .unwrap(),
    )
    .unwrap()
}

fn results<'a>(value: &'a Value, action: &str, ids: &[&str]) -> &'a [Value] {
    assert_eq!(value["action"], action, "{value}");
    let rows = value["results"].as_array().expect("batch result array");
    assert_eq!(rows.len(), ids.len());
    for (row, address) in rows.iter().zip(ids) {
        assert_eq!(row["id"], *address, "results must retain input order");
        assert!(row["ok"].is_boolean(), "{row}");
        if row["ok"] == true {
            assert!(row.get("result").is_some() && row.get("error").is_none());
        } else {
            assert!(row.get("error").is_some() && row.get("result").is_none());
        }
    }
    rows
}

#[tokio::test]
async fn malformed_handles_report_validation_errors_without_consuming_or_releasing_output() {
    use microcrab_session::policy::{Action, Matcher};
    for action in ["release", "read"] {
        for encoded in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let f = fixture(&root, "job-handle-validation", Limits::default());
            // Exercise the native builtin Allow rather than an explicit job grant.
            let policy = f.session.permission_controller().snapshot().unwrap();
            let mut config = policy.config;
            config
                .overrides
                .retain(|rule| rule.tool != Some(Matcher::Exact("job".into())));
            assert!(config.defaults.iter().any(|rule| {
                rule.tool == Some(Matcher::Exact("job".into())) && rule.action == Action::Allow
            }));
            f.session
                .replace_permissions(policy.revision, config)
                .unwrap();
            let receipt = f.start(waiting(), 0).await.unwrap();
            let address = id(&receipt);
            f.finish(address, "retained across malformed job calls")
                .await;
            let before = f.read(address, 0, 65536).unwrap();
            let ids = if encoded {
                let mut ids: Vec<_> = (1..=8).map(|id| format!("{}/{id}", f.owner)).collect();
                ids[7] = format!("{}/0", f.owner);
                json!(serde_json::to_string(&ids).unwrap())
            } else {
                json!([address, "foreign/not-a-number"])
            };
            let text = f
                .invoke("job", json!({"id":ids,"action":action}))
                .await
                .unwrap();
            let result: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(result["diagnostic"]["code"], "invalid_argument", "{result}");
            assert_eq!(result["diagnostic"]["layer"], "runtime");
            assert_eq!(result["diagnostic"]["phase"], "validation");
            assert_eq!(result["diagnostic"]["effects"], "not_started");
            assert_eq!(result["diagnostic"]["operation"], "job");
            assert!(result["error"].as_str().unwrap().contains("JSON array"));
            assert!(result["error"].as_str().unwrap().contains("JSON-encoded"));
            assert!(!text.contains("permission denied"));
            let after = f.read(address, 0, 65536).unwrap();
            assert_eq!(channel(&after, "stdout"), channel(&before, "stdout"));
            assert_eq!(after.cursor, before.cursor);
            assert_eq!(after.next_cursor, before.next_cursor);
            let row = f.session.executions().get(f.id(address).unwrap()).unwrap();
            assert_eq!(row.output_availability, Some(OutputAvailability::Retained));
            f.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn encoded_handles_run_batch_read_and_explicit_release_with_native_allow() {
    use microcrab_session::policy::Matcher;
    for action in ["read", "release"] {
        let root = tempfile::tempdir().unwrap();
        let f = fixture(&root, "encoded-job", Limits::default());
        let policy = f.session.permission_controller().snapshot().unwrap();
        let mut config = policy.config;
        config
            .overrides
            .retain(|rule| rule.tool != Some(Matcher::Exact("job".into())));
        f.session
            .replace_permissions(policy.revision, config)
            .unwrap();
        let mut addresses = Vec::new();
        for index in 0..8 {
            let receipt = f.start(waiting(), 0).await.unwrap();
            let address = id(&receipt).to_owned();
            f.finish(&address, &format!("buffer {index}")).await;
            addresses.push(address);
        }
        let encoded = json!(serde_json::to_string(&addresses).unwrap());
        let response = job(&f, encoded, action, json!({})).await;
        let refs: Vec<_> = addresses.iter().map(String::as_str).collect();
        for (index, row) in results(&response, action, &refs).iter().enumerate() {
            assert_eq!(row["ok"], true, "{row}");
            if action == "read" {
                let page: OutputPage = serde_json::from_value(row["result"].clone()).unwrap();
                assert!(channel(&page, "stdout").contains(&format!("buffer {index}")));
            }
            let target = f
                .session
                .executions()
                .get(f.id(&addresses[index]).unwrap())
                .unwrap();
            assert_eq!(
                target.output_availability,
                Some(if action == "read" {
                    OutputAvailability::Retained
                } else {
                    OutputAvailability::Released
                })
            );
        }
        f.close().await.unwrap();
    }
}

#[tokio::test]
async fn encoded_batch_release_is_denied_once_before_any_target_mutates() {
    for match_encoded_input in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let f = fixture(&root, "encoded-deny", Limits::default());
        let first = f.start(waiting(), 0).await.unwrap();
        let second = f.start(waiting(), 0).await.unwrap();
        f.finish(id(&first), "first retained").await;
        f.finish(id(&second), "second retained").await;
        let policy = f.session.permission_controller().snapshot().unwrap();
        let mut config = policy.config;
        let encoded = serde_json::to_string(&[id(&first), id(&second)]).unwrap();
        // Policy sees the original whole call. A string predicate can select the
        // encoded batch itself; it never quantifies over decoded target handles.
        let predicate = if match_encoded_input {
            json!({"pointer":"/id","matcher":{"kind":"exact","value":encoded}})
        } else {
            json!({"pointer":"/action","matcher":{"kind":"exact","value":"release"}})
        };
        let rule =
            json!({"tool":{"kind":"exact","value":"job"},"args":[predicate],"action":"deny"});
        config.overrides.push(serde_json::from_value(rule).unwrap());
        f.session
            .replace_permissions(policy.revision, config)
            .unwrap();
        let response = job(&f, json!(encoded), "release", json!({})).await;
        assert_eq!(
            response["diagnostic"]["code"], "permission_denied",
            "{response}"
        );
        for address in [id(&first), id(&second)] {
            let row = f.session.executions().get(f.id(address).unwrap()).unwrap();
            assert_eq!(row.output_availability, Some(OutputAvailability::Retained));
            assert!(!channel(&f.read(address, 0, 65536).unwrap(), "stdout").is_empty());
        }
        f.close().await.unwrap();
    }
}

#[tokio::test]
async fn scalar_metadata_stays_compatible_and_arrays_keep_shape_and_input_order() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "batch-shape", Limits::default());
    let first = f.start(waiting(), 0).await.unwrap();
    let second = f.start(waiting(), 0).await.unwrap();
    f.finish(id(&first), "first").await;
    f.finish(id(&second), "second").await;

    let scalar = job(&f, json!(id(&first)), "status", json!({})).await;
    assert_eq!(scalar["execution_id"], first["execution_id"]);
    assert_eq!(scalar["session_id"], "batch-shape");
    assert_eq!(scalar["status"], "succeeded");
    assert!(scalar.get("results").is_none());
    assert!(scalar["output"]["end_cursor"].as_u64().unwrap() > 0);

    // Reverse creation order: completion order and registry order are not the API order.
    for addresses in [vec![id(&second), id(&first)], vec![id(&first)]] {
        let response = job(&f, json!(addresses), "status", json!({})).await;
        for (row, address) in results(&response, "status", &addresses)
            .iter()
            .zip(&addresses)
        {
            assert_eq!(row["ok"], true);
            assert_eq!(
                row["result"]["execution_id"],
                json!(f.id(address).unwrap().0)
            );
            assert_eq!(row["result"]["status"], "succeeded");
            assert!(row["result"]["output"]["end_cursor"].as_u64().unwrap() > 0);
        }
    }
    f.close().await.unwrap();
}

#[tokio::test]
async fn invalid_batch_members_are_rejected_before_a_valid_terminal_sibling_is_released() {
    // Each case owns a fresh Session because schema-level rejection may quarantine
    // a malformed model response; that must not obscure the execution-side assertion.
    for case in 0..20 {
        let variant = case % 10;
        let root = tempfile::tempdir().unwrap();
        let f = fixture(&root, "batch-validation", Limits::default());
        let receipt = f.start(waiting(), 0).await.unwrap();
        let address = id(&receipt);
        f.finish(address, "must survive rejected release").await;
        let before = f.read(address, 0, 65536).unwrap();
        let ids = match variant {
            0 => json!([address, "batch-validation/18446744073709551615"]),
            1 => json!([address, "another-session/1"]),
            2 => json!([address, "not-an-execution-handle"]),
            3 => json!([address, address]),
            4 => json!([
                address,
                format!("batch-validation/0{}", receipt["execution_id"])
            ]),
            5 => json!([]),
            6 => json!([address, ""]),
            7 => json!([address, null]),
            8 => json!(
                std::iter::once(address.to_owned())
                    .chain((100..116).map(|number| format!("batch-validation/{number}")))
                    .collect::<Vec<_>>()
            ),
            _ => json!([address]),
        };
        let ids = if case >= 10 {
            json!(ids.to_string())
        } else {
            ids
        };
        let args = if variant == 9 {
            json!({"cursor":0})
        } else {
            json!({})
        };
        match f
            .invoke("job", json!({"id":ids,"action":"release","args":args}))
            .await
        {
            Ok(text) => {
                let rejected: Value = serde_json::from_str(&text).unwrap();
                assert!(rejected.get("error").is_some(), "case {case}: {rejected}");
                assert!(
                    rejected.get("results").is_none(),
                    "admission errors are whole-call errors"
                );
            }
            Err(Error::Protocol(_) | Error::Tool(_) | Error::Denied(_)) => {}
            Err(error) => panic!("unexpected admission failure in case {case}: {error}"),
        }
        let after = f.read(address, 0, 65536).unwrap();
        assert_eq!(
            channel(&after, "stdout"),
            channel(&before, "stdout"),
            "case {case}"
        );
        assert_eq!(after.cursor, before.cursor);
        assert_eq!(after.next_cursor, before.next_cursor);
        let row = f.session.executions().get(f.id(address).unwrap()).unwrap();
        assert_eq!(
            row.output_availability,
            Some(OutputAvailability::Retained),
            "case {case} released a sibling"
        );
        f.close().await.unwrap();
    }
}

#[tokio::test]
async fn release_reports_running_sibling_error_and_still_reclaims_terminal_sibling() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "batch-partial", Limits::default());
    let running = f.start(waiting(), 0).await.unwrap();
    let finished = f.start(waiting(), 0).await.unwrap();
    f.finish(id(&finished), "discard only terminal output")
        .await;
    let addresses = [id(&running), id(&finished)];
    let response = job(&f, json!(addresses), "release", json!({})).await;
    let rows = results(&response, "release", &addresses);
    assert_eq!(rows[0]["ok"], false);
    assert_eq!(rows[1]["ok"], true);
    let registry = f.session.executions();
    let still_running = registry.get(f.id(id(&running)).unwrap()).unwrap();
    assert_eq!(still_running.status, ExecutionStatus::Running);
    assert_eq!(
        still_running.output_availability,
        Some(OutputAvailability::Retained)
    );
    let released = registry.get(f.id(id(&finished)).unwrap()).unwrap();
    assert_eq!(released.status, ExecutionStatus::Succeeded);
    assert_eq!(
        released.output_availability,
        Some(OutputAvailability::Released)
    );
    let output = released.output.unwrap();
    assert_eq!(output.start_cursor, output.end_cursor);
    // A failed member remains usable under its original handle.
    f.finish(id(&running), "still accepts input").await;
    f.close().await.unwrap();
}

#[tokio::test]
async fn write_broadcast_delivers_the_same_input_and_eof_to_every_target() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(&root, "batch-broadcast", Limits::default());
    let first = f.start(waiting(), 0).await.unwrap();
    let second = f.start(waiting(), 0).await.unwrap();
    let addresses = [id(&second), id(&first)];
    let response = job(
        &f,
        json!(addresses),
        "write",
        json!({"data":"one broadcast\n","eof":true}),
    )
    .await;
    for row in results(&response, "write", &addresses) {
        assert_eq!(row["ok"], true);
        assert_eq!(row["result"]["accepted"], true);
    }
    for address in addresses {
        assert_eq!(f.terminal(address).await.status, ExecutionStatus::Succeeded);
        let output = f.read(address, 0, 65536).unwrap();
        assert_eq!(channel(&output, "stdout").trim_end(), "one broadcast");
        assert!(output.closed);
    }
    f.close().await.unwrap();
}

#[tokio::test]
async fn repeated_and_paginated_reads_share_one_escaped_utf8_budget_and_preserve_each_cursor() {
    let root = tempfile::tempdir().unwrap();
    let budget = 16 * 1024;
    let f = fixture(
        &root,
        "batch-pages",
        Limits {
            max_tool_output_bytes: budget,
            output_buffer_bytes: 128 * 1024,
            ..Limits::default()
        },
    );
    let first = f.start(waiting(), 0).await.unwrap();
    let second = f.start(waiting(), 0).await.unwrap();
    // Quotes/backslashes exercise JSON expansion; multibyte text exercises safe
    // byte cursors. Each producer alone exceeds the entire batch payload budget.
    let payload = "é🦀\\\"".repeat(4096);
    let addresses = [id(&first), id(&second)];
    for address in addresses {
        f.finish(address, &payload).await;
    }
    let originals: Vec<OutputPage> = addresses
        .iter()
        .map(|address| f.read(address, 0, 128 * 1024).unwrap())
        .collect();
    let mut observations = Vec::new();
    for _ in 0..2 {
        let text = f
            .invoke("job", json!({"id":addresses,"action":"read"}))
            .await
            .unwrap();
        assert!(
            text.len() <= budget,
            "read multiplied the whole-call budget"
        );
        let value: Value = serde_json::from_str(&text).unwrap();
        let pages: Vec<OutputPage> = results(&value, "read", &addresses)
            .iter()
            .map(|row| {
                assert_eq!(row["ok"], true);
                serde_json::from_value(row["result"].clone()).unwrap()
            })
            .collect();
        for (page, original) in pages.iter().zip(&originals) {
            assert_eq!(page.execution_id, original.execution_id);
            assert_eq!(page.cursor, 0);
            assert!(page.next_cursor > 0 && page.next_cursor < page.end_cursor);
            assert_eq!(page.end_cursor, original.end_cursor);
            assert_eq!(page.start_cursor, 0);
            assert!(channel(original, "stdout").starts_with(&channel(page, "stdout")));
        }
        observations.push(serde_json::to_value(pages).unwrap());
    }
    assert_eq!(observations[0], observations[1], "read must not consume");

    // Batch args expose one shared input cursor; independent output chunking and
    // envelope budgets need not produce equal next_cursor values. The repeated
    // two-target reads above verify the whole-call bound. Finish each retained
    // stream from its own returned cursor rather than borrowing its sibling's.
    for (index, address) in addresses.iter().enumerate() {
        let mut cursor = 0;
        let mut collected = String::new();
        for _ in 0..128 {
            let text = f
                .invoke(
                    "job",
                    json!({"id":[address],"action":"read","args":{"cursor":cursor}}),
                )
                .await
                .unwrap();
            assert!(
                text.len() <= budget,
                "read multiplied the whole-call budget"
            );
            let value: Value = serde_json::from_str(&text).unwrap();
            let rows = results(&value, "read", &[*address]);
            assert_eq!(rows[0]["ok"], true);
            let page: OutputPage = serde_json::from_value(rows[0]["result"].clone()).unwrap();
            assert_eq!(page.execution_id, originals[index].execution_id);
            assert_eq!(page.cursor, cursor);
            assert_eq!(page.end_cursor, originals[index].end_cursor);
            assert!(page.closed);
            if cursor < page.end_cursor {
                assert!(page.next_cursor > cursor, "bounded read must progress");
            }
            collected.push_str(&channel(&page, "stdout"));
            cursor = page.next_cursor;
            if cursor == page.end_cursor {
                break;
            }
        }
        assert_eq!(
            cursor, originals[index].end_cursor,
            "all pages must be reachable"
        );
        assert_eq!(collected, channel(&originals[index], "stdout"));
    }
    f.close().await.unwrap();
}
