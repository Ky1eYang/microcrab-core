//! Observe table cleanup directly without exposing resource internals publicly.
use super::*;
use std::time::Duration;

fn file() -> (tempfile::NamedTempFile, File) {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let file = temp.reopen().unwrap();
    (temp, file)
}
async fn empty(table: &HandleTable) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if table.files.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn owner_cancellation_retires_resources_without_any_followup_operation() {
    let table = Arc::new(HandleTable::default());
    let (temp, file) = file();
    let owner = OpContext::default();
    let handle = table
        .insert_file(file, temp.path().into(), owner.clone())
        .unwrap();
    let retained = table.file(&owner, handle).unwrap();
    owner.cancel.cancel();
    empty(&table).await;
    // Retirement removes both the table identity and the descriptor even when a
    // caller retained the internal entry before cancellation won admission.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if retained.lock().unwrap().file.is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn owner_deadline_retires_resources_without_polling_the_handle() {
    let table = Arc::new(HandleTable::default());
    let (temp, file) = file();
    let owner = OpContext {
        deadline: Some(std::time::Instant::now() + Duration::from_millis(20)),
        ..OpContext::default()
    };
    table.insert_file(file, temp.path().into(), owner).unwrap();
    empty(&table).await;
}

#[tokio::test]
async fn resource_ceiling_is_bounded_and_explicit_close_releases_a_slot() {
    let table = Arc::new(HandleTable::default());
    let temp = tempfile::NamedTempFile::new().unwrap();
    let owner = OpContext::default();
    let mut handles = Vec::new();
    for _ in 0..MAX_HANDLES {
        handles.push(
            table
                .insert_file(temp.reopen().unwrap(), temp.path().into(), owner.clone())
                .unwrap(),
        );
    }
    assert!(
        table
            .insert_file(temp.reopen().unwrap(), temp.path().into(), owner.clone())
            .is_err()
    );
    table.close_file(&owner, handles[0]).unwrap();
    let replacement = table
        .insert_file(temp.reopen().unwrap(), temp.path().into(), owner.clone())
        .unwrap();
    assert_ne!(replacement, handles[0]);
    owner.cancel.cancel();
    empty(&table).await;
}
