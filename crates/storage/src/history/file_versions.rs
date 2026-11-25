//! Creator-owned store routing. File and SQLite reuse their canonical writer fences.
use super::{Backend, CreatorJournal, Error};
pub(super) use microcrab_session::runtime::journal::FileVersions;
#[cfg(any(feature = "storage-file", feature = "storage-sqlite"))]
pub(super) use microcrab_session::runtime::journal::MAX_FILE_VERSION_BYTES as MAX_BYTES;

impl CreatorJournal {
    pub(super) async fn read_file_versions(
        &self,
        session: &str,
    ) -> Result<Option<FileVersions>, Error> {
        if session != self.session_id {
            return Err(Error::config("file version Session identity mismatch"));
        }
        let ledger = match self.backend.as_ref() {
            Backend::Memory(backend) => backend.file_versions().await?,
            #[cfg(feature = "storage-file")]
            Backend::Disk(backend) => backend.file_versions().await?,
            #[cfg(feature = "storage-sqlite")]
            Backend::Sqlite(backend) => backend.file_versions().await?,
        };
        if let Some(ledger) = &ledger {
            ledger.validate(session)?;
        }
        Ok(ledger)
    }
    pub(super) async fn store_file_versions(
        &self,
        session: &str,
        next: FileVersions,
    ) -> Result<(), Error> {
        if session != self.session_id {
            return Err(Error::config("file version Session identity mismatch"));
        }
        match self.backend.as_ref() {
            Backend::Memory(backend) => backend.store_file_versions(session, next).await,
            #[cfg(feature = "storage-file")]
            Backend::Disk(backend) => backend.store_file_versions(session.to_owned(), next).await,
            #[cfg(feature = "storage-sqlite")]
            Backend::Sqlite(backend) => backend.store_file_versions(next).await,
        }
    }
}

#[cfg(all(test, any(feature = "storage-file", feature = "storage-sqlite")))]
mod tests {
    use super::*;
    use microcrab_session::Journal;
    use serde_json::{Value, json};

    fn initial() -> Value {
        json!({"session":"session","revision":1,"generation":1,
            "salt":"01234567-89ab-4cde-8fab-0123456789ab",
            "authority":{"host":"host","epoch":7},
            "entries":{"fv_0123456789abcdef":{"generation":1,"path":"notes.txt","version":"f".repeat(64),"observed":false}}})
    }

    async fn persist(journal: &dyn Journal) {
        let candidate: FileVersions = serde_json::from_value(initial()).unwrap();
        assert!(
            journal
                .load_file_versions("session")
                .await
                .unwrap()
                .is_none()
        );
        journal
            .commit_file_versions("session", candidate.clone())
            .await
            .unwrap();
        journal
            .commit_file_versions("session", candidate)
            .await
            .unwrap();
        assert!(journal.load_file_versions("other-session").await.is_err());
    }

    async fn recover_and_retire(journal: &dyn Journal) {
        let reopened = journal
            .load_file_versions("session")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::to_value(&reopened).unwrap(), initial());
        let mut exact_observation = initial();
        exact_observation["revision"] = json!(2);
        exact_observation["entries"]["fv_0123456789abcdef"]["observed"] = json!(true);
        let mut retargeted = exact_observation.clone();
        retargeted["entries"]["fv_0123456789abcdef"]["path"] = json!("different.txt");
        assert!(
            journal
                .commit_file_versions("session", serde_json::from_value(retargeted).unwrap())
                .await
                .is_err()
        );
        journal
            .commit_file_versions(
                "session",
                serde_json::from_value(exact_observation).unwrap(),
            )
            .await
            .unwrap();
        journal.retire_file_versions("session").await.unwrap();
        let retired = serde_json::to_value(
            journal
                .load_file_versions("session")
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(retired["generation"], 2);
        assert_eq!(retired["revision"], 3);
        assert!(retired["authority"].is_null());
        assert_eq!(retired["entries"]["fv_0123456789abcdef"]["generation"], 1);
        assert!(
            journal
                .commit_file_versions("session", reopened)
                .await
                .is_err()
        );
    }

    #[cfg(feature = "storage-file")]
    #[tokio::test]
    async fn file_reopen_keeps_reservations_and_irreversible_retirement() {
        let temp =
            std::env::temp_dir().join(format!("microcrab-file-versions-{}", uuid::Uuid::new_v4()));
        let dir = temp.to_str().unwrap();
        let open = || CreatorJournal::open(dir, "session".into(), true, false);
        let (journal, _) = open().await.unwrap();
        persist(&journal).await;
        journal.close().await.unwrap();
        let (journal, _) = open().await.unwrap();
        recover_and_retire(&journal).await;
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn sqlite_reopen_keeps_reservations_and_irreversible_retirement() {
        let path = std::env::temp_dir().join(format!(
            "microcrab-sqlite-file-versions-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let open = || CreatorJournal::open_sqlite(&path, "session".into(), false);
        let (journal, _) = open().await.unwrap();
        persist(&journal).await;
        journal.close().await.unwrap();
        let (journal, _) = open().await.unwrap();
        recover_and_retire(&journal).await;
    }
}
