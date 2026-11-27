//! SQLite-backed owner-isolated Personal Content Library with separate blob and mount tables.
use super::{
    CreateLibraryObject, LibraryLimits, LibraryObjectRecord, LibraryObjectSummary, LibraryQuota,
    PersonalLibrary, UpdateLibraryObject, apply_object_update, build_created_summary,
    validate_owner, validate_target_id,
};
use crate::sqlite::{failure, open};
use async_trait::async_trait;
use microcrab_session::Error;
use microcrab_session::state::reference::valid_library_id;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct SqlitePersonalLibrary {
    state: Arc<Mutex<Option<Connection>>>,
}

impl SqlitePersonalLibrary {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref().to_owned();
        let connection = tokio::task::spawn_blocking(move || {
            let connection = open(&path, "personal_library")?;
            connection
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS library_objects(
                        owner TEXT NOT NULL,
                        id TEXT NOT NULL,
                        summary BLOB NOT NULL,
                        is_local INTEGER NOT NULL,
                        byte_length INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        PRIMARY KEY(owner, id)
                    );
                    CREATE TABLE IF NOT EXISTS library_blobs(
                        owner TEXT NOT NULL,
                        id TEXT NOT NULL,
                        data BLOB NOT NULL,
                        PRIMARY KEY(owner, id),
                        FOREIGN KEY(owner, id) REFERENCES library_objects(owner, id) ON DELETE CASCADE
                    );
                    CREATE TABLE IF NOT EXISTS library_session_mounts(
                        owner TEXT NOT NULL,
                        session_id TEXT NOT NULL,
                        object_id TEXT NOT NULL,
                        PRIMARY KEY(owner, session_id, object_id),
                        FOREIGN KEY(owner, object_id) REFERENCES library_objects(owner, id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_library_session_mounts_obj
                        ON library_session_mounts(owner, object_id);
                    CREATE TABLE IF NOT EXISTS library_project_mounts(
                        owner TEXT NOT NULL,
                        project_id TEXT NOT NULL,
                        object_id TEXT NOT NULL,
                        PRIMARY KEY(owner, project_id, object_id),
                        FOREIGN KEY(owner, object_id) REFERENCES library_objects(owner, id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_library_project_mounts_obj
                        ON library_project_mounts(owner, object_id);",
                )
                .map_err(failure)?;
            Ok::<_, Error>(connection)
        })
        .await
        .map_err(|_| Error::config("SQLite library open worker failed"))??;
        Ok(Self {
            state: Arc::new(Mutex::new(Some(connection))),
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = state
                .lock()
                .map_err(|_| Error::Protocol("SQLite library state poisoned".into()))?;
            f(guard.as_mut().ok_or(Error::Closed)?)
        })
        .await
        .map_err(|_| Error::config("SQLite library worker failed"))?
    }
}

fn read_usage(conn: &Connection, owner: &str) -> Result<(u64, u64), Error> {
    let (used, count): (i64, i64) = conn
        .query_row(
            "SELECT coalesce(sum(CASE WHEN is_local=1 THEN byte_length ELSE 0 END), 0), count(*)
             FROM library_objects WHERE owner=?1",
            params![owner],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(failure)?;
    Ok((
        u64::try_from(used).unwrap_or(0),
        u64::try_from(count).unwrap_or(0),
    ))
}

fn read_summary(conn: &Connection, owner: &str, id: &str) -> Result<Option<LibraryObjectSummary>, Error> {
    let raw: Option<Vec<u8>> = conn
        .query_row(
            "SELECT summary FROM library_objects WHERE owner=?1 AND id=?2",
            params![owner, id],
            |row| row.get(0),
        )
        .optional()
        .map_err(failure)?;
    raw.map(|bytes| serde_json::from_slice(&bytes).map_err(Error::from))
        .transpose()
}

fn read_mounts(conn: &Connection, owner: &str, id: &str) -> Result<(Vec<String>, Vec<String>), Error> {
    let mut s_stmt = conn
        .prepare("SELECT session_id FROM library_session_mounts WHERE owner=?1 AND object_id=?2 ORDER BY session_id")
        .map_err(failure)?;
    let sessions = s_stmt
        .query_map(params![owner, id], |row| row.get(0))
        .map_err(failure)?
        .collect::<Result<Vec<String>, _>>()
        .map_err(failure)?;
    let mut p_stmt = conn
        .prepare("SELECT project_id FROM library_project_mounts WHERE owner=?1 AND object_id=?2 ORDER BY project_id")
        .map_err(failure)?;
    let projects = p_stmt
        .query_map(params![owner, id], |row| row.get(0))
        .map_err(failure)?
        .collect::<Result<Vec<String>, _>>()
        .map_err(failure)?;
    Ok((sessions, projects))
}

#[async_trait]
impl PersonalLibrary for SqlitePersonalLibrary {
    async fn quota(&self, owner: &str, limits: LibraryLimits) -> Result<LibraryQuota, Error> {
        validate_owner(owner)?;
        let owner = owner.to_owned();
        self.run(move |conn| {
            let (used_bytes, object_count) = read_usage(conn, &owner)?;
            Ok(LibraryQuota {
                used_bytes,
                max_bytes: limits.max_user_bytes,
                object_count,
                max_entries: limits.max_user_entries,
            })
        })
        .await
    }

    async fn list(&self, owner: &str) -> Result<Vec<LibraryObjectRecord>, Error> {
        validate_owner(owner)?;
        let owner = owner.to_owned();
        self.run(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT summary FROM library_objects WHERE owner=?1 ORDER BY updated_at DESC, id ASC",
                )
                .map_err(failure)?;
            let summaries = stmt
                .query_map(params![owner], |row| row.get::<_, Vec<u8>>(0))
                .map_err(failure)?
                .map(|res| {
                    let bytes = res.map_err(failure)?;
                    serde_json::from_slice::<LibraryObjectSummary>(&bytes).map_err(Error::from)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut out = Vec::with_capacity(summaries.len());
            for summary in summaries {
                let (mounted_sessions, mounted_projects) =
                    read_mounts(conn, &owner, &summary.id)?;
                out.push(LibraryObjectRecord {
                    summary,
                    mounted_sessions,
                    mounted_projects,
                });
            }
            Ok(out)
        })
        .await
    }

    async fn get(&self, owner: &str, id: &str) -> Result<Option<LibraryObjectRecord>, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Ok(None);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |conn| {
            let Some(summary) = read_summary(conn, &owner, &id)? else {
                return Ok(None);
            };
            let (mounted_sessions, mounted_projects) = read_mounts(conn, &owner, &id)?;
            Ok(Some(LibraryObjectRecord {
                summary,
                mounted_sessions,
                mounted_projects,
            }))
        })
        .await
    }

    async fn read_blob(&self, owner: &str, id: &str) -> Result<Option<Vec<u8>>, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Ok(None);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |conn| {
            conn.query_row(
                "SELECT data FROM library_blobs WHERE owner=?1 AND id=?2",
                params![owner, id],
                |row| row.get(0),
            )
            .optional()
            .map_err(failure)
        })
        .await
    }

    async fn create(
        &self,
        owner: &str,
        limits: LibraryLimits,
        request: CreateLibraryObject,
        session_mount: Option<&str>,
        project_mount: Option<&str>,
    ) -> Result<LibraryObjectRecord, Error> {
        validate_owner(owner)?;
        if let Some(session_id) = session_mount {
            validate_target_id(session_id, "session")?;
        }
        if let Some(project_id) = project_mount {
            validate_target_id(project_id, "project")?;
        }
        let owner = owner.to_owned();
        let session_mount = session_mount.map(str::to_owned);
        let project_mount = project_mount.map(str::to_owned);
        self.run(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(failure)?;
            let (used_bytes, object_count) = read_usage(&tx, &owner)?;
            let (summary, blob) =
                build_created_summary(request, limits, used_bytes, object_count)?;
            let encoded = serde_json::to_vec(&summary)?;
            let is_local = i64::from(summary.source.is_local_blob());
            let byte_length = i64::try_from(summary.byte_length.unwrap_or(0)).unwrap_or(i64::MAX);
            let updated_at = i64::try_from(summary.updated_at_unix_ms).unwrap_or(i64::MAX);
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM library_objects WHERE owner=?1 AND id=?2",
                    params![owner, summary.id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(failure)?;
            if exists.is_some() {
                return Err(Error::config("reference id already exists"));
            }
            tx.execute(
                "INSERT INTO library_objects(owner, id, summary, is_local, byte_length, updated_at)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    owner,
                    summary.id,
                    encoded,
                    is_local,
                    byte_length,
                    updated_at
                ],
            )
            .map_err(failure)?;
            if let Some(data) = blob {
                tx.execute(
                    "INSERT INTO library_blobs(owner, id, data) VALUES(?1, ?2, ?3)",
                    params![owner, summary.id, data],
                )
                .map_err(failure)?;
            }
            let mut mounted_sessions = Vec::new();
            if let Some(session_id) = session_mount {
                tx.execute(
                    "INSERT INTO library_session_mounts(owner, session_id, object_id) VALUES(?1, ?2, ?3)",
                    params![owner, session_id, summary.id],
                )
                .map_err(failure)?;
                mounted_sessions.push(session_id);
            }
            let mut mounted_projects = Vec::new();
            if let Some(project_id) = project_mount {
                tx.execute(
                    "INSERT INTO library_project_mounts(owner, project_id, object_id) VALUES(?1, ?2, ?3)",
                    params![owner, project_id, summary.id],
                )
                .map_err(failure)?;
                mounted_projects.push(project_id);
            }
            tx.commit().map_err(failure)?;
            Ok(LibraryObjectRecord {
                summary,
                mounted_sessions,
                mounted_projects,
            })
        })
        .await
    }

    async fn update(
        &self,
        owner: &str,
        limits: LibraryLimits,
        id: &str,
        update: UpdateLibraryObject,
    ) -> Result<LibraryObjectRecord, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Err(Error::reference("requested reference was not found"));
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(failure)?;
            let (used_bytes, _) = read_usage(&tx, &owner)?;
            let mut summary = read_summary(&tx, &owner, &id)?
                .ok_or_else(|| Error::reference("requested reference was not found"))?;
            let (_next_used, replacement_blob) =
                apply_object_update(&mut summary, limits, used_bytes, update)?;
            let encoded = serde_json::to_vec(&summary)?;
            let byte_length = i64::try_from(summary.byte_length.unwrap_or(0)).unwrap_or(i64::MAX);
            let updated_at = i64::try_from(summary.updated_at_unix_ms).unwrap_or(i64::MAX);
            tx.execute(
                "UPDATE library_objects SET summary=?3, byte_length=?4, updated_at=?5 WHERE owner=?1 AND id=?2",
                params![owner, id, encoded, byte_length, updated_at],
            )
            .map_err(failure)?;
            if let Some(data) = replacement_blob {
                tx.execute(
                    "INSERT INTO library_blobs(owner, id, data) VALUES(?1, ?2, ?3)
                     ON CONFLICT(owner, id) DO UPDATE SET data=excluded.data",
                    params![owner, id, data],
                )
                .map_err(failure)?;
            }
            let (mounted_sessions, mounted_projects) = read_mounts(&tx, &owner, &id)?;
            tx.commit().map_err(failure)?;
            Ok(LibraryObjectRecord {
                summary,
                mounted_sessions,
                mounted_projects,
            })
        })
        .await
    }

    async fn delete(&self, owner: &str, id: &str) -> Result<bool, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |conn| {
            let count = conn
                .execute(
                    "DELETE FROM library_objects WHERE owner=?1 AND id=?2",
                    params![owner, id],
                )
                .map_err(failure)?;
            Ok(count > 0)
        })
        .await
    }

    async fn mount_session(
        &self,
        owner: &str,
        id: &str,
        session_id: &str,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        validate_target_id(session_id, "session")?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        let session_id = session_id.to_owned();
        self.run(move |conn| {
            let exists: Option<i64> = conn
                .query_row(
                    "SELECT 1 FROM library_objects WHERE owner=?1 AND id=?2",
                    params![owner, id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(failure)?;
            if exists.is_none() {
                return Ok(false);
            }
            conn.execute(
                "INSERT OR IGNORE INTO library_session_mounts(owner, session_id, object_id) VALUES(?1, ?2, ?3)",
                params![owner, session_id, id],
            )
            .map_err(failure)?;
            Ok(true)
        })
        .await
    }

    async fn unmount_session(
        &self,
        owner: &str,
        id: &str,
        session_id: &str,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        validate_target_id(session_id, "session")?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        let session_id = session_id.to_owned();
        self.run(move |conn| {
            let count = conn
                .execute(
                    "DELETE FROM library_session_mounts WHERE owner=?1 AND session_id=?2 AND object_id=?3",
                    params![owner, session_id, id],
                )
                .map_err(failure)?;
            Ok(count > 0)
        })
        .await
    }

    async fn list_session_mounts(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Vec<LibraryObjectSummary>, Error> {
        validate_owner(owner)?;
        validate_target_id(session_id, "session")?;
        let owner = owner.to_owned();
        let session_id = session_id.to_owned();
        self.run(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT o.summary FROM library_objects o
                     INNER JOIN library_session_mounts m ON m.owner=o.owner AND m.object_id=o.id
                     WHERE m.owner=?1 AND m.session_id=?2
                     ORDER BY o.id ASC",
                )
                .map_err(failure)?;
            stmt.query_map(params![owner, session_id], |row| row.get::<_, Vec<u8>>(0))
                .map_err(failure)?
                .map(|res| {
                    let bytes = res.map_err(failure)?;
                    serde_json::from_slice(&bytes).map_err(Error::from)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .await
    }

    async fn copy_session_mounts(
        &self,
        owner: &str,
        source_session_id: &str,
        target_session_id: &str,
    ) -> Result<(), Error> {
        validate_owner(owner)?;
        validate_target_id(source_session_id, "source session")?;
        validate_target_id(target_session_id, "target session")?;
        let owner = owner.to_owned();
        let source_session_id = source_session_id.to_owned();
        let target_session_id = target_session_id.to_owned();
        self.run(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO library_session_mounts(owner, session_id, object_id)
                 SELECT owner, ?3, object_id FROM library_session_mounts
                 WHERE owner=?1 AND session_id=?2",
                params![owner, source_session_id, target_session_id],
            )
            .map_err(failure)?;
            Ok(())
        })
        .await
    }

    async fn mount_project(
        &self,
        owner: &str,
        id: &str,
        project_id: &str,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        validate_target_id(project_id, "project")?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        let project_id = project_id.to_owned();
        self.run(move |conn| {
            let exists: Option<i64> = conn
                .query_row(
                    "SELECT 1 FROM library_objects WHERE owner=?1 AND id=?2",
                    params![owner, id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(failure)?;
            if exists.is_none() {
                return Ok(false);
            }
            conn.execute(
                "INSERT OR IGNORE INTO library_project_mounts(owner, project_id, object_id) VALUES(?1, ?2, ?3)",
                params![owner, project_id, id],
            )
            .map_err(failure)?;
            Ok(true)
        })
        .await
    }

    async fn unmount_project(
        &self,
        owner: &str,
        id: &str,
        project_id: &str,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        validate_target_id(project_id, "project")?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        let project_id = project_id.to_owned();
        self.run(move |conn| {
            let count = conn
                .execute(
                    "DELETE FROM library_project_mounts WHERE owner=?1 AND project_id=?2 AND object_id=?3",
                    params![owner, project_id, id],
                )
                .map_err(failure)?;
            Ok(count > 0)
        })
        .await
    }

    async fn list_project_mounts(
        &self,
        owner: &str,
        project_id: &str,
    ) -> Result<Vec<LibraryObjectSummary>, Error> {
        validate_owner(owner)?;
        validate_target_id(project_id, "project")?;
        let owner = owner.to_owned();
        let project_id = project_id.to_owned();
        self.run(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT o.summary FROM library_objects o
                     INNER JOIN library_project_mounts m ON m.owner=o.owner AND m.object_id=o.id
                     WHERE m.owner=?1 AND m.project_id=?2
                     ORDER BY o.id ASC",
                )
                .map_err(failure)?;
            stmt.query_map(params![owner, project_id], |row| row.get::<_, Vec<u8>>(0))
                .map_err(failure)?
                .map(|res| {
                    let bytes = res.map_err(failure)?;
                    serde_json::from_slice(&bytes).map_err(Error::from)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .await
    }

    async fn mount_project_to_session(
        &self,
        owner: &str,
        project_id: &str,
        session_id: &str,
    ) -> Result<(), Error> {
        validate_owner(owner)?;
        validate_target_id(project_id, "project")?;
        validate_target_id(session_id, "session")?;
        let owner = owner.to_owned();
        let project_id = project_id.to_owned();
        let session_id = session_id.to_owned();
        self.run(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO library_session_mounts(owner, session_id, object_id)
                 SELECT owner, ?3, object_id FROM library_project_mounts
                 WHERE owner=?1 AND project_id=?2",
                params![owner, project_id, session_id],
            )
            .map_err(failure)?;
            Ok(())
        })
        .await
    }

    async fn update_shortcut_metadata(
        &self,
        owner: &str,
        id: &str,
        byte_length: Option<u64>,
        revision: String,
        subjects: Option<Vec<Value>>,
    ) -> Result<bool, Error> {
        validate_owner(owner)?;
        if !valid_library_id(id) {
            return Ok(false);
        }
        let owner = owner.to_owned();
        let id = id.to_owned();
        self.run(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(failure)?;
            let Some(mut summary) = read_summary(&tx, &owner, &id)? else {
                return Ok(false);
            };
            if summary.source.is_local_blob() {
                return Ok(false);
            }
            summary.byte_length = byte_length;
            summary.revision = revision;
            summary.subjects = subjects;
            summary.updated_at_unix_ms = super::unix_ms();
            let encoded = serde_json::to_vec(&summary)?;
            let updated_at = i64::try_from(summary.updated_at_unix_ms).unwrap_or(i64::MAX);
            tx.execute(
                "UPDATE library_objects SET summary=?3, updated_at=?4 WHERE owner=?1 AND id=?2",
                params![owner, id, encoded, updated_at],
            )
            .map_err(failure)?;
            tx.commit().map_err(failure)?;
            Ok(true)
        })
        .await
    }

    async fn close(&self) -> Result<(), Error> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = state
                .lock()
                .map_err(|_| Error::Protocol("SQLite library state poisoned".into()))?;
            guard.take();
            Ok(())
        })
        .await
        .map_err(|_| Error::config("SQLite library close worker failed"))?
    }
}
