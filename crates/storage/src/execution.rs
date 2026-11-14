//! Immutable execution identity pins, shared by file and SQLite deployments.
//! Pins are published with create_new under a process-local gate and never replaced;
//! crash leftovers cannot change an existing identity or silently migrate old history.
use microcrab_session::Error;
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectBinding {
    pub project_id: String,
    pub owner: String,
    pub host_id: String,
    pub workspace_id: String,
    pub workspace_root: String,
    pub descriptor_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionBinding {
    pub session_id: String,
    pub project: ProjectBinding,
    pub component_digest: Option<String>,
    pub discovery_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionComponent {
    pub session_id: String,
    pub component_digest: Option<String>,
    pub discovery_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectGrant {
    pub project_id: String,
    pub owner: String,
    pub backend_id: String,
    pub binding_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationGrant {
    pub conversation_id: String,
    pub browser_id: String,
    pub workspace_id: String,
    pub owner: String,
    pub backend_id: String,
    pub binding_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationBinding {
    pub conversation_id: String,
    pub owner: String,
    pub host_id: String,
    pub workspace_id: String,
    pub workspace_root: String,
    pub descriptor_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationSessionBinding {
    pub session_id: String,
    pub conversation: ConversationBinding,
    pub component_digest: Option<String>,
    pub discovery_digest: String,
}
/// Upper bound on cached pin bodies (each at most 128 KiB, usually < 1 KiB).
const PIN_CACHE_ENTRIES: usize = 4096;

#[derive(Clone)]
pub struct ExecutionPins {
    root: PathBuf,
    gate: Arc<Mutex<()>>,
    /// Pins are write-once (published by hard link, never replaced or
    /// removed), so a body observed on disk stays authoritative for the life
    /// of the process. Only present pins are cached; absence is re-read.
    cache: Arc<std::sync::Mutex<std::collections::HashMap<PathBuf, Arc<Vec<u8>>>>>,
}
impl ExecutionPins {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_owned(),
            gate: Arc::new(Mutex::new(())),
            cache: Arc::default(),
        }
    }
    fn cached(&self, path: &Path) -> Option<Arc<Vec<u8>>> {
        self.cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(path)
            .cloned()
    }
    fn remember(&self, path: PathBuf, bytes: Vec<u8>) {
        let mut cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
        if cache.len() >= PIN_CACHE_ENTRIES {
            cache.clear();
        }
        cache.insert(path, Arc::new(bytes));
    }
    async fn read<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        kind: &str,
        id: &str,
    ) -> Result<Option<T>, Error> {
        let path = self.path(kind, id)?;
        if let Some(bytes) = self.cached(&path) {
            return Ok(Some(serde_json::from_slice(&bytes)?));
        }
        let key = path.clone();
        let _gate = self.gate.lock().await;
        let bytes = tokio::task::spawn_blocking(move || {
            let mut file = match std::fs::File::open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let mut bytes = vec![];
            (&mut file).take(128 * 1024 + 1).read_to_end(&mut bytes)?;
            if bytes.len() > 128 * 1024 {
                return Err(Error::config("execution identity exceeds budget"));
            }
            Ok::<_, Error>(Some(bytes))
        })
        .await
        .map_err(|_| Error::Protocol("identity read worker failed".into()))??;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let value = serde_json::from_slice(&bytes)?;
        self.remember(key, bytes);
        Ok(Some(value))
    }
    fn path(&self, kind: &str, id: &str) -> Result<PathBuf, Error> {
        super::directory::identity(id)?;
        let key = super::history::content_hash(&(kind, id))?;
        Ok(self.root.join(format!("{kind}-{key}.json")))
    }
    async fn pin<T: Serialize + serde::de::DeserializeOwned + PartialEq + Send + 'static>(
        &self,
        kind: &str,
        id: &str,
        value: T,
    ) -> Result<T, Error> {
        let path = self.path(kind, id)?;
        let root = self.root.clone();
        let _gate = self.gate.lock().await;
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&root)?;
            let bytes = serde_json::to_vec(&value)?;
            if bytes.len() > 128 * 1024 {
                return Err(Error::config("execution identity exceeds budget"));
            }
            // A completed staging file is linked into place without replacing a pin.
            // Both paths are on one filesystem. Hard-link failure is fail-closed.
            let staging = root.join(format!("pending-{}", uuid::Uuid::new_v4()));
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            let published = std::fs::hard_link(&staging, &path);
            std::fs::remove_file(&staging)?;
            match published {
                Ok(()) => Ok(value),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let mut bytes = vec![];
                    std::fs::File::open(path)?
                        .take(128 * 1024 + 1)
                        .read_to_end(&mut bytes)?;
                    if bytes.len() > 128 * 1024 {
                        return Err(Error::config("execution identity exceeds budget"));
                    }
                    let current: T = serde_json::from_slice(&bytes)?;
                    if current != value {
                        return Err(super::directory::conflict());
                    }
                    Ok(current)
                }
                Err(error) => Err(error.into()),
            }
        })
        .await
        .map_err(|_| Error::Protocol("identity publication worker failed".into()))?
    }
    pub async fn backend_id(&self) -> Result<String, Error> {
        if let Some(id) = self.read::<String>("backend", "identity").await? {
            return Ok(id);
        }
        match self
            .pin("backend", "identity", uuid::Uuid::new_v4().to_string())
            .await
        {
            Ok(id) => Ok(id),
            Err(error) => self.read("backend", "identity").await?.ok_or(error),
        }
    }
    pub async fn conversation_grant(&self, id: &str) -> Result<Option<ConversationGrant>, Error> {
        self.read("conversation-grant", id).await
    }
    pub async fn pin_conversation_grant(
        &self,
        value: ConversationGrant,
    ) -> Result<ConversationGrant, Error> {
        for field in [
            &value.conversation_id,
            &value.browser_id,
            &value.workspace_id,
            &value.owner,
            &value.backend_id,
            &value.binding_id,
        ] {
            super::directory::identity(field)?;
        }
        self.pin("conversation-grant", &value.conversation_id.clone(), value)
            .await
    }
    pub async fn conversation(&self, id: &str) -> Result<Option<ConversationBinding>, Error> {
        self.read("conversation", id).await
    }
    pub async fn pin_conversation(
        &self,
        value: ConversationBinding,
    ) -> Result<ConversationBinding, Error> {
        for field in [
            &value.conversation_id,
            &value.owner,
            &value.host_id,
            &value.workspace_id,
        ] {
            super::directory::identity(field)?;
        }
        if value.workspace_root.trim().is_empty()
            || value.workspace_root.len() > 32768
            || value.workspace_root.contains('\0')
        {
            return Err(Error::config("invalid browser workspace root"));
        }
        super::history::valid_hash(&value.descriptor_digest)?;
        if self
            .conversation_grant(&value.conversation_id)
            .await?
            .is_none_or(|grant| {
                grant.owner != value.owner
                    || grant.browser_id != value.host_id
                    || grant.workspace_id != value.workspace_id
            })
        {
            return Err(super::directory::conflict());
        }
        self.pin("conversation", &value.conversation_id.clone(), value)
            .await
    }
    pub async fn pin_conversation_workspace(
        &self,
        key: &str,
        value: &ConversationBinding,
    ) -> Result<String, Error> {
        self.pin("conversation-workspace", key, value.conversation_id.clone())
            .await
    }
    pub async fn conversation_session(
        &self,
        id: &str,
    ) -> Result<Option<ConversationSessionBinding>, Error> {
        self.read("conversation-session", id).await
    }
    pub async fn pin_conversation_session(
        &self,
        value: ConversationSessionBinding,
    ) -> Result<ConversationSessionBinding, Error> {
        super::directory::identity(&value.session_id)?;
        // One Conversation owns exactly one durable workspace; forks must request
        // a distinct binding rather than silently sharing another Conversation.
        if value.session_id != value.conversation.conversation_id
            || self.conversation(&value.session_id).await?.as_ref() != Some(&value.conversation)
        {
            return Err(super::directory::conflict());
        }
        if let Some(digest) = &value.component_digest {
            super::history::valid_hash(digest)?;
        }
        super::history::valid_hash(&value.discovery_digest)?;
        self.pin("conversation-session", &value.session_id.clone(), value)
            .await
    }
    pub async fn grant(&self, project: &str) -> Result<Option<ProjectGrant>, Error> {
        self.read("grant", project).await
    }
    pub async fn pin_grant(&self, value: ProjectGrant) -> Result<ProjectGrant, Error> {
        for field in [
            &value.project_id,
            &value.owner,
            &value.backend_id,
            &value.binding_id,
        ] {
            super::directory::identity(field)?;
        }
        self.pin("grant", &value.project_id.clone(), value).await
    }
    pub async fn component(&self, session: &str) -> Result<Option<SessionComponent>, Error> {
        self.read("component", session).await
    }
    pub async fn pin_component(&self, value: SessionComponent) -> Result<SessionComponent, Error> {
        super::directory::identity(&value.session_id)?;
        if let Some(digest) = &value.component_digest {
            super::history::valid_hash(digest)?;
        }
        super::history::valid_hash(&value.discovery_digest)?;
        self.pin("component", &value.session_id.clone(), value)
            .await
    }
    pub async fn project(&self, project: &str) -> Result<Option<ProjectBinding>, Error> {
        self.read("project", project).await
    }
    pub async fn session(&self, session: &str) -> Result<Option<SessionBinding>, Error> {
        self.read("session", session).await
    }
    pub async fn pin_project(&self, value: ProjectBinding) -> Result<ProjectBinding, Error> {
        for field in [
            &value.project_id,
            &value.owner,
            &value.host_id,
            &value.workspace_id,
        ] {
            super::directory::identity(field)?;
        }
        if value.workspace_root.trim().is_empty()
            || value.workspace_root.len() > 32768
            || value.workspace_root.contains('\0')
        {
            return Err(Error::config("invalid bridge workspace root"));
        }
        super::history::valid_hash(&value.descriptor_digest)?;
        self.pin("project", &value.project_id.clone(), value).await
    }
    pub async fn pin_session(&self, value: SessionBinding) -> Result<SessionBinding, Error> {
        super::directory::identity(&value.session_id)?;
        if let Some(digest) = &value.component_digest {
            super::history::valid_hash(digest)?;
        }
        super::history::valid_hash(&value.discovery_digest)?;
        if self.project(&value.project.project_id).await?.as_ref() != Some(&value.project) {
            return Err(super::directory::conflict());
        }
        self.pin("session", &value.session_id.clone(), value).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn conversation_pins_reopen_without_projects_and_refuse_identity_changes() {
        let root = std::env::temp_dir().join(format!(
            "microcrab-conversation-pins-{}",
            uuid::Uuid::new_v4()
        ));
        let pins = ExecutionPins::new(&root);
        let grant = ConversationGrant {
            conversation_id: "c".into(),
            browser_id: "browser".into(),
            workspace_id: "workspace".into(),
            owner: "alice".into(),
            backend_id: "backend".into(),
            binding_id: "binding".into(),
        };
        pins.pin_conversation_grant(grant.clone()).await.unwrap();
        let mut foreign = grant.clone();
        foreign.owner = "bob".into();
        assert!(pins.pin_conversation_grant(foreign).await.is_err());
        let binding = ConversationBinding {
            conversation_id: "c".into(),
            owner: "alice".into(),
            host_id: "browser".into(),
            workspace_id: "workspace".into(),
            workspace_root: "/workspace".into(),
            descriptor_digest: "a".repeat(64),
        };
        pins.pin_conversation(binding.clone()).await.unwrap();
        let session = ConversationSessionBinding {
            session_id: "c".into(),
            conversation: binding.clone(),
            component_digest: Some("b".repeat(64)),
            discovery_digest: "c".repeat(64),
        };
        pins.pin_conversation_session(session.clone())
            .await
            .unwrap();
        let reopened = ExecutionPins::new(&root);
        assert_eq!(reopened.conversation_grant("c").await.unwrap(), Some(grant));
        assert_eq!(
            reopened.conversation_session("c").await.unwrap(),
            Some(session.clone())
        );
        assert!(reopened.project("c").await.unwrap().is_none());
        assert!(reopened.session("c").await.unwrap().is_none());
        let mut changed = binding;
        changed.workspace_root = "/other".into();
        assert!(reopened.pin_conversation(changed).await.is_err());
        let mut changed = session.clone();
        changed.session_id = "different".into();
        assert!(reopened.pin_conversation_session(changed).await.is_err());
        let mut changed = session;
        changed.component_digest = Some("d".repeat(64));
        assert!(reopened.pin_conversation_session(changed).await.is_err());
    }
    #[tokio::test]
    async fn identities_survive_reopen_and_refuse_other_owner_root_or_component() {
        let root = std::env::temp_dir().join(format!("microcrab-pins-{}", uuid::Uuid::new_v4()));
        let pins = ExecutionPins::new(&root);
        let project = ProjectBinding {
            project_id: "p".into(),
            owner: "alice".into(),
            host_id: "host".into(),
            workspace_id: "workspace".into(),
            workspace_root: "/bridge/work".into(),
            descriptor_digest: "a".repeat(64),
        };
        pins.pin_project(project.clone()).await.unwrap();
        for (owner, workspace_root) in [("bob", "/bridge/work"), ("alice", "/other")] {
            let mut changed = project.clone();
            changed.owner = owner.into();
            changed.workspace_root = workspace_root.into();
            assert!(pins.pin_project(changed).await.is_err());
        }
        let session = SessionBinding {
            session_id: "s".into(),
            project: project.clone(),
            component_digest: Some("b".repeat(64)),
            discovery_digest: "c".repeat(64),
        };
        pins.pin_session(session.clone()).await.unwrap();
        let reopened = ExecutionPins::new(&root);
        assert_eq!(reopened.project("p").await.unwrap(), Some(project));
        assert_eq!(reopened.session("s").await.unwrap(), Some(session.clone()));
        let mut changed = session;
        changed.component_digest = Some("d".repeat(64));
        assert!(reopened.pin_session(changed).await.is_err());
    }
}
