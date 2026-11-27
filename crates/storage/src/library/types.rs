//! Owner-scoped Personal Content Library object types and validation helpers.
use microcrab_session::{
    Error, ReferenceQuota,
    budget::{LimitKind, check},
    state::reference::{canonical_mime, detect_mime_from_bytes},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const DEFAULT_MAX_USER_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_MAX_USER_ENTRIES: u64 = 2048;
pub const DEFAULT_MAX_OBJECT_BYTES: usize = 32 * 1024 * 1024;

pub type LibraryQuota = ReferenceQuota;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalBlobOrigin {
    Upload,
    JobRedirect,
    ToolCapture,
    AgentLiteral,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShortcutTarget {
    HostPath {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workspace_id: Option<String>,
    },
    HostGlob {
        root: String,
        pattern: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workspace_id: Option<String>,
    },
    PublicHttps {
        url: String,
    },
    DirectoryUrl {
        url: String,
    },
}

impl ShortcutTarget {
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::HostPath { .. } => "shortcut_host_path",
            Self::HostGlob { .. } => "shortcut_host_glob",
            Self::PublicHttps { .. } => "shortcut_public_https",
            Self::DirectoryUrl { .. } => "shortcut_directory_url",
        }
    }

    pub fn location(&self) -> String {
        match self {
            Self::HostPath { path, .. } => path.clone(),
            Self::HostGlob { root, pattern, .. } => {
                let root = root.trim_end_matches('/');
                if root.is_empty() || root == "." {
                    pattern.clone()
                } else {
                    format!("{root}/{pattern}")
                }
            }
            Self::PublicHttps { url } | Self::DirectoryUrl { url } => url.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LibrarySource {
    LocalBlob {
        origin: LocalBlobOrigin,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_execution_id: Option<String>,
    },
    Shortcut {
        target: ShortcutTarget,
    },
}

impl LibrarySource {
    pub fn source_kind(&self) -> &'static str {
        match self {
            Self::LocalBlob { origin, .. } => match origin {
                LocalBlobOrigin::Upload => "upload",
                LocalBlobOrigin::JobRedirect => "job_redirect",
                LocalBlobOrigin::ToolCapture => "tool_capture",
                LocalBlobOrigin::AgentLiteral => "agent_literal",
            },
            Self::Shortcut { target } => target.kind_name(),
        }
    }

    pub fn is_local_blob(&self) -> bool {
        matches!(self, Self::LocalBlob { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibraryObjectSummary {
    pub id: String,
    pub title: String,
    pub source_kind: String,
    pub source: LibrarySource,
    pub media_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_length: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    pub revision: String,
    pub read_only: bool,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subjects: Option<Vec<Value>>,
}

impl LibraryObjectSummary {
    pub fn projection_value(&self, short_model: bool) -> Value {
        let mut row = json!({
            "id": self.id,
            "title": self.title,
            "source_kind": self.source_kind,
            "media_type": self.media_type,
            "revision": if short_model && self.revision.len() > 16 {
                &self.revision[..16]
            } else {
                &self.revision
            },
            "read_only": self.read_only,
        });
        if let Some(bytes) = self.byte_length {
            row["byte_length"] = json!(bytes);
        }
        if let Some(width) = self.width {
            row["width"] = json!(width);
        }
        if let Some(height) = self.height {
            row["height"] = json!(height);
        }
        if let LibrarySource::Shortcut { target } = &self.source {
            row["location"] = json!(target.location());
        }
        if let Some(subjects) = &self.subjects {
            row["subjects"] = Value::Array(subjects.clone());
        }
        row
    }
}

#[derive(Clone, Debug)]
pub enum CreateLibraryObject {
    LocalBlob {
        id: Option<String>,
        title: String,
        media_type: String,
        data: Vec<u8>,
        origin: LocalBlobOrigin,
        origin_execution_id: Option<String>,
    },
    Shortcut {
        title: String,
        target: ShortcutTarget,
    },
}

#[derive(Clone, Debug, Default)]
pub struct UpdateLibraryObject {
    pub expected_revision: Option<String>,
    pub title: Option<String>,
    pub media_type: Option<String>,
    pub data: Option<Vec<u8>>,
    pub target: Option<ShortcutTarget>,
}

pub fn normalize_shortcut(target: &ShortcutTarget) -> Result<(), Error> {
    match target {
        ShortcutTarget::HostPath { path, .. } => validate_host_path(path),
        ShortcutTarget::HostGlob { root, pattern, .. } => {
            if !root.is_empty() && root != "." {
                validate_host_path(root)?;
            }
            if pattern.is_empty() || pattern.len() > 1024 || pattern.contains('\0') {
                return Err(Error::config("invalid host_glob pattern"));
            }
            Ok(())
        }
        ShortcutTarget::PublicHttps { url } | ShortcutTarget::DirectoryUrl { url } => {
            validate_https_url(url)
        }
    }
}

fn validate_host_path(path: &str) -> Result<(), Error> {
    if path.is_empty() || path.len() > 4096 || path.contains('\0') {
        return Err(Error::config("host_path must be nonempty and bounded"));
    }
    Ok(())
}

fn validate_https_url(url: &str) -> Result<(), Error> {
    let Some(rest) = url.strip_prefix("https://") else {
        return Err(Error::reference("shortcut URL must use HTTPS"));
    };
    let authority = rest.split('/').next().unwrap_or("");
    if authority.is_empty()
        || authority.contains('@')
        || url.contains(char::is_whitespace)
        || url.contains('\0')
        || url.len() > 4096
    {
        return Err(Error::reference(
            "shortcut URL must be valid HTTPS without credentials",
        ));
    }
    Ok(())
}

pub fn check_shortcut_mime(path: Option<&str>) -> String {
    detect_mime_from_bytes(path, b"")
}

pub fn check_canonical_mime(media_type: &str) -> Result<String, Error> {
    canonical_mime(media_type).map_err(Into::into)
}

pub fn check_byte_budget(required: usize, max: usize) -> Result<(), Error> {
    check(LimitKind::LibraryStorageBytes, required, max)
}
