//! Host-owned `Reference` implementations closest to Host FS or memory capabilities.
//!
//! Whether content comes from a Host filesystem path/directory or an in-memory
//! fallback when FS is unavailable is decided entirely inside these implementations.
use crate::markdown::{
    DocumentRevision, MarkdownDocument, compact_markdown_node, is_markdown_path,
    is_skill_manifest_path, is_valid_skill_file, markdown_metadata_json, parse_markdown,
    stable_reference_id,
};
use crate::{GlobEntryKind, GlobRequest, Host, OpContext, PrivateFileBuffer, ReadRequest};
use async_trait::async_trait;
use microcrab_protocol::{
    ReferenceCapabilities, ReferenceContent, ReferenceEntry, ReferenceError, ReferenceNodeKind,
    ReferencePayload, ReferenceProjection, detect_mime_from_bytes,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

const DEFAULT_MAX_DOCUMENTS: usize = 512;
const DEFAULT_MAX_FILE_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
enum Backing {
    HostFs(Arc<dyn Host>),
    Memory(Arc<BTreeMap<String, Vec<u8>>>),
}

impl Backing {
    fn from_host(host: Arc<dyn Host>) -> Self {
        let caps = host.tool_capabilities();
        if caps.iter().any(|c| c == "read") && caps.iter().any(|c| c == "glob") {
            Self::HostFs(host)
        } else {
            Self::Memory(Arc::new(BTreeMap::new()))
        }
    }

    async fn read_bytes(
        &self,
        path: &str,
        max_bytes: usize,
        cancel: &CancellationToken,
    ) -> Result<Vec<u8>, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        match self {
            Self::Memory(files) => files
                .get(path)
                .cloned()
                .ok_or_else(|| ReferenceError::not_found(format!("memory reference `{path}` not found"))),
            Self::HostFs(host) => {
                if host.supports_private_file_read() {
                    let op = OpContext {
                        cancel: cancel.clone(),
                        principal: "host_reference".into(),
                        trace_id: path.into(),
                        deadline: None,
                    };
                    let buf = host
                        .read_private_file(
                            PathBuf::from(path),
                            PrivateFileBuffer::new(max_bytes, ()),
                            &op,
                        )
                        .await
                        .map_err(|e| ReferenceError::failed(e.to_string()))?;
                    return Ok(buf.into_bytes());
                }
                let text = read_host_text_paged(host.as_ref(), path, max_bytes, cancel).await?;
                Ok(text.into_bytes())
            }
        }
    }
}

async fn read_host_text_paged(
    host: &dyn Host,
    path: &str,
    max_bytes: usize,
    cancel: &CancellationToken,
) -> Result<String, ReferenceError> {
    let mut content = String::new();
    let mut offset = 1usize;
    let mut version = None;
    loop {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        let remaining = max_bytes
            .checked_sub(content.len())
            .filter(|r| *r > 0)
            .ok_or_else(|| ReferenceError::limit("host reference byte budget exceeded"))?;
        let read = host
            .read_text(
                ReadRequest {
                    path: PathBuf::from(path),
                    offset_line: offset,
                    max_lines: 100_000,
                    max_bytes: remaining.min(1024 * 1024),
                },
                cancel.clone(),
            )
            .await
            .map_err(|e| ReferenceError::failed(e.to_string()))?;
        if read.start_line != offset
            || version.as_ref().is_some_and(|prev| prev != &read.version)
        {
            return Err(ReferenceError::conflict(
                "host reference changed during paged read",
            ));
        }
        version = Some(read.version);
        if read.content.len() > remaining {
            return Err(ReferenceError::limit("host reference byte budget exceeded"));
        }
        content.push_str(&read.content);
        match read.next_line {
            Some(next) if read.truncated && next > offset && !read.content.is_empty() => {
                offset = next;
            }
            None if !read.truncated => break,
            _ => return Err(ReferenceError::failed("invalid host pagination")),
        }
    }
    Ok(content)
}

fn format_markdown_row(
    location: &str,
    document: &MarkdownDocument,
    revision: &DocumentRevision,
    short: bool,
) -> Result<Value, ReferenceError> {
    let projection = compact_markdown_node(&document.root);
    let Value::Object(mut fields) = projection else {
        unreachable!("markdown node projection is always an object");
    };
    if short {
        fields.insert("revision".into(), json!(revision.label));
    } else {
        fields.insert("content_digest".into(), json!(revision.digest));
    }
    fields.insert("path".into(), json!(location));
    if let Some(meta) = markdown_metadata_json(&document.metadata)
        .map_err(|e| ReferenceError::failed(e.to_string()))?
    {
        fields.insert("metadata".into(), meta);
    }
    Ok(Value::Object(fields))
}

#[derive(Default)]
struct MarkdownCatalog {
    entries: Vec<ReferenceEntry>,
    bodies: BTreeMap<String, String>,
    rows: Vec<(String, MarkdownDocument, DocumentRevision)>,
}

/// Explicit Host/Inline reference mount specification (path/key, optional display title, plain-text mode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostReferenceSpec {
    pub path: String,
    pub title: Option<String>,
    pub plain_text: bool,
}

impl HostReferenceSpec {
    pub fn markdown(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            title: None,
            plain_text: false,
        }
    }
}

/// Host-owned Markdown/Text reference implementation (e.g. `AGENTS.md`, workspace docs, or inline specs).
pub struct HostMarkdownReference {
    kind: String,
    specs: Vec<HostReferenceSpec>,
    backing: Backing,
    catalog: RwLock<MarkdownCatalog>,
}

impl HostMarkdownReference {
    pub fn from_host(
        kind: impl Into<String>,
        host: Arc<dyn Host>,
        paths: Vec<PathBuf>,
    ) -> Self {
        let specs = paths
            .into_iter()
            .map(|p| HostReferenceSpec::markdown(p.to_string_lossy().replace('\\', "/")))
            .collect();
        Self::from_host_specs(kind, host, specs)
    }

    pub fn from_host_specs(
        kind: impl Into<String>,
        host: Arc<dyn Host>,
        specs: Vec<HostReferenceSpec>,
    ) -> Self {
        Self {
            kind: kind.into(),
            specs,
            backing: Backing::from_host(host),
            catalog: RwLock::new(MarkdownCatalog::default()),
        }
    }

    pub fn from_memory(
        kind: impl Into<String>,
        files: BTreeMap<String, String>,
    ) -> Self {
        let specs = files
            .into_iter()
            .map(|(path, body)| (HostReferenceSpec::markdown(path), body))
            .collect();
        Self::from_memory_specs(kind, specs)
    }

    pub fn from_memory_specs(
        kind: impl Into<String>,
        files: Vec<(HostReferenceSpec, String)>,
    ) -> Self {
        let mut specs = Vec::with_capacity(files.len());
        let mut bytes = BTreeMap::new();
        for (spec, body) in files {
            bytes.insert(spec.path.clone(), body.into_bytes());
            specs.push(spec);
        }
        Self {
            kind: kind.into(),
            specs,
            backing: Backing::Memory(Arc::new(bytes)),
            catalog: RwLock::new(MarkdownCatalog::default()),
        }
    }
}

#[async_trait]
impl crate::Reference for HostMarkdownReference {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities::read_only()
    }

    async fn load(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        let mut next = MarkdownCatalog::default();
        let mut total_bytes = 0usize;
        for spec in &self.specs {
            let location = match &self.backing {
                Backing::HostFs(host) => host
                    .normalize_resource(PathBuf::from(&spec.path), cancel.clone())
                    .await
                    .map_err(|e| ReferenceError::failed(e.to_string()))?,
                Backing::Memory(_) => spec.path.clone(),
            };
            let bytes = self
                .backing
                .read_bytes(&location, DEFAULT_MAX_FILE_BYTES, &cancel)
                .await?;
            total_bytes = total_bytes
                .checked_add(bytes.len())
                .filter(|total| *total <= DEFAULT_MAX_TOTAL_BYTES)
                .ok_or_else(|| ReferenceError::limit("host reference total byte budget exceeded"))?;
            let text = String::from_utf8(bytes)
                .map_err(|_| ReferenceError::failed("reference file is not valid UTF-8"))?;
            let rev = DocumentRevision::from_bytes(text.as_bytes());
            let file_id = stable_reference_id("hf", &format!("{}:{location}", self.kind));
            if spec.plain_text {
                let fallback_title = location
                    .rsplit('/')
                    .next()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&location)
                    .to_owned();
                let title = spec.title.clone().unwrap_or(fallback_title);
                let doc = MarkdownDocument {
                    metadata: crate::markdown::MarkdownMetadata {
                        name: Some(title.clone()),
                        description: None,
                        license: None,
                        compatibility: None,
                        title_depth: 0,
                        tags: Vec::new(),
                        metadata: serde_yaml_ng::Value::Null,
                        allowed_tools: serde_yaml_ng::Value::Null,
                        extra: BTreeMap::new(),
                    },
                    root: crate::markdown::MarkdownNode {
                        id: file_id.clone(),
                        title: title.clone(),
                        level: 0,
                        char_length: text.chars().count(),
                        important: None,
                        subjects: Vec::new(),
                    },
                    bodies: BTreeMap::new(),
                };
                next.entries.push(ReferenceEntry {
                    id: file_id.clone(),
                    node_kind: ReferenceNodeKind::File,
                    parent_id: None,
                    title,
                    location: Some(location.clone()),
                    description: None,
                    media_type: Some("text/plain".into()),
                    revision: rev.label.clone(),
                    writable: false,
                    metadata: None,
                });
                next.bodies.insert(file_id, text);
                next.rows.push((location, doc, rev));
                continue;
            }
            let identity = format!("{}:{}:host:{}", self.kind.len(), self.kind, location);
            let mut doc = parse_markdown(&identity, &text)
                .map_err(|e| ReferenceError::failed(e.to_string()))?;
            if let Some(title) = &spec.title {
                doc.root.title = title.clone();
            }
            next.entries.push(ReferenceEntry {
                id: file_id.clone(),
                node_kind: ReferenceNodeKind::File,
                parent_id: None,
                title: doc.root.title.clone(),
                location: Some(location.clone()),
                description: doc.metadata.description.clone(),
                media_type: Some("text/markdown".into()),
                revision: rev.label.clone(),
                writable: false,
                metadata: None,
            });
            next.bodies.insert(file_id.clone(), text);
            fn collect_sections(
                node: &crate::markdown::MarkdownNode,
                parent_id: &str,
                location: &str,
                rev: &str,
                doc: &MarkdownDocument,
                entries: &mut Vec<ReferenceEntry>,
                bodies: &mut BTreeMap<String, String>,
            ) {
                if node.char_length > 0 {
                    if let Some(body) = doc.bodies.get(&node.id) {
                        bodies.insert(node.id.clone(), body.clone());
                    }
                    entries.push(ReferenceEntry {
                        id: node.id.clone(),
                        node_kind: ReferenceNodeKind::Section,
                        parent_id: Some(parent_id.to_owned()),
                        title: node.title.clone(),
                        location: Some(location.to_owned()),
                        description: doc.metadata.description.clone(),
                        media_type: Some("text/markdown".into()),
                        revision: rev.to_owned(),
                        writable: false,
                        metadata: None,
                    });
                }
                for child in &node.subjects {
                    collect_sections(child, parent_id, location, rev, doc, entries, bodies);
                }
            }
            collect_sections(
                &doc.root,
                &file_id,
                &location,
                &rev.label,
                &doc,
                &mut next.entries,
                &mut next.bodies,
            );
            next.rows.push((location, doc, rev));
        }

        let mut guard = self.catalog.write().unwrap_or_else(|e| e.into_inner());
        let previous: BTreeMap<_, _> = guard
            .rows
            .iter()
            .map(|(loc, _, rev)| (loc.clone(), rev.clone()))
            .collect();
        for (loc, _, rev) in &mut next.rows {
            if let Some(prev) = previous.get(loc) {
                rev.observe(prev).map_err(ReferenceError::limit)?;
            }
        }
        *guard = next;
        Ok(())
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        Ok(self
            .catalog
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .clone())
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        let guard = self.catalog.read().unwrap_or_else(|e| e.into_inner());
        if guard.rows.is_empty() {
            return Ok(ReferenceProjection::empty());
        }
        let mut exact = Vec::with_capacity(guard.rows.len());
        let mut short = Vec::with_capacity(guard.rows.len());
        for (loc, doc, rev) in &guard.rows {
            exact.push(format_markdown_row(loc, doc, rev, false)?);
            short.push(format_markdown_row(loc, doc, rev, true)?);
        }
        Ok(ReferenceProjection {
            exact: Some(Value::Array(exact)),
            short_model: Some(Value::Array(short)),
        })
    }

    async fn expand(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        let guard = self.catalog.read().unwrap_or_else(|e| e.into_inner());
        Ok(guard
            .bodies
            .get(id)
            .map(|body| ReferenceContent::text_with_mime(id, "text/markdown", body.clone())))
    }
}

#[derive(Clone)]
struct CachedFile {
    media_type: String,
    payload: ReferencePayload,
    sections: BTreeMap<String, String>,
}

#[derive(Clone)]
struct SkillDirectoryNode {
    id: String,
    path: String,
    children: Vec<SkillDirChild>,
}

#[derive(Clone, serde::Serialize)]
struct SkillDirChild {
    id: String,
    node_kind: ReferenceNodeKind,
    name: String,
    path: String,
}

#[derive(Clone)]
struct SkillFileNode {
    id: String,
    path: String,
    media_type: String,
}

#[derive(Default)]
struct SkillsCatalog {
    entries: Vec<ReferenceEntry>,
    directories: BTreeMap<String, SkillDirectoryNode>,
    files: BTreeMap<String, SkillFileNode>,
    section_to_file: BTreeMap<String, String>,
    projection_rows: Vec<(Value, Value)>,
}

/// Host-owned Skill directory reference implementation.
///
/// Discovers `{skill_dir}/SKILL.md`, filters invalid root/repository clutter,
/// projects `SKILL.md` sections + accompanying `Directory`/`File` resource trees,
/// and caches whole expanded files (including all parsed sections) in memory.
pub struct HostSkillsReference {
    kind: String,
    roots: Vec<String>,
    backing: Backing,
    catalog: RwLock<SkillsCatalog>,
    file_cache: RwLock<BTreeMap<String, CachedFile>>,
}

impl HostSkillsReference {
    pub fn from_host(
        kind: impl Into<String>,
        host: Arc<dyn Host>,
        roots: Vec<PathBuf>,
    ) -> Self {
        let roots = if roots.is_empty() {
            vec![".".into()]
        } else {
            roots
                .into_iter()
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .collect()
        };
        Self {
            kind: kind.into(),
            roots,
            backing: Backing::from_host(host),
            catalog: RwLock::new(SkillsCatalog::default()),
            file_cache: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn from_memory(
        kind: impl Into<String>,
        files: BTreeMap<String, Vec<u8>>,
    ) -> Self {
        Self {
            kind: kind.into(),
            roots: vec![".".into()],
            backing: Backing::Memory(Arc::new(files)),
            catalog: RwLock::new(SkillsCatalog::default()),
            file_cache: RwLock::new(BTreeMap::new()),
        }
    }

    async fn discover_all_paths(&self, cancel: &CancellationToken) -> Result<Vec<String>, ReferenceError> {
        match &self.backing {
            Backing::Memory(files) => Ok(files.keys().cloned().collect()),
            Backing::HostFs(host) => {
                let mut paths = BTreeSet::new();
                for root in &self.roots {
                    let result = host
                        .glob(
                            GlobRequest {
                                root: PathBuf::from(root),
                                pattern: "**/*".into(),
                                include_hidden: false,
                                include_ignored: false,
                                include_directories: false,
                                max_results: 4096,
                            },
                            cancel.clone(),
                        )
                        .await
                        .map_err(|e| ReferenceError::failed(e.to_string()))?;
                    for entry in result.entries {
                        if entry.kind == GlobEntryKind::File {
                            paths.insert(entry.path.to_string_lossy().replace('\\', "/"));
                        }
                    }
                }
                Ok(paths.into_iter().collect())
            }
        }
    }

    async fn load_and_cache_file(
        &self,
        file_id: &str,
        file_path: &str,
        hinted_mime: &str,
        cancel: &CancellationToken,
    ) -> Result<CachedFile, ReferenceError> {
        if let Some(cached) = self
            .file_cache
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(file_id)
            .cloned()
        {
            return Ok(cached);
        }
        let bytes = self
            .backing
            .read_bytes(file_path, DEFAULT_MAX_FILE_BYTES, cancel)
            .await?;
        let mime = detect_mime_from_bytes(Some(file_path), &bytes);
        let effective_mime = if mime == "application/octet-stream" && hinted_mime != mime {
            hinted_mime.to_owned()
        } else {
            mime
        };
        let (payload, sections) = if effective_mime.starts_with("text/")
            || effective_mime == "application/json"
            || is_markdown_path(file_path)
        {
            let text = String::from_utf8(bytes)
                .map_err(|_| ReferenceError::failed("skill file is not valid UTF-8"))?;
            let sections = if is_markdown_path(file_path) {
                let identity = format!("{}:{}:skill:{}", self.kind.len(), self.kind, file_path);
                parse_markdown(&identity, &text)
                    .map_err(|e| ReferenceError::failed(e.to_string()))?
                    .bodies
            } else {
                BTreeMap::new()
            };
            (ReferencePayload::Text(text), sections)
        } else {
            (
                ReferencePayload::Binary {
                    data: bytes,
                    width: None,
                    height: None,
                },
                BTreeMap::new(),
            )
        };
        let cached = CachedFile {
            media_type: effective_mime,
            payload,
            sections,
        };
        self.file_cache
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(file_id.to_owned(), cached.clone());
        Ok(cached)
    }
}

#[async_trait]
impl crate::Reference for HostSkillsReference {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities::read_only()
    }

    async fn load(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        let all_paths = self.discover_all_paths(&cancel).await?;
        let mut skill_dirs: BTreeSet<String> = BTreeSet::new();
        for path in &all_paths {
            if is_skill_manifest_path(path) {
                let dir = path
                    .rsplit_once('/')
                    .map(|(d, _)| d.to_owned())
                    .unwrap_or_default();
                skill_dirs.insert(dir);
            }
        }

        let mut next = SkillsCatalog::default();
        let mut next_cache: BTreeMap<String, CachedFile> = BTreeMap::new();
        let mut skill_count = 0usize;

        for skill_dir in skill_dirs {
            skill_count += 1;
            if skill_count > DEFAULT_MAX_DOCUMENTS {
                return Err(ReferenceError::limit("skill document count exceeded"));
            }
            let manifest_path = if skill_dir.is_empty() {
                "SKILL.md".to_owned()
            } else {
                format!("{skill_dir}/SKILL.md")
            };
            let manifest_bytes = self
                .backing
                .read_bytes(&manifest_path, DEFAULT_MAX_FILE_BYTES, &cancel)
                .await?;
            let manifest_text = String::from_utf8(manifest_bytes)
                .map_err(|_| ReferenceError::failed("SKILL.md is not valid UTF-8"))?;
            let digest = format!("{:x}", Sha256::digest(manifest_text.as_bytes()));
            let short_rev = digest[..16].to_owned();
            let identity = format!("{}:{}:skill:{}", self.kind.len(), self.kind, manifest_path);
            let doc = parse_markdown(&identity, &manifest_text)
                .map_err(|e| ReferenceError::failed(e.to_string()))?;

            let skill_dir_id = stable_reference_id("hsd", &format!("{}:{skill_dir}", self.kind));
            let manifest_file_id =
                stable_reference_id("hsf", &format!("{}:{manifest_path}", self.kind));
            let skill_title = doc.metadata.name.clone().unwrap_or_else(|| doc.root.title.clone());

            // Collect valid files under this skill directory.
            let prefix = if skill_dir.is_empty() {
                String::new()
            } else {
                format!("{skill_dir}/")
            };
            let mut valid_rel_files = Vec::new();
            for candidate in &all_paths {
                let rel = if prefix.is_empty() {
                    candidate.as_str()
                } else if let Some(rest) = candidate.strip_prefix(&prefix) {
                    rest
                } else {
                    continue;
                };
                if is_valid_skill_file(rel) {
                    valid_rel_files.push((rel.to_owned(), candidate.clone()));
                }
            }

            // Build Directory / File entries for this skill.
            let mut dir_children: BTreeMap<String, Vec<SkillDirChild>> = BTreeMap::new();
            let mut resource_summary = Vec::new();

            for (rel, full_path) in &valid_rel_files {
                let file_id = if rel == "SKILL.md" {
                    manifest_file_id.clone()
                } else {
                    stable_reference_id("hsf", &format!("{}:{full_path}", self.kind))
                };
                let mime = detect_mime_from_bytes(Some(full_path), b"");
                let parent_rel = rel.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
                let parent_dir_id = if parent_rel.is_empty() {
                    skill_dir_id.clone()
                } else {
                    let full_sub = if skill_dir.is_empty() {
                        parent_rel.to_owned()
                    } else {
                        format!("{skill_dir}/{parent_rel}")
                    };
                    stable_reference_id("hsd", &format!("{}:{full_sub}", self.kind))
                };

                // Register intermediate subdirectories up to skill_dir.
                let mut accum = String::new();
                let segs: Vec<&str> = rel.split('/').collect();
                for sub in &segs[..segs.len().saturating_sub(1)] {
                    let parent_sub_id = if accum.is_empty() {
                        skill_dir_id.clone()
                    } else {
                        let p = if skill_dir.is_empty() {
                            accum.clone()
                        } else {
                            format!("{skill_dir}/{accum}")
                        };
                        stable_reference_id("hsd", &format!("{}:{p}", self.kind))
                    };
                    if !accum.is_empty() {
                        accum.push('/');
                    }
                    accum.push_str(sub);
                    let cur_full = if skill_dir.is_empty() {
                        accum.clone()
                    } else {
                        format!("{skill_dir}/{accum}")
                    };
                    let cur_id = stable_reference_id("hsd", &format!("{}:{cur_full}", self.kind));
                    let siblings = dir_children.entry(parent_sub_id.clone()).or_default();
                    if !siblings.iter().any(|c| c.id == cur_id) {
                        siblings.push(SkillDirChild {
                            id: cur_id.clone(),
                            node_kind: ReferenceNodeKind::Directory,
                            name: (*sub).to_owned(),
                            path: cur_full.clone(),
                        });
                        next.entries.push(ReferenceEntry {
                            id: cur_id.clone(),
                            node_kind: ReferenceNodeKind::Directory,
                            parent_id: Some(parent_sub_id),
                            title: (*sub).to_owned(),
                            location: Some(cur_full.clone()),
                            description: None,
                            media_type: None,
                            revision: short_rev.clone(),
                            writable: false,
                            metadata: None,
                        });
                        next.directories.insert(
                            cur_id.clone(),
                            SkillDirectoryNode {
                                id: cur_id,
                                path: cur_full,
                                children: Vec::new(),
                            },
                        );
                    }
                }

                let file_name = segs.last().copied().unwrap_or(rel.as_str()).to_owned();
                dir_children
                    .entry(parent_dir_id.clone())
                    .or_default()
                    .push(SkillDirChild {
                        id: file_id.clone(),
                        node_kind: ReferenceNodeKind::File,
                        name: file_name.clone(),
                        path: full_path.clone(),
                    });
                next.entries.push(ReferenceEntry {
                    id: file_id.clone(),
                    node_kind: ReferenceNodeKind::File,
                    parent_id: Some(parent_dir_id),
                    title: file_name,
                    location: Some(full_path.clone()),
                    description: None,
                    media_type: Some(mime.clone()),
                    revision: short_rev.clone(),
                    writable: false,
                    metadata: None,
                });
                next.files.insert(
                    file_id.clone(),
                    SkillFileNode {
                        id: file_id.clone(),
                        path: full_path.clone(),
                        media_type: mime,
                    },
                );
                if rel != "SKILL.md" {
                    resource_summary.push(json!({
                        "id": file_id,
                        "path": rel,
                    }));
                }
            }

            next.entries.push(ReferenceEntry {
                id: skill_dir_id.clone(),
                node_kind: ReferenceNodeKind::Directory,
                parent_id: None,
                title: skill_title.clone(),
                location: Some(if skill_dir.is_empty() {
                    ".".into()
                } else {
                    skill_dir.clone()
                }),
                description: doc.metadata.description.clone(),
                media_type: None,
                revision: short_rev.clone(),
                writable: false,
                metadata: None,
            });
            next.directories.insert(
                skill_dir_id.clone(),
                SkillDirectoryNode {
                    id: skill_dir_id.clone(),
                    path: if skill_dir.is_empty() {
                        ".".into()
                    } else {
                        skill_dir.clone()
                    },
                    children: Vec::new(),
                },
            );

            for (dir_id, children) in dir_children {
                if let Some(dir) = next.directories.get_mut(&dir_id) {
                    dir.children = children;
                }
            }

            // Cache manifest file + its sections in memory.
            let mut section_bodies = BTreeMap::new();
            struct SkillSectionMeta<'a> {
                manifest_file_id: &'a str,
                manifest_path: &'a str,
                short_rev: &'a str,
                doc: &'a MarkdownDocument,
            }
            fn register_sections(
                node: &crate::markdown::MarkdownNode,
                meta: &SkillSectionMeta<'_>,
                entries: &mut Vec<ReferenceEntry>,
                section_to_file: &mut BTreeMap<String, String>,
                section_bodies: &mut BTreeMap<String, String>,
            ) {
                if node.char_length > 0 {
                    if let Some(body) = meta.doc.bodies.get(&node.id) {
                        section_bodies.insert(node.id.clone(), body.clone());
                        section_to_file.insert(node.id.clone(), meta.manifest_file_id.to_owned());
                    }
                    entries.push(ReferenceEntry {
                        id: node.id.clone(),
                        node_kind: ReferenceNodeKind::Section,
                        parent_id: Some(meta.manifest_file_id.to_owned()),
                        title: node.title.clone(),
                        location: Some(meta.manifest_path.to_owned()),
                        description: meta.doc.metadata.description.clone(),
                        media_type: Some("text/markdown".into()),
                        revision: meta.short_rev.to_owned(),
                        writable: false,
                        metadata: None,
                    });
                }
                for child in &node.subjects {
                    register_sections(child, meta, entries, section_to_file, section_bodies);
                }
            }
            let section_meta = SkillSectionMeta {
                manifest_file_id: &manifest_file_id,
                manifest_path: &manifest_path,
                short_rev: &short_rev,
                doc: &doc,
            };
            register_sections(
                &doc.root,
                &section_meta,
                &mut next.entries,
                &mut next.section_to_file,
                &mut section_bodies,
            );
            next_cache.insert(
                manifest_file_id.clone(),
                CachedFile {
                    media_type: "text/markdown".into(),
                    payload: ReferencePayload::Text(manifest_text),
                    sections: section_bodies,
                },
            );

            let rev = DocumentRevision::new(digest);
            let mut exact_row = format_markdown_row(&manifest_path, &doc, &rev, false)?;
            let mut short_row = format_markdown_row(&manifest_path, &doc, &rev, true)?;
            for row in [&mut exact_row, &mut short_row] {
                if let Some(map) = row.as_object_mut() {
                    map.insert("skill_id".into(), json!(skill_dir_id));
                    map.insert("file_id".into(), json!(manifest_file_id));
                    if !resource_summary.is_empty() {
                        map.insert("resources".into(), Value::Array(resource_summary.clone()));
                    }
                }
            }
            next.projection_rows.push((exact_row, short_row));
        }

        *self.catalog.write().unwrap_or_else(|e| e.into_inner()) = next;
        *self.file_cache.write().unwrap_or_else(|e| e.into_inner()) = next_cache;
        Ok(())
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        Ok(self
            .catalog
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .clone())
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        let guard = self.catalog.read().unwrap_or_else(|e| e.into_inner());
        if guard.projection_rows.is_empty() {
            return Ok(ReferenceProjection::empty());
        }
        let exact = Value::Array(guard.projection_rows.iter().map(|(e, _)| e.clone()).collect());
        let short = Value::Array(guard.projection_rows.iter().map(|(_, s)| s.clone()).collect());
        Ok(ReferenceProjection {
            exact: Some(exact),
            short_model: Some(short),
        })
    }

    async fn expand(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        let (dir_node, file_node, section_owner) = {
            let guard = self.catalog.read().unwrap_or_else(|e| e.into_inner());
            (
                guard.directories.get(id).cloned(),
                guard.files.get(id).cloned(),
                guard
                    .section_to_file
                    .get(id)
                    .and_then(|fid| guard.files.get(fid).cloned()),
            )
        };
        if let Some(dir) = dir_node {
            let body = serde_json::to_string(&json!({
                "directory": dir.path,
                "children": dir.children,
            }))
            .map_err(|e| ReferenceError::failed(e.to_string()))?;
            return Ok(Some(ReferenceContent::text_with_mime(
                dir.id,
                "application/json",
                body,
            )));
        }
        if let Some(file) = file_node {
            let cached = self
                .load_and_cache_file(&file.id, &file.path, &file.media_type, &cancel)
                .await?;
            return Ok(Some(ReferenceContent {
                id: file.id,
                media_type: cached.media_type,
                payload: cached.payload,
            }));
        }
        if let Some(owner_file) = section_owner {
            let cached = self
                .load_and_cache_file(
                    &owner_file.id,
                    &owner_file.path,
                    &owner_file.media_type,
                    &cancel,
                )
                .await?;
            if let Some(section_body) = cached.sections.get(id) {
                return Ok(Some(ReferenceContent::text_with_mime(
                    id,
                    "text/markdown",
                    section_body.clone(),
                )));
            }
        }
        Ok(None)
    }
}
