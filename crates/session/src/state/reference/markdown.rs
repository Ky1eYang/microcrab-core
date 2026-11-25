//! Explicit bounded Markdown sources. Loading is asynchronous; projection/read use
//! one atomically replaced catalog, so State snapshots never perform filesystem IO.
use super::{Reference, ReferenceContent, ReferenceInfo, ReferenceProjection};
use crate::{
    Error,
    utils::markdown::{MarkdownDocument, parse_markdown},
};
use microcrab_host_api::{GlobRequest, Host, ReadRequest};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, RwLock},
};
use tokio_util::sync::CancellationToken;

/// Sources are explicit authority: Host paths stay inside the bound workspace;
/// URL sources use HTTP(S), never a backend filesystem fallback.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReferenceSource {
    HostPath { path: PathBuf },
    HostGlob { root: PathBuf, pattern: String },
    Url { url: String },
    DirectoryUrl { url: String },
}
#[derive(Debug, Clone)]
pub struct ReferenceLimits {
    pub max_documents: usize,
    pub max_document_bytes: usize,
    pub max_total_bytes: usize,
}
impl Default for ReferenceLimits {
    fn default() -> Self {
        Self {
            max_documents: 256,
            max_document_bytes: 1024 * 1024,
            max_total_bytes: 8 * 1024 * 1024,
        }
    }
}
#[derive(Default)]
struct Catalog {
    documents: Vec<DocumentRow>,
    listed: Vec<ReferenceInfo>,
    bodies: BTreeMap<String, String>,
}

struct DocumentRow {
    location: String,
    fields: serde_json::Map<String, Value>,
    metadata: Option<Value>,
    revision: DocumentRevision,
}

impl DocumentRow {
    fn projection(&self, short: bool) -> Value {
        let mut fields = self.fields.clone();
        let (key, value) = if short {
            ("revision", &self.revision.label)
        } else {
            ("content_digest", &self.revision.digest)
        };
        fields.insert(key.into(), json!(value));
        fields.insert("path".into(), json!(self.location));
        if let Some(metadata) = &self.metadata {
            fields.insert("metadata".into(), metadata.clone());
        }
        Value::Object(fields)
    }
}

struct DocumentRevision {
    digest: String,
    marker: String,
    label: String,
    overflow: u64,
}

impl DocumentRevision {
    fn new(digest: String) -> Self {
        use sha2::{Digest, Sha256};
        // Derive the short marker from the complete digest, not a prefix of
        // it. A canonical prefix collision does not survive an ordinary reload.
        let mut hash = Sha256::new();
        hash.update(b"microcrab-markdown-revision-v1:\0");
        hash.update(digest.as_bytes());
        let marker = format!("{:x}", hash.finalize());
        let label = marker[..16].to_owned();
        Self {
            digest,
            marker,
            label,
            overflow: 0,
        }
    }

    fn observe(&mut self, previous: &Self) -> Result<(), Error> {
        self.overflow = previous.overflow;
        if self.digest == previous.digest {
            self.label.clone_from(&previous.label);
        } else if self.marker[..16] == previous.marker[..16] {
            if let Some(length) = [20, 24].into_iter().find(|&length| {
                length >= previous.label.len() && self.marker[..length] != previous.marker[..length]
            }) {
                self.label = self.marker[..length].to_owned();
            } else {
                self.overflow = self.overflow.checked_add(1).ok_or_else(|| {
                    crate::budget::reject(
                        crate::LimitKind::Arithmetic,
                        "Markdown revision collision counter exhausted",
                    )
                })?;
                self.label = format!("{}~{:x}", &self.marker[..24], self.overflow);
            }
        }
        Ok(())
    }
}

/// A concrete Reference implementation for Markdown/skill documents. Reloading
/// publishes all-or-nothing; failed loads preserve the last complete catalog.
pub struct MarkdownReferences {
    kind: String,
    sources: Vec<ReferenceSource>,
    host: Option<Arc<dyn Host>>,
    limits: ReferenceLimits,
    catalog: RwLock<Catalog>,
}
impl MarkdownReferences {
    pub fn new(
        kind: impl Into<String>,
        sources: Vec<ReferenceSource>,
        host: Option<Arc<dyn Host>>,
    ) -> Self {
        Self {
            kind: kind.into(),
            sources,
            host,
            limits: ReferenceLimits::default(),
            catalog: RwLock::new(Catalog::default()),
        }
    }
    pub fn limits(mut self, limits: ReferenceLimits) -> Self {
        self.limits = limits;
        self
    }
    async fn build(&self) -> Result<Catalog, Error> {
        if self.limits.max_documents == 0
            || self.limits.max_document_bytes == 0
            || self.limits.max_total_bytes == 0
        {
            return Err(Error::config("reference limits must be positive"));
        }
        let client = microcrab_transport::http::client_builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(reference_error)?;
        let mut documents = BTreeMap::new();
        let mut total = 0usize;
        for source in &self.sources {
            match source {
                ReferenceSource::HostPath { path } => {
                    self.load_host(path.clone(), &mut documents, &mut total)
                        .await?;
                }
                ReferenceSource::HostGlob { root, pattern } => {
                    let host = self.host()?;
                    let max_results = self.limits.max_documents.saturating_add(1).min(4096);
                    let result = host
                        .glob(
                            GlobRequest {
                                root: root.clone(),
                                pattern: pattern.clone(),
                                include_hidden: true,
                                include_ignored: false,
                                include_directories: false,
                                max_results,
                            },
                            CancellationToken::new(),
                        )
                        .await
                        .map_err(reference_error)?;
                    if result.truncated {
                        return Err(crate::budget::reject(
                            crate::LimitKind::ReferenceCount,
                            "reference glob was truncated",
                        ));
                    }
                    crate::budget::check(
                        crate::LimitKind::ReferenceCount,
                        result.entries.len(),
                        max_results,
                    )
                    .map_err(|error| match error {
                        Error::Limit(limit) => {
                            Error::Limit(limit.with_label("reference glob was truncated"))
                        }
                        error => error,
                    })?;
                    for entry in result.entries {
                        if entry.kind == microcrab_host_api::GlobEntryKind::File
                            && markdown_path(&entry.path.to_string_lossy())
                        {
                            self.load_host(entry.path, &mut documents, &mut total)
                                .await?;
                        }
                    }
                }
                ReferenceSource::Url { url } => {
                    let url = checked_url(url)?;
                    self.load_url(&client, url, &mut documents, &mut total)
                        .await?;
                }
                ReferenceSource::DirectoryUrl { url } => {
                    let base = checked_url(url)?;
                    if !base.path().ends_with('/') {
                        return Err(Error::reference("directory URL must end with /"));
                    }
                    let html = fetch(&client, &base, self.remaining_limit(total)?).await?;
                    self.charge(&mut total, html.len())?;
                    let children = directory_links(&base, &html)?;
                    let additional = children
                        .iter()
                        .filter(|child| !documents.contains_key(child.as_str()))
                        .count();
                    crate::budget::check(
                        crate::LimitKind::ReferenceCount,
                        documents.len().saturating_add(additional),
                        self.limits.max_documents,
                    )
                    .map_err(|error| match error {
                        Error::Limit(limit) => {
                            Error::Limit(limit.with_label("reference document count exceeded"))
                        }
                        error => error,
                    })?;
                    for child in children {
                        self.load_url(&client, child, &mut documents, &mut total)
                            .await?;
                    }
                }
            }
        }
        let mut catalog = Catalog::default();
        for (location, text) in documents {
            let identity = format!("{}:{}:{}", self.kind.len(), self.kind, location);
            let mut document = parse_markdown(&identity, &text).map_err(reference_error)?;
            if document.metadata.name.is_none()
                && !document.root.subjects.iter().any(|node| node.level == 1)
            {
                let path = location.strip_prefix("host:").unwrap_or(&location);
                document.root.title = path
                    .split(['/', '\\'])
                    .next_back()
                    .unwrap_or(path)
                    .to_owned();
            }
            let content_digest = format!(
                "{:x}",
                <sha2::Sha256 as sha2::Digest>::digest(text.as_bytes())
            );
            add_document(&mut catalog, location, document, content_digest)?;
        }
        Ok(catalog)
    }
    fn host(&self) -> Result<&Arc<dyn Host>, Error> {
        self.host
            .as_ref()
            .ok_or_else(|| Error::config("Host reference source requires an injected Host"))
    }
    async fn load_host(
        &self,
        path: PathBuf,
        documents: &mut BTreeMap<String, String>,
        total: &mut usize,
    ) -> Result<(), Error> {
        let host = self.host()?;
        let normalized = host
            .normalize_resource(path.clone(), CancellationToken::new())
            .await
            .map_err(reference_error)?;
        let location = format!("host:{normalized}");
        if documents.contains_key(&location) {
            return Ok(());
        }
        self.check_count(documents.len())?;
        let limit = self.remaining_limit(*total)?;
        let mut content = String::new();
        let mut offset = 1;
        let mut version = None;
        loop {
            let remaining = limit
                .checked_sub(content.len())
                .filter(|remaining| *remaining > 0)
                .ok_or_else(|| {
                    crate::budget::reject(
                        crate::LimitKind::ReferenceBytes,
                        "reference document was truncated",
                    )
                })?;
            let read = host
                .read_text(
                    ReadRequest {
                        path: path.clone(),
                        offset_line: offset,
                        max_lines: 100_000,
                        max_bytes: remaining.min(1024 * 1024),
                    },
                    CancellationToken::new(),
                )
                .await
                .map_err(reference_error)?;
            if read.start_line != offset
                || version
                    .as_ref()
                    .is_some_and(|previous| previous != &read.version)
            {
                return Err(Error::reference("reference changed during paged read"));
            }
            version = Some(read.version);
            crate::budget::check(
                crate::LimitKind::ReferenceBytes,
                read.content.len(),
                remaining,
            )
            .map_err(|error| match error {
                Error::Limit(limit) => {
                    Error::Limit(limit.with_label("reference byte budget exceeded"))
                }
                error => error,
            })?;
            content.push_str(&read.content);
            match read.next_line {
                Some(next) if read.truncated && next > offset && !read.content.is_empty() => {
                    offset = next
                }
                None if !read.truncated => break,
                _ => return Err(Error::reference("invalid reference Host pagination")),
            }
        }
        self.insert(documents, total, location, content)
    }
    async fn load_url(
        &self,
        client: &reqwest::Client,
        url: reqwest::Url,
        documents: &mut BTreeMap<String, String>,
        total: &mut usize,
    ) -> Result<(), Error> {
        let location = url.to_string();
        if documents.contains_key(&location) {
            return Ok(());
        }
        self.check_count(documents.len())?;
        let content = fetch(client, &url, self.remaining_limit(*total)?).await?;
        self.insert(documents, total, location, content)
    }
    fn check_count(&self, count: usize) -> Result<(), Error> {
        let required = count.checked_add(1).ok_or_else(|| {
            crate::budget::reject(
                crate::LimitKind::ReferenceCount,
                "reference document count exceeded",
            )
        })?;
        crate::budget::check(
            crate::LimitKind::ReferenceCount,
            required,
            self.limits.max_documents,
        )
        .map_err(|error| match error {
            Error::Limit(limit) => {
                Error::Limit(limit.with_label("reference document count exceeded"))
            }
            error => error,
        })?;
        Ok(())
    }
    fn remaining_limit(&self, total: usize) -> Result<usize, Error> {
        self.limits
            .max_total_bytes
            .checked_sub(total)
            .map(|remaining| remaining.min(self.limits.max_document_bytes))
            .filter(|remaining| *remaining > 0)
            .ok_or_else(|| {
                crate::budget::reject(
                    crate::LimitKind::ReferenceBytes,
                    "reference byte budget exceeded",
                )
            })
    }
    fn charge(&self, total: &mut usize, bytes: usize) -> Result<(), Error> {
        let next = total.checked_add(bytes).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "reference byte overflow")
        })?;
        crate::budget::check(
            crate::LimitKind::ReferenceBytes,
            bytes,
            self.limits.max_document_bytes,
        )
        .map_err(|error| match error {
            Error::Limit(limit) => Error::Limit(limit.with_label("reference byte budget exceeded")),
            error => error,
        })?;
        crate::budget::check(
            crate::LimitKind::ReferenceBytes,
            next,
            self.limits.max_total_bytes,
        )
        .map_err(|error| match error {
            Error::Limit(limit) => Error::Limit(limit.with_label("reference byte budget exceeded")),
            error => error,
        })?;
        *total = next;
        Ok(())
    }
    fn insert(
        &self,
        documents: &mut BTreeMap<String, String>,
        total: &mut usize,
        location: String,
        content: String,
    ) -> Result<(), Error> {
        self.charge(total, content.len())?;
        documents.insert(location, content);
        Ok(())
    }
}
impl Reference for MarkdownReferences {
    type Error = Error;
    fn kind(&self) -> &str {
        &self.kind
    }
    async fn load(&self) -> Result<(), Error> {
        let mut next = self.build().await?;
        let mut current = self.catalog.write().unwrap_or_else(|e| e.into_inner());
        let previous: BTreeMap<_, _> = current
            .documents
            .iter()
            .map(|row| (&row.location, &row.revision))
            .collect();
        for row in &mut next.documents {
            if let Some(revision) = previous.get(&row.location) {
                row.revision.observe(revision)?;
            }
        }
        *current = next;
        Ok(())
    }
    fn list(&self) -> Result<Vec<ReferenceInfo>, Error> {
        Ok(self
            .catalog
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .listed
            .clone())
    }
    fn read(&self, id: &str) -> Result<Option<ReferenceContent>, Error> {
        Ok(self
            .catalog
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .bodies
            .get(id)
            .map(|body| ReferenceContent {
                id: id.into(),
                body: body.clone(),
            }))
    }
    fn change(&self) -> Result<Option<Value>, Error> {
        let catalog = self.catalog.read().unwrap_or_else(|e| e.into_inner());
        Ok(Some(Value::Array(
            catalog
                .documents
                .iter()
                .map(|row| row.projection(false))
                .collect(),
        )))
    }
    fn projections(&self) -> Result<ReferenceProjection, Error> {
        let catalog = self.catalog.read().unwrap_or_else(|e| e.into_inner());
        let projection = |short| {
            Some(Value::Array(
                catalog
                    .documents
                    .iter()
                    .map(|row| row.projection(short))
                    .collect(),
            ))
        };
        Ok(ReferenceProjection {
            exact: projection(false),
            short_model: projection(true),
        })
    }
}
fn reference_error(error: impl std::fmt::Display) -> Error {
    Error::Reference(error.to_string().into())
}
fn markdown_path(path: &str) -> bool {
    path.rsplit('.')
        .next()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("md") || ext.eq_ignore_ascii_case("markdown"))
}
fn checked_url(raw: &str) -> Result<reqwest::Url, Error> {
    let mut url = reqwest::Url::parse(raw).map_err(reference_error)?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error::reference(
            "reference URL must be HTTP(S) without credentials",
        ));
    }
    url.set_fragment(None);
    Ok(url)
}
async fn fetch(
    client: &reqwest::Client,
    url: &reqwest::Url,
    limit: usize,
) -> Result<String, Error> {
    let mut response = client
        .get(url.clone())
        .send()
        .await
        .map_err(reference_error)?
        .error_for_status()
        .map_err(reference_error)?;
    if response.status().is_redirection() {
        return Err(Error::reference(
            "reference URL redirects require an explicit final URL",
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(reference_error)? {
        crate::budget::check(
            crate::LimitKind::ReferenceBytes,
            bytes.len().saturating_add(chunk.len()),
            limit,
        )
        .map_err(|error| match error {
            Error::Limit(limit) => {
                Error::Limit(limit.with_label("reference HTTP byte budget exceeded"))
            }
            error => error,
        })?;
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(reference_error)
}
fn add_document(
    catalog: &mut Catalog,
    location: String,
    document: MarkdownDocument,
    content_digest: String,
) -> Result<(), Error> {
    fn visit(
        node: &crate::utils::markdown::MarkdownNode,
        path: &str,
        description: &str,
        listed: &mut Vec<ReferenceInfo>,
    ) {
        if node.char_length > 0 {
            listed.push(ReferenceInfo {
                id: node.id.clone(),
                title: node.title.clone(),
                location: path.into(),
                description: description.into(),
            });
        }
        for child in &node.subjects {
            visit(child, path, description, listed);
        }
    }
    fn compact_node(node: &crate::utils::markdown::MarkdownNode) -> Value {
        let mut value = json!({});
        // Empty preambles are document containers, not useful expansion choices.
        // Their body lookup is retained independently; never alias it to a child.
        if node.char_length > 0 {
            value["id"] = json!(node.id);
            value["title"] = json!(node.title);
            value["char_length"] = json!(node.char_length);
            if let Some(important) = &node.important {
                value["important"] = json!(important);
            }
        }
        if !node.subjects.is_empty() {
            value["subjects"] = Value::Array(node.subjects.iter().map(compact_node).collect());
        }
        value
    }
    let description = document.metadata.description.as_deref().unwrap_or("");
    visit(&document.root, &location, description, &mut catalog.listed);
    let projection = compact_node(&document.root);
    let mut metadata = serde_json::to_value(&document.metadata)?;
    let fields = metadata
        .as_object_mut()
        .expect("MarkdownMetadata serializes as an object");
    // Prune generated fields only: nulls/empty entries inside authored arrays or
    // objects may be meaningful positions or values and must remain byte-faithful.
    fields.retain(|key, value| {
        key != "title_depth"
            && match value {
                Value::Null => false,
                Value::String(text) => !text.is_empty(),
                Value::Array(values) => !values.is_empty(),
                Value::Object(values) => !values.is_empty(),
                _ => true,
            }
    });
    let metadata = (!fields.is_empty()).then_some(metadata);
    let Value::Object(fields) = projection else {
        unreachable!("Markdown node projection is an object")
    };
    catalog.documents.push(DocumentRow {
        location,
        fields,
        metadata,
        revision: DocumentRevision::new(content_digest),
    });
    for (id, body) in document.bodies {
        if catalog.bodies.insert(id, body).is_some() {
            return Err(Error::reference("duplicate Markdown reference ID"));
        }
    }
    Ok(())
}

/// Static HTML directory listings only. Accept quoted anchor href attributes,
/// same-origin direct children and Markdown filenames; skip parents/subdirs,
/// queries, credentials, scripts and external links. No recursive crawl.
fn directory_links(base: &reqwest::Url, html: &str) -> Result<Vec<reqwest::Url>, Error> {
    let mut result = BTreeSet::new();
    let mut rest = html;
    while let Some(start) = rest.find('<') {
        rest = &rest[start + 1..];
        if rest.starts_with("!--") {
            let Some(end) = rest.find("-->") else { break };
            rest = &rest[end + 3..];
            continue;
        }
        // An angle bracket inside a quoted attribute does not terminate its tag.
        let mut quote = None;
        let mut end = None;
        for (index, character) in rest.char_indices() {
            match (quote, character) {
                (Some(open), value) if open == value => quote = None,
                (None, '\'' | '"') => quote = Some(character),
                (None, '>') => {
                    end = Some(index);
                    break;
                }
                _ => {}
            }
        }
        let Some(end) = end else { break };
        let tag = &rest[..end];
        rest = &rest[end + 1..];
        let name_end = tag
            .find(|character: char| character.is_ascii_whitespace() || character == '/')
            .unwrap_or(tag.len());
        let name = &tag[..name_end];
        if name.eq_ignore_ascii_case("script") || name.eq_ignore_ascii_case("style") {
            let closing = format!("</{}", name.to_ascii_lowercase());
            let end =
                rest.as_bytes()
                    .windows(closing.len())
                    .enumerate()
                    .find_map(|(index, window)| {
                        (window.eq_ignore_ascii_case(closing.as_bytes())
                            && rest
                                .as_bytes()
                                .get(index + closing.len())
                                .is_some_and(|byte| {
                                    byte.is_ascii_whitespace() || matches!(*byte, b'>' | b'/')
                                }))
                        .then_some(index)
                    });
            let Some(end) = end else { break };
            rest = &rest[end..];
            continue;
        }
        if !name.eq_ignore_ascii_case("a") {
            continue;
        }
        let Some(raw) = quoted_href(&tag[name_end..]) else {
            continue;
        };
        let raw = raw
            .replace("&amp;", "&")
            .replace("&quot;", "\"")
            .replace("&#39;", "'");
        let Ok(url) = base.join(&raw) else { continue };
        if url.origin() != base.origin()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.path().starts_with(base.path())
        {
            continue;
        }
        let child = &url.path()[base.path().len()..];
        let lower = child.to_ascii_lowercase();
        if !child.is_empty()
            && !child.contains('/')
            && !lower.contains("%2f")
            && !lower.contains("%5c")
            && markdown_path(child)
        {
            result.insert(url.to_string());
        }
    }
    result.into_iter().map(|url| checked_url(&url)).collect()
}

fn quoted_href(mut attributes: &str) -> Option<&str> {
    while !attributes.is_empty() {
        attributes = attributes.trim_start_matches(|character: char| {
            character.is_ascii_whitespace() || character == '/'
        });
        let end = attributes
            .find(|character: char| character.is_ascii_whitespace() || character == '=')
            .unwrap_or(attributes.len());
        if end == 0 {
            return None;
        }
        let name = &attributes[..end];
        attributes = attributes[end..].trim_start();
        let Some(value) = attributes.strip_prefix('=') else {
            continue;
        };
        attributes = value.trim_start();
        let quote = attributes.chars().next()?;
        if quote == '\'' || quote == '"' {
            let end = attributes[1..].find(quote)?;
            let value = &attributes[1..end + 1];
            attributes = &attributes[end + 2..];
            if name.eq_ignore_ascii_case("href") {
                return Some(value);
            }
        } else {
            let end = attributes
                .find(char::is_whitespace)
                .unwrap_or(attributes.len());
            attributes = &attributes[end..];
        }
    }
    None
}

#[cfg(test)]
mod tests;
