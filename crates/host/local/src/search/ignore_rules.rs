//! State ignore evidence discovery and matching, separate from legacy disk rules.
use crate::{
    paths::{Policy, admit, cancelled, protected, reparse, returned_text},
    workspace::open_regular,
};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use microcrab_host_api::{
    GlobIgnoreRuleFile, GlobIgnoreSnapshot, MAX_GLOB_IGNORE_RULE_BYTES, MAX_GLOB_IGNORE_RULE_FILES,
    MAX_GLOB_IGNORE_RULE_LINES_PER_FILE, ToolError, glob_ignore_rule_json_bytes,
    validate_glob_ignore_rules,
};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

const SOURCES: [&str; 2] = [".gitignore", ".ignore"];

#[derive(Default)]
pub(super) struct ExplicitRules(BTreeMap<PathBuf, Gitignore>);

fn key(path: &Path) -> PathBuf {
    #[cfg(windows)]
    return PathBuf::from(path.to_string_lossy().to_lowercase());
    #[cfg(not(windows))]
    path.to_path_buf()
}

fn compile(rules: &[GlobIgnoreRuleFile]) -> Result<Gitignore, ToolError> {
    let mut builder = GitignoreBuilder::new(".");
    for rule in rules {
        // Match Git/ignore's handling of the BOM only at the start of each file.
        for (index, line) in rule.content.lines().enumerate() {
            let line = if index == 0 {
                line.trim_start_matches('\u{feff}')
            } else {
                line
            };
            builder
                .add_line(None, line)
                .map_err(|_| ToolError::invalid_request("invalid ignore rule"))?;
        }
    }
    builder
        .build()
        .map_err(|_| ToolError::invalid_request("invalid ignore rules"))
}

impl ExplicitRules {
    pub(super) fn new(
        root: &Path,
        policy: &Policy,
        rules: Vec<GlobIgnoreRuleFile>,
        token: &CancellationToken,
    ) -> Result<Self, ToolError> {
        validate_glob_ignore_rules(&rules)?;
        let mut scopes: BTreeMap<PathBuf, Vec<_>> = BTreeMap::new();
        for rule in rules {
            cancelled(token)?;
            // Rules are data, never paths to open. A previously discovered path
            // may no longer exist. A protected scope must not govern glob.
            if protected(&root.join(&rule.path), policy) {
                return Err(ToolError::invalid_request("ignore rule path is protected"));
            }
            let parent = rule.path.parent().ok_or(ToolError::PathOutsideRoot)?;
            scopes.entry(key(parent)).or_default().push(rule);
        }
        let mut out = Self::default();
        for (scope, mut sources) in scopes {
            cancelled(token)?;
            sources.sort_by_key(|rule| rule.path.file_name().is_some_and(|n| n == SOURCES[1]));
            out.0.insert(scope, compile(&sources)?);
        }
        Ok(out)
    }

    pub(super) fn excludes(&self, root: &Path, path: &Path, is_dir: bool) -> bool {
        let Ok(relative) = path.strip_prefix(root) else {
            return false;
        };
        for scope in relative.ancestors().skip(1) {
            if let Some(rules) = self.0.get(&key(scope)) {
                // scope is an ancestor of relative by construction.
                if let Ok(scoped) = relative.strip_prefix(scope) {
                    let matched = rules.matched(scoped, is_dir);
                    if !matched.is_none() {
                        return matched.is_ignore();
                    }
                }
            }
        }
        false
    }

    pub(super) fn insert_scope(
        &mut self,
        root: &Path,
        directory: &Path,
        sources: &[GlobIgnoreRuleFile],
    ) -> Result<(), ToolError> {
        let scope = directory
            .strip_prefix(root)
            .map_err(|_| ToolError::PathOutsideRoot)?;
        self.0.insert(key(scope), compile(sources)?);
        Ok(())
    }

    /// An explicitly named search root cannot bypass its ignored ancestors.
    pub(super) fn excludes_search_root(&self, root: &Path, path: &Path, is_dir: bool) -> bool {
        path.ancestors()
            .take_while(|ancestor| *ancestor != root && ancestor.starts_with(root))
            .enumerate()
            .any(|(index, ancestor)| self.excludes(root, ancestor, is_dir || index != 0))
    }
}

pub(crate) fn snapshot(
    root: &Path,
    policy: &Policy,
    token: &CancellationToken,
) -> Result<GlobIgnoreSnapshot, ToolError> {
    scan(root, policy, token, None)
}

pub(crate) fn resolved_paths(
    root: &Path,
    policy: &Policy,
    token: &CancellationToken,
) -> Result<Vec<String>, ToolError> {
    let mut paths = ResolvedPaths::default();
    scan(root, policy, token, Some(&mut paths))?;
    paths.paths.sort_unstable();
    Ok(paths.paths)
}

#[derive(Default)]
struct ResolvedPaths {
    paths: Vec<String>,
    // Payload bytes without list brackets. Include the separator for each
    // non-first entry; reject before retaining an over-budget list.
    bytes: usize,
}

impl ResolvedPaths {
    fn record(&mut self, root: &Path, path: &Path, is_dir: bool) -> Result<(), ToolError> {
        use microcrab_host_api::{
            MAX_GLOB_IGNORED_PATH_BYTES, MAX_GLOB_IGNORED_PATHS, glob_ignored_path_json_bytes,
            is_normalized_file_path,
        };
        let mut text = returned_text(root, path)?;
        if !is_normalized_file_path(&text, false) {
            return Err(ToolError::invalid_request(
                "ignored node has no portable workspace-relative path",
            ));
        }
        if is_dir {
            text.push('/');
        }
        if self.paths.len() == MAX_GLOB_IGNORED_PATHS {
            return Err(ToolError::operation_limit(
                "glob ignore resolution exceeds 4096 ignored paths",
            ));
        }
        self.bytes += glob_ignored_path_json_bytes(&text) + usize::from(!self.paths.is_empty());
        if self.bytes + 2 > MAX_GLOB_IGNORED_PATH_BYTES {
            return Err(ToolError::operation_limit(
                "glob ignore resolution exceeds 24576 path-list JSON bytes",
            ));
        }
        self.paths.push(text);
        Ok(())
    }
}

fn scan(
    root: &Path,
    policy: &Policy,
    token: &CancellationToken,
    mut resolved: Option<&mut ResolvedPaths>,
) -> Result<GlobIgnoreSnapshot, ToolError> {
    let mut out = GlobIgnoreSnapshot {
        rules: Vec::new(),
        truncated: false,
        pruned: false,
    };
    let mut rules = ExplicitRules::default();
    let mut pending = vec![(root.to_path_buf(), 0)];
    let mut observed = 0;
    let mut bytes = 2;
    'scan: while let Some((directory, depth)) = pending.pop() {
        cancelled(token)?;
        admit(root, &directory, policy)?;
        let mut sources = Vec::new();
        for name in SOURCES {
            cancelled(token)?;
            let path = directory.join(name);
            if protected(&path, policy) {
                continue;
            }
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if reparse(&metadata) || !metadata.is_file() {
                continue;
            }
            let relative = returned_text(root, &path)?;
            if out.rules.len() + sources.len() == MAX_GLOB_IGNORE_RULE_FILES
                || metadata.len() > (MAX_GLOB_IGNORE_RULE_BYTES - bytes) as u64
            {
                if resolved.is_some() {
                    return Err(ToolError::operation_limit(
                        "glob ignore resolution exceeds ignore-source limits (256 files, 24576 JSON bytes, 1024 lines per file)",
                    ));
                }
                out.truncated = true;
                break 'scan;
            }
            let mut content = Vec::new();
            open_regular(&path)?
                .take((MAX_GLOB_IGNORE_RULE_BYTES - bytes + 1) as u64)
                .read_to_end(&mut content)?;
            if content.len() > MAX_GLOB_IGNORE_RULE_BYTES - bytes {
                if resolved.is_some() {
                    return Err(ToolError::operation_limit(
                        "glob ignore resolution exceeds ignore-source limits (256 files, 24576 JSON bytes, 1024 lines per file)",
                    ));
                }
                out.truncated = true;
                break 'scan;
            }
            let content = String::from_utf8(content)
                .map_err(|_| ToolError::invalid_request("ignore rules must be UTF-8"))?;
            if content.contains('\0') {
                return Err(ToolError::invalid_request(
                    "ignore rules must not contain NUL",
                ));
            }
            bytes += glob_ignore_rule_json_bytes(&relative, &content)
                + usize::from(!out.rules.is_empty() || !sources.is_empty());
            if bytes > MAX_GLOB_IGNORE_RULE_BYTES
                || content.lines().count() > MAX_GLOB_IGNORE_RULE_LINES_PER_FILE
            {
                if resolved.is_some() {
                    return Err(ToolError::operation_limit(
                        "glob ignore resolution exceeds ignore-source limits (256 files, 24576 JSON bytes, 1024 lines per file)",
                    ));
                }
                out.truncated = true;
                break 'scan;
            }
            sources.push(GlobIgnoreRuleFile {
                path: relative.into(),
                content,
            });
        }
        if !sources.is_empty() {
            rules.insert_scope(root, &directory, &sources)?;
            out.rules.append(&mut sources);
        }
        // Hidden directories matter when the caller enables include_hidden.
        // Ignored directories never do under these rules: a nested whitelist
        // cannot reopen an excluded parent. Record skipped coverage explicitly.
        for entry in fs::read_dir(&directory)? {
            cancelled(token)?;
            observed += 1;
            if observed > super::ENTRIES {
                if resolved.is_some() {
                    return Err(ToolError::operation_limit(
                        "glob ignore resolution exceeds 16384 observed directory entries",
                    ));
                }
                out.truncated = true;
                break 'scan;
            }
            let entry = entry?;
            if resolved.is_none() && !entry.file_type()?.is_dir() {
                continue;
            }
            let path = entry.path();
            if protected(&path, policy) {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)?;
            let is_link = reparse(&metadata);
            let is_dir = !is_link && metadata.is_dir();
            if !is_dir && (resolved.is_none() || (!is_link && !metadata.is_file())) {
                continue;
            }
            if rules.excludes(root, &path, is_dir) {
                if let Some(paths) = resolved.as_deref_mut() {
                    // Links are exact nodes; their targets are never inspected.
                    if !is_link || crate::paths::link_node(&path, &metadata)? {
                        paths.record(root, &path, is_dir)?;
                    }
                }
                out.pruned |= is_dir;
                continue;
            }
            if !is_dir {
                continue;
            }
            if depth >= super::DEPTH {
                if resolved.is_some() {
                    return Err(ToolError::operation_limit(
                        "glob ignore resolution exceeds depth 64",
                    ));
                }
                out.truncated = true;
                break 'scan;
            }
            pending.push((path, depth + 1));
        }
    }
    out.rules.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

#[cfg(test)]
mod tests;
