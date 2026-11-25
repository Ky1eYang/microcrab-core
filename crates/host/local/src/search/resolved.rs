//! Resolved path filtering and planned traversal for the new glob capability.
use super::{DEPTH, ENTRIES, LINE_BYTES, RESULTS, glob, match_path};
use crate::paths::{
    Policy, admit, admit_link_node, cancelled, link_node, protected, reparse, returned,
};
use microcrab_host_api::{
    GlobEntry, GlobEntryKind, GlobRequest, GlobScanIncomplete, GlobScanReason, ResolvedGlobOutput,
    ToolError, is_portable_file_name, validate_glob_ignored_paths,
};
use std::{collections::BTreeSet, fs, path::Path};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct IgnoredPaths<'a> {
    exact: BTreeSet<&'a str>,
    subtrees: BTreeSet<&'a str>,
}

impl<'a> IgnoredPaths<'a> {
    fn new(paths: &'a [String]) -> Self {
        let mut list = Self::default();
        for path in paths {
            if let Some(directory) = path.strip_suffix('/') {
                list.subtrees.insert(directory);
            } else {
                list.exact.insert(path);
            }
        }
        list
    }

    // Exact exclusions suppress a node, but only subtree exclusions prune a directory.
    fn exclusion(&self, workspace: &Path, path: &Path, is_dir: bool) -> (bool, bool) {
        if self.exact.is_empty() && self.subtrees.is_empty() {
            return (false, false);
        }
        let Ok(relative) = path.strip_prefix(workspace) else {
            return (false, false);
        };
        let Some(text) = relative.to_str() else {
            return (false, false);
        };
        #[cfg(windows)]
        let text = &text.replace('\\', "/");
        let subtree = (is_dir && self.subtrees.contains(text))
            || text
                .match_indices('/')
                .any(|(index, _)| self.subtrees.contains(&text[..index]));
        (subtree || self.exact.contains(text), subtree)
    }
}

/// Only complete literal components constrain traversal. Keep matching against
/// the original root even after narrowing the walk. Compare actual entry names
/// rather than probing synthesized names, which can resolve to aliases or a
/// differently cased entry on some mounts.
struct Plan<'a> {
    components: Vec<&'a str>,
    exact: bool,
}

impl<'a> Plan<'a> {
    fn new(pattern: &'a str) -> Self {
        let mut components = Vec::new();
        for part in pattern.split('/') {
            if part.contains(['*', '?', '[', ']', '{', '}']) || !is_portable_file_name(part) {
                return Self {
                    components,
                    exact: false,
                };
            }
            components.push(part);
        }
        Self {
            components,
            exact: true,
        }
    }
}

pub(crate) fn search(
    root: &Path,
    policy: &Policy,
    request: GlobRequest,
    paths: Vec<String>,
    token: &CancellationToken,
) -> Result<ResolvedGlobOutput, ToolError> {
    cancelled(token)?;
    if request.include_ignored {
        return Err(ToolError::invalid_request(
            "resolved glob uses only ignored_list; use an empty list to disable filtering",
        ));
    }
    if request.max_results == 0 || request.max_results > RESULTS {
        return Err(ToolError::invalid_request(
            "glob result budget must be between 1 and 4096",
        ));
    }
    validate_glob_ignored_paths(&paths)?;
    let ignored = IgnoredPaths::new(&paths);
    let matcher = glob(&request.pattern)?;
    let search = admit(root, &request.root, policy)?;
    let metadata = fs::symlink_metadata(&search)?;
    let mut out = ResolvedGlobOutput {
        entries: Vec::new(),
        truncated: false,
        scan_incomplete: None,
    };
    let mut output_bytes = 0;
    let mut emit = |out: &mut ResolvedGlobOutput, path: &Path, kind| {
        if !matcher.is_match(match_path(path, &search)) {
            return;
        }
        let relative = returned(root, path);
        let size = relative.as_os_str().len() + 32;
        if out.entries.len() == request.max_results || size > LINE_BYTES - output_bytes {
            out.truncated = true;
        } else {
            output_bytes += size;
            out.entries.push(GlobEntry {
                path: relative.to_path_buf(),
                kind,
            });
        }
    };
    let (excluded, pruned) = ignored.exclusion(root, &search, metadata.is_dir());
    if metadata.is_file() {
        // Named file roots retain legacy semantics: match their basename and
        // do not apply the child-entry hidden filter to the requested root.
        if !excluded {
            emit(&mut out, &search, GlobEntryKind::File);
        }
        return Ok(out);
    }
    if !metadata.is_dir() {
        return Err(ToolError::invalid_request(
            "search root must be an ordinary directory or file",
        ));
    }
    if pruned {
        return Ok(out);
    }

    let plan = Plan::new(&request.pattern);
    let mut pending = vec![(search.clone(), 0)];
    let mut observed = 0;
    'scan: while let Some((directory, depth)) = pending.pop() {
        cancelled(token)?;
        admit(root, &directory, policy)?;
        let required = plan.components.get(depth);
        for entry in fs::read_dir(&directory)? {
            cancelled(token)?;
            observed += 1;
            if observed > ENTRIES {
                out.scan_incomplete = Some(GlobScanIncomplete {
                    reason: GlobScanReason::Entries,
                    limit: ENTRIES,
                    observed,
                });
                break 'scan;
            }
            let entry = entry?;
            let name = entry.file_name();
            if required.is_some_and(|part| name != *part) {
                continue;
            }
            // In a literal-prefix directory this is the sole possibly relevant
            // child, regardless of whether it is hidden, protected or a link.
            let mut select = || -> Result<(), ToolError> {
                let path = entry.path();
                if (!request.include_hidden && name.to_string_lossy().starts_with('.'))
                    || protected(&path, policy)
                {
                    return Ok(());
                }
                let metadata = fs::symlink_metadata(&path)?;
                let kind = if reparse(&metadata) {
                    if !request.include_directories {
                        return Ok(());
                    }
                    let node = admit_link_node(root, &path, policy)?;
                    if !link_node(&node, &metadata)? {
                        return Ok(());
                    }
                    GlobEntryKind::Symlink
                } else if metadata.is_dir() {
                    GlobEntryKind::Directory
                } else if metadata.is_file() {
                    GlobEntryKind::File
                } else {
                    return Ok(());
                };
                let (excluded, pruned) =
                    ignored.exclusion(root, &path, kind == GlobEntryKind::Directory);
                if pruned {
                    return Ok(());
                }
                if kind != GlobEntryKind::Symlink {
                    admit(root, &path, policy)?;
                }
                if !excluded && (kind == GlobEntryKind::File || request.include_directories) {
                    emit(&mut out, &path, kind);
                }
                if kind == GlobEntryKind::Directory
                    && (!plan.exact || depth + 1 < plan.components.len())
                {
                    if depth == DEPTH {
                        out.scan_incomplete.get_or_insert(GlobScanIncomplete {
                            reason: GlobScanReason::Depth,
                            limit: DEPTH,
                            observed: depth + 1,
                        });
                    } else {
                        pending.push((path, depth + 1));
                    }
                }
                Ok(())
            };
            select()?;
            if required.is_some() {
                break;
            }
        }
    }
    out.entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

#[cfg(test)]
mod tests;
