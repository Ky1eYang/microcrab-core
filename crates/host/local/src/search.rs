//! Bounded traversal and search. Every observed directory entry counts, including
//! hidden and ignored entries; links are never followed. Ignore files are bounded.
use crate::{
    paths::{admit, cancelled, reparse},
    workspace::open_regular,
};
use globset::{GlobBuilder, GlobMatcher};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use microcrab_host_api::{
    GlobEntry, GlobEntryKind, GlobIgnoreRuleFile, GlobOutput, GlobRequest, GrepMatch, GrepOutput,
    GrepRequest, ToolError,
};
use std::{
    fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

mod ignore_rules;
mod resolved;
use ignore_rules::ExplicitRules;
pub(crate) use ignore_rules::{
    resolved_paths as glob_ignored_paths, snapshot as glob_ignore_snapshot,
};
pub(crate) use resolved::search as glob_with_ignored_paths;

const ENTRIES: usize = 16_384;
const DEPTH: usize = 64;
const SCAN_BYTES: usize = 64 * 1024 * 1024;
const LINE_BYTES: usize = 1024 * 1024;
const RESULTS: usize = 4096;
const PATTERN_BYTES: usize = 1024;

fn match_path<'a>(path: &'a Path, root: &Path) -> &'a Path {
    match path.strip_prefix(root) {
        Ok(relative) if !relative.as_os_str().is_empty() => relative,
        _ => path.file_name().map(Path::new).unwrap_or(path),
    }
}

fn glob(pattern: &str) -> Result<GlobMatcher, ToolError> {
    if pattern.is_empty() || pattern.len() > PATTERN_BYTES {
        return Err(ToolError::invalid_request(
            "glob pattern must be nonempty and at most 1024 bytes",
        ));
    }
    GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .build()
        .map(|glob| glob.compile_matcher())
        .map_err(|_| ToolError::invalid_request("invalid glob pattern"))
}

fn rules(
    directory: &Path,
    protected: &crate::paths::Policy,
    remaining: &mut usize,
    token: &CancellationToken,
) -> Result<Gitignore, ToolError> {
    let mut builder = GitignoreBuilder::new(directory);
    for name in [".gitignore", ".ignore"] {
        cancelled(token)?;
        let path = directory.join(name);
        if crate::paths::protected(&path, protected) {
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
        if metadata.len() > 64 * 1024 || metadata.len() as usize > *remaining {
            return Err(ToolError::FileLimit);
        }
        let file = open_regular(&path)?;
        let mut bytes = Vec::new();
        file.take(64 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 || bytes.len() > *remaining {
            return Err(ToolError::FileLimit);
        }
        *remaining -= bytes.len();
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| ToolError::invalid_request("ignore rules must be UTF-8"))?;
        if text.lines().count() > 1024 {
            return Err(ToolError::FileLimit);
        }
        for line in text.lines() {
            builder
                .add_line(Some(path.clone()), line)
                .map_err(|_| ToolError::invalid_request("invalid ignore rule"))?;
        }
    }
    builder
        .build()
        .map_err(|_| ToolError::invalid_request("invalid ignore rules"))
}

#[derive(Default)]
struct WalkOptions {
    hidden: bool,
    ignored: bool,
    include_directories: bool,
}

/// Callback returns false when its own budget is exhausted. True result means
/// traversal was truncated, including exhaustion of any defensive scan ceiling.
/// Only glob opts into directory/link nodes; grep retains ordinary files only.
fn walk(
    root: &Path,
    protected: &crate::paths::Policy,
    requested: &Path,
    options: WalkOptions,
    explicit: Option<&ExplicitRules>,
    token: &CancellationToken,
    mut visit: impl FnMut(&Path, &Path, GlobEntryKind) -> Result<bool, ToolError>,
) -> Result<bool, ToolError> {
    cancelled(token)?;
    let search = admit(root, requested, protected)?;
    let metadata = fs::symlink_metadata(&search)?;
    if explicit.is_some_and(|rules| rules.excludes_search_root(root, &search, metadata.is_dir())) {
        return Ok(false);
    }
    if metadata.is_file() {
        return Ok(!visit(
            &search,
            crate::paths::returned(root, &search),
            GlobEntryKind::File,
        )?);
    }
    if !metadata.is_dir() {
        return Err(ToolError::invalid_request(
            "search root must be an ordinary directory or file",
        ));
    }
    let mut pending: Vec<(PathBuf, Vec<Arc<Gitignore>>, usize)> =
        vec![(search.clone(), Vec::new(), 0)];
    let mut observed = 0usize;
    let mut rule_bytes = 256 * 1024;
    while let Some((directory, mut inherited, depth)) = pending.pop() {
        cancelled(token)?;
        admit(root, &directory, protected)?;
        if !options.ignored && explicit.is_none() {
            match rules(&directory, protected, &mut rule_bytes, token) {
                Ok(rule) => inherited.push(Arc::new(rule)),
                Err(ToolError::FileLimit) => return Ok(true),
                Err(error) => return Err(error),
            }
        }
        for entry in fs::read_dir(&directory)? {
            cancelled(token)?;
            observed += 1;
            if observed > ENTRIES {
                return Ok(true);
            }
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            if !options.hidden && name.to_string_lossy().starts_with('.') {
                continue;
            }
            if crate::paths::protected(&path, protected) {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)?;
            let kind = if reparse(&metadata) {
                if !options.include_directories {
                    continue;
                }
                let node = crate::paths::admit_link_node(root, &path, protected)?;
                if !crate::paths::link_node(&node, &metadata)? {
                    continue;
                }
                GlobEntryKind::Symlink
            } else if metadata.is_dir() {
                GlobEntryKind::Directory
            } else if metadata.is_file() {
                GlobEntryKind::File
            } else {
                continue;
            };
            if let Some(rules) = explicit {
                if rules.excludes(root, &path, kind == GlobEntryKind::Directory) {
                    continue;
                }
            } else if !options.ignored {
                let mut excluded = false;
                for rule in &inherited {
                    let matched = rule.matched(&path, kind == GlobEntryKind::Directory);
                    if matched.is_ignore() {
                        excluded = true;
                    } else if matched.is_whitelist() {
                        excluded = false;
                    }
                }
                if excluded {
                    continue;
                }
            }
            if kind != GlobEntryKind::Symlink {
                admit(root, &path, protected)?;
            }
            let relative = crate::paths::returned(root, &path);
            if (kind == GlobEntryKind::File || options.include_directories)
                && !visit(&path, relative, kind)?
            {
                return Ok(true);
            }
            if kind == GlobEntryKind::Directory {
                if depth >= DEPTH {
                    return Ok(true);
                }
                pending.push((path, inherited.clone(), depth + 1));
            }
        }
    }
    Ok(false)
}

pub(crate) fn glob_files(
    root: &Path,
    protected: &crate::paths::Policy,
    request: GlobRequest,
    token: &CancellationToken,
) -> Result<GlobOutput, ToolError> {
    glob_selection(root, protected, request, None, token)
}

pub(crate) fn glob_with_ignore_rules(
    root: &Path,
    protected: &crate::paths::Policy,
    request: GlobRequest,
    rules: Vec<GlobIgnoreRuleFile>,
    token: &CancellationToken,
) -> Result<GlobOutput, ToolError> {
    cancelled(token)?;
    let explicit = if request.include_ignored {
        None
    } else {
        Some(ExplicitRules::new(root, protected, rules, token)?)
    };
    glob_selection(root, protected, request, explicit.as_ref(), token)
}

fn glob_selection(
    root: &Path,
    protected: &crate::paths::Policy,
    request: GlobRequest,
    explicit: Option<&ExplicitRules>,
    token: &CancellationToken,
) -> Result<GlobOutput, ToolError> {
    cancelled(token)?;
    if request.max_results == 0 || request.max_results > RESULTS {
        return Err(ToolError::invalid_request(
            "glob result budget must be between 1 and 4096",
        ));
    }
    let matcher = glob(&request.pattern)?;
    let search = admit(root, &request.root, protected)?;
    let mut entries = Vec::new();
    let mut output_bytes = 0usize;
    let truncated = walk(
        root,
        protected,
        &request.root,
        WalkOptions {
            hidden: request.include_hidden,
            ignored: request.include_ignored,
            include_directories: request.include_directories,
        },
        explicit,
        token,
        |path, relative, kind| {
            if matcher.is_match(match_path(path, &search)) {
                // Include the bounded kind/record overhead as well as the path.
                let size = relative.as_os_str().len() + 32;
                if entries.len() == request.max_results || size > LINE_BYTES - output_bytes {
                    return Ok(false);
                }
                output_bytes += size;
                entries.push(GlobEntry {
                    path: relative.to_path_buf(),
                    kind,
                });
            }
            Ok(true)
        },
    )?;
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(GlobOutput { entries, truncated })
}

pub(crate) fn grep_files(
    root: &Path,
    protected: &crate::paths::Policy,
    request: GrepRequest,
    token: &CancellationToken,
) -> Result<GrepOutput, ToolError> {
    cancelled(token)?;
    if request.max_matches == 0
        || request.max_matches > RESULTS
        || request.max_bytes == 0
        || request.max_bytes > LINE_BYTES
        || request.pattern.is_empty()
        || request.pattern.len() > PATTERN_BYTES
    {
        return Err(ToolError::invalid_request(
            "grep requires bounded positive pattern, match and byte budgets",
        ));
    }
    let pattern = if request.literal {
        regex::escape(&request.pattern)
    } else {
        request.pattern.clone()
    };
    let matcher = regex::RegexBuilder::new(&pattern)
        .case_insensitive(!request.case_sensitive)
        .size_limit(256 * 1024)
        .dfa_size_limit(256 * 1024)
        .build()
        .map_err(|_| ToolError::invalid_request("invalid or oversized regular expression"))?;
    let include = request.include_glob.as_deref().map(glob).transpose()?;
    let search = admit(root, &request.root, protected)?;
    let mut matches = Vec::new();
    let mut output_bytes = 0usize;
    let mut scanned = 0usize;
    let truncated = walk(
        root,
        protected,
        &request.root,
        WalkOptions::default(),
        None,
        token,
        |path, relative, _kind| {
            if include
                .as_ref()
                .is_some_and(|glob| !glob.is_match(match_path(path, &search)))
            {
                return Ok(true);
            }
            let file = open_regular(path)?;
            let mut reader = BufReader::with_capacity(8192, file);
            let mut line = 0usize;
            loop {
                cancelled(token)?;
                if scanned >= SCAN_BYTES {
                    return Ok(false);
                }
                let limit = (SCAN_BYTES - scanned).min(LINE_BYTES);
                let mut bytes = Vec::new();
                // take bounds allocation even for a giant unterminated line.
                let count = reader
                    .by_ref()
                    .take((limit + 1) as u64)
                    .read_until(b'\n', &mut bytes)?;
                scanned += count;
                if count == 0 {
                    break;
                }
                if count > limit {
                    return Ok(false);
                }
                line += 1;
                if bytes.contains(&0) {
                    break;
                }
                let Ok(text) = std::str::from_utf8(&bytes) else {
                    break;
                };
                let text = text.trim_end_matches(['\r', '\n']);
                if matcher.is_match(text) {
                    let size = text.len() + relative.as_os_str().len();
                    if matches.len() == request.max_matches
                        || size > request.max_bytes - output_bytes
                    {
                        return Ok(false);
                    }
                    output_bytes += size;
                    matches.push(GrepMatch {
                        path: relative.to_path_buf(),
                        line,
                        text: text.into(),
                    });
                }
            }
            Ok(true)
        },
    )?;
    matches.sort_by(|a, b| a.path.cmp(&b.path).then(a.line.cmp(&b.line)));
    Ok(GrepOutput { matches, truncated })
}
