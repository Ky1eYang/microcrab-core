//! Portable file evidence syntax, independent of the server's operating system.
//! Validation is not filesystem authority: only the executing Host admits IO.

/// Stable Host-authored scope fact authenticated by HostInfo's descriptor digest.
/// Older confined Hosts omit it. Do not rewrite an existing Session pin to opt in.
pub const UNRESTRICTED_FILESYSTEM_DESCRIPTION: &str = "Filesystem scope: unrestricted ordinary paths; private paths, device/ADS and link/reparse restrictions still apply. Relative paths resolve from the workspace, including parent traversal. Returned paths are workspace-relative inside the workspace and absolute outside it. Shell/Process authority is independent.";

/// Reject portable device aliases and Windows ADS even on non-Windows servers.
pub fn is_portable_file_name(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();
    !name.is_empty()
        && !matches!(name, "." | "..")
        && !name.ends_with(['.', ' '])
        && !name
            .chars()
            .any(|c| c.is_control() || "<>:\"/\\|?*".contains(c))
        && !matches!(
            stem.as_str(),
            "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
        )
        && !((stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(
                &stem[3..],
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            ))
}

/// File evidence is workspace-relative, or explicitly absolute when its consumer
/// allows that syntax. Absolute grammar is /path, C:/path or //server/share/path;
/// no OS-dependent Path parsing, backslashes, dot segments or empty segments.
/// This describes an ordinary file identity, so roots/shares alone are invalid.
pub fn is_normalized_file_path(path: &str, allow_absolute: bool) -> bool {
    if path.is_empty() || path.len() > 32_768 {
        return false;
    }
    let body = if let Some(unc) = path.strip_prefix("//") {
        if !allow_absolute || unc.split('/').count() < 3 {
            return false;
        }
        unc
    } else if let Some(absolute) = path.strip_prefix('/') {
        if !allow_absolute {
            return false;
        }
        absolute
    } else if path.as_bytes().get(1) == Some(&b':') {
        if !allow_absolute
            || !path.as_bytes()[0].is_ascii_alphabetic()
            || path.as_bytes().get(2) != Some(&b'/')
        {
            return false;
        }
        &path[3..]
    } else {
        path
    };
    body.split('/').all(is_portable_file_name)
}

/// Retained cache evidence preserves its historical relative-path grammar.
/// Non-Local Hosts may have emitted names forbidden by LocalHost's IO admission;
/// accepting those observations never authorizes IO. New absolute identities use
/// the strict portable grammar independently of the decoder's operating system.
pub fn is_normalized_cache_path(path: &str, allow_absolute: bool) -> bool {
    let legacy_relative = !path.is_empty()
        && path.len() <= 32_768
        && !path.contains(['\\', ':', '\0'])
        && !path.split('/').any(|part| matches!(part, "" | "." | ".."));
    legacy_relative || (allow_absolute && is_normalized_file_path(path, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_relative_evidence_preserves_legacy_non_local_names() {
        for path in [
            "a?b",
            "CON",
            "trailing.",
            "space ",
            "nested/a?b",
            "line\nbreak",
        ] {
            assert!(is_normalized_cache_path(path, false), "{path:?}");
            assert!(!is_normalized_file_path(path, false), "{path:?}");
        }
        for path in [
            "/tmp/a?b",
            "C:/CON",
            "//server/share/trailing.",
            "../relative",
            "a/./b",
            "a//b",
            "file:ads",
            "a\\b",
            "a\0b",
        ] {
            assert!(!is_normalized_cache_path(path, true), "{path:?}");
        }
        for path in ["/tmp/file", "C:/outside/file", "//server/share/file"] {
            assert!(is_normalized_cache_path(path, true));
            assert!(!is_normalized_cache_path(path, false));
        }
    }

    #[test]
    fn evidence_grammar_is_portable_and_scope_is_explicit() {
        for path in ["file", "nested/λ.txt"] {
            assert!(is_normalized_file_path(path, false));
            assert!(is_normalized_file_path(path, true));
        }
        for path in [
            "/tmp/file",
            "C:/outside/file",
            "z:/file",
            "//server/share/file",
        ] {
            assert!(!is_normalized_file_path(path, false), "{path}");
            assert!(is_normalized_file_path(path, true), "{path}");
        }
        for path in [
            "",
            ".",
            "..",
            "a/../b",
            "a/./b",
            "a//b",
            "a/",
            "/",
            "//server/share",
            "C:/",
            "C:relative",
            "1:/file",
            "C://file",
            "//?/C:/file",
            "//./device/file",
            "a\\b",
            "file:ads",
            "NUL",
            "a/COM¹.txt",
            "x/LPT9",
            "trailing.",
            "space ",
            "/a/aux.txt",
            "/tmp/with\ncontrol",
            "a\0b",
        ] {
            assert!(!is_normalized_file_path(path, true), "{path:?}");
        }
    }
}
