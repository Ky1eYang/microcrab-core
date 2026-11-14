//! Bounded public diagnostic descriptions. Callers exclude bodies before formatting.
//! This scanner is defense in depth, not a provenance detector for arbitrary prose.
pub const MAX_REASON_BYTES: usize = 2048;
pub const MAX_MESSAGE_BYTES: usize = 4096;
const SCAN_BYTES: usize = 16 * 1024;
const REDACTED: &str = "[redacted]";

fn boundary(text: &str, mut end: usize) -> usize {
    end = end.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    end
}
fn secret(key: &str) -> bool {
    let key = key
        .trim_matches(['\'', '"', '`', '-'])
        .to_ascii_lowercase()
        .replace('-', "_");
    matches!(
        key.as_str(),
        "authorization"
            | "proxy_authorization"
            | "cookie"
            | "set_cookie"
            | "password"
            | "passwd"
            | "api_key"
            | "apikey"
            | "token"
            | "access_token"
            | "refresh_token"
            | "id_token"
            | "client_secret"
            | "jwt_secret"
            | "pairing_token"
            | "credential"
            | "credentials"
            | "capability"
            | "execute_capability"
            | "secret"
            | "code"
            | "state"
            | "nonce"
            | "code_verifier"
            | "id_token_hint"
    ) || key.ends_with("_secret")
        || key.ends_with("_token")
        || key.ends_with("_credential")
}
fn body(key: &str) -> bool {
    matches!(
        key.trim_matches(['\'', '"', '`'])
            .to_ascii_lowercase()
            .as_str(),
        "body"
            | "request_body"
            | "response_body"
            | "payload"
            | "prompt"
            | "user_input"
            | "arguments"
            | "tool_args"
            | "content"
            | "old_string"
            | "new_string"
    )
}
fn value_end(text: &str, start: usize, header: bool) -> usize {
    let bytes = text.as_bytes();
    if header {
        return text[start..]
            .find(['\r', '\n'])
            .map_or(text.len(), |n| start + n);
    }
    let Some(&first) = bytes.get(start) else {
        return start;
    };
    if matches!(first, b'\'' | b'"' | b'`') {
        let mut escaped = false;
        for (n, &byte) in bytes.iter().enumerate().skip(start + 1) {
            if !escaped && byte == first {
                return n + 1;
            }
            escaped = !escaped && byte == b'\\';
        }
        return text.len();
    }
    if matches!(first, b'{' | b'[') {
        let mut depth = 0usize;
        let mut quote = false;
        let mut escaped = false;
        for (n, &byte) in bytes.iter().enumerate().skip(start) {
            if quote {
                if byte == b'"' && !escaped {
                    quote = false;
                }
                escaped = !escaped && byte == b'\\';
                continue;
            }
            match byte {
                b'"' => quote = true,
                b'{' | b'[' => depth += 1,
                b'}' | b']' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return n + 1;
                    }
                }
                _ => {}
            }
        }
        return text.len();
    }
    text[start..]
        .find(|c: char| c.is_whitespace() || matches!(c, ',' | ';' | '&' | '}' | ']'))
        .map_or(text.len(), |n| start + n)
}

/// Redact descriptions before truncation. Input and all intermediate storage are bounded.
/// Raw bodies, DTOs and secret values must never be supplied as descriptions.
pub fn sanitize(text: &str, max_bytes: usize) -> String {
    let end = boundary(text, SCAN_BYTES);
    let input = &text[..end];
    let mut out = String::new();
    let mut at = 0;
    while at < input.len() {
        let rest = &input[at..];
        if rest.starts_with("[truncated]") {
            out.push_str("[truncated]");
            at += "[truncated]".len();
            continue;
        }
        if rest.starts_with(REDACTED) {
            out.push_str(REDACTED);
            at += REDACTED.len();
            continue;
        }
        if ["sk-", "ghp_", "github_pat_", "eyJ"]
            .iter()
            .any(|prefix| rest.starts_with(prefix))
            && value_end(input, at, false).saturating_sub(at) > 20
        {
            out.push_str(REDACTED);
            at = value_end(input, at, false);
            continue;
        }
        if rest.starts_with("-----BEGIN") {
            out.push_str(REDACTED);
            break;
        }
        if ["http://", "https://", "ws://", "wss://"]
            .iter()
            .any(|scheme| {
                rest.get(..scheme.len())
                    .is_some_and(|head| head.eq_ignore_ascii_case(scheme))
            })
        {
            let end = rest
                .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ')' | '>'))
                .unwrap_or(rest.len());
            let url = &rest[..end];
            let scheme = url.find("://").unwrap_or(0) + 3;
            let authority_end = url[scheme..]
                .find(['/', '?', '#'])
                .map_or(url.len(), |n| scheme + n);
            out.push_str(&url[..scheme]);
            let authority = &url[scheme..authority_end];
            if let Some((_, host)) = authority.rsplit_once('@') {
                out.push_str(REDACTED);
                out.push('@');
                out.push_str(host);
            } else {
                out.push_str(authority);
            }
            let tail = &url[authority_end..];
            let stop = tail.find(['?', '#']).unwrap_or(tail.len());
            out.push_str(&tail[..stop]);
            if stop < tail.len() {
                out.push_str("?[redacted]");
            }
            at += end;
            continue;
        }
        let lower_prefix = rest.get(..7).unwrap_or("");
        let auth_width = if lower_prefix.eq_ignore_ascii_case("bearer ") {
            7
        } else if rest
            .get(..6)
            .is_some_and(|v| v.eq_ignore_ascii_case("basic "))
        {
            6
        } else {
            0
        };
        if auth_width > 0 {
            out.push_str(&rest[..auth_width]);
            out.push_str(REDACTED);
            at = value_end(input, at + auth_width, false);
            continue;
        }
        // Match labels without copying their values, including quoted JSON keys.
        let key_start = if rest.starts_with(['"', '\'', '`']) {
            1
        } else {
            0
        };
        let key_len = rest[key_start..]
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
            .unwrap_or(rest.len() - key_start);
        if key_len > 0 {
            let key = &rest[key_start..key_start + key_len];
            let mut sep = key_start + key_len;
            if key_start > 0 && rest.as_bytes().get(sep) == rest.as_bytes().first() {
                sep += 1;
            }
            while rest
                .as_bytes()
                .get(sep)
                .is_some_and(u8::is_ascii_whitespace)
            {
                sep += 1;
            }
            let assignment = rest
                .as_bytes()
                .get(sep)
                .is_some_and(|b| matches!(b, b':' | b'='));
            let cli = key.starts_with("--") && sep > key_start + key_len;
            if (assignment || cli) && (secret(key) || body(key)) {
                if assignment {
                    sep += 1;
                }
                while rest
                    .as_bytes()
                    .get(sep)
                    .is_some_and(u8::is_ascii_whitespace)
                {
                    sep += 1;
                }
                out.push_str(&rest[..sep]);
                out.push_str(REDACTED);
                let header = matches!(
                    key.to_ascii_lowercase().as_str(),
                    "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
                );
                let unquoted_body = body(key) && !rest[sep..].starts_with(['"', '\'', '{', '[']);
                let quoted_or_structured = rest[sep..].starts_with(['"', '\'', '`', '{', '[']);
                at = if unquoted_body {
                    input.len()
                } else if secret(key) && !cli && !header && !quoted_or_structured {
                    input[at + sep..]
                        .find(['\r', '\n', ';', ',', '&', '}', ']'])
                        .map_or(input.len(), |width| at + sep + width)
                } else {
                    value_end(input, at + sep, header)
                };
                continue;
            }
            if key_start == 0 {
                if key_len > 128 {
                    out.push_str(REDACTED);
                } else {
                    out.push_str(&rest[..key_len]);
                }
                at += key_len;
                continue;
            }
        }
        let ch = rest.chars().next().expect("remaining text has a character");
        // Generic quoted values and dumps may contain bodies. Schema extraction below
        // supplies trusted field facts independently rather than echoing input values.
        if matches!(ch, '"' | '\'' | '`') {
            out.push_str(REDACTED);
            at = value_end(input, at, false);
            let mut sep = at;
            while input
                .as_bytes()
                .get(sep)
                .is_some_and(u8::is_ascii_whitespace)
            {
                sep += 1;
            }
            if input
                .as_bytes()
                .get(sep)
                .is_some_and(|b| matches!(b, b':' | b'='))
            {
                sep += 1;
                while input
                    .as_bytes()
                    .get(sep)
                    .is_some_and(u8::is_ascii_whitespace)
                {
                    sep += 1;
                }
                out.push_str(": [redacted]");
                at = value_end(input, sep, false);
            }
            continue;
        }
        if matches!(ch, '{' | '[') {
            out.push_str(REDACTED);
            at = value_end(input, at, false);
            continue;
        }
        if ch == '\u{1b}' {
            // Consume a CSI escape sequence including its parameters.
            at += ch.len_utf8();
            if input[at..].starts_with('[') {
                at += 1;
                while at < input.len() {
                    let b = input.as_bytes()[at];
                    at += 1;
                    if (0x40..=0x7e).contains(&b) {
                        break;
                    }
                }
            }
            continue;
        }
        if ch == '\n' {
            out.push_str("\n  ");
            at += 1;
            while input
                .as_bytes()
                .get(at)
                .is_some_and(|b| matches!(b, b' ' | b'\t'))
            {
                at += 1;
            }
            continue;
        } else if ch.is_control() || matches!(ch, '\u{2028}' | '\u{2029}') {
            out.push(' ');
        } else {
            out.push(ch);
        }
        at += ch.len_utf8();
    }
    let truncated = end < text.len() || out.len() > max_bytes;
    if truncated {
        let marker = if max_bytes < " [truncated]".len() {
            "..."
        } else {
            " [truncated]"
        };
        let keep = max_bytes.saturating_sub(marker.len());
        let mut keep = boundary(&out, keep);
        if let Some(open) = out[..keep].rfind('[') {
            if !out[open..keep].contains(']') {
                keep = open;
            }
        }
        out.truncate(keep);
        out.push_str(&marker[..boundary(marker, max_bytes.saturating_sub(out.len()))]);
    }
    out
}

/// Format external error descriptions without allocating an unbounded Display result.
pub fn describe(value: &(impl std::fmt::Display + ?Sized), max_bytes: usize) -> String {
    struct Bounded(String);
    impl std::fmt::Write for Bounded {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let remaining = SCAN_BYTES.saturating_sub(self.0.len());
            self.0.push_str(&text[..boundary(text, remaining)]);
            if text.len() > remaining {
                Err(std::fmt::Error)
            } else {
                Ok(())
            }
        }
    }
    let mut buffer = Bounded(String::new());
    let clipped = std::fmt::write(&mut buffer, format_args!("{value}")).is_err();
    if clipped {
        buffer.0.push_str(" [truncated]");
    }
    sanitize(&buffer.0, max_bytes)
}

/// Exact parser structure and positions, never the input buffer or rejected value.
pub fn json_reason(error: &serde_json::Error) -> String {
    let display = error.to_string();
    let text = display
        .rsplit_once(" at line ")
        .map_or(display.as_str(), |(s, _)| s);
    let detail = match text {
        "expected `:`" => "expected colon".to_owned(),
        "expected `,` or `}`" => "expected comma or object end".to_owned(),
        "expected `,` or `]`" => "expected comma or array end".to_owned(),
        _ => {
            // Missing/duplicate names originate in the schema. Only known schema
            // identifiers survive; custom deserializers can manufacture this text.
            const FIELDS: &[&str] = &[
                "path",
                "content",
                "old_string",
                "new_string",
                "command",
                "cwd",
                "id",
                "name",
                "type",
                "root",
                "text",
                "parts",
                "input",
                "arguments",
                "pattern",
                "max_bytes",
                "operation_id",
                "failed_operation_id",
                "project_id",
                "session_id",
                "conversation_id",
                "title",
                "model",
                "provider",
                "provider_id",
                "api_key",
                "auth",
                "issuer",
                "client_id",
                "jwt_secret",
                "allowed_origins",
                "redirect_uri",
                "url",
                "token",
                "directory",
                "descriptor",
                "lease_generation",
                "display_watermark",
                "history_lineage",
                "child_id",
                "instruction",
                "checkpoint_id",
                "reply",
                "allow",
                "remember",
                "revoke",
                "relay_release",
                "browser_id",
                "workspace_id",
                "expected_revision",
            ];
            if let Some((prefix, rest)) = text.split_once('`') {
                if matches!(prefix, "missing field " | "duplicate field ") {
                    let field = rest.split('`').next().unwrap_or("");
                    if FIELDS.contains(&field) {
                        format!("{prefix}{field}")
                    } else {
                        format!("{prefix}[redacted]")
                    }
                } else {
                    sanitize(text, MAX_REASON_BYTES - 96)
                }
            } else {
                sanitize(text, MAX_REASON_BYTES - 96)
            }
        }
    };
    format!(
        "{detail} (line {}, column {})",
        error.line(),
        error.column()
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dynamic_causes_survive_while_credentials_and_bodies_do_not() {
        let source = "open cache failed: Access denied (os error 5)\nAuthorization: Bearer private-token\nresponse_body={\"text\":\"private-body\"}; api_key=private-key https://user:pass@host/path?token=private-query";
        let result = sanitize(source, MAX_REASON_BYTES);
        assert!(result.contains("Access denied (os error 5)"));
        for secret in [
            "private-token",
            "private-body",
            "private-key",
            "private-query",
            "user:pass",
        ] {
            assert!(!result.contains(secret), "{result}");
        }
        assert_eq!(sanitize(&result, MAX_REASON_BYTES), result);
    }
    #[test]
    fn clipped_secret_unicode_and_controls_stay_bounded() {
        let input = format!("precise 原因 password=\"{}\"", "secret".repeat(SCAN_BYTES));
        let result = sanitize(&input, 64);
        assert!(result.len() <= 64);
        assert!(!result.contains("secret"));
        assert_eq!(sanitize("TLS\u{1b}[31m refused\0", 80), "TLS refused ");
    }
    #[test]
    fn adversarial_assignments_urls_and_clipping_are_stable() {
        for text in [
            "HTTPS://user:private@host/path?%74oken=private#private",
            "--token private operation failed",
            "AuThOrIzAtIoN: Basic private\nrequest_body: private user text",
            r#"{"api_key":"private","content":"private"}"#,
            r#""api\u005fkey": "private""#,
            "Cookie: name=private; other=private",
            "error body: private multiline body text",
        ] {
            let result = sanitize(text, 256);
            assert!(!result.contains("private"), "{text} => {result}");
            assert_eq!(sanitize(&result, 256), result);
        }
        for max in [0, 1, 2, 3, 8, 12, 64, 2048] {
            let result = sanitize(&"é".repeat(SCAN_BYTES), max);
            assert!(result.len() <= max);
            assert_eq!(sanitize(&result, max), result);
        }
        let long = "a".repeat(SCAN_BYTES);
        assert!(sanitize(&long, 64).contains("[redacted]"));
    }
    #[test]
    fn serde_values_are_not_body_snippets() {
        let error = serde_json::from_str::<u64>("\"private user body\"").unwrap_err();
        let result = json_reason(&error);
        assert!(result.contains("invalid type"));
        assert!(result.contains("expected u64"));
        assert!(result.contains("line 1"));
        assert!(!result.contains("private user body"));
    }
}
