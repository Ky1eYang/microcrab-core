//! Parse the sole model image argument; keep sensitive source material invocation-local.
use crate::Error;
use crate::admission::image::{MAX_IMAGE_BYTES, image_type};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

pub(super) const MAX_ENCODED_BYTES: usize = MAX_IMAGE_BYTES.div_ceil(3) * 4;
const MAX_URI_BYTES: usize = 4096;
const PREFIX: &str = "{{var:base64:";
pub(super) const HTTPS_EFFECTS: &str =
    "Send the URL to the selected model service and download it once to retain the image";

pub(super) fn bytes_reference(value: &str) -> Option<&str> {
    value.strip_prefix(PREFIX)?.strip_suffix("}}")
}

#[derive(Clone)]
pub(crate) enum Source {
    Inline {
        mime: Option<&'static str>,
        encoded: String,
    },
    File(PathBuf),
    Bytes(String),
    Https(microcrab_transport::public_https::PublicHttpsUrl),
}

impl Source {
    pub(super) fn parse(value: &str, state: &crate::State) -> Result<Self, Error> {
        if value.is_empty()
            || value.len() > MAX_ENCODED_BYTES + 64
            || value.chars().any(char::is_whitespace)
        {
            return Err(Error::tool(
                "image_url must be a nonempty image source within its size limit, without whitespace",
            ));
        }
        if let Some(variable) = bytes_reference(value) {
            return if let Some(selector) = variable.strip_prefix('@') {
                let (key, rev) = selector
                    .strip_prefix("bytes_")
                    .and_then(|value| value.split_once('_'))
                    .ok_or_else(|| Error::tool("invalid exact Bytes selector"))?;
                if key.len() != 32
                    || rev.len() != 16
                    || !key
                        .bytes()
                        .chain(rev.bytes())
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(Error::tool("invalid exact Bytes selector"));
                }
                state.with_image_bytes(selector, |_, _| Ok(()))?;
                Ok(Self::Bytes(selector.into()))
            } else {
                Ok(Self::Bytes(state.image_bytes_selector_for_name(variable)?))
            };
        }
        if value.starts_with("file:") {
            return file_path(value).map(Self::File);
        }
        if value
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
        {
            return microcrab_transport::public_https::PublicHttpsUrl::parse(value)
                .map(Self::Https)
                .map_err(|error| Error::tool_dynamic(error.to_string()));
        }
        let (mime, encoded) = if let Some(data) = value.strip_prefix("data:") {
            let (header, encoded) = data
                .split_once(',')
                .ok_or_else(|| Error::tool("image data URL must declare MIME and base64"))?;
            let mime = header
                .strip_suffix(";base64")
                .and_then(image_type)
                .ok_or_else(|| {
                    Error::tool("image data URL requires a supported raster MIME and base64")
                })?;
            (Some(mime), encoded)
        } else {
            (None, value)
        };
        if encoded.is_empty() || encoded.len() > MAX_ENCODED_BYTES || encoded.len() % 4 != 0 {
            return Err(Error::tool(
                "image_url requires a file URI, public HTTPS URL or standard Base64 image",
            ));
        }
        let (body, padding) = encoded.split_at(encoded.trim_end_matches('=').len());
        if body.is_empty()
            || padding.len() > 2
            || !body
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
        {
            return Err(Error::tool(
                "image_url requires a file URI, public HTTPS URL or standard Base64 image",
            ));
        }
        Ok(Self::Inline {
            mime,
            encoded: encoded.into(),
        })
    }

    /// Private matcher and dispatch witness. This is not a tool argument for
    /// Context replay, the execution descriptor, or the authored ToolCall audit.
    pub(crate) fn policy_arguments(&self) -> Value {
        let (kind, fingerprint, destination) = match self {
            Self::Inline { mime, encoded } => {
                let mut digest = Sha256::new();
                digest.update(mime.unwrap_or_default());
                digest.update(b"\0");
                digest.update(encoded);
                ("inline", format!("{:x}", digest.finalize()), None)
            }
            Self::File(path) => (
                "file",
                digest(path.as_os_str().as_encoded_bytes()),
                Some(path.to_string_lossy().into_owned()),
            ),
            Self::Bytes(selector) => ("bytes", digest(selector), None),
            Self::Https(url) => (
                "https",
                digest(url.as_url().as_str()),
                Some(url.destination()),
            ),
        };
        let mut arguments = json!({"image_url":format!("{kind}:{fingerprint}")});
        if let Some(destination) = destination {
            arguments["destination"] = json!(destination);
        }
        if kind == "https" {
            arguments["effects"] = json!(HTTPS_EFFECTS);
        }
        arguments
    }
}

fn digest(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Reject ambiguous encodings before URL normalization can remove dot segments.
fn file_path(value: &str) -> Result<PathBuf, Error> {
    let bad = || {
        Error::tool(
            "image_url must be a local absolute file URI, such as file:///workspace/image.png",
        )
    };
    let raw = value.strip_prefix("file://").ok_or_else(bad)?;
    if value.len() > MAX_URI_BYTES
        || !raw.starts_with('/')
        || raw.starts_with("//")
        || raw.contains(['?', '#', '\\'])
    {
        return Err(bad());
    }
    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut offset = 0;
    while offset < bytes.len() {
        let byte = if bytes[offset] == b'%' {
            let hi = bytes
                .get(offset + 1)
                .and_then(|b| (*b as char).to_digit(16))
                .ok_or_else(bad)?;
            let lo = bytes
                .get(offset + 2)
                .and_then(|b| (*b as char).to_digit(16))
                .ok_or_else(bad)?;
            offset += 3;
            let byte = ((hi << 4) | lo) as u8;
            if matches!(byte, b'/' | b'\\' | b'%') {
                return Err(bad());
            }
            byte
        } else {
            offset += 1;
            bytes[offset - 1]
        };
        if byte.is_ascii_control() {
            return Err(bad());
        }
        decoded.push(byte);
    }
    let decoded = String::from_utf8(decoded).map_err(|_| bad())?;
    if decoded
        .split('/')
        .skip(1)
        .any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(bad());
    }
    let url = reqwest::Url::parse(value).map_err(|_| bad())?;
    let path = url.to_file_path().map_err(|_| bad())?;
    if !path.is_absolute() {
        return Err(bad());
    }
    Ok(path)
}
