//! A small real PNG shared by live image admission fixtures.
use base64::{Engine as _, engine::general_purpose::STANDARD};

pub const PNG_BASE64: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=";

pub fn png() -> Vec<u8> {
    STANDARD.decode(PNG_BASE64).unwrap()
}
