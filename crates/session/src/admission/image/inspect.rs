//! Format-specific bounded header inspection; no raster allocations or decoding.
use super::{ImageMetadata, check_dimensions};
use crate::Error;

fn invalid() -> Error {
    Error::config("invalid or unsupported encoded image metadata")
}
fn be32(data: &[u8]) -> Result<u32, Error> {
    Ok(u32::from_be_bytes(data.try_into().map_err(|_| invalid())?))
}
fn le24(data: &[u8]) -> Result<u32, Error> {
    let [a, b, c] = data else {
        return Err(invalid());
    };
    Ok(u32::from(*a) | u32::from(*b) << 8 | u32::from(*c) << 16)
}
fn le16(data: &[u8]) -> Result<u32, Error> {
    Ok(u32::from(u16::from_le_bytes(
        data.try_into().map_err(|_| invalid())?,
    )))
}

pub(super) fn inspect(mime: &str, data: &[u8]) -> Result<ImageMetadata, Error> {
    match super::image_type(mime) {
        Some("image/png") => png(data),
        Some("image/jpeg") => jpeg(data),
        Some("image/gif") => gif(data),
        Some("image/webp") => webp(data),
        _ => Err(Error::config("unsupported image media type")),
    }
}

fn png(data: &[u8]) -> Result<ImageMetadata, Error> {
    if data.len() < 33 || &data[..8] != b"\x89PNG\r\n\x1a\n" || &data[8..16] != b"\0\0\0\rIHDR" {
        return Err(invalid());
    }
    // Validate the header fields that determine how a decoder interprets sizes.
    let valid_depth = match data[25] {
        0 => matches!(data[24], 1 | 2 | 4 | 8 | 16),
        2 | 4 | 6 => matches!(data[24], 8 | 16),
        3 => matches!(data[24], 1 | 2 | 4 | 8),
        _ => false,
    };
    if !valid_depth || data[26] != 0 || data[27] != 0 || data[28] > 1 {
        return Err(invalid());
    }
    Ok(ImageMetadata {
        width: be32(&data[16..20])?,
        height: be32(&data[20..24])?,
    })
}

fn jpeg(data: &[u8]) -> Result<ImageMetadata, Error> {
    if !data.starts_with(&[0xff, 0xd8]) {
        return Err(invalid());
    }
    let mut offset = 2;
    while offset < data.len() {
        if data[offset] != 0xff {
            return Err(invalid());
        }
        while data.get(offset) == Some(&0xff) {
            offset += 1;
        }
        let marker = *data.get(offset).ok_or_else(invalid)?;
        offset += 1;
        if marker == 0xd9 || marker == 0xda || marker == 0 || marker == 0xd8 {
            return Err(invalid());
        }
        if marker == 0x01 || (0xd0..=0xd7).contains(&marker) {
            continue;
        }
        let length_bytes = data.get(offset..offset + 2).ok_or_else(invalid)?;
        let length = usize::from(u16::from_be_bytes([length_bytes[0], length_bytes[1]]));
        if length < 2 {
            return Err(invalid());
        }
        let segment = data.get(offset..offset + length).ok_or_else(invalid)?;
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
            if segment.len() < 8 {
                return Err(invalid());
            }
            let components = usize::from(segment[7]);
            if components == 0 || segment.len() != 8 + 3 * components {
                return Err(invalid());
            }
            return Ok(ImageMetadata {
                height: u32::from(u16::from_be_bytes([segment[3], segment[4]])),
                width: u32::from(u16::from_be_bytes([segment[5], segment[6]])),
            });
        }
        offset += length;
    }
    Err(invalid())
}

fn gif(data: &[u8]) -> Result<ImageMetadata, Error> {
    if data.len() < 13 || !matches!(&data[..6], b"GIF87a" | b"GIF89a") {
        return Err(invalid());
    }
    let metadata = ImageMetadata {
        width: le16(&data[6..8])?,
        height: le16(&data[8..10])?,
    };
    // Every image descriptor can declare its own raster size. Inspect frames as
    // well as the logical screen so a small canvas cannot hide an oversized frame.
    let mut offset = 13;
    if data[10] & 0x80 != 0 {
        offset += 3 * (1usize << (usize::from(data[10] & 7) + 1));
    }
    while offset < data.len() {
        let tag = data[offset];
        offset += 1;
        match tag {
            0x3b => return Ok(metadata),
            0x21 => {
                offset += 1;
            }
            0x2c => {
                let descriptor = data.get(offset..offset + 9).ok_or_else(invalid)?;
                check_dimensions(le16(&descriptor[4..6])?, le16(&descriptor[6..8])?)?;
                offset += 9;
                if descriptor[8] & 0x80 != 0 {
                    offset += 3 * (1usize << (usize::from(descriptor[8] & 7) + 1));
                }
                // LZW minimum code size precedes the sub-blocks.
                if !(2..=8).contains(data.get(offset).ok_or_else(invalid)?) {
                    return Err(invalid());
                }
                offset += 1;
            }
            _ => return Err(invalid()),
        }
        loop {
            let length = usize::from(*data.get(offset).ok_or_else(invalid)?);
            offset += 1;
            if length == 0 {
                break;
            }
            data.get(offset..offset + length).ok_or_else(invalid)?;
            offset += length;
        }
    }
    Err(invalid())
}

fn webp(data: &[u8]) -> Result<ImageMetadata, Error> {
    if data.len() < 20 || &data[..4] != b"RIFF" || &data[8..12] != b"WEBP" {
        return Err(invalid());
    }
    let declared = u32::from_le_bytes(data[4..8].try_into().map_err(|_| invalid())?);
    if u64::from(declared) + 8 != data.len() as u64 {
        return Err(invalid());
    }
    let mut offset = 12;
    let mut dimensions = None;
    while offset < data.len() {
        let header = data.get(offset..offset + 8).ok_or_else(invalid)?;
        let length = u32::from_le_bytes(header[4..8].try_into().map_err(|_| invalid())?) as usize;
        offset += 8;
        let chunk = data
            .get(offset..offset.checked_add(length).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        let found = match &header[..4] {
            b"VP8X" if chunk.len() == 10 => Some(ImageMetadata {
                width: le24(&chunk[4..7])? + 1,
                height: le24(&chunk[7..10])? + 1,
            }),
            b"VP8 "
                if chunk.len() >= 10 && chunk[0] & 1 == 0 && &chunk[3..6] == b"\x9d\x01\x2a" =>
            {
                Some(ImageMetadata {
                    width: le16(&chunk[6..8])? & 0x3fff,
                    height: le16(&chunk[8..10])? & 0x3fff,
                })
            }
            b"VP8L" if chunk.len() >= 5 && chunk[0] == 0x2f => {
                let bits = u32::from_le_bytes(chunk[1..5].try_into().map_err(|_| invalid())?);
                Some(ImageMetadata {
                    width: (bits & 0x3fff) + 1,
                    height: ((bits >> 14) & 0x3fff) + 1,
                })
            }
            b"ANMF" if chunk.len() >= 16 => Some(ImageMetadata {
                width: le24(&chunk[6..9])? + 1,
                height: le24(&chunk[9..12])? + 1,
            }),
            b"VP8X" | b"VP8 " | b"VP8L" | b"ANMF" => return Err(invalid()),
            _ => None,
        };
        if let Some(found) = found {
            check_dimensions(found.width, found.height)?;
            dimensions.get_or_insert(found);
        }
        offset += length + (length & 1);
        if offset > data.len() {
            return Err(invalid());
        }
    }
    dimensions.ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn jpeg_skips_metadata_segments_and_rejects_truncated_lengths() {
        let bytes = [
            0xff, 0xd8, 0xff, 0xe0, 0, 4, 0, 0, 0xff, 0xc0, 0, 11, 8, 0, 2, 0, 3, 1, 1, 0x11, 0,
        ];
        assert_eq!(
            jpeg(&bytes).unwrap(),
            ImageMetadata {
                width: 3,
                height: 2
            }
        );
        for end in 0..bytes.len() {
            assert!(jpeg(&bytes[..end]).is_err());
        }
    }
    #[test]
    fn gif_checks_screen_and_each_frame_metadata() {
        let mut bytes = b"GIF89a\x02\0\x03\0\0\0\0".to_vec();
        bytes.extend_from_slice(&[0x2c, 0, 0, 0, 0, 2, 0, 3, 0, 0, 2, 1, 0, 0, 0x3b]);
        assert_eq!(
            gif(&bytes).unwrap(),
            ImageMetadata {
                width: 2,
                height: 3
            }
        );
        bytes[18..20].copy_from_slice(&8193u16.to_le_bytes());
        assert!(gif(&bytes).is_err());
    }
    #[test]
    fn webp_lossless_and_extended_headers_are_bounded() {
        let mut bytes = b"RIFF\x12\0\0\0WEBPVP8L\x05\0\0\0".to_vec();
        bytes.extend_from_slice(&[0x2f, 1, 0x80, 0, 0, 0]);
        assert_eq!(
            webp(&bytes).unwrap(),
            ImageMetadata {
                width: 2,
                height: 3
            }
        );
        bytes[4] = 0xff;
        assert!(webp(&bytes).is_err());
        assert!(png(b"not an image").is_err());
    }
}
