//! Capture boundary invariants independent of operating-system pipe scheduling.
use super::{Capture, Decoder, Stream};
use microcrab_host_api::{ErrorCategory, ShellPosition};

#[test]
fn stream_switches_share_positions_and_drain_seals_partial_lines() {
    let mut capture = Capture::new(64);
    capture.append(Stream::Stdout, "ab");
    capture.append(Stream::Stderr, "err");
    capture.append(Stream::Stdout, "cd\n");
    let middle = capture
        .read(Some(ShellPosition { line: 2, column: 1 }))
        .unwrap();
    assert_eq!(middle.stdout, "cd\n");
    assert_eq!(middle.stderr, "rr");
    capture.append(Stream::Stdout, "pending");
    let first = capture.drain().unwrap();
    assert_eq!(first.stdout, "abcd\npending");
    capture.append(Stream::Stdout, "later");
    let next = capture.read(None).unwrap();
    assert_eq!(next.stdout, "later");
    assert_eq!(
        capture
            .read(Some(ShellPosition { line: 1, column: 0 }))
            .unwrap()
            .expired
            .unwrap()
            .through_line,
        4
    );
}

#[test]
fn tiny_cap_expires_entire_unicode_fragments_without_unbounded_buffers() {
    let mut capture = Capture::new(1);
    capture.append(Stream::Stdout, "é");
    let read = capture.read(None).unwrap();
    assert!(read.stdout.is_empty());
    assert_eq!(read.expired.unwrap().through_line, 1);
    assert!(read.truncated);
    capture.append(Stream::Stderr, "z");
    assert_eq!(capture.counts(), (0, 1));
    assert!(matches!(
        capture.read(Some(ShellPosition { line: 2, column: 2 })),
        Err(error) if error.category == ErrorCategory::Conflict
    ));
}

#[test]
fn decoder_preserves_split_scalars_and_replaces_invalid_or_incomplete_bytes() {
    let mut decoder = Decoder::default();
    assert_eq!(decoder.push(&[0xc3], false), "");
    assert_eq!(decoder.push(&[0xa9, 0xff, 0xe2], false), "é�");
    assert_eq!(decoder.push(&[], true), "�");
}
