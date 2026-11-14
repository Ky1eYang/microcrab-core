//! Replayable line diffs of canonical sent documents, with three lines of context.
//! Hirschberg alignment keeps scratch space linear rather than allocating an edit matrix.
//! A shared cell budget bounds quadratic alignment; oversized unmatched regions use
//! replayable replacement after preserving their common prefix and suffix.

const ALIGNMENT_CELLS: usize = 8_000_000;

fn lengths(old: &[&str], new: &[&str], reverse: bool) -> Vec<usize> {
    let mut row = vec![0; new.len() + 1];
    for i in 0..old.len() {
        let left = old[if reverse { old.len() - 1 - i } else { i }];
        let mut diagonal = 0;
        for j in 0..new.len() {
            let above = row[j + 1];
            let right = new[if reverse { new.len() - 1 - j } else { j }];
            row[j + 1] = if left == right {
                diagonal + 1
            } else {
                row[j].max(above)
            };
            diagonal = above;
        }
    }
    row
}

fn align(old: &[&str], new: &[&str], edits: &mut Vec<(char, String)>, budget: &mut usize) {
    let prefix = old.iter().zip(new).take_while(|(a, b)| a == b).count();
    edits.extend(old[..prefix].iter().map(|line| (' ', (*line).into())));
    let old = &old[prefix..];
    let new = &new[prefix..];
    let suffix = old
        .iter()
        .rev()
        .zip(new.iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let left = &old[..old.len() - suffix];
    let right = &new[..new.len() - suffix];
    // The two half-row passes together visit left.len() * right.len() cells.
    // Reserve before allocating rows; recursive calls consume the same budget.
    // Linear prefix/suffix scans and output are bounded separately by input size.
    let affordable = if let Some(work) = left
        .len()
        .checked_mul(right.len())
        .filter(|work| *work <= *budget)
    {
        *budget -= work;
        true
    } else {
        false
    };
    if !affordable {
        *budget = 0;
        edits.extend(left.iter().map(|line| ('-', (*line).into())));
        edits.extend(right.iter().map(|line| ('+', (*line).into())));
    } else if left.is_empty() {
        edits.extend(right.iter().map(|line| ('+', (*line).into())));
    } else if right.is_empty() {
        edits.extend(left.iter().map(|line| ('-', (*line).into())));
    } else if left.len() == 1 {
        if let Some(index) = right.iter().position(|line| *line == left[0]) {
            edits.extend(right[..index].iter().map(|line| ('+', (*line).into())));
            edits.push((' ', left[0].into()));
            edits.extend(right[index + 1..].iter().map(|line| ('+', (*line).into())));
        } else {
            edits.push(('-', left[0].into()));
            edits.extend(right.iter().map(|line| ('+', (*line).into())));
        }
    } else {
        let middle = left.len() / 2;
        let split = {
            let forward = lengths(&left[..middle], right, false);
            let backward = lengths(&left[middle..], right, true);
            (0..=right.len())
                .max_by_key(|&i| forward[i] + backward[right.len() - i])
                .expect("nonempty split range")
        };
        align(&left[..middle], &right[..split], edits, budget);
        align(&left[middle..], &right[split..], edits, budget);
    }
    edits.extend(
        old[old.len() - suffix..]
            .iter()
            .map(|line| (' ', (*line).into())),
    );
}

pub(super) fn patch(previous: &str, current: &str, from: &str, to: &str) -> String {
    if previous == current {
        return String::new();
    }
    // Retaining LF in comparisons also represents a final-newline-only edit exactly.
    let before = previous.split_inclusive('\n').collect::<Vec<_>>();
    let after = current.split_inclusive('\n').collect::<Vec<_>>();
    let mut edits = Vec::new();
    let mut budget = ALIGNMENT_CELLS;
    align(&before, &after, &mut edits, &mut budget);
    let mut groups: Vec<(usize, usize)> = Vec::new();
    for (index, (kind, _)) in edits.iter().enumerate() {
        if *kind == ' ' {
            continue;
        }
        let start = index.saturating_sub(3);
        let end = (index + 4).min(edits.len());
        if let Some(last) = groups.last_mut().filter(|last| start <= last.1) {
            last.1 = end;
        } else {
            groups.push((start, end));
        }
    }
    let mut out = format!("--- {from}\n+++ {to}\n");
    let mut cursor = 0;
    let mut old_line = 0;
    let mut new_line = 0;
    for (start, end) in groups {
        for (kind, _) in &edits[cursor..start] {
            old_line += usize::from(*kind != '+');
            new_line += usize::from(*kind != '-');
        }
        let old_count = edits[start..end]
            .iter()
            .filter(|(kind, _)| *kind != '+')
            .count();
        let new_count = edits[start..end]
            .iter()
            .filter(|(kind, _)| *kind != '-')
            .count();
        out.push_str(&format!(
            "@@ -{},{} +{},{} @@\n",
            old_line + usize::from(old_count != 0),
            old_count,
            new_line + usize::from(new_count != 0),
            new_count
        ));
        for (kind, line) in &edits[start..end] {
            out.push(*kind);
            out.push_str(line);
            if !line.ends_with('\n') {
                out.push_str("\n\\ No newline at end of file\n");
            }
        }
        old_line += old_count;
        new_line += new_count;
        cursor = end;
    }
    out
}

/// Replays one emitted patch against its declared base. Test-only: the runtime
/// never reconstructs a document from a patch.
#[cfg(test)]
pub(crate) fn replay_patch(base: &str, patch: &str) -> String {
    if patch.is_empty() {
        return base.into();
    }
    let source = base.split_inclusive('\n').collect::<Vec<_>>();
    let mut lines = patch.lines().peekable();
    assert!(lines.next().unwrap().starts_with("--- "));
    assert!(lines.next().unwrap().starts_with("+++ "));
    let mut cursor = 0;
    let mut output = String::new();
    while let Some(header) = lines.next() {
        let fields = header.split_whitespace().collect::<Vec<_>>();
        assert_eq!(fields[0], "@@");
        let range = |field: &str| {
            let (start, count) = field[1..].split_once(',').unwrap();
            (
                start.parse::<usize>().unwrap(),
                count.parse::<usize>().unwrap(),
            )
        };
        let (old_start, old_count) = range(fields[1]);
        let (new_start, new_count) = range(fields[2]);
        let start = old_start - usize::from(old_count != 0);
        assert!(start >= cursor && start <= source.len());
        output.extend(source[cursor..start].iter().copied());
        cursor = start;
        assert_eq!(
            output.split_inclusive('\n').count(),
            new_start - usize::from(new_count != 0)
        );
        let (mut removed, mut added) = (0, 0);
        while lines.peek().is_some_and(|line| !line.starts_with("@@ ")) {
            let line = lines.next().unwrap();
            let (kind, text) = line.split_at(1);
            let mut text = format!("{text}\n");
            if lines.peek() == Some(&"\\ No newline at end of file") {
                lines.next();
                text.pop();
            }
            match kind {
                " " | "-" => {
                    assert_eq!(source[cursor], text);
                    cursor += 1;
                    removed += 1;
                }
                "+" => {}
                _ => panic!("invalid patch line"),
            }
            if kind != "-" {
                output.push_str(&text);
                added += 1;
            }
        }
        assert_eq!((removed, added), (old_count, new_count));
    }
    output.extend(source[cursor..].iter().copied());
    output
}

#[cfg(test)]
mod tests {
    use super::patch;
    use crate::audit::diff::replay_patch;

    #[test]
    fn separated_changes_keep_context_and_skip_unchanged_middle() {
        let old = (0..30).map(|i| format!("line-{i}\n")).collect::<String>();
        let new = old
            .replace("line-4\n", "changed-4\n")
            .replace("line-24\n", "changed-24\n");
        let diff = patch(&old, &new, "old", "new");
        assert_eq!(diff.matches("@@ -").count(), 2);
        assert!(diff.contains(" line-3\n-line-4\n+changed-4\n line-5\n"));
        assert!(!diff.contains("line-15"));
        assert_eq!(replay_patch(&old, &diff), new);
    }

    #[test]
    fn initial_deletion_newlines_unicode_and_literal_headers_replay() {
        for (old, new) in [
            ("", "你好\n"),
            ("你好\n", ""),
            ("same", "same\n"),
            ("same\n", "same"),
            ("--- baseline\n+++ request\n", "+++ request\n@@ literal\n"),
            ("a\nb\nc\n", "c\na\nb\n"),
            ("", ""),
        ] {
            let diff = patch(old, new, "old", "new");
            assert_eq!(replay_patch(old, &diff), new);
        }
        assert_eq!(
            patch("", "one\n", "empty", "new"),
            "--- empty\n+++ new\n@@ -0,0 +1,1 @@\n+one\n"
        );
        assert_eq!(
            patch("one\n", "", "old", "empty"),
            "--- old\n+++ empty\n@@ -1,1 +0,0 @@\n-one\n"
        );
    }

    #[test]
    fn oversized_alignment_replaces_only_trimmed_region_with_bounded_work() {
        let prefix = (0..20).map(|i| format!("prefix-{i}\n")).collect::<String>();
        let suffix = (0..20).map(|i| format!("suffix-{i}\n")).collect::<String>();
        let middle = |label: &str| {
            (0..3000)
                .map(|i| {
                    if i == 1500 {
                        "interior-common-anchor\n".to_owned()
                    } else {
                        format!("{label}-{i}\n")
                    }
                })
                .collect::<String>()
        };
        let old = format!("{prefix}{}{suffix}", middle("old"));
        let new = format!("{prefix}{}{suffix}", middle("new"));
        let before = old.split_inclusive('\n').collect::<Vec<_>>();
        let after = new.split_inclusive('\n').collect::<Vec<_>>();
        let mut edits = Vec::new();
        let mut budget = super::ALIGNMENT_CELLS;
        super::align(&before, &after, &mut edits, &mut budget);
        assert_eq!(budget, 0);
        assert_eq!(edits.iter().filter(|(kind, _)| *kind == ' ').count(), 40);
        assert!(
            edits
                .iter()
                .any(|(kind, line)| *kind == '-' && line == "interior-common-anchor\n")
        );
        assert!(
            edits
                .iter()
                .any(|(kind, line)| *kind == '+' && line == "interior-common-anchor\n")
        );
        let diff = patch(&old, &new, "old", "new");
        assert!(!diff.contains("prefix-0"));
        assert!(!diff.contains("suffix-19"));
        assert!(diff.contains(" prefix-19\n"));
        assert!(diff.contains(" suffix-0\n"));
        assert_eq!(replay_patch(&old, &diff), new);
    }

    #[test]
    fn exhaustive_short_repeated_lines_replay_with_minimum_edits() {
        let mut documents = vec![String::new()];
        for length in 1..=4 {
            for bits in 0..(1 << length) {
                documents.push(
                    (0..length)
                        .map(|i| if bits & (1 << i) == 0 { "a\n" } else { "b\n" })
                        .collect::<String>(),
                );
            }
        }
        for old in &documents {
            for new in &documents {
                let diff = patch(old, new, "old", "new");
                assert_eq!(replay_patch(old, &diff), *new);
                // Independent full edit-distance matrix is small only in this oracle.
                let a = old.lines().collect::<Vec<_>>();
                let b = new.lines().collect::<Vec<_>>();
                let mut distance = vec![vec![0; b.len() + 1]; a.len() + 1];
                for (i, row) in distance.iter_mut().enumerate() {
                    row[0] = i;
                }
                for (j, cell) in distance[0].iter_mut().enumerate() {
                    *cell = j;
                }
                for i in 1..=a.len() {
                    for j in 1..=b.len() {
                        distance[i][j] = if a[i - 1] == b[j - 1] {
                            distance[i - 1][j - 1]
                        } else {
                            1 + distance[i - 1][j].min(distance[i][j - 1])
                        };
                    }
                }
                let edits = diff
                    .lines()
                    .skip(2)
                    .filter(|line| line.starts_with(['+', '-']))
                    .count();
                assert_eq!(edits, distance[a.len()][b.len()], "{old:?} -> {new:?}");
            }
        }
    }
}
