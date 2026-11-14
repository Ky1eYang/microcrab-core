//! Field-scoped unified patches. Siblings are never aligned against one another.
//! A linear prefix/suffix hunk bounds work without a quadratic edit matrix.
use serde_json::{Map, Value};

pub(super) fn request_diff(previous: &str, current: &str) -> String {
    match (parse_object(previous), parse_object(current)) {
        (Some(old), Some(new)) => diff_object(&old, &new, ""),
        _ => unified(previous, current, "baseline", "request"),
    }
}

fn parse_object(text: &str) -> Option<Map<String, Value>> {
    if text.trim().is_empty() {
        return Some(Map::new());
    }
    match serde_json::from_str(text).ok()? {
        Value::Object(value) => Some(value),
        _ => None,
    }
}

fn diff_object(old: &Map<String, Value>, new: &Map<String, Value>, prefix: &str) -> String {
    let mut keys = old.keys().chain(new.keys()).collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    let mut sections = Vec::new();
    for key in keys {
        // JSON-pointer escaping keeps sibling names and nested paths unambiguous;
        // escape control characters so a key cannot introduce patch header lines.
        let key_path = key
            .replace('~', "~0")
            .replace('/', "~1")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t");
        let path = format!("{prefix}/{key_path}");
        match (old.get(key), new.get(key)) {
            (Some(left), Some(right)) if left == right => {}
            (Some(Value::Object(left)), Some(Value::Object(right))) => {
                let child = diff_object(left, right, &path);
                if !child.is_empty() {
                    sections.push(child);
                }
            }
            (left, right) => sections.push(unified(
                &value_text(left),
                &value_text(right),
                &format!("baseline{path}"),
                &format!("request{path}"),
            )),
        }
    }
    sections.join("\n")
}

fn value_text(value: Option<&Value>) -> String {
    // serde_json::Value has no fallible custom serializers or non-string map keys.
    value
        .map(|value| serde_json::to_string_pretty(value).expect("JSON value serializes") + "\n")
        .unwrap_or_default()
}

fn unified(previous: &str, current: &str, old_label: &str, new_label: &str) -> String {
    if previous == current {
        return String::new();
    }
    // Keep terminators in comparisons so changing only the final newline is an edit.
    let old = previous.split_inclusive('\n').collect::<Vec<_>>();
    let new = current.split_inclusive('\n').collect::<Vec<_>>();
    let prefix = old.iter().zip(&new).take_while(|(a, b)| a == b).count();
    let suffix = old[prefix..]
        .iter()
        .rev()
        .zip(new[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let context = 3;
    let start = prefix.saturating_sub(context);
    let old_changed_end = old.len() - suffix;
    let new_changed_end = new.len() - suffix;
    let trailing_context = suffix.min(context);
    let old_count = old_changed_end + trailing_context - start;
    let new_count = new_changed_end + trailing_context - start;
    // Empty ranges use the line *before* the insertion/deletion, as unified diff requires.
    let old_start = start + usize::from(old_count != 0);
    let new_start = start + usize::from(new_count != 0);
    let mut diff = format!(
        "--- {old_label}\n+++ {new_label}\n@@ -{old_start},{old_count} +{new_start},{new_count} @@\n"
    );
    for line in &old[start..prefix] {
        append_line(&mut diff, ' ', line);
    }
    for line in &old[prefix..old_changed_end] {
        append_line(&mut diff, '-', line);
    }
    for line in &new[prefix..new_changed_end] {
        append_line(&mut diff, '+', line);
    }
    for line in &old[old_changed_end..old_changed_end + trailing_context] {
        append_line(&mut diff, ' ', line);
    }
    diff
}

fn append_line(diff: &mut String, kind: char, line: &str) {
    diff.push(kind);
    diff.push_str(line);
    if !line.ends_with('\n') {
        diff.push_str("\n\\ No newline at end of file\n");
    }
}
