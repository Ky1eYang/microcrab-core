//! Typed Session communication; only dynamic values cross as JSON text.
use super::super::microcrab::session::variables::{
    CallFacts, MetadataEntry, Persistence, Role, ScopedEntry, VariableChangeSet, VariableEntry,
    Visibility,
};
use super::{
    CommunicationError, CommunicationErrorCode, RuntimeState, apply_change, bounded, failure,
    session_binding, variable_get_import,
};
use microcrab_protocol::{
    ScopedVariableSet, VariableMetadata, VariablePersistence, VariableRole, VariableVisibility,
    VariablesChange,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use wasmtime::{
    StoreContextMut,
    component::{ComponentType, Linker},
};

pub(in crate::runtime::wasm) const TYPED_VARIABLES_INTERFACE: &str =
    "microcrab:session/variables@0.2.0";

pub(in crate::runtime::wasm) fn add_typed_to_linker(
    linker: &mut Linker<RuntimeState>,
) -> wasmtime::Result<()> {
    let mut interface = linker.instance(TYPED_VARIABLES_INTERFACE)?;
    interface.func_wrap("variable-get", variable_get_import)?;
    interface.func_wrap("variable-get-all", variable_get_all)?;
    interface.func_wrap("variable-metadata", variable_metadata)?;
    interface.func_wrap("variable-change", variable_change)?;
    interface.func_wrap("tool-call-facts", tool_call_facts)?;
    Ok(())
}

// Canonical memory32 layout sizes, plus UTF-8 payload bytes and one byte per
// Role. Counting descriptors also bounds lists of empty strings/records.
const LIST_BYTES: usize = Vec::<u8>::SIZE32;
const STRING_BYTES: usize = String::SIZE32;
const VARIABLE_ENTRY_BYTES: usize = VariableEntry::SIZE32;
const METADATA_ENTRY_BYTES: usize = MetadataEntry::SIZE32;
const SCOPED_ENTRY_BYTES: usize = ScopedEntry::SIZE32;
const CALL_FACTS_BYTES: usize = CallFacts::SIZE32;
const ROLE_BYTES: usize = Role::SIZE32;

fn variable_get_all(
    caller: StoreContextMut<'_, RuntimeState>,
    (): (),
) -> wasmtime::Result<(Result<Vec<VariableEntry>, CommunicationError>,)> {
    let state = caller.data();
    let limit = state.max_output_bytes;
    let result = (|| {
        let value = session_binding(state)?.get_all().map_err(unavailable)?;
        let Value::Object(values) = value else {
            return Err(failure(
                CommunicationErrorCode::Unavailable,
                "Session variable projection is not an object",
            ));
        };
        let mut bytes =
            LIST_BYTES.saturating_add(values.len().saturating_mul(VARIABLE_ENTRY_BYTES));
        check_response_bytes(bytes, limit)?;
        let mut entries = Vec::with_capacity(values.len());
        for (name, value) in values {
            let value_json = serde_json::to_string(&value).map_err(rejected)?;
            bytes = bytes
                .saturating_add(name.len())
                .saturating_add(value_json.len());
            check_response_bytes(bytes, limit)?;
            entries.push(VariableEntry { name, value_json });
        }
        Ok(entries)
    })();
    // The typed list's aggregate size was checked above. Bound error messages too.
    bounded(result, |_| 0, limit)
}

fn variable_metadata(
    caller: StoreContextMut<'_, RuntimeState>,
    (): (),
) -> wasmtime::Result<(Result<Vec<MetadataEntry>, CommunicationError>,)> {
    let state = caller.data();
    let limit = state.max_output_bytes;
    let result = (|| {
        let value = session_binding(state)?.metadata().map_err(unavailable)?;
        let metadata: BTreeMap<String, VariableMetadata> =
            serde_json::from_value(value).map_err(rejected)?;
        let mut bytes =
            LIST_BYTES.saturating_add(metadata.len().saturating_mul(METADATA_ENTRY_BYTES));
        check_response_bytes(bytes, limit)?;
        let mut entries = Vec::with_capacity(metadata.len());
        for (name, attributes) in metadata {
            bytes = bytes
                .saturating_add(name.len())
                .saturating_add(attributes.editors.len().saturating_mul(ROLE_BYTES));
            check_response_bytes(bytes, limit)?;
            entries.push(MetadataEntry {
                name,
                visibility: attributes.visibility.into(),
                persistence: attributes.persistence.into(),
                owner: attributes.owner.into(),
                editors: attributes.editors.into_iter().map(Role::from).collect(),
            });
        }
        Ok(entries)
    })();
    bounded(result, |_| 0, limit)
}

fn variable_change(
    caller: StoreContextMut<'_, RuntimeState>,
    (change,): (VariableChangeSet,),
) -> wasmtime::Result<(Result<(), CommunicationError>,)> {
    let limit = caller.data().max_input_bytes;
    if crate::budget::check(
        crate::LimitKind::ToolArguments,
        change_bytes(&change),
        limit,
    )
    .is_err()
    {
        return bounded(
            Err(failure(
                CommunicationErrorCode::Rejected,
                "host variable change byte limit exceeded",
            )),
            |()| 0,
            caller.data().max_output_bytes,
        );
    }
    apply_change(caller, || decode_change(change))
}

fn change_bytes(change: &VariableChangeSet) -> usize {
    let mut bytes = VariableChangeSet::SIZE32;
    for entry in &change.set {
        bytes = bytes
            .saturating_add(VARIABLE_ENTRY_BYTES)
            .saturating_add(entry.name.len())
            .saturating_add(entry.value_json.len());
    }
    for name in &change.remove {
        bytes = bytes
            .saturating_add(STRING_BYTES)
            .saturating_add(name.len());
    }
    for entry in &change.scoped_set {
        bytes = bytes
            .saturating_add(SCOPED_ENTRY_BYTES)
            .saturating_add(entry.name.len())
            .saturating_add(entry.value_json.len())
            .saturating_add(
                entry
                    .editors
                    .as_ref()
                    .map_or(0, Vec::len)
                    .saturating_mul(ROLE_BYTES),
            );
    }
    bytes
}

fn decode_change(change: VariableChangeSet) -> Result<VariablesChange, CommunicationError> {
    // Unlike the JSON object ABI, a typed list can contain duplicate set names.
    // Check all names before converting to the native map, which would collapse them.
    let mut seen = HashSet::new();
    for name in change
        .set
        .iter()
        .map(|entry| &entry.name)
        .chain(&change.remove)
        .chain(change.scoped_set.iter().map(|entry| &entry.name))
    {
        crate::state::access::validate_custom_key(name).map_err(rejected)?;
        if !seen.insert(name) {
            return Err(rejected("duplicate or overlapping variable changes"));
        }
    }
    let change = VariablesChange {
        set: change
            .set
            .into_iter()
            .map(|entry| Ok((entry.name, decode_value(&entry.value_json)?)))
            .collect::<Result<_, CommunicationError>>()?,
        remove: change.remove,
        scoped_set: change
            .scoped_set
            .into_iter()
            .map(|entry| {
                Ok(ScopedVariableSet {
                    name: entry.name,
                    value: decode_value(&entry.value_json)?,
                    visibility: entry.visibility.into(),
                    persistence: entry.persistence.into(),
                    editors: entry
                        .editors
                        .map(|roles| roles.into_iter().map(VariableRole::from).collect()),
                })
            })
            .collect::<Result<_, CommunicationError>>()?,
    };
    crate::state::access::validate_change(&change).map_err(rejected)?;
    Ok(change)
}

fn decode_value(value_json: &str) -> Result<Value, CommunicationError> {
    serde_json::from_str(value_json)
        .map_err(|error| rejected(format!("variable value JSON: {error}")))
}

fn unavailable(error: impl ToString) -> CommunicationError {
    failure(CommunicationErrorCode::Unavailable, error.to_string())
}

fn rejected(error: impl ToString) -> CommunicationError {
    failure(CommunicationErrorCode::Rejected, error.to_string())
}

fn check_response_bytes(bytes: usize, limit: usize) -> Result<(), CommunicationError> {
    crate::budget::check(crate::LimitKind::WasmBytes, bytes, limit)
        .map_err(|_| rejected("host communication response byte limit exceeded"))
}

fn tool_call_facts(
    caller: StoreContextMut<'_, RuntimeState>,
    (): (),
) -> wasmtime::Result<(Result<CallFacts, CommunicationError>,)> {
    let state = caller.data();
    let result = if state.cancellation.is_cancelled() || state.epoch_cancellation.is_cancelled() {
        Err(failure(
            CommunicationErrorCode::Cancelled,
            "invocation cancelled",
        ))
    } else {
        admitted_call_facts(state.tool_context.as_ref())
    };
    bounded(
        result,
        |facts| {
            CALL_FACTS_BYTES
                .saturating_add(facts.session_id.len())
                .saturating_add(facts.admitted_mode.as_ref().map_or(0, String::len))
        },
        state.max_output_bytes,
    )
}

fn admitted_call_facts(
    context: Option<&super::super::ToolContext>,
) -> Result<CallFacts, CommunicationError> {
    let context = context
        .filter(|context| context.subject.is_tool())
        .ok_or_else(|| unavailable("no admitted tool invocation"))?;
    // Compiled::run already bound this context to the specific component.
    // Revalidate the invocation proof here, including revocation by cancellation.
    context.ensure_admitted(None).map_err(|error| {
        let code = match error {
            crate::policy::PermissionError::Cancelled => CommunicationErrorCode::Cancelled,
            _ => CommunicationErrorCode::Rejected,
        };
        failure(code, error.public_reason())
    })?;
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| unavailable("UTC clock precedes the Unix epoch"))?;
    let observed_at_unix_ms = u64::try_from(elapsed.as_millis())
        .map_err(|_| unavailable("UTC clock exceeds the Unix millisecond range"))?;
    Ok(CallFacts {
        session_id: context.session_id.clone(),
        admitted_mode: context.mode.clone(),
        observed_at_unix_ms,
    })
}

impl From<Visibility> for VariableVisibility {
    fn from(value: Visibility) -> Self {
        match value {
            Visibility::Visible => Self::Visible,
            Visibility::Invisible => Self::Invisible,
            Visibility::Secret => Self::Secret,
        }
    }
}

impl From<VariableVisibility> for Visibility {
    fn from(value: VariableVisibility) -> Self {
        match value {
            VariableVisibility::Visible => Self::Visible,
            VariableVisibility::Invisible => Self::Invisible,
            VariableVisibility::Secret => Self::Secret,
        }
    }
}

impl From<Persistence> for VariablePersistence {
    fn from(value: Persistence) -> Self {
        match value {
            Persistence::Persisted => Self::Persisted,
            Persistence::Ephemeral => Self::Ephemeral,
        }
    }
}

impl From<VariablePersistence> for Persistence {
    fn from(value: VariablePersistence) -> Self {
        match value {
            VariablePersistence::Persisted => Self::Persisted,
            VariablePersistence::Ephemeral => Self::Ephemeral,
        }
    }
}

impl From<Role> for VariableRole {
    fn from(value: Role) -> Self {
        match value {
            Role::Runtime => Self::Runtime,
            Role::Client => Self::Client,
            Role::Agent => Self::Agent,
        }
    }
}

impl From<VariableRole> for Role {
    fn from(value: VariableRole) -> Self {
        match value {
            VariableRole::Runtime => Self::Runtime,
            VariableRole::Client => Self::Client,
            VariableRole::Agent => Self::Agent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::{ToolContext, test_admission};
    use super::*;

    fn entry(name: &str, value_json: &str) -> VariableEntry {
        VariableEntry {
            name: name.into(),
            value_json: value_json.into(),
        }
    }

    fn scoped_entry(name: &str, editors: Option<Vec<Role>>) -> ScopedEntry {
        ScopedEntry {
            name: name.into(),
            value_json: "null".into(),
            visibility: Visibility::Invisible,
            persistence: Persistence::Ephemeral,
            editors,
        }
    }

    #[test]
    fn typed_change_keeps_arbitrary_values_and_optional_editors() {
        let change = decode_change(VariableChangeSet {
            set: vec![entry("a", r#"[null,1,"text",{"nested":true}]"#)],
            remove: vec!["obsolete".into()],
            scoped_set: vec![
                scoped_entry("defaults", None),
                scoped_entry("empty", Some(vec![])),
                scoped_entry("shared", Some(vec![Role::Client, Role::Runtime])),
            ],
        })
        .unwrap();
        assert_eq!(
            change.set["a"],
            serde_json::json!([null, 1, "text", {"nested": true}])
        );
        assert_eq!(change.remove, ["obsolete"]);
        assert_eq!(change.scoped_set[0].value, Value::Null);
        assert_eq!(change.scoped_set[0].editors, None);
        assert_eq!(change.scoped_set[1].editors, Some(vec![]));
        assert_eq!(
            change.scoped_set[2].editors,
            Some(vec![VariableRole::Client, VariableRole::Runtime])
        );
    }

    #[test]
    fn typed_change_rejects_duplicate_overlapping_reserved_and_malformed_entries() {
        let change = |set, remove, scoped_set| VariableChangeSet {
            set,
            remove,
            scoped_set,
        };
        for invalid in [
            change(vec![entry("a", "1"), entry("a", "2")], vec![], vec![]),
            change(vec![], vec!["a".into(), "a".into()], vec![]),
            change(
                vec![],
                vec![],
                vec![scoped_entry("a", None), scoped_entry("a", None)],
            ),
            change(vec![entry("a", "1")], vec!["a".into()], vec![]),
            change(vec![entry("a", "1")], vec![], vec![scoped_entry("a", None)]),
            change(vec![], vec!["a".into()], vec![scoped_entry("a", None)]),
            change(vec![entry("session", "1")], vec![], vec![]),
            change(vec![entry("a", "null true")], vec![], vec![]),
        ] {
            assert!(matches!(
                decode_change(invalid).unwrap_err().code,
                CommunicationErrorCode::Rejected
            ));
        }
    }

    #[test]
    fn typed_change_byte_count_includes_names_values_and_list_payloads() {
        let change = VariableChangeSet {
            set: vec![entry("é", "null")],
            remove: vec![String::new()],
            scoped_set: vec![scoped_entry("x", Some(vec![Role::Agent, Role::Client]))],
        };
        assert_eq!(
            change_bytes(&change),
            24 + (16 + 2 + 4) + 8 + (32 + 1 + 4 + 2)
        );
    }

    #[test]
    fn tool_call_facts_require_proof_and_ignore_mutated_live_mode() {
        let mut context = ToolContext::detached(
            crate::SessionState::new(Default::default()),
            Default::default(),
        );
        context.call_id = "invocation-id".into();
        context.tool = "invocation-probe".into();
        context.mode = Some("admitted-mode".into());
        assert!(matches!(
            admitted_call_facts(Some(&context)).unwrap_err().code,
            CommunicationErrorCode::Rejected
        ));
        test_admission::admit(&mut context, Some("component-a".into()));
        context.begin_dispatch(Some("component-a")).unwrap();
        context.state.update(|snapshot| {
            snapshot.selected_mode = Some("different-live-mode".into());
        });
        let output = admitted_call_facts(Some(&context)).unwrap();
        assert_eq!(output.admitted_mode.as_deref(), Some("admitted-mode"));
        assert_eq!(output.session_id, context.session_id);
        assert!(output.observed_at_unix_ms > 0);
        assert!(matches!(
            admitted_call_facts(None).unwrap_err().code,
            CommunicationErrorCode::Unavailable
        ));

        let mut forged = context.clone();
        forged.mode = Some("different-live-mode".into());
        assert!(matches!(
            admitted_call_facts(Some(&forged)).unwrap_err().code,
            CommunicationErrorCode::Rejected
        ));
        context.cancellation.cancel();
        assert!(matches!(
            admitted_call_facts(Some(&context)).unwrap_err().code,
            CommunicationErrorCode::Cancelled
        ));
    }
}
