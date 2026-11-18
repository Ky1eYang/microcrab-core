use super::*;
use serde_json::json;

#[test]
fn historical_state_without_identity_remains_absent_and_byte_exact() {
    let historical = json!({"epoch_id":0,"turn_id":null,"round_id":null,"selected_mode":null,
        "limits": crate::Limits::default(), "variables":{},"last_turn_id":0});
    let bytes = serde_json::to_vec(&historical).unwrap();
    let state: crate::SessionState = serde_json::from_slice(&bytes).unwrap();
    assert!(state.communication.is_none());
    assert_eq!(serde_json::to_vec(&state).unwrap(), bytes);
}

#[test]
fn address_decoding_rejects_aliases_and_noncanonical_tokens() {
    for value in [
        "session-1",
        "agt_0123456789ABCDEF",
        "agt_0123456789abcde",
        "agt_0123456789abcdef0",
        "agt_0123456789abcdeg",
    ] {
        assert!(AgentId::try_from(value.to_owned()).is_err());
        assert!(serde_json::from_value::<AgentId>(json!(value)).is_err());
    }
    let first = AgentId::fresh();
    let second = AgentId::fresh();
    assert_ne!(first, second);
    assert_eq!(
        serde_json::from_value::<AgentId>(json!(first)).unwrap(),
        first
    );
}

#[test]
fn projection_discloses_addresses_without_lineage_or_credentials() {
    let state = crate::State::new(crate::Limits::default());
    let communication = CommunicationState::fresh(0, SessionInteraction::Persistent);
    let identity = communication.identity.clone();
    state.update(|snapshot| snapshot.communication = Some(communication));
    let projected = state.view().unwrap();
    assert_eq!(projected["session"]["agent_id"], json!(identity.agent_id));
    assert_eq!(projected["session"]["depth"], 0);
    assert_eq!(projected["session"]["interaction"], "persistent");
    assert_eq!(projected["correspondents"], json!([]));
    assert_eq!(projected["external_speakers"], json!([]));
    let encoded = projected.to_string();
    assert!(!encoded.contains(&identity.lineage));
    assert!(!encoded.contains("api_key"));
}
