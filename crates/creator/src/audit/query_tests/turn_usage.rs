use super::*;

fn completed(recorder: &mut Recorder, call: &str, outcome: &str, usage: Value) {
    send(recorder, call, "identical request body");
    recorder
        .publish(json!({"event":"round_completed","call_id":call,"outcome":outcome,"usage":usage}))
        .unwrap();
}

#[test]
fn whole_turn_metadata_keeps_first_transmitted_retry_and_more_than_one_page_of_rounds() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = recorder.effective(effective("failed-first", None)).unwrap();
    completed(
        &mut recorder,
        "failed-first",
        "rejected",
        json!({"input_tokens":11,"output_tokens":999}),
    );
    let mut head = first.clone();
    for round in 1..=300 {
        let call = format!("accepted-{round}");
        let mut request = effective(&call, Some(head));
        request.round = Some(round);
        head = recorder.effective(request).unwrap();
        completed(
            &mut recorder,
            &call,
            "accepted",
            json!({"input_tokens":round+100,"output_tokens":round}),
        );
    }
    let mut helper = effective("helper", Some(head.clone()));
    helper.subagent = Some("rename".into());
    recorder.effective(helper).unwrap();
    completed(
        &mut recorder,
        "helper",
        "accepted",
        json!({"output_tokens":999}),
    );
    recorder
        .effective(effective("unselected", Some(first.clone())))
        .unwrap();
    completed(
        &mut recorder,
        "unselected",
        "accepted",
        json!({"output_tokens":999}),
    );
    let result = query_selected_turn_metadata(directory.path(), &head, &[(2, 3)]).unwrap();
    assert!(result.selected_ancestry_complete);
    assert!(result.selected_identities_complete);
    assert_eq!(result.attempts.len(), 301);
    assert_eq!(result.attempts[0].request_hash, first);
    assert_eq!(result.attempts[0].outcome.as_ref().unwrap(), "rejected");
    assert!(
        result
            .attempts
            .iter()
            .all(|attempt| attempt.sent_request.is_none()
                && attempt.response.is_none()
                && attempt.request_document.is_none())
    );
    assert!(result.attempts[0].transmitted_sequence < result.attempts[1].transmitted_sequence);
    assert!(
        query_selected_turn_metadata(directory.path(), &head, &[(99, 99)])
            .unwrap()
            .attempts
            .is_empty()
    );
    assert!(query_selected_turn_metadata(directory.path(), &head, &vec![(2, 3); 130]).is_err());
}

#[test]
fn missing_selected_parent_is_explicitly_incomplete_and_reused_calls_are_ambiguous() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let head = recorder
        .effective(effective("known-suffix", Some("a".repeat(64))))
        .unwrap();
    completed(
        &mut recorder,
        "known-suffix",
        "accepted",
        json!({"input_tokens":10,"output_tokens":5}),
    );
    let result = query_selected_turn_metadata(directory.path(), &head, &[(2, 3)]).unwrap();
    assert!(!result.selected_ancestry_complete);
    assert_eq!(result.attempts.len(), 1);
    let head = recorder
        .effective(effective("known-suffix", Some(head)))
        .unwrap();
    completed(
        &mut recorder,
        "known-suffix",
        "accepted",
        json!({"input_tokens":20,"output_tokens":7}),
    );
    let result = query_selected_turn_metadata(directory.path(), &head, &[(2, 3)]).unwrap();
    assert!(!result.selected_identities_complete);
}

#[test]
fn incomplete_transmission_and_contradictory_terminal_usage_are_not_complete_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let head = recorder
        .effective(effective("not-transmitted", None))
        .unwrap();
    recorder.publish(json!({"event":"round_completed","call_id":"not-transmitted","outcome":"accepted","usage":{"input_tokens":10,"output_tokens":3}})).unwrap();
    let result = query_selected_turn_metadata(directory.path(), &head, &[(2, 3)]).unwrap();
    assert!(!result.selected_identities_complete);
    let head = recorder
        .effective(effective("contradictory", None))
        .unwrap();
    send(&mut recorder, "contradictory", "same body");
    recorder.publish(json!({"event":"round_completed","call_id":"contradictory","usage":{"output_tokens":3}})).unwrap();
    recorder.publish(json!({"event":"round_completed","call_id":"contradictory","outcome":"accepted","usage":{"output_tokens":8}})).unwrap();
    let result = query_selected_turn_metadata(directory.path(), &head, &[(2, 3)]).unwrap();
    assert!(!result.selected_identities_complete);
}

#[test]
fn transmission_order_is_retained_separately_from_preparation_order() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = recorder
        .effective(effective("prepared-first", None))
        .unwrap();
    let head = recorder
        .effective(effective("transmitted-first", Some(first)))
        .unwrap();
    completed(
        &mut recorder,
        "transmitted-first",
        "rejected",
        json!({"input_tokens":17}),
    );
    completed(
        &mut recorder,
        "prepared-first",
        "accepted",
        json!({"input_tokens":42,"output_tokens":3}),
    );
    let result = query_selected_turn_metadata(directory.path(), &head, &[(2, 3)]).unwrap();
    assert!(result.attempts[0].sequence < result.attempts[1].sequence);
    assert!(result.attempts[1].transmitted_sequence < result.attempts[0].transmitted_sequence);
}
