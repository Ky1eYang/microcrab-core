//! Empty completion classification never weakens local response or replay validation.
use super::*;
use microcrab_protocol::{AssistantPart, AssistantProviderMetadata, Usage};

fn response(content: &str, parts: Vec<AssistantPart>) -> Response {
    Response {
        content: content.into(),
        tool_calls: vec![],
        usage: Usage::default(),
        provider_metadata: Some(AssistantProviderMetadata::Neutral {
            parts,
            replay_hints: vec![],
        }),
    }
}

#[test]
fn empty_completion_is_closed_bounded_and_keeps_strict_embedding_validation() {
    for (parts, reasoning_present) in [
        (vec![], false),
        (
            vec![AssistantPart::Reasoning {
                text: "fixture reasoning".into(),
            }],
            true,
        ),
        (vec![AssistantPart::RedactedReasoning], true),
    ] {
        let response = response("", parts);
        assert!(matches!(
            validate_response(&response, &[]),
            Err(Error::Protocol(_))
        ));
        for finish in [
            EmptyCompletionFinish::Stop,
            EmptyCompletionFinish::Length,
            EmptyCompletionFinish::Unknown,
        ] {
            let error = validate_response_with_finish(&response, &[], finish).unwrap_err();
            let Error::Provider(cause) = error else {
                panic!("expected provider error")
            };
            assert_eq!(
                cause.empty_completion_details(),
                Some(EmptyCompletionFacts {
                    reasoning_present,
                    finish
                })
            );
            assert!(cause.protocol_error().is_none());
            assert!(cause.failure_details().is_none());
            assert!(!format!("{cause:?} {cause}").contains("fixture reasoning"));
            assert_eq!(
                failure::response_error(Error::Provider(cause.clone())).diagnostic_reason(),
                cause.public_reason().unwrap()
            );
        }
    }
}

#[test]
fn malformed_projection_wins_over_empty_completion_and_valid_answer_stays_exact() {
    let malformed = response(
        "",
        vec![AssistantPart::Text {
            text: "not in projection".into(),
        }],
    );
    assert!(matches!(
        validate_response_with_finish(&malformed, &[], EmptyCompletionFinish::Stop),
        Err(Error::Protocol(_))
    ));
    let valid = response(
        "answer",
        vec![
            AssistantPart::Reasoning {
                text: "reasoning".into(),
            },
            AssistantPart::Text {
                text: "answer".into(),
            },
        ],
    );
    let before = serde_json::to_vec(&valid).unwrap();
    validate_response_with_finish(&valid, &[], EmptyCompletionFinish::Stop).unwrap();
    assert_eq!(serde_json::to_vec(&valid).unwrap(), before);
    let whitespace = response(" \n", vec![AssistantPart::Text { text: " \n".into() }]);
    let Err(Error::Provider(cause)) =
        validate_response_with_finish(&whitespace, &[], EmptyCompletionFinish::Stop)
    else {
        panic!("whitespace is not an answer")
    };
    assert!(!cause.empty_completion_details().unwrap().reasoning_present);
}
