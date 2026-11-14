//! Per-invocation conduit to Session-owned variables, never a second store.
use super::State;
use crate::{Error, VariablesChange};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct SessionBinding {
    state: State,
}
impl SessionBinding {
    pub(crate) fn from_state(state: State) -> Self {
        Self {
            state: state.for_agent(),
        }
    }
    pub(crate) fn get(&self, name: &str) -> Result<Option<Value>, Error> {
        self.state.wasm_extract(name)
    }
    pub(crate) fn get_all(&self) -> Result<Value, Error> {
        self.state.ensure_variable_open()?;
        self.state.view_all()
    }
    pub(crate) fn metadata(&self) -> Result<Value, Error> {
        self.state.ensure_variable_open()?;
        self.state.variable_metadata()
    }
    pub(crate) async fn change(
        &self,
        change: VariablesChange,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(Error::Cancelled),
            result = self.state.change(change) => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;
    use crate::boundary::Boundary;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn reads_are_session_local_builtin_aware_and_keep_null_distinct_from_absent() {
        let closed = Arc::new(AtomicBool::new(false));
        let faulted = Arc::new(AtomicBool::new(false));
        let (state, _endpoint) =
            State::new(Limits::default()).bind_writer(closed.clone(), faulted.clone());
        let (other, _other_endpoint) = State::new(Limits::default()).bind_writer(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        state.inject("value", json!(null)).await.unwrap();
        other.inject("value", json!(42)).await.unwrap();
        state.refresh(Boundary::RoundStarted).unwrap();
        let binding = SessionBinding::from_state(state.clone());
        assert_eq!(binding.get("value").unwrap(), Some(Value::Null));
        assert_eq!(
            SessionBinding::from_state(other).get("value").unwrap(),
            Some(json!(42))
        );
        assert_eq!(binding.get("missing").unwrap(), None);
        for key in ["host", "agent", "mode"] {
            assert_eq!(binding.get(key).unwrap(), None);
        }
        assert!(binding.get("time").unwrap().is_some());
        assert_eq!(
            binding.get("session").unwrap().unwrap()["boundary"],
            "round_started"
        );
        assert!(state.extract("time").is_none());
        assert!(state.extract("session").is_none());
        faulted.store(true, Ordering::SeqCst);
        assert!(binding.get("value").is_err());
        faulted.store(false, Ordering::SeqCst);
        closed.store(true, Ordering::SeqCst);
        assert!(binding.get("value").is_err());
        assert!(
            SessionBinding::from_state(State::new(Limits::default()))
                .get("value")
                .is_err()
        );
    }

    #[tokio::test]
    async fn binding_always_stamps_agent_and_rejects_client_owner_edits() {
        let (client, _endpoint) = State::new(Limits::default()).bind_writer(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        client.inject("client_owned", json!(1)).await.unwrap();
        let guest = SessionBinding::from_state(client.clone());
        let change = |key: &str| VariablesChange {
            set: serde_json::Map::from_iter([(key.into(), json!(2))]),
            ..Default::default()
        };
        assert!(matches!(
            guest
                .change(change("client_owned"), CancellationToken::new())
                .await,
            Err(Error::VariableForbidden(_))
        ));
        guest
            .change(change("guest_owned"), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            client.variable_metadata().unwrap()["guest_owned"]["owner"],
            "agent"
        );
        assert!(matches!(
            client.remove("guest_owned").await,
            Err(Error::VariableForbidden(_))
        ));
    }

    #[tokio::test]
    async fn changes_use_existing_atomic_validation_and_queued_commands_survive_cancellation() {
        let (state, endpoint) = State::new(Limits::default()).bind_writer(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let binding = SessionBinding::from_state(state.clone());
        binding
            .change(
                VariablesChange {
                    scoped_set: vec![],
                    set: serde_json::Map::from_iter([
                        ("keep".into(), json!(1)),
                        ("remove".into(), json!(2)),
                    ]),
                    remove: vec![],
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        for key in ["time", "session", "host", "agent", "mode"] {
            let change = VariablesChange {
                scoped_set: vec![],
                set: serde_json::Map::from_iter([
                    ("partial".into(), json!(true)),
                    (key.into(), json!(1)),
                ]),
                remove: vec![],
            };
            assert!(
                binding
                    .change(change, CancellationToken::new())
                    .await
                    .is_err()
            );
            assert!(state.extract("partial").is_none());
        }
        let fence = endpoint.fence().await.unwrap();
        let cancel = CancellationToken::new();
        let change = binding.change(
            VariablesChange {
                scoped_set: vec![],
                set: serde_json::Map::from_iter([("queued".into(), json!(7))]),
                remove: vec!["remove".into()],
            },
            cancel.clone(),
        );
        tokio::pin!(change);
        // Poll once while the fence holds. The command is enqueued but cannot ack.
        assert!(futures_util::poll!(&mut change).is_pending());
        cancel.cancel();
        assert!(matches!(change.await, Err(Error::Cancelled)));
        drop(fence);
        let _settled = endpoint.fence().await.unwrap();
        assert_eq!(state.extract("queued"), Some(json!(7)));
        assert!(state.extract("remove").is_none());
        assert_eq!(state.extract("keep"), Some(json!(1)));
    }
}
