//! Opaque staged Secret writes composed with an application-owned interaction claim.
//! This guard owns the existing variable queue fence; it creates no second store.
use super::{State, VariableCommit, VariableWitness, Variables, Writer};
use crate::Error;
use crate::state::endpoint::{VariableEndpoint, VariableFence};
use std::{collections::BTreeMap, sync::Arc};
#[cfg(test)]
mod tests;

/// An unpublished Secret-only candidate. Dropping it aborts without writing.
///
/// The caller must reserve the exact pending human interaction before preparation,
/// commit once, and retain that reservation through public receipt publication.
/// A failed public receipt after commit must never replay the Secret write.
#[must_use = "drop aborts the prepared Secret write"]
pub struct PreparedSecretWrite {
    state: State,
    endpoint: Arc<VariableEndpoint>,
    expected: VariableWitness,
    references_revision: u64,
    candidate: Variables,
    _fence: VariableFence,
}
impl std::fmt::Debug for PreparedSecretWrite {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedSecretWrite")
            .finish_non_exhaustive()
    }
}
impl PreparedSecretWrite {
    /// Publish the complete batch once. No plaintext appears in the acknowledgement.
    pub fn commit(self) -> Result<VariableCommit, Error> {
        self.endpoint.ensure_open()?;
        let mut live = self
            .state
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Shutdown and replacement may invalidate a prepared candidate. An ordinary
        // queued variable write cannot pass the retained fence.
        self.endpoint.ensure_open()?;
        if live.witness() != self.expected || live.references_revision != self.references_revision {
            return Err(Error::VariableConflict);
        }
        crate::admission::variables::admit(
            &self.candidate.admission_value(),
            live.snapshot.limits.max_context_bytes,
        )?;
        let changed = live.snapshot.variables != self.candidate;
        if changed {
            let next = live.variables_revision.checked_add(1).ok_or_else(|| {
                crate::budget::reject(crate::LimitKind::Arithmetic, "variable revision overflow")
            })?;
            live.snapshot.variables = self.candidate.clone();
            live.variables_revision = next;
        }
        Ok(VariableCommit {
            before: self.expected.clone(),
            after: live.witness(),
            changed,
        })
    }
}
impl State {
    /// Prepare a Client-only Secret write while an application retains its exact
    /// first-winner interaction claim. Generic turn/hook variables cannot create Secrets.
    pub async fn prepare_secret_write(
        &self,
        values: BTreeMap<String, String>,
    ) -> Result<PreparedSecretWrite, Error> {
        if self.actor != microcrab_protocol::VariableRole::Client {
            return Err(Error::config("only Client may submit Secret values"));
        }
        let Writer::Session(owner) = &self.writer else {
            return Err(Error::Closed);
        };
        let endpoint = owner.upgrade().ok_or(Error::Closed)?;
        let fence = endpoint.fence().await?;
        endpoint.ensure_open()?;
        let (expected, references_revision, mut candidate, references, budget) = {
            let live = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            if values.keys().any(|name| live.bytes.owns(name)) {
                return Err(Error::VariableForbidden(
                    "Bytes variables require the typed Client endpoint".into(),
                ));
            }
            (
                live.witness(),
                live.references_revision,
                live.snapshot.variables.clone(),
                live.snapshot.references.clone(),
                live.snapshot.limits.max_context_bytes,
            )
        };
        // Reference keys are authoritative even when they satisfy the Secret grammar.
        if values.keys().any(|name| references.owns(name)) {
            return Err(Error::config("Secret key is owned by a reference"));
        }
        candidate.write_secrets(&values)?;
        crate::admission::variables::admit(&candidate.admission_value(), budget)?;
        Ok(PreparedSecretWrite {
            state: self.clone(),
            endpoint,
            expected,
            references_revision,
            candidate,
            _fence: fence,
        })
    }

    /// Capture the entire Secret set under one live-State lock. Raw access is crate-private.
    pub(crate) fn injection_view(&self) -> super::super::variables::InjectionView {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot
            .variables
            .injection_view()
    }
    pub(crate) fn secret_snapshot(&self) -> super::super::variables::SecretSnapshot {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot
            .variables
            .secret_snapshot()
    }
}
