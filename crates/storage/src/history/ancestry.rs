//! Bounded, non-authoritative metadata derived from canonical checkpoint verification.
use super::{RestorePoint, valid_hash};
use microcrab_session::Error;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{Arc, Mutex},
};

pub(super) const MAX_NODES: usize = 4096;
pub(super) const BYTE_BUDGET: usize = 64 * 1024 * 1024;
// The per-scope bound includes metadata learned across different branches. The
// same scope is never evicted while its worker is running or waiting to start.
const MAX_SCOPES: usize = 8;

pub(super) struct Scopes<K, S>(Mutex<HashMap<K, Arc<Mutex<S>>>>);

impl<K, S> Default for Scopes<K, S> {
    fn default() -> Self {
        Self(Mutex::new(HashMap::new()))
    }
}

impl<K: Eq + Hash + Clone, S: Default> Scopes<K, S> {
    pub(super) fn get(&self, key: K) -> Result<Arc<Mutex<S>>, Error> {
        let mut scopes = self.0.lock().map_err(|_| cache_failure())?;
        if let Some(scope) = scopes.get(&key) {
            return Ok(scope.clone());
        }
        if scopes.len() == MAX_SCOPES {
            let idle = scopes
                .iter()
                .find(|(_, scope)| Arc::strong_count(scope) == 1)
                .map(|(key, _)| key.clone())
                .ok_or(Error::Busy)?;
            scopes.remove(&idle);
        }
        let scope = Arc::new(Mutex::new(S::default()));
        scopes.insert(key, scope.clone());
        Ok(scope)
    }
}

pub(super) fn cache_failure() -> Error {
    Error::protocol("journal ancestry cache mutex poisoned")
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct VerifiedNode {
    pub(super) id: String,
    pub(super) parent: Option<String>,
    pub(super) epoch: u64,
    pub(super) request_head: Option<String>,
}

impl VerifiedNode {
    pub(super) fn from_point(point: RestorePoint) -> Self {
        Self {
            id: point.cursor.checkpoint_id,
            parent: point.parent,
            epoch: point.cursor.epoch,
            request_head: point.checkpoint.request_head,
        }
    }
}

pub(super) struct Cache<Stamp>(HashMap<String, (Stamp, VerifiedNode)>);

impl<Stamp> Default for Cache<Stamp> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

/// The backend supplies a witnessed storage stamp and a canonical verification
/// callback. Descending through a parent is legal only after one of those
/// witnesses matches a previously verified node or the callback succeeds.
/// Progress on a budget-limited read is reusable, but never returned as proof.
impl<Stamp: Eq> Cache<Stamp> {
    pub(super) fn walk(
        &mut self,
        head: String,
        mut remaining: usize,
        mut inspect: impl FnMut(&str) -> Result<(Option<Stamp>, usize), Error>,
        mut verify: impl FnMut(&str, usize, Option<&Stamp>) -> Result<VerifiedNode, Error>,
        mut stop: impl FnMut(&VerifiedNode) -> bool,
    ) -> Result<Vec<VerifiedNode>, Error> {
        let mut next = Some(head);
        let mut seen = HashSet::new();
        let mut verified = Vec::new();
        while let Some(id) = next {
            valid_hash(&id)?;
            if seen.len() == MAX_NODES {
                return Err(Error::Limit("journal ancestry node budget exceeded".into()));
            }
            if !seen.insert(id.clone()) {
                return Err(Error::protocol("journal ancestry cycle"));
            }
            let (stamp, bytes) = inspect(&id)?;
            let cached = stamp.as_ref().and_then(|stamp| {
                self.0
                    .get(&id)
                    .filter(|(checked, _)| checked == stamp)
                    .map(|(_, node)| node.clone())
            });
            let node = match cached {
                Some(node) => node,
                None => {
                    remaining = remaining.checked_sub(bytes).ok_or_else(|| {
                        Error::Limit("journal ancestry hydration byte budget exceeded".into())
                    })?;
                    let node = verify(&id, bytes, stamp.as_ref())?;
                    if let Some(stamp) = stamp {
                        // Evict unrelated branches before discarding progress on
                        // this chain (including an earlier budget-limited read).
                        if self.0.len() == MAX_NODES && !self.0.contains_key(&id) {
                            if let Some(victim) = self.0.keys().find(|key| !seen.contains(*key)) {
                                let victim = victim.clone();
                                self.0.remove(&victim);
                            }
                        }
                        if self.0.len() < MAX_NODES || self.0.contains_key(&id) {
                            self.0.insert(id, (stamp, node.clone()));
                        }
                    }
                    node
                }
            };
            next = if stop(&node) {
                None
            } else {
                node.parent.clone()
            };
            verified.push(node);
        }
        Ok(verified)
    }
}
