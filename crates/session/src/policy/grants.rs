//! Deferred, exact-owner future grants. A dispatch ACK starts confirmation;
//! successful terminal cleanup does not cancel the independent confirmation.
use super::*;
use std::sync::atomic::Ordering;

#[derive(Clone)]
pub(super) struct StagedGrant {
    pub owner: Arc<PermissionController>,
    pub expected: PolicySnapshot,
    pub request: PermissionRequest,
    pub grant: FutureGrant,
    pub root: Arc<PermissionController>,
    pub root_revision: u64,
    pub lineage: Vec<(Arc<PermissionController>, PolicySnapshot)>,
    pub cancellation: CancellationToken,
    pub terminal: tokio::sync::watch::Receiver<Option<crate::runtime::execution::ExecutionStatus>>,
}
impl PermissionController {
    pub fn bind_session_id(&self, id: &str) -> Result<(), PermissionError> {
        if id.is_empty() || id.len() > 256 {
            return Err(PermissionError::InvalidConfiguration(
                "invalid permission owning Session identity".into(),
            ));
        }
        let mut slot = self
            .session_id
            .write()
            .map_err(|_| PermissionError::Unavailable)?;
        if slot.as_deref().is_some_and(|current| current != id) {
            return Err(PermissionError::InvalidConfiguration(
                "controller already belongs to another Session".into(),
            ));
        }
        *slot = Some(id.into());
        Ok(())
    }
    pub(crate) fn bind_closed(
        &self,
        closed: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(), PermissionError> {
        *self
            .session_closed
            .write()
            .map_err(|_| PermissionError::Unavailable)? = Some(closed);
        Ok(())
    }
    pub(crate) fn close_interactions(&self) {
        if let Ok(lifetime) = self.lifetime.read() {
            lifetime.cancel();
        }
    }
    pub(crate) fn bind_lifetime(&self, lifetime: CancellationToken) -> Result<(), PermissionError> {
        *self
            .lifetime
            .write()
            .map_err(|_| PermissionError::Unavailable)? = lifetime;
        Ok(())
    }
    pub(super) async fn lineage_changed(
        &self,
        revision: u64,
        lineage: &[(Arc<PermissionController>, PolicySnapshot)],
    ) -> Result<(), PermissionError> {
        let mut receivers = vec![self.changed.subscribe()];
        receivers.extend(lineage.iter().map(|(owner, _)| owner.changed.subscribe()));
        self.ensure_witness(revision, lineage)?;
        let waits = receivers
            .into_iter()
            .map(|mut receiver| {
                Box::pin(async move {
                    let _ = receiver.changed().await;
                })
            })
            .collect::<Vec<_>>();
        let _ = futures_util::future::select_all(waits).await;
        Ok(())
    }
    pub fn set_future_grant_persistence(
        &self,
        persistence: Arc<dyn FutureGrantPersistence>,
    ) -> Result<(), PermissionError> {
        *self
            .grant_persistence
            .write()
            .map_err(|_| PermissionError::Unavailable)? = Some(persistence);
        Ok(())
    }
    pub(super) fn owning_session_id(&self) -> Result<String, PermissionError> {
        self.session_id
            .read()
            .map_err(|_| PermissionError::Unavailable)?
            .clone()
            .ok_or(PermissionError::Unavailable)
    }
    pub(super) fn ensure_not_reserved(&self) -> Result<(), PermissionError> {
        if self.grant_reserved.load(Ordering::Acquire) {
            Err(PermissionError::StalePolicy)
        } else {
            Ok(())
        }
    }
}
impl StagedGrant {
    fn cancelled(&self) -> bool {
        if self.root.session_closed.read().map_or(true, |slot| {
            slot.as_ref()
                .is_some_and(|closed| closed.load(Ordering::Acquire))
        }) {
            return true;
        }
        if self
            .root
            .lifetime
            .read()
            .map_or(true, |lifetime| lifetime.is_cancelled())
        {
            return true;
        }
        self.cancellation.is_cancelled()
            && *self.terminal.borrow()
                != Some(crate::runtime::execution::ExecutionStatus::Succeeded)
    }
    pub fn dispatch(self) {
        tokio::spawn(async move {
            if self.cancelled()
                || self
                    .root
                    .ensure_witness(self.root_revision, &self.lineage)
                    .is_err()
            {
                return;
            }
            let Ok(session_id) = self.owner.owning_session_id() else {
                return;
            };
            if self
                .owner
                .grant_persistence
                .read()
                .map_or(true, |slot| slot.is_none())
            {
                return;
            }
            let proposal = FutureGrantProposal {
                session_id,
                controller_id: self.owner.identity,
                revision: self.expected.revision,
                call_id: self.request.call_id.clone(),
                agent: self.expected.config.agent.clone(),
                mode: self.expected.config.mode.clone(),
                tool: self.request.tool.clone(),
                args: self.grant.args.clone(),
            };
            let adapter = self.owner.adapter.clone();
            let adoption = Arc::new(self);
            let monitor = adoption.clone();
            let cancelled = async move {
                let lifetime = match monitor.root.lifetime.read() {
                    Ok(token) => token.clone(),
                    Err(_) => return,
                };
                let mut terminal = monitor.terminal.clone();
                loop {
                    if monitor.cancelled()
                        || monitor
                            .root
                            .ensure_witness(monitor.root_revision, &monitor.lineage)
                            .is_err()
                    {
                        return;
                    }
                    let succeeded = *terminal.borrow()
                        == Some(crate::runtime::execution::ExecutionStatus::Succeeded);
                    tokio::select! {biased;
                        _=lifetime.cancelled()=>return,
                        _=monitor.root.lineage_changed(monitor.root_revision,&monitor.lineage)=>return,
                        _=monitor.cancellation.cancelled(),if !succeeded=>{if *terminal.borrow()!=Some(crate::runtime::execution::ExecutionStatus::Succeeded){return;}},
                        result=terminal.changed(),if !succeeded=>{if result.is_err(){return;}}
                    }
                }
            };
            tokio::select! {biased;
                _=cancelled=>{},
                _=adapter.propose_future_grant(proposal,adoption)=>{},
            }
        });
    }
}
/// Reservation markers are tested under every policy writer's ordinary lock.
/// Reserve every lineage member before awaiting storage, without holding locks
/// across await. Competing transactions fail stale and release only their markers.
struct Reservation {
    owners: Vec<Arc<PermissionController>>,
    quarantined: bool,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if self.quarantined {
            for owner in &self.owners {
                owner.grant_faulted.store(true, Ordering::Release);
                owner.changed.send_replace(());
            }
        }
        if !self.quarantined {
            for owner in &self.owners {
                owner.grant_reserved.store(false, Ordering::Release);
            }
        }
    }
}
impl StagedGrant {
    fn reserve(&self) -> Result<Reservation, PermissionError> {
        let mut reservation = Reservation {
            owners: Vec::new(),
            quarantined: false,
        };
        let mut chain = vec![(self.root.clone(), self.root_revision)];
        chain.extend(
            self.lineage
                .iter()
                .map(|(owner, snapshot)| (owner.clone(), snapshot.revision)),
        );
        for (owner, revision) in chain {
            {
                let policy = owner
                    .policy
                    .write()
                    .map_err(|_| PermissionError::Unavailable)?;
                if policy.revision != revision || owner.grant_reserved.swap(true, Ordering::AcqRel)
                {
                    return Err(PermissionError::StalePolicy);
                }
            }
            reservation.owners.push(owner);
        }
        self.root
            .ensure_witness(self.root_revision, &self.lineage)?;
        Ok(reservation)
    }
}
#[async_trait]
impl FutureGrantAdoption for StagedGrant {
    async fn adopt(&self) -> Result<PolicySnapshot, PermissionError> {
        let owned = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = owned.adopt_owned().await;
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            PermissionError::PersistenceUncertain(
                "grant adoption worker stopped before acknowledgement".into(),
            )
        })?
    }
}
impl StagedGrant {
    async fn adopt_owned(&self) -> Result<PolicySnapshot, PermissionError> {
        if self.cancelled() {
            return Err(PermissionError::Cancelled);
        }
        self.root
            .ensure_witness(self.root_revision, &self.lineage)?;
        let persistence = self
            .owner
            .grant_persistence
            .read()
            .map_err(|_| PermissionError::Unavailable)?
            .clone()
            .ok_or(PermissionError::Unavailable)?;
        if !self
            .owner
            .tools
            .read()
            .map_err(|_| PermissionError::Unavailable)?
            .iter()
            .any(|(tool, _)| tool == &self.request.tool)
        {
            return Err(PermissionError::InvalidConfiguration(
                "future grant tool is no longer registered in the owning Session".into(),
            ));
        }
        let session_id = self.owner.owning_session_id()?;
        let mut next = self.expected.clone();
        next.revision = next.revision.checked_add(1).ok_or_else(|| {
            PermissionError::InvalidConfiguration("policy revision exhausted".into())
        })?;
        next.config.overrides.push(Permission {
            agent: Some(self.expected.config.agent.clone()),
            mode: Some(self.expected.config.mode.clone()),
            tool: Some(Matcher::Exact(self.request.tool.clone())),
            args: self.grant.args.clone(),
            action: Action::Allow,
        });
        validate(&next.config).map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?;
        validate_review_result(
            &ReviewResult {
                action: Action::Allow,
                reason: String::new(),
                future_grant: Some(self.grant.clone()),
            },
            &self.request,
        )
        .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))?;
        let mut reservation = self.reserve()?;
        if self.owner.snapshot()? != self.expected {
            return Err(PermissionError::StalePolicy);
        }
        reservation.quarantined = true;
        if let Err(error) = persistence
            .persist(&session_id, self.owner.identity, &self.expected, &next)
            .await
        {
            reservation.quarantined = matches!(error, PermissionError::PersistenceUncertain(_));
            return Err(error);
        }
        // All lineage revisions remain reserved through publication. Durability
        // has succeeded; finish publication even if the confirming socket closes.
        self.root
            .ensure_witness(self.root_revision, &self.lineage)?;
        let mut current = self
            .owner
            .policy
            .write()
            .map_err(|_| PermissionError::Unavailable)?;
        if *current != self.expected {
            return Err(PermissionError::StalePolicy);
        }
        *current = next.clone();
        self.owner.changed.send_replace(());
        reservation.quarantined = false;
        Ok(next)
    }
}
