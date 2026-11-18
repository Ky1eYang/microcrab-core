//! One bounded, in-memory insertion owner. Claim snapshots never remove bytes;
//! only the Context append owner acknowledges them after a successful transaction.
use crate::runtime::execution::{ExecutionId, ExecutionStatus, ToolFailure};
use crate::{Error, SessionInput};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::hash::BuildHasher;
use std::sync::Mutex;

/// Read-only user insertion facts from the shared inbox, excluding framework notices.
#[derive(Clone, Debug, Serialize)]
pub struct InboxSnapshot {
    pub inbox_id: String,
    pub items: Vec<PendingInput>,
    pub active: bool,
    /// Runtime-only foreground execution timing, not queue admission time.
    #[serde(skip)]
    pub execution_started_at_ms: Option<u64>,
    #[serde(skip)]
    pub execution_ended_at_ms: Option<u64>,
    pub paused: bool,
    pub max_items: usize,
    pub max_bytes: usize,
    pub used_bytes: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct PendingInput {
    pub id: u64,
    pub revision: u64,
    pub claimed: bool,
    pub mode: PendingInputMode,
    pub input: SessionInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submission_operation_id: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingInputMode {
    Queue,
    Steer,
}

/// Typed CAS/admission failures. Details never contain input or private bodies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PendingInputError {
    #[error("Session inbox incarnation changed")]
    InboxChanged,
    #[error("Pending input no longer exists")]
    NotFound,
    #[error("Pending input revision changed")]
    RevisionConflict,
    #[error("Pending input is claimed for admission")]
    Claimed,
    #[error("Only the oldest pending user input can be resumed")]
    NotFirst,
    #[error("Resume or retry the retained continuation before pending input")]
    Continuation,
}

const USER_ITEMS: usize = 128;
const USER_BYTES: usize = 1024 * 1024;
const NOTICE_ITEMS: usize = 128;
const NOTICE_BYTES: usize = 128 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lane {
    NextTurn,
    NextStep,
    Observe,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StateDelivery {
    Wake,
    Continue,
    Passive,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NoticeKind {
    Terminal,
    Lifecycle,
    Metadata,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NoticeKey {
    Execution {
        session_id: String,
        execution_id: ExecutionId,
        change: NoticeKind,
    },
}
/// Facts deliberately exclude output and interaction bodies. Producers are wired
/// only after the execution/delivery architecture audit, not by observing rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NoticeFact {
    pub status: ExecutionStatus,
    pub failure: Option<ToolFailure>,
    pub exit_code: Option<i32>,
}
#[derive(Clone, Debug, Serialize)]
pub struct StateNotice {
    pub delivery: StateDelivery,
    pub key: NoticeKey,
    pub fact: NoticeFact,
}
#[derive(Clone)]
pub(crate) struct UserEntry {
    pub id: u64,
    pub revision: u64,
    pub claimed: bool,
    pub lane: Lane,
    pub input: SessionInput,
    pub submission_operation_id: Option<String>,
    bytes: usize,
}
#[derive(Clone)]
pub(crate) struct NoticeEntry {
    pub id: u64,
    pub ordinal: u64,
    pub lane: Lane,
    pub notice: StateNotice,
    bytes: usize,
}
fn execution_now_ms() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}

#[cfg(test)]
mod timing_tests {
    use super::*;

    #[test]
    fn entry_capacity_rejection_preserves_retention_and_ordinal() {
        let mut inbox = InboxState::default();
        for _ in 0..USER_ITEMS {
            inbox.enqueue("pending".into(), false).unwrap();
        }
        let before = serde_json::to_value(inbox.snapshot()).unwrap();
        let error = inbox.enqueue("excess".into(), false).unwrap_err();
        let Error::Limit(limit) = error else {
            panic!("expected entry capacity")
        };
        assert_eq!(limit.kind, crate::LimitKind::InboxEntries);
        assert!(!limit.is_pre_effect());
        assert_eq!(serde_json::to_value(inbox.snapshot()).unwrap(), before);
        let first = inbox.snapshot().items[0].clone();
        inbox.remove("", first.id, first.revision).unwrap();
        inbox.enqueue("replacement".into(), false).unwrap();
        assert_eq!(
            inbox.snapshot().items.last().unwrap().id,
            USER_ITEMS as u64 + 1
        );
    }

    #[test]
    fn submission_identity_survives_edit_conversion_claim_and_removal() {
        let mut inbox = InboxState::default();
        inbox
            .enqueue_submission("same".into(), false, Some("first".into()))
            .unwrap();
        inbox
            .enqueue_submission("same".into(), false, Some("second".into()))
            .unwrap();
        let first = inbox.snapshot().items[0].clone();
        let changed = inbox
            .update(
                "",
                first.id,
                first.revision,
                Some("transformed".into()),
                Some(PendingInputMode::Steer),
            )
            .unwrap();
        assert_eq!(changed.submission_operation_id.as_deref(), Some("first"));
        assert_eq!(
            inbox.snapshot().items[1].submission_operation_id.as_deref(),
            Some("second")
        );
        let claim = inbox
            .claim_pending_resume("", changed.id, changed.revision)
            .unwrap();
        assert_eq!(claim.submission_operation_id.as_deref(), Some("first"));
        assert!(inbox.remove("", claim.id, claim.revision).is_err());
        inbox.release_claims(&[claim.id]);
        let removed = inbox.remove("", claim.id, claim.revision).unwrap();
        assert_eq!(removed.submission_operation_id.as_deref(), Some("first"));
        assert_eq!(
            inbox.snapshot().items[0].submission_operation_id.as_deref(),
            Some("second")
        );
    }

    #[test]
    fn execution_timing_is_owned_by_actual_foreground_transitions() {
        let mut inbox = InboxState::default();
        let initial = inbox.snapshot();
        assert!(!initial.active);
        assert!(initial.execution_started_at_ms.is_none());
        assert!(initial.execution_ended_at_ms.is_none());
        inbox.end_turn(true);
        assert!(inbox.snapshot().execution_ended_at_ms.is_none());
        inbox.begin_turn();
        assert!(inbox.snapshot().execution_started_at_ms.is_some());
        // Repeated observations/steering must not reset a running clock.
        inbox.execution_started_at_ms = Some(42);
        inbox.begin_turn();
        assert_eq!(inbox.snapshot().execution_started_at_ms, Some(42));
        inbox.end_turn(true);
        let stopped = inbox.snapshot();
        assert!(!stopped.active);
        assert!(stopped.paused);
        assert!(stopped.execution_ended_at_ms.is_some());
        inbox.end_turn(false);
        assert_eq!(
            inbox.snapshot().execution_ended_at_ms,
            stopped.execution_ended_at_ms
        );
        inbox.begin_turn();
        assert!(inbox.snapshot().execution_ended_at_ms.is_none());
        assert_ne!(inbox.snapshot().execution_started_at_ms, Some(42));
    }
}

#[derive(Default)]
pub(crate) struct InboxState {
    inbox_id: String,
    pub active: bool,
    execution_started_at_ms: Option<u64>,
    execution_ended_at_ms: Option<u64>,
    pub paused: bool,
    pub retired: bool,
    pub retiring: bool,
    next_id: u64,
    carriers: Vec<crate::state::communication::inbox::AcceptedCarrier>,
    communication_enabled: bool,
    users: VecDeque<UserEntry>,
    notices: Vec<NoticeEntry>,
    user_bytes: usize,
    notice_bytes: usize,
}
pub(crate) struct Inbox {
    pub state: Mutex<InboxState>,
    pub changed: tokio::sync::Notify,
    // Separate from the dispatcher: observers must not consume its wake permit.
    pub steering_changed: tokio::sync::Notify,
    pub notices_changed: tokio::sync::broadcast::Sender<StateNotice>,
}
impl Inbox {
    /// Observe retained unclaimed steering input, including arrivals before the
    /// observer starts. Register before inspecting state to avoid a lost wake.
    pub async fn wait_for_steer(&self) {
        loop {
            let changed = self.steering_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                if state.active
                    && state
                        .users
                        .iter()
                        .any(|entry| entry.lane == Lane::NextStep && !entry.claimed)
                {
                    return;
                }
            }
            changed.await;
        }
    }
}
impl Default for Inbox {
    fn default() -> Self {
        Self {
            state: Mutex::new(InboxState {
                inbox_id: format!(
                    "{:016x}{:016x}",
                    std::collections::hash_map::RandomState::new().hash_one("inbox-instance-a"),
                    std::collections::hash_map::RandomState::new().hash_one("inbox-instance-b")
                ),
                ..Default::default()
            }),
            steering_changed: tokio::sync::Notify::new(),
            changed: tokio::sync::Notify::new(),
            notices_changed: tokio::sync::broadcast::channel(128).0,
        }
    }
}
#[derive(Clone)]
pub(crate) enum TurnEntry {
    User(UserEntry),
    Framework(NoticeEntry),
    Carriers(Vec<IdleCarrier>),
}
#[derive(Clone)]
pub(crate) enum IdleCarrier {
    Framework(NoticeEntry),
    Accepted(crate::state::communication::inbox::AcceptedCarrier),
}
impl IdleCarrier {
    pub fn id(&self) -> u64 {
        match self {
            Self::Framework(entry) => entry.ordinal,
            Self::Accepted(entry) => entry.ordinal,
        }
    }
    pub fn input(&self) -> Result<(SessionInput, microcrab_transport::UserSource), Error> {
        match self {
            Self::Framework(entry) => Ok((
                TurnEntry::Framework(entry.clone()).input()?,
                microcrab_transport::UserSource::Framework,
            )),
            Self::Accepted(entry) => {
                Ok((SessionInput::text(entry.body.clone()), entry.source.clone()))
            }
        }
    }
}
impl TurnEntry {
    pub fn input(&self) -> Result<SessionInput, Error> {
        match self {
            Self::User(entry) => Ok(entry.input.clone()),
            Self::Carriers(entries) => entries
                .first()
                .ok_or_else(|| Error::config("empty carrier run"))
                .and_then(|entry| entry.input().map(|(input, _)| input)),
            Self::Framework(entry) => Ok(SessionInput::text(format!(
                "Framework execution notice. Inspect output with job when needed.\n{}",
                serde_json::to_string(&(&entry.notice.key, &entry.notice.fact))?
            ))),
        }
    }
    pub fn id(&self) -> u64 {
        match self {
            Self::User(entry) => entry.id,
            Self::Carriers(entries) => entries[0].id(),
            Self::Framework(entry) => entry.id,
        }
    }
    pub fn framework(&self) -> bool {
        matches!(self, Self::Framework(_) | Self::Carriers(_))
    }
}
#[derive(Default)]
pub(crate) struct StepSnapshot {
    pub users: Vec<UserEntry>,
    pub notices: Vec<NoticeEntry>,
}
impl InboxState {
    pub fn snapshot(&self) -> InboxSnapshot {
        InboxSnapshot {
            inbox_id: self.inbox_id.clone(),
            items: self
                .users
                .iter()
                .map(|entry| PendingInput {
                    id: entry.id,
                    revision: entry.revision,
                    claimed: entry.claimed,
                    mode: if entry.lane == Lane::NextStep {
                        PendingInputMode::Steer
                    } else {
                        PendingInputMode::Queue
                    },
                    input: entry.input.clone(),
                    submission_operation_id: entry.submission_operation_id.clone(),
                })
                .collect(),
            active: self.active,
            execution_started_at_ms: self.execution_started_at_ms,
            execution_ended_at_ms: self.execution_ended_at_ms,
            paused: self.paused,
            max_items: USER_ITEMS,
            max_bytes: USER_BYTES,
            used_bytes: self.user_bytes,
        }
    }
    pub(crate) fn id(&mut self) -> Result<u64, Error> {
        self.next_id = self.next_id.checked_add(1).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "inbox ordinal overflow")
        })?;
        Ok(self.next_id)
    }
    pub fn enqueue(&mut self, input: SessionInput, steer: bool) -> Result<(), Error> {
        self.enqueue_submission(input, steer, None)
    }
    pub fn submission_operation_id(&self, id: u64) -> Option<String> {
        self.users
            .iter()
            .find(|entry| entry.id == id)
            .and_then(|entry| entry.submission_operation_id.clone())
    }
    pub fn enqueue_submission(
        &mut self,
        input: SessionInput,
        steer: bool,
        submission_operation_id: Option<String>,
    ) -> Result<(), Error> {
        if self.retired || self.retiring {
            return Err(Error::config("Session conversation is retired or retiring"));
        }
        crate::admission::variables::validate_change(&input.variables)?;
        // A bounded writer rejects oversized encoded input before allocation.
        let bytes = super::input::retained_size(&input, USER_BYTES)?;
        crate::budget::check(
            crate::LimitKind::InboxEntries,
            self.users.len().saturating_add(1),
            USER_ITEMS,
        )?;
        super::Admission::check(
            super::AdmissionSource::Inbox,
            crate::LimitKind::InboxBytes,
            bytes,
            USER_BYTES.saturating_sub(self.user_bytes),
        )?;
        let id = self.id()?;
        self.users.push_back(UserEntry {
            id,
            revision: 0,
            claimed: false,
            lane: if steer && self.active {
                Lane::NextStep
            } else {
                Lane::NextTurn
            },
            input,
            submission_operation_id,
            bytes,
        });
        self.user_bytes += bytes;
        Ok(())
    }
    fn pending_index(&self, inbox_id: &str, id: u64, revision: u64) -> Result<usize, Error> {
        if self.inbox_id != inbox_id {
            return Err(PendingInputError::InboxChanged.into());
        }
        let index = self
            .users
            .iter()
            .position(|entry| entry.id == id)
            .ok_or(PendingInputError::NotFound)?;
        let entry = &self.users[index];
        if entry.claimed {
            return Err(PendingInputError::Claimed.into());
        }
        if entry.revision != revision {
            return Err(PendingInputError::RevisionConflict.into());
        }
        if self.retired || self.retiring {
            return Err(Error::Closed);
        }
        Ok(index)
    }
    /// Every successful write intent increments revision, including no-ops.
    /// Validation and budget arithmetic precede the sole atomic replacement.
    pub fn update(
        &mut self,
        inbox_id: &str,
        id: u64,
        revision: u64,
        input: Option<SessionInput>,
        mode: Option<PendingInputMode>,
    ) -> Result<PendingInput, Error> {
        let index = self.pending_index(inbox_id, id, revision)?;
        if input.is_none() && mode.is_none() {
            return Err(Error::config("empty inbox update"));
        }
        let old = &self.users[index];
        let next_revision = old.revision.checked_add(1).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "inbox revision overflow")
        })?;
        let input = input.unwrap_or_else(|| old.input.clone());
        super::variables::validate_change(&input.variables)?;
        let bytes = super::input::retained_size(&input, USER_BYTES)?;
        let retained = self.user_bytes - old.bytes;
        super::Admission::check(
            super::AdmissionSource::Inbox,
            crate::LimitKind::InboxBytes,
            bytes,
            USER_BYTES.saturating_sub(retained),
        )?;
        let lane = match mode {
            Some(PendingInputMode::Steer) if self.active => Lane::NextStep,
            Some(_) => Lane::NextTurn,
            None => old.lane,
        };
        let submission_operation_id = old.submission_operation_id.clone();
        self.users[index] = UserEntry {
            id,
            revision: next_revision,
            claimed: false,
            lane,
            input: input.clone(),
            submission_operation_id: submission_operation_id.clone(),
            bytes,
        };
        self.user_bytes = retained + bytes;
        Ok(PendingInput {
            id,
            revision: next_revision,
            claimed: false,
            input,
            submission_operation_id,
            mode: if lane == Lane::NextStep {
                PendingInputMode::Steer
            } else {
                PendingInputMode::Queue
            },
        })
    }
    pub fn remove(
        &mut self,
        inbox_id: &str,
        id: u64,
        revision: u64,
    ) -> Result<PendingInput, Error> {
        let index = self.pending_index(inbox_id, id, revision)?;
        let entry = self
            .users
            .remove(index)
            .expect("validated pending input index");
        self.user_bytes -= entry.bytes;
        Ok(PendingInput {
            id: entry.id,
            revision: entry.revision,
            claimed: false,
            mode: if entry.lane == Lane::NextStep {
                PendingInputMode::Steer
            } else {
                PendingInputMode::Queue
            },
            input: entry.input,
            submission_operation_id: entry.submission_operation_id,
        })
    }
    /// Explicit user admission resumes only the witnessed FIFO head. Failed CAS
    /// leaves both retention and deliberate pause unchanged.
    pub fn claim_pending_resume(
        &mut self,
        inbox_id: &str,
        id: u64,
        revision: u64,
    ) -> Result<UserEntry, Error> {
        let index = self.pending_index(inbox_id, id, revision)?;
        if index != 0 {
            return Err(PendingInputError::NotFirst.into());
        }
        if self.active {
            return Err(Error::Busy);
        }
        self.users[index].claimed = true;
        Ok(self.users[index].clone())
    }
    pub fn release_claims(&mut self, ids: &[u64]) {
        for entry in &mut self.users {
            if ids.contains(&entry.id) {
                entry.claimed = false;
            }
        }
    }
    pub fn post(&mut self, mut notice: StateNotice) -> Result<(), Error> {
        let key_valid = match &notice.key {
            NoticeKey::Execution { session_id, .. } => {
                !session_id.is_empty() && session_id.len() <= 256
            }
        };
        if !key_valid {
            return Err(Error::config("invalid framework notice key"));
        }
        if let Some(failure) = &notice.fact.failure {
            failure.validate()?;
        }
        let bytes = super::Admission::serialized(
            super::AdmissionSource::StateDelivery,
            crate::LimitKind::StateDeliveryBytes,
            &(&notice.key, &notice.fact),
            NOTICE_BYTES,
        )?
        .measurement
        .retained_bytes
        .expect("StateDelivery serialization measures retained storage");
        let old = self
            .notices
            .iter()
            .position(|entry| entry.notice.key == notice.key);
        let previous = old.map(|index| self.notices[index].bytes).unwrap_or(0);
        crate::budget::check(
            crate::LimitKind::InboxEntries,
            self.notices
                .len()
                .saturating_add(usize::from(old.is_none())),
            NOTICE_ITEMS,
        )?;
        super::Admission::check(
            super::AdmissionSource::StateDelivery,
            crate::LimitKind::StateDeliveryBytes,
            bytes,
            NOTICE_BYTES.saturating_sub(self.notice_bytes - previous),
        )?;
        let id = self.id()?;
        let ordinal = old.map(|index| self.notices[index].ordinal).unwrap_or(id);
        // Idle Wake is NextTurn. During a live turn it shares the settled
        // Continue boundary, never a user message inside a pending tool batch.
        let mut lane = match (self.active, notice.delivery) {
            (true, StateDelivery::Wake | StateDelivery::Continue) => Lane::NextStep,
            (false, StateDelivery::Wake) => Lane::NextTurn,
            _ => Lane::Observe,
        };
        if let Some(index) = old {
            let previous = &self.notices[index];
            let strength = |delivery| match delivery {
                StateDelivery::Passive => 0,
                StateDelivery::Continue => 1,
                StateDelivery::Wake => 2,
            };
            // Coalescing replaces the fact and acknowledgement identity, not an
            // outstanding admission request. Observe entries have already been
            // downgraded (or arrived idle); never revive their old delivery.
            if previous.lane != Lane::Observe
                && strength(previous.notice.delivery) > strength(notice.delivery)
            {
                notice.delivery = previous.notice.delivery;
                lane = previous.lane;
            }
        }
        let entry = NoticeEntry {
            id,
            ordinal,
            lane,
            notice,
            bytes,
        };
        if let Some(index) = old {
            self.notices[index] = entry;
        } else {
            self.notices.push(entry);
        }
        self.notice_bytes = self.notice_bytes - previous + bytes;
        Ok(())
    }
    pub fn step(&mut self, allow_framework: bool, sending: bool) -> StepSnapshot {
        let sending = sending
            || self.users.iter().any(|entry| entry.lane == Lane::NextStep)
            || (allow_framework
                && self
                    .notices
                    .iter()
                    .any(|entry| entry.lane == Lane::NextStep));
        let snapshot = StepSnapshot {
            users: self
                .users
                .iter()
                .filter(|entry| entry.lane == Lane::NextStep && !entry.claimed)
                .cloned()
                .collect(),
            notices: self
                .notices
                .iter()
                .filter(|entry| {
                    (allow_framework && entry.lane == Lane::NextStep)
                        || (sending && (entry.lane == Lane::Observe || !allow_framework))
                })
                .cloned()
                .collect(),
        };
        for entry in &mut self.users {
            if snapshot
                .users
                .iter()
                .any(|selected| selected.id == entry.id)
            {
                entry.claimed = true;
            }
        }
        snapshot
    }
    /// Retirement transfers only unclaimed framework facts. Success means a
    /// subscribed observer accepted the bounded event; absent observers retain
    /// the facts for the next subscription rather than silently discarding them.
    pub fn handoff_notices(&mut self, sender: &tokio::sync::broadcast::Sender<StateNotice>) {
        let ids = self
            .notices
            .iter()
            .filter_map(|entry| sender.send(entry.notice.clone()).ok().map(|_| entry.id))
            .collect::<Vec<_>>();
        self.acknowledge(&[], &ids);
    }
    pub fn deliver_notice(
        &mut self,
        notice: StateNotice,
        sender: &tokio::sync::broadcast::Sender<StateNotice>,
    ) -> Result<(), Error> {
        if !self.retired {
            return self.post(notice);
        }
        if sender.send(notice.clone()).is_err() {
            self.post(notice)?;
        }
        Ok(())
    }
    pub fn pending_inputs(&self) -> Vec<SessionInput> {
        self.users.iter().map(|entry| entry.input.clone()).collect()
    }
    pub fn ready(&self) -> bool {
        !self.users.is_empty()
            || !self.carriers.is_empty()
            || self.notices.iter().any(|entry| entry.lane != Lane::Observe)
    }
    pub fn has_next_turn(&self) -> bool {
        !self.paused
            && !self.active
            && !self.retired
            && !self.retiring
            && (!self.users.is_empty()
                || !self.carriers.is_empty()
                || self
                    .notices
                    .iter()
                    .any(|entry| entry.lane == Lane::NextTurn))
    }
    pub fn next_turn(&mut self) -> Option<TurnEntry> {
        if !self.has_next_turn() {
            return None;
        }
        if self.communication_enabled {
            let user_id = self.users.front().map(|entry| entry.id).unwrap_or(u64::MAX);
            let mut run = self
                .carriers
                .iter()
                .filter(|entry| entry.ordinal < user_id)
                .cloned()
                .map(IdleCarrier::Accepted)
                .chain(
                    self.notices
                        .iter()
                        .filter(|entry| entry.lane == Lane::NextTurn && entry.ordinal < user_id)
                        .cloned()
                        .map(IdleCarrier::Framework),
                )
                .collect::<Vec<_>>();
            run.sort_by_key(IdleCarrier::id);
            run.truncate(32);
            if !run.is_empty() {
                return Some(TurnEntry::Carriers(run));
            }
        }
        let user = self.users.front().filter(|entry| !entry.claimed);
        let notice = self
            .notices
            .iter()
            .filter(|entry| entry.lane == Lane::NextTurn)
            .min_by_key(|entry| entry.ordinal);
        let selected = match (user, notice) {
            (Some(user), Some(notice)) if notice.ordinal < user.id => {
                Some(TurnEntry::Framework(notice.clone()))
            }
            (Some(user), _) => Some(TurnEntry::User(user.clone())),
            (None, Some(notice)) => Some(TurnEntry::Framework(notice.clone())),
            _ => None,
        };
        if let Some(TurnEntry::User(selected)) = &selected {
            self.users
                .iter_mut()
                .find(|entry| entry.id == selected.id)
                .expect("selected pending entry")
                .claimed = true;
        }
        selected
    }
    pub(crate) fn install_carriers(
        &mut self,
        pending: Vec<crate::state::communication::inbox::AcceptedCarrier>,
        next_ordinal: u64,
    ) {
        self.communication_enabled = true;
        self.next_id = self.next_id.max(next_ordinal);
        self.carriers = pending;
    }
    pub fn acknowledge(&mut self, users: &[u64], notices: &[u64]) {
        self.users.retain(|entry| {
            let keep = !users.contains(&entry.id);
            if !keep {
                self.user_bytes -= entry.bytes;
            }
            keep
        });
        self.notices.retain(|entry| {
            let keep = !notices.contains(&entry.id);
            if !keep {
                self.notice_bytes -= entry.bytes;
            }
            keep
        });
    }
    pub fn activate_notices(&mut self) {
        for entry in &mut self.notices {
            if entry.lane == Lane::NextTurn {
                entry.lane = Lane::NextStep;
            }
        }
    }
    pub fn begin_turn(&mut self) {
        if !self.active {
            self.execution_started_at_ms = execution_now_ms();
            self.execution_ended_at_ms = None;
        }
        self.active = true;
    }
    pub fn end_turn(&mut self, pause: bool) {
        if self.active {
            self.execution_ended_at_ms = execution_now_ms();
        }
        self.active = false;
        self.paused |= pause;
        for entry in &mut self.users {
            if entry.lane != Lane::NextTurn {
                entry.lane = Lane::NextTurn;
                // At the ordinal ceiling subsequent CAS writes reject overflow.
                entry.revision = entry.revision.saturating_add(1);
            }
        }
        for entry in &mut self.notices {
            if entry.lane == Lane::NextStep {
                entry.lane = if entry.notice.delivery == StateDelivery::Wake {
                    Lane::NextTurn
                } else {
                    Lane::Observe
                };
            }
        }
    }
    pub fn downgrade_framework(&mut self) {
        for entry in &mut self.notices {
            entry.lane = Lane::Observe;
        }
    }
}

#[cfg(test)]
mod carrier_order_tests {
    use super::*;
    fn carrier(id: u64) -> crate::state::communication::inbox::AcceptedCarrier {
        crate::state::communication::inbox::AcceptedCarrier {
            operation_id: format!("operation{id}"),
            ordinal: id,
            body: format!("body{id}"),
            source: microcrab_transport::UserSource::External {
                identity: "speaker".into(),
            },
            correspondent: None,
            speaker: Some(crate::state::communication::ExternalSpeaker {
                identity: "speaker".into(),
            }),
        }
    }
    fn notice() -> StateNotice {
        StateNotice {
            delivery: StateDelivery::Wake,
            key: NoticeKey::Execution {
                session_id: "session".into(),
                execution_id: ExecutionId(1),
                change: NoticeKind::Metadata,
            },
            fact: NoticeFact {
                status: ExecutionStatus::Running,
                failure: None,
                exit_code: None,
            },
        }
    }
    #[test]
    fn repeated_carriers_and_coalesced_wake_keep_actual_first_ordinal_before_user() {
        let mut inbox = InboxState::default();
        inbox.install_carriers(vec![], 0);
        inbox.post(notice()).unwrap();
        let ordinal = inbox.id().unwrap();
        inbox.install_carriers(vec![carrier(ordinal)], ordinal);
        inbox.post(notice()).unwrap();
        inbox.enqueue("direct user".into(), false).unwrap();
        let late = inbox.id().unwrap();
        inbox.install_carriers(vec![carrier(ordinal), carrier(late)], late);
        let TurnEntry::Carriers(run) = inbox.next_turn().unwrap() else {
            panic!("leading carrier run")
        };
        assert_eq!(
            run.iter().map(IdleCarrier::id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(matches!(&run[0], IdleCarrier::Framework(_)));
        let ack = run
            .iter()
            .filter_map(|entry| match entry {
                IdleCarrier::Framework(entry) => Some(entry.id),
                _ => None,
            })
            .collect::<Vec<_>>();
        inbox.acknowledge(&[], &ack);
        inbox.install_carriers(vec![carrier(late)], late);
        assert!(matches!(inbox.next_turn().unwrap(), TurnEntry::User(_)));
    }
    #[test]
    fn busy_and_continue_or_observe_never_enter_idle_carrier_run() {
        let mut inbox = InboxState::default();
        inbox.install_carriers(vec![carrier(1)], 1);
        inbox.begin_turn();
        let mut continuing = notice();
        continuing.delivery = StateDelivery::Continue;
        inbox.post(continuing).unwrap();
        assert!(inbox.next_turn().is_none());
        assert_eq!(inbox.step(true, false).notices.len(), 1);
        inbox.end_turn(false);
        let TurnEntry::Carriers(run) = inbox.next_turn().unwrap() else {
            panic!("agent/external alone")
        };
        assert_eq!(run.len(), 1);
        assert!(matches!(&run[0], IdleCarrier::Accepted(_)));
        inbox.paused = true;
        assert!(inbox.next_turn().is_none());
    }
}
