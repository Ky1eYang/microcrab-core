//! A small, embeddable, session-owned agent runtime.
//! Context is materialized into a provider-neutral AST before transport.
#![forbid(unsafe_code)]
pub mod admission;
mod boundary;
pub mod budget;
pub mod context;
mod error;
mod inner;
pub mod observation;
pub mod policy;
pub mod runtime;
mod session;
pub mod state;
pub mod utils;

pub use admission::inbox::{
    InboxSnapshot, NoticeFact, NoticeKey, NoticeKind, PendingInput, PendingInputError,
    PendingInputMode, StateDelivery, StateNotice,
};
pub use boundary::{ApplyPoint, ChangeOrigin, ConfigurationEffect, TransportImpact};
pub use budget::{LimitError, LimitKind, Limits};
pub use error::{Error, ErrorReason, ProviderTimeoutDetails};
pub use microcrab_auditor::{
    AuditBody, AuditOutcome, AuditRecord, Auditor, AuditorError, ChannelAuditor, HookContext,
    HookPayload, Injection, Moment, NoopAuditor, VariableView,
};
pub use microcrab_protocol as protocol;
pub use microcrab_protocol::{ImageSource, InputPart, SessionInput, VariablesChange};
pub use microcrab_transport::{Client, ModelEvent};
pub use observation::activity::{
    CompletedTurn, InboxFailure, InboxFailureKind, RoundLimitStop, SessionActivity,
};
pub use session::{
    DelegateRequest, DelegationRelationship, HostDetachCode, InterruptReason,
    RecoveredChildReservation, Session, SessionBuilder, SessionEvent, SessionKind, SessionStream,
    SettlementGuard, TurnRouting,
};
pub use state::{
    DynReference, ListedReference, Reference, ReferenceContent, ReferenceInfo, SessionState, State,
    VariableCommit, VariableDescriptor, VariableInspection, VariableWitness,
};

pub use microcrab_agent_types::{AgentInfo, AgentMode, AgentRole, HostInfo, OsFamily};
pub use runtime::execution;
pub use runtime::journal::{Journal, NavigationBoundary, Recover, RoundContinuation};
pub use runtime::tool::delegate::DelegateExecutor;

pub use inner::delegation::{
    ChildFactory, ChildSession, Children, Completion, DelegateUsage, OwnedDelegateExecutor,
};
