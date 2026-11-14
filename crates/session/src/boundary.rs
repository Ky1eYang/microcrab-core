//! Closed cross-trunk refresh vocabulary. Identifies projection timing, not plugin
//! delivery. State owns presentation labels; observers own evidence coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Boundary {
    SessionCreated,
    SessionRestored,
    EpochStarted,
    EpochEnded,
    TurnStarted,
    TurnEnded,
    RoundStarted,
    RoundSuspended,
    RoundResumed,
    RoundEnded,
    SessionTerminated,
}

/// Local request/cache consequences; this makes no remote cache-hit guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportImpact {
    CacheInvalidation,
    PrefixMutation,
    StatePatch,
    NonInteractive,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOrigin {
    UserInitiated,
    RuntimeDriven,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyPoint {
    Immediate,
    NextBoundary,
    SettledRoundBoundary,
    NextTurnAdmission,
    IdleRequiredAtApi,
}
/// Closed descriptor shared by the configuration owner and Session admission.
/// It carries no mutable settings or persistence authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigurationEffect {
    pub impact: TransportImpact,
    pub origin: ChangeOrigin,
    pub apply_point: ApplyPoint,
}
impl ConfigurationEffect {
    pub const MODEL_SELECTION: Self = Self {
        impact: TransportImpact::CacheInvalidation,
        origin: ChangeOrigin::UserInitiated,
        apply_point: ApplyPoint::NextTurnAdmission,
    };
}
