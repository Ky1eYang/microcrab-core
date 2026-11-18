//! Invocation-scoped construction and explicit file evidence adapters.
use super::*;
impl ToolContext {
    #[expect(
        clippy::too_many_arguments,
        reason = "Embedding callers construct invocation contexts with explicit identity and ownership fields; grouping them changes the public constructor"
    )]
    pub fn new(
        state: crate::State,
        processes: Arc<dyn crate::runtime::execution::process::ProcessControl>,
        cancellation: CancellationToken,
        call_id: String,
        tool: String,
        session_id: String,
        agent: String,
        mode: Option<String>,
    ) -> Self {
        let state = state.for_agent();
        let snapshot = state.snapshot();
        let cache_scope = state.cache_points().scope();
        let redaction = super::super::execution::InvocationRedaction::default();
        let processes = processes
            .for_invocation(redaction.clone())
            .unwrap_or(processes);
        let wakeup = processes.wakeup();
        Self {
            subject: crate::policy::InvocationSubject::Tool,
            cache_collector: state.cache_points(),
            state,
            processes,
            wakeup,
            cancellation,
            observation_interrupt: None,
            call_id,
            tool,
            session_id,
            agent,
            mode,
            turn_id: snapshot.turn_id,
            round_id: snapshot.round_id,
            admission: None,
            interaction_timeout_ms: snapshot.limits.interaction_timeout_ms,
            execution_id: None,
            execution_progress: None,
            resolved_arguments: None,
            literal_mask: super::injection::LiteralMask::default(),
            redaction,
            cache_scope,
        }
    }

    /// Build an isolated context for direct calls without a configured Host.
    pub fn detached(state: crate::SessionState, cancellation: CancellationToken) -> Self {
        let turn_id = state.turn_id;
        let round_id = state.round_id;
        let mode = state.selected_mode.clone();
        let processes: Arc<dyn crate::runtime::execution::process::ProcessControl> =
            crate::runtime::execution::process::ProcessRegistry::detached(state.limits.clone());
        let redaction = super::super::execution::InvocationRedaction::default();
        let processes = processes
            .for_invocation(redaction.clone())
            .unwrap_or(processes);
        let wakeup = processes.wakeup();
        let state = crate::State::from_snapshot(state).for_agent();
        // Direct embedding has an isolated synthetic round and no journal owner.
        // Session execution supplies its real counters through Runtime instead.
        state
            .cache_points()
            .begin(
                state.snapshot().epoch_id,
                turn_id.unwrap_or(0),
                round_id.unwrap_or(0),
            )
            .expect("a fresh detached collector has generation zero");
        let cache_scope = state.cache_points().scope();
        Self {
            subject: crate::policy::InvocationSubject::Tool,
            state: state.clone(),
            cache_collector: state.cache_points(),
            processes,
            wakeup,
            cancellation,
            observation_interrupt: None,
            call_id: String::new(),
            tool: String::new(),
            session_id: "detached".into(),
            agent: "detached".into(),
            mode,
            turn_id,
            round_id,
            interaction_timeout_ms: state.snapshot().limits.interaction_timeout_ms,
            execution_id: None,
            execution_progress: None,
            resolved_arguments: None,
            literal_mask: super::injection::LiteralMask::default(),
            redaction,
            admission: None,
            cache_scope,
        }
    }

    pub(crate) fn begin_initialization_cache(&mut self) -> Result<(), Error> {
        if self.subject.is_tool() {
            return Err(Error::lifecycle(
                "init cache requires lifecycle attribution",
            ));
        }
        if !self.cache_collector.is_init() {
            self.cache_collector = Arc::new(super::super::cache::Collector::default());
            self.cache_collector.begin_init()?;
            self.cache_scope = self.cache_collector.scope();
        }
        Ok(())
    }
    pub(crate) fn finish_initialization_cache(
        &self,
    ) -> Vec<super::super::journal::RecordedCachePoint> {
        self.cache_collector.finish_init()
    }

    /// Record an explicit file observation supplied by this tool. Native tools
    /// are trusted embedder code and must obtain truthful Host observations.
    /// Runtime checks path/hash/budgets, not filesystem provenance. WASM capture
    /// routes through the bound Host; the invocation generation rejects late writes.
    pub(crate) fn release_cache_reservation(&self) {
        self.cache_collector
            .release(self.cache_scope, &self.call_id);
    }
    pub(crate) fn reserve_cache_points(&self, points: usize, bytes: usize) -> Result<(), Error> {
        self.cache_collector
            .reserve(self.cache_scope, &self.call_id, points, bytes)
    }
    pub fn record_cache_point(
        &self,
        point: microcrab_host_api::FileCachePoint,
    ) -> Result<(), Error> {
        self.cache_collector
            .record(self.cache_scope, &self.call_id, &self.tool, point)
    }
}
