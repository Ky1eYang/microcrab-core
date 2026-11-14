//! Shared retention arithmetic. Each resource owner selects its own capacity;
//! eligibility and acknowledged cleanup remain duties of the owning registry.
use crate::Limits;

#[derive(Clone, Copy)]
pub(crate) struct RetentionPolicy {
    retained: usize,
    admission_reserve: usize,
}
impl RetentionPolicy {
    pub(crate) fn executions(limits: &Limits) -> Self {
        Self {
            retained: limits.max_retained_executions,
            admission_reserve: 1,
        }
    }
    pub(crate) fn processes(limits: &Limits) -> Self {
        Self {
            retained: limits.max_retained_processes,
            admission_reserve: 0,
        }
    }
    pub(crate) fn evict_count(self, eligible: usize) -> usize {
        eligible.saturating_sub(self.retained.saturating_sub(self.admission_reserve))
    }
    pub(crate) fn execution_history_capacity(self, limits: &Limits) -> usize {
        limits
            .max_pending_executions
            .saturating_add(self.retained)
            .saturating_add(limits.max_execution_buffers)
    }
}
