//! Small object-safe auditors shared by integration fixtures; no ACK channel protocol.
#![allow(dead_code)]
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, HookPayload, Injection, Moment,
};
use serde_json::Value;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Seen {
    pub moment: Moment,
    pub epoch: u64,
    pub subagent: Option<String>,
    pub turn: Option<u64>,
    pub round: Option<u64>,
    pub call_id: Option<String>,
    pub payload: HookPayload,
    pub session: Option<Value>,
    pub time: Option<Value>,
}

#[derive(Default)]
pub struct Recording {
    pub seen: Mutex<Vec<Seen>>,
    pub published: Mutex<Vec<(Moment, AuditBody)>>,
}
impl Recording {
    pub fn moments(&self) -> Vec<Moment> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|seen| seen.moment)
            .collect()
    }
    pub fn clear(&self) {
        self.seen.lock().unwrap().clear();
        self.published.lock().unwrap().clear();
    }
}
impl Auditor for Recording {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        self.published.lock().unwrap().push((moment, body));
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        ctx: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        self.seen.lock().unwrap().push(Seen {
            moment,
            epoch: ctx.epoch,
            subagent: ctx.subagent,
            turn: ctx.turn,
            round: ctx.round,
            call_id: ctx.call_id,
            payload: ctx.payload,
            session: ctx.variables.get("session"),
            time: ctx.variables.get("time"),
        });
        Box::pin(async { Ok(Injection::default()) })
    }
}
