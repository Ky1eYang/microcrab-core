//! Explicit tool-selected file observations, isolated from model variables and Host globals.
pub use super::journal::{RecordedCachePoint, RoundCacheReport};
use crate::Error;
use microcrab_host_api::FileCachePoint;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Mutex};

const MAX_POINTS: usize = 128;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_ID_BYTES: usize = 4096;

impl RoundCacheReport {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        validate_points(&self.points)
    }
}
pub(crate) fn validate_points(points: &[RecordedCachePoint]) -> Result<(), Error> {
    crate::budget::check(crate::LimitKind::CachePoints, points.len(), MAX_POINTS)
        .map_err(|error| cache_label(error, "cache point count"))?;
    let mut bytes = 0usize;
    let mut sequences = HashMap::<&str, u32>::new();
    for point in points {
        let sequence = sequences.entry(&point.call_id).or_default();
        if point.sequence != *sequence {
            return Err(Error::config("cache point invocation sequence mismatch"));
        }
        *sequence += 1;
        bytes = bytes
            .checked_add(point_bytes(&point.call_id, &point.tool, &point.point)?)
            .ok_or_else(|| {
                crate::budget::reject(crate::LimitKind::CacheBytes, "cache byte budget")
            })?;
        crate::budget::check(crate::LimitKind::CacheBytes, bytes, MAX_BYTES)
            .map_err(|error| cache_label(error, "cache byte budget"))?;
    }
    Ok(())
}
fn point_bytes(call_id: &str, tool: &str, point: &FileCachePoint) -> Result<usize, Error> {
    crate::budget::check(
        crate::LimitKind::CacheBytes,
        call_id.len().max(tool.len()).max(point.path.len()),
        MAX_ID_BYTES,
    )
    .map_err(|error| cache_label(error, "cache point identity byte budget"))?;
    // Retained observations are data, not authority to reopen paths. Host imports
    // independently bind and admit the exact requested path before recording it.
    if !microcrab_host_api::is_normalized_cache_path(&point.path, true) {
        return Err(Error::config(
            "cache point path must be normalized relative or portable absolute",
        ));
    }
    match (&point.data, &point.version) {
        (None, None) => {}
        (Some(data), Some(version)) if *version == format!("{:x}", Sha256::digest(data)) => {}
        _ => return Err(Error::config("cache point data/version mismatch")),
    }
    point
        .path
        .len()
        .checked_add(call_id.len())
        .and_then(|bytes| bytes.checked_add(tool.len()))
        .and_then(|bytes| bytes.checked_add(point.version.as_ref().map_or(0, String::len)))
        .and_then(|bytes| bytes.checked_add(point.data.as_ref().map_or(0, Vec::len)))
        .ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::CacheBytes, "round cache byte budget")
        })
}

fn cache_label(error: Error, label: &str) -> Error {
    match error {
        Error::Limit(limit) => Error::Limit(limit.with_label(label)),
        other => other,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Scope(u64);
#[derive(Default)]
pub(crate) struct Collector {
    inner: Mutex<Pending>,
}
#[derive(Default)]
struct Pending {
    generation: u64,
    report: Option<Report>,
    bytes: usize,
    reservations: HashMap<String, (usize, usize)>,
}
enum Report {
    Round(RoundCacheReport),
    Init(Vec<RecordedCachePoint>),
}
impl Report {
    fn points(&self) -> &Vec<RecordedCachePoint> {
        match self {
            Self::Round(report) => &report.points,
            Self::Init(points) => points,
        }
    }
    fn points_mut(&mut self) -> &mut Vec<RecordedCachePoint> {
        match self {
            Self::Round(report) => &mut report.points,
            Self::Init(points) => points,
        }
    }
}
impl Collector {
    pub(crate) fn begin_init(&self) -> Result<(), Error> {
        let mut pending = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        pending.generation = pending.generation.checked_add(1).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "cache scope generation")
        })?;
        pending.report = Some(Report::Init(Vec::new()));
        pending.bytes = 0;
        pending.reservations.clear();
        Ok(())
    }
    pub(crate) fn is_init(&self) -> bool {
        matches!(
            &self
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .report,
            Some(Report::Init(_))
        )
    }
    pub(crate) fn finish_init(&self) -> Vec<RecordedCachePoint> {
        let mut pending = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        pending.bytes = 0;
        pending.reservations.clear();
        match pending.report.take() {
            Some(Report::Init(points)) => points,
            _ => Vec::new(),
        }
    }
    pub(crate) fn begin(&self, epoch_id: u64, turn_id: u64, round_id: u64) -> Result<(), Error> {
        let mut pending = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        pending.generation = pending.generation.checked_add(1).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::Arithmetic, "cache scope generation")
        })?;
        pending.report = Some(Report::Round(RoundCacheReport {
            epoch_id,
            turn_id,
            round_id,
            points: Vec::new(),
        }));
        pending.bytes = 0;
        pending.reservations.clear();
        Ok(())
    }
    pub(crate) fn scope(&self) -> Option<Scope> {
        let pending = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        pending.report.as_ref().map(|_| Scope(pending.generation))
    }
    pub(crate) fn release(&self, scope: Option<Scope>, call_id: &str) {
        let mut pending = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if scope.is_some_and(|scope| scope.0 == pending.generation) {
            pending.reservations.remove(call_id);
        }
    }
    pub(crate) fn reserve(
        &self,
        scope: Option<Scope>,
        call_id: &str,
        points: usize,
        bytes: usize,
    ) -> Result<(), Error> {
        let mut pending = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if scope.is_none_or(|scope| scope.0 != pending.generation) || pending.report.is_none() {
            return Err(Error::config(
                "evidence reservation belongs to an inactive round",
            ));
        }
        if pending.reservations.contains_key(call_id) {
            return Err(Error::config("invocation already reserved patch evidence"));
        }
        let reserved_points: usize = pending.reservations.values().map(|value| value.0).sum();
        let reserved_bytes: usize = pending.reservations.values().map(|value| value.1).sum();
        let retained = pending
            .report
            .as_ref()
            .map_or(0, |report| report.points().len());
        crate::budget::check(
            crate::LimitKind::CachePoints,
            retained
                .saturating_add(reserved_points)
                .saturating_add(points),
            MAX_POINTS,
        )?;
        crate::budget::check(
            crate::LimitKind::CacheBytes,
            pending
                .bytes
                .saturating_add(reserved_bytes)
                .saturating_add(bytes),
            MAX_BYTES,
        )?;
        pending.reservations.insert(call_id.into(), (points, bytes));
        Ok(())
    }
    pub(crate) fn record(
        &self,
        scope: Option<Scope>,
        call_id: &str,
        tool: &str,
        point: FileCachePoint,
    ) -> Result<(), Error> {
        let size = point_bytes(call_id, tool, &point)?;
        let mut pending = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if scope.is_none_or(|scope| scope.0 != pending.generation) || pending.report.is_none() {
            return Err(Error::config("cache point belongs to an inactive round"));
        }
        let own = pending
            .reservations
            .get(call_id)
            .copied()
            .unwrap_or_default();
        if pending.reservations.contains_key(call_id) && (own.0 == 0 || size > own.1) {
            return Err(crate::budget::reject(
                crate::LimitKind::CacheBytes,
                "patch evidence exceeds reserved envelope",
            ));
        }
        let other_points: usize = pending
            .reservations
            .iter()
            .filter(|(id, _)| id.as_str() != call_id)
            .map(|(_, value)| value.0)
            .sum();
        let other_bytes: usize = pending
            .reservations
            .iter()
            .filter(|(id, _)| id.as_str() != call_id)
            .map(|(_, value)| value.1)
            .sum();
        let bytes = pending.bytes.checked_add(size).ok_or_else(|| {
            crate::budget::reject(crate::LimitKind::CacheBytes, "round cache byte budget")
        })?;
        crate::budget::check(
            crate::LimitKind::CacheBytes,
            bytes.saturating_add(other_bytes),
            MAX_BYTES,
        )
        .map_err(|error| cache_label(error, "round cache point budget"))?;
        if let Some(report) = &pending.report {
            crate::budget::check(
                crate::LimitKind::CachePoints,
                report.points().len().saturating_add(other_points),
                MAX_POINTS - 1,
            )
            .map_err(|error| cache_label(error, "round cache point budget"))?;
        }
        pending.bytes = bytes;
        if let Some(reservation) = pending.reservations.get_mut(call_id) {
            reservation.0 -= 1;
            reservation.1 -= size;
        }
        // Presence and the 128-point bound were checked under the same lock.
        if let Some(report) = pending.report.as_mut() {
            let sequence = report
                .points()
                .iter()
                .filter(|item| item.call_id == call_id)
                .count() as u32;
            report.points_mut().push(RecordedCachePoint {
                call_id: call_id.into(),
                tool: tool.into(),
                sequence,
                point,
            });
        }
        Ok(())
    }
    pub(crate) fn finish(&self) -> Option<RoundCacheReport> {
        let mut pending = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        pending.bytes = 0;
        pending.reservations.clear();
        match pending.report.take() {
            Some(Report::Round(report)) => Some(report),
            _ => None,
        }
    }
    pub(crate) fn clear(&self) {
        let _ = self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn point(data: Option<Vec<u8>>) -> FileCachePoint {
        FileCachePoint {
            path: "selected.txt".into(),
            version: data
                .as_ref()
                .map(|data| format!("{:x}", Sha256::digest(data))),
            data,
        }
    }
    #[test]
    fn frozen_relative_evidence_from_non_local_hosts_keeps_original_bytes() {
        for path in [
            "a?b",
            "CON",
            "trailing.",
            "space ",
            "nested/a?b",
            "line\nbreak",
        ] {
            // Decode retained bytes directly: this never calls Host normalization
            // or grants authority to open the historical path on this machine.
            let bytes = serde_json::to_vec(&serde_json::json!({
                "epoch_id": 1, "turn_id": 2, "round_id": 3,
                "points": [{"call_id":"old-call", "tool":"write", "sequence":0,
                    "path":path, "data":null, "version":null}]
            }))
            .unwrap();
            let decoded: RoundCacheReport = serde_json::from_slice(&bytes).unwrap();
            decoded.validate().unwrap();
            assert_eq!(decoded.points[0].point.path, path);
            let encoded = serde_json::to_vec(&decoded).unwrap();
            let restored: RoundCacheReport = serde_json::from_slice(&encoded).unwrap();
            restored.validate().unwrap();
            assert_eq!(serde_json::to_vec(&restored).unwrap(), encoded);
            assert!(!microcrab_host_api::is_normalized_file_path(path, false));
        }
    }

    #[test]
    fn portable_absolute_cache_evidence_preserves_identity_and_roundtrip() {
        let collector = Collector::default();
        collector.begin(1, 2, 3).unwrap();
        for path in [
            "selected.txt",
            "/outside/file",
            "C:/outside/file",
            "//server/share/file",
        ] {
            let mut observation = point(Some(b"evidence".to_vec()));
            observation.path = path.into();
            collector
                .record(collector.scope(), "call", "write", observation)
                .unwrap();
        }
        for path in [
            "../outside",
            "C:relative",
            "//server/share",
            "/outside/../file",
            "C:/file:ads",
            "C:/NUL",
            "C:\\outside\\file",
        ] {
            let mut observation = point(None);
            observation.path = path.into();
            assert!(
                collector
                    .record(collector.scope(), "call", "write", observation)
                    .is_err()
            );
        }
        let report = collector.finish().unwrap();
        assert_eq!(report.points.len(), 4);
        assert_eq!(report.points[2].point.path, "C:/outside/file");
        let bytes = serde_json::to_vec(&report).unwrap();
        let decoded: RoundCacheReport = serde_json::from_slice(&bytes).unwrap();
        decoded.validate().unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
        assert_eq!(
            decoded
                .points
                .iter()
                .map(|point| point.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn whole_patch_reservation_prevents_other_calls_from_stealing_capacity() {
        let collector = Collector::default();
        collector.begin(1, 2, 3).unwrap();
        let scope = collector.scope();
        collector
            .reserve(scope, "patch", 127, MAX_BYTES - 1024)
            .unwrap();
        assert!(collector.reserve(scope, "other", 2, 1).is_err());
        assert!(
            collector
                .record(scope, "other", "tool", point(Some(vec![0; 2048])))
                .is_err()
        );
        collector
            .record(scope, "patch", "tool", point(Some(vec![1; 64])))
            .unwrap();
        collector.release(scope, "patch");
        collector.reserve(scope, "other", 1, 4096).unwrap();
        collector
            .record(scope, "other", "tool", point(Some(vec![2; 1024])))
            .unwrap();
        assert_eq!(collector.finish().unwrap().points.len(), 2);
    }
    #[test]
    fn initialization_points_have_no_synthetic_round_and_preserve_scope_bounds() {
        let round = Collector::default();
        round.begin(1, 2, 3).unwrap();
        let init = Collector::default();
        init.begin_init().unwrap();
        let scope = init.scope();
        init.record(scope, "agent-init:1", "", point(None)).unwrap();
        let points = init.finish_init();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].call_id, "agent-init:1");
        assert!(points[0].tool.is_empty());
        validate_points(&points).unwrap();
        assert!(init.record(scope, "agent-init:1", "", point(None)).is_err());
        assert!(round.finish().unwrap().points.is_empty());
    }

    #[test]
    fn scopes_isolate_rounds_and_sessions_and_preserve_missing_and_empty() {
        let first = Collector::default();
        let second = Collector::default();
        first.begin(1, 2, 3).unwrap();
        second.begin(1, 2, 3).unwrap();
        let old = first.scope();
        first.record(old, "a", "write", point(None)).unwrap();
        first.record(old, "b", "edit", point(None)).unwrap();
        first
            .record(old, "a", "write", point(Some(vec![])))
            .unwrap();
        let report = first.finish().unwrap();
        assert_eq!(
            (report.epoch_id, report.turn_id, report.round_id),
            (1, 2, 3)
        );
        assert!(report.points[0].point.data.is_none());
        assert_eq!(report.points[2].point.data, Some(vec![]));
        assert_eq!(
            report
                .points
                .iter()
                .map(|item| item.sequence)
                .collect::<Vec<_>>(),
            vec![0, 0, 1]
        );
        let serialized = serde_json::to_value(&report).unwrap();
        assert_eq!(serialized["points"][2]["path"], "selected.txt");
        let decoded: RoundCacheReport = serde_json::from_value(serialized).unwrap();
        decoded.validate().unwrap();
        assert!(second.finish().unwrap().points.is_empty());
        assert!(first.record(old, "a", "write", point(None)).is_err());
        first.begin(1, 2, 4).unwrap();
        assert!(first.record(old, "a", "write", point(None)).is_err());
        first
            .record(first.scope(), "c", "write", point(Some(vec![1])))
            .unwrap();
        assert_eq!(first.finish().unwrap().points.len(), 1);
    }
    #[test]
    fn bounds_and_content_invariants_reject_before_mutating_the_report() {
        let collector = Collector::default();
        collector.begin(0, 1, 1).unwrap();
        let error = collector
            .record(
                collector.scope(),
                "a",
                "write",
                point(Some(vec![0; MAX_BYTES])),
            )
            .unwrap_err();
        let Error::Limit(limit) = error else {
            panic!("expected typed cache byte limit")
        };
        assert_eq!(limit.kind, crate::LimitKind::CacheBytes);
        assert_eq!(limit.budget_bytes, Some(MAX_BYTES));
        assert!(!limit.is_pre_effect());
        let mut invalid = point(Some(vec![]));
        invalid.version = None;
        assert!(
            collector
                .record(collector.scope(), "a", "write", invalid)
                .is_err()
        );
        let mut invalid = point(None);
        invalid.path = "../escape".into();
        assert!(
            collector
                .record(collector.scope(), "a", "write", invalid)
                .is_err()
        );
        for _ in 0..MAX_POINTS {
            collector
                .record(collector.scope(), "a", "write", point(None))
                .unwrap();
        }
        assert!(
            collector
                .record(collector.scope(), "a", "write", point(None))
                .is_err()
        );
        let report = collector.finish().unwrap();
        assert_eq!(report.points.len(), MAX_POINTS);
        report.validate().unwrap();
    }
}
