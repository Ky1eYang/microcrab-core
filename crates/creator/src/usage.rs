//! Consumption is append-only creator data: history travel never rewinds billing.
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, Injection, Moment,
};
use microcrab_session::{Error, protocol::Usage};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
    sync::{Arc, Mutex, mpsc},
};
use tokio::sync::oneshot;
enum Persistence {
    File(File),
    #[cfg(feature = "storage-sqlite")]
    Sqlite(microcrab_storage::usage::SqliteUsageStore),
}
impl Persistence {
    fn append(&mut self, usage: &Usage) -> Result<(), Error> {
        let mut bytes = serde_json::to_vec(usage)?;
        bytes.push(b'\n');
        match self {
            Self::File(file) => {
                file.write_all(&bytes)?;
                file.sync_all()?;
                Ok(())
            }
            #[cfg(feature = "storage-sqlite")]
            Self::Sqlite(store) => store.append(&bytes),
        }
    }
}
enum Command {
    Add(Usage),
    Flush(oneshot::Sender<Result<(), String>>),
    Stop,
}
/// Cache measurements cover only attempts with a known, valid input/cache pair.
/// These are reported samples, not an estimate of unreported consumption.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ReportedCacheUsage {
    pub scope: &'static str,
    pub attempted_samples: u64,
    pub paired_samples: u64,
    pub input_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_hit_rate: Option<f64>,
}

/// One coherent view of strict consumption and its separately measured cache subset.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageSnapshot {
    pub total: Usage,
    pub cache: ReportedCacheUsage,
}

#[derive(Default)]
struct CacheAccumulator {
    attempted_samples: u64,
    paired_samples: u64,
    input_tokens: u64,
    cache_read_tokens: u64,
    overflowed: bool,
}
impl CacheAccumulator {
    fn add(&mut self, usage: &Usage) {
        if usage.samples == 0 {
            return;
        }
        fn checked_sum(total: &mut u64, value: u64, overflowed: &mut bool) {
            match total.checked_add(value) {
                Some(sum) => *total = sum,
                None => {
                    *total = u64::MAX;
                    *overflowed = true;
                }
            }
        }
        checked_sum(
            &mut self.attempted_samples,
            usage.samples,
            &mut self.overflowed,
        );
        let Some((input, cached)) = usage.input_tokens.zip(usage.cache_read_tokens) else {
            return;
        };
        if cached > input {
            return;
        }
        checked_sum(
            &mut self.paired_samples,
            usage.samples,
            &mut self.overflowed,
        );
        checked_sum(&mut self.input_tokens, input, &mut self.overflowed);
        checked_sum(&mut self.cache_read_tokens, cached, &mut self.overflowed);
    }

    fn snapshot(&self) -> ReportedCacheUsage {
        let known = self.paired_samples > 0 && !self.overflowed;
        ReportedCacheUsage {
            scope: "reported_samples",
            attempted_samples: self.attempted_samples,
            paired_samples: self.paired_samples,
            input_tokens: known.then_some(self.input_tokens),
            cache_read_tokens: known.then_some(self.cache_read_tokens),
            cache_hit_rate: (known && self.input_tokens > 0)
                .then(|| self.cache_read_tokens as f64 / self.input_tokens as f64),
        }
    }
}

#[derive(Default)]
struct Consumption {
    total: Usage,
    cache: CacheAccumulator,
}
impl Consumption {
    fn add(&mut self, usage: &Usage) {
        self.total.accumulate(usage);
        self.cache.add(usage);
    }

    fn snapshot(&self) -> UsageSnapshot {
        UsageSnapshot {
            total: self.total.clone(),
            cache: self.cache.snapshot(),
        }
    }
}

struct Inner {
    sender: mpsc::Sender<Command>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    consumption: Arc<Mutex<Consumption>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.sender.send(Command::Stop);
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = worker.join();
        }
    }
}
#[derive(Clone)]
pub struct UsageLedger(Arc<Inner>);
impl UsageLedger {
    pub fn open(directory: Option<&Path>) -> Result<Self, Error> {
        Self::open_named(directory, "usage.jsonl")
    }
    /// Read persisted totals without creating a ledger or reopening a writer.
    pub fn read_total_named(directory: &Path, filename: &str) -> Result<Usage, Error> {
        Ok(Self::read_snapshot_named(directory, filename)?.total)
    }
    /// Replay existing consumption rows without creating or changing their file.
    pub fn read_snapshot_named(directory: &Path, filename: &str) -> Result<UsageSnapshot, Error> {
        Ok(Self::read_consumption_named(directory, filename)?.snapshot())
    }
    fn read_consumption_named(directory: &Path, filename: &str) -> Result<Consumption, Error> {
        let mut consumption = Consumption::default();
        let path = directory.join(filename);
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(consumption),
            Err(error) => return Err(error.into()),
        };
        for line in BufReader::new(file).lines() {
            consumption.add(&serde_json::from_str::<Usage>(&line?)?);
        }
        Ok(consumption)
    }
    pub fn open_named(directory: Option<&Path>, filename: &str) -> Result<Self, Error> {
        let mut consumption = Consumption::default();
        let mut file = None;
        if let Some(directory) = directory {
            let path = directory.join(filename);
            consumption = Self::read_consumption_named(directory, filename)?;
            file = Some(Persistence::File(
                OpenOptions::new().create(true).append(true).open(path)?,
            ));
        }
        Self::start(consumption, file)
    }
    #[cfg(feature = "storage-sqlite")]
    pub fn open_sqlite(
        path: &Path,
        scope: microcrab_storage::audit::AuditScope,
        name: &str,
    ) -> Result<Self, Error> {
        let store = microcrab_storage::usage::SqliteUsageStore::open(path, scope, name)?;
        let mut consumption = Consumption::default();
        for row in store.read()? {
            consumption.add(&serde_json::from_slice::<Usage>(&row)?);
        }
        Self::start(consumption, Some(Persistence::Sqlite(store)))
    }
    #[cfg(feature = "storage-sqlite")]
    pub fn read_snapshot_sqlite(
        path: &Path,
        scope: &microcrab_storage::audit::AuditScope,
        name: &str,
    ) -> Result<UsageSnapshot, Error> {
        let mut consumption = Consumption::default();
        for row in microcrab_storage::usage::SqliteUsageStore::inspect(path, scope, name)? {
            consumption.add(&serde_json::from_slice::<Usage>(&row)?);
        }
        Ok(consumption.snapshot())
    }
    fn start(consumption: Consumption, mut file: Option<Persistence>) -> Result<Self, Error> {
        let consumption = Arc::new(Mutex::new(consumption));

        let (sender, receiver) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("microcrab-usage".into())
            .spawn(move || {
                let mut failure = None;
                for command in receiver {
                    match command {
                        Command::Add(usage) => {
                            if failure.is_none()
                                && let Some(file) = &mut file
                            {
                                let written = file.append(&usage);
                                if let Err(error) = written {
                                    failure = Some(error.to_string());
                                }
                            }
                        }
                        Command::Flush(reply) => {
                            let _ = reply
                                .send(failure.as_ref().map_or(Ok(()), |error| Err(error.clone())));
                        }
                        Command::Stop => break,
                    }
                }
            })?;
        Ok(Self(Arc::new(Inner {
            sender,
            worker: Mutex::new(Some(worker)),
            consumption,
        })))
    }
    pub fn publish(&self, usage: Usage) {
        self.0
            .consumption
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .add(&usage);
        let _ = self.0.sender.send(Command::Add(usage));
    }
    pub fn total(&self) -> Usage {
        self.snapshot().total
    }
    /// Capture both views under one lock so coverage and totals share a boundary.
    pub fn snapshot(&self) -> UsageSnapshot {
        self.0
            .consumption
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot()
    }
    pub async fn flush(&self) -> Result<(), Error> {
        let (reply, response) = oneshot::channel();
        self.0
            .sender
            .send(Command::Flush(reply))
            .map_err(|_| Error::audit("usage writer disconnected"))?;
        response
            .await
            .map_err(|_| Error::audit("usage writer disconnected"))?
            .map_err(|error| Error::audit_with_private("usage ledger flush failed", error))
    }
}

/// Account each completed provider attempt once, independently of history travel.
/// The sink owns hooks/recording; this tee never serializes credentials or DTOs.
#[derive(Clone)]
pub struct UsageAuditor {
    ledger: UsageLedger,
    aggregate: UsageLedger,
    inner: Arc<dyn Auditor>,
}

impl UsageAuditor {
    pub fn new(ledger: UsageLedger, aggregate: UsageLedger, inner: Arc<dyn Auditor>) -> Self {
        Self {
            ledger,
            aggregate,
            inner,
        }
    }
}

impl Auditor for UsageAuditor {
    fn is_enabled(&self) -> bool {
        true
    }

    fn publish(&self, event: Moment, body: AuditBody) {
        if event == Moment::RoundCompleted && body.call_id.is_some() && body.outcome.is_some() {
            let usage = body.usage.clone().unwrap_or_else(|| Usage {
                samples: 1,
                ..Default::default()
            });
            self.ledger.publish(usage.clone());
            self.aggregate.publish(usage);
        }
        self.inner.publish(event, body);
    }

    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        self.inner.intervene(moment, context)
    }

    fn flush(&self) -> BoxFuture<'_, Result<(), AuditorError>> {
        Box::pin(async move {
            // Attempt every flush even if a hook/sink fails; closing the owner
            // must not leave an unrelated usage writer unacknowledged.
            let sink = self.inner.flush().await;
            let ledger = self
                .ledger
                .flush()
                .await
                .map_err(|error| AuditorError(error.to_string()));
            let aggregate = self
                .aggregate
                .flush()
                .await
                .map_err(|error| AuditorError(error.to_string()));
            sink.and(ledger).and(aggregate)
        })
    }
}

impl microcrab_session::DelegateUsage for UsageLedger {
    fn total(&self) -> microcrab_protocol::Usage {
        UsageLedger::total(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(input: Option<u64>, cached: Option<u64>) -> Usage {
        Usage {
            input_tokens: input,
            cache_read_tokens: cached,
            samples: 1,
            ..Default::default()
        }
    }

    #[test]
    fn reported_cache_pairs_its_denominator_and_preserves_strict_unknowns() {
        let mut consumption = Consumption::default();
        consumption.add(&usage(Some(100), Some(50)));
        consumption.add(&usage(Some(900), None));
        consumption.add(&usage(None, None));
        consumption.add(&usage(None, Some(80)));
        consumption.add(&usage(Some(10), Some(11)));
        consumption.add(&Usage::default());
        let snapshot = consumption.snapshot();
        assert_eq!(snapshot.total.samples, 5);
        assert_eq!(snapshot.total.input_tokens, None);
        assert_eq!(snapshot.total.cache_read_tokens, None);
        assert_eq!(snapshot.cache.scope, "reported_samples");
        assert_eq!(snapshot.cache.attempted_samples, 5);
        assert_eq!(snapshot.cache.paired_samples, 1);
        assert_eq!(snapshot.cache.input_tokens, Some(100));
        assert_eq!(snapshot.cache.cache_read_tokens, Some(50));
        assert_eq!(snapshot.cache.cache_hit_rate, Some(0.5));
        consumption.add(&usage(Some(100), Some(0)));
        let cache = consumption.snapshot().cache;
        assert_eq!(cache.paired_samples, 2);
        assert_eq!(cache.input_tokens, Some(200));
        assert_eq!(cache.cache_read_tokens, Some(50));
        assert_eq!(cache.cache_hit_rate, Some(0.25));
    }

    #[test]
    fn reported_cache_unknown_zero_and_overflow_are_distinct() {
        let mut consumption = Consumption::default();
        consumption.add(&usage(None, None));
        assert_eq!(consumption.snapshot().cache.input_tokens, None);
        assert_eq!(consumption.snapshot().cache.cache_hit_rate, None);
        consumption.add(&usage(Some(0), Some(0)));
        let cache = consumption.snapshot().cache;
        assert_eq!(cache.paired_samples, 1);
        assert_eq!(cache.input_tokens, Some(0));
        assert_eq!(cache.cache_read_tokens, Some(0));
        assert_eq!(cache.cache_hit_rate, None);
        consumption.add(&usage(Some(u64::MAX), Some(u64::MAX)));
        consumption.add(&usage(Some(1), Some(1)));
        consumption.add(&usage(Some(100), Some(50)));
        let cache = consumption.snapshot().cache;
        assert_eq!(cache.paired_samples, 4);
        assert_eq!(cache.input_tokens, None);
        assert_eq!(cache.cache_read_tokens, None);
        assert_eq!(cache.cache_hit_rate, None);
        let mut consumption = Consumption::default();
        let mut many = usage(Some(1), Some(0));
        many.samples = u64::MAX;
        consumption.add(&many);
        consumption.add(&usage(Some(1), Some(0)));
        let cache = consumption.snapshot().cache;
        assert_eq!(cache.attempted_samples, u64::MAX);
        assert_eq!(cache.paired_samples, u64::MAX);
        assert_eq!(cache.cache_hit_rate, None);
    }

    #[tokio::test]
    async fn reported_cache_replays_frozen_rows_without_rewriting_them() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("usage.jsonl");
        let rows = [
            usage(Some(100), Some(50)),
            usage(Some(900), None),
            usage(None, None),
        ];
        let mut original = Vec::new();
        for row in &rows {
            original.extend(serde_json::to_vec(row).unwrap());
            original.push(b'\n');
        }
        std::fs::write(&path, &original).unwrap();
        let detached = UsageLedger::read_snapshot_named(directory.path(), "usage.jsonl").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let ledger = UsageLedger::open(Some(directory.path())).unwrap();
        assert_eq!(ledger.snapshot(), detached);
        ledger.publish(usage(Some(100), Some(0)));
        ledger.flush().await.unwrap();
        let live = ledger.snapshot();
        drop(ledger);
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(&original));
        let mut expected = original;
        expected.extend(serde_json::to_vec(&usage(Some(100), Some(0))).unwrap());
        expected.push(b'\n');
        assert_eq!(bytes, expected);
        assert_eq!(
            UsageLedger::read_snapshot_named(directory.path(), "usage.jsonl").unwrap(),
            live,
        );
        assert_eq!(live.total.input_tokens, None);
        assert_eq!(live.cache.attempted_samples, 4);
        assert_eq!(live.cache.paired_samples, 2);
        assert_eq!(live.cache.cache_hit_rate, Some(0.25));
        let reopened = UsageLedger::open(Some(directory.path())).unwrap();
        assert_eq!(reopened.snapshot(), live);
    }

    #[tokio::test]
    async fn usage_flush_reopens_append_only_totals() {
        let directory = tempfile::tempdir().unwrap();
        let ledger = UsageLedger::open(Some(directory.path())).unwrap();
        ledger.publish(Usage {
            input_tokens: Some(7),
            output_tokens: Some(2),
            samples: 1,
            ..Default::default()
        });
        ledger.flush().await.unwrap();
        let checkpoint_total = ledger.total();
        ledger.publish(Usage {
            input_tokens: Some(3),
            output_tokens: Some(1),
            samples: 1,
            ..Default::default()
        });
        ledger.flush().await.unwrap();
        assert_eq!(checkpoint_total.input_tokens, Some(7));
        assert_eq!(ledger.total().input_tokens, Some(10));
        drop(ledger);
        let reopened = UsageLedger::open(Some(directory.path())).unwrap();
        assert_eq!(reopened.total().samples, 2);
        assert_eq!(reopened.total().input_tokens, Some(10));
        assert_eq!(reopened.total().output_tokens, Some(3));
    }
    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn sqlite_consumption_reopens_with_identical_unknown_and_reported_cache_semantics() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("storage.sqlite");
        let scope = microcrab_storage::audit::AuditScope::new("owner", "root", "root");
        let ledger = UsageLedger::open_sqlite(&database, scope.clone(), "usage.jsonl").unwrap();
        for row in [
            usage(Some(100), Some(50)),
            usage(Some(900), None),
            usage(None, None),
        ] {
            ledger.publish(row);
        }
        ledger.flush().await.unwrap();
        let live = ledger.snapshot();
        assert_eq!(live.total.input_tokens, None);
        assert_eq!(live.cache.paired_samples, 1);
        assert_eq!(
            UsageLedger::read_snapshot_sqlite(&database, &scope, "usage.jsonl").unwrap(),
            live
        );
        assert_eq!(
            UsageLedger::read_snapshot_sqlite(
                &database,
                &microcrab_storage::audit::AuditScope::new("other", "root", "root"),
                "usage.jsonl"
            )
            .unwrap()
            .total
            .samples,
            0
        );
        drop(ledger);
        let reopened = UsageLedger::open_sqlite(&database, scope, "usage.jsonl").unwrap();
        assert_eq!(reopened.snapshot(), live);
        assert!(!directory.path().join("usage.jsonl").exists());
        assert!(!directory.path().join("aggregate_usage.jsonl").exists());
    }
}
