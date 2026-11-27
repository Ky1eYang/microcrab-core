//! Reference callbacks belong to real lifecycle refreshes, never admission previews.
use microcrab_auditor::{AuditBody, AuditorError, BoxFuture, HookContext, Injection, Moment};
use microcrab_protocol::{Response, Usage};
use microcrab_session::context::{Content, ContentItem};
use microcrab_session::runtime::Runtime;
use microcrab_session::runtime::journal::{Journal, MemoryJournal, Recover};
use microcrab_session::runtime::tool::DenyAll;
use microcrab_session::{
    Auditor, Client, DynReference, Error, Limits, Reference, ReferenceCapabilities,
    ReferenceContent, ReferenceEntry, ReferenceError, ReferenceProjection, Session, SessionInput,
    State,
};
use microcrab_transport::StateFrame;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Load,
    Change,
    Hook(Moment),
}

#[derive(Default)]
struct Probe {
    loads: AtomicUsize,
    changes: AtomicUsize,
    generation: AtomicUsize,
    revision: AtomicUsize,
    fail_load: AtomicBool,
    absent: AtomicBool,
    writer: Mutex<Option<State>>,
    trace: Mutex<Vec<Call>>,
}

struct LoadedReference(Arc<Probe>);
#[async_trait::async_trait]
impl Reference for LoadedReference {
    fn kind(&self) -> &str {
        "skills"
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities::read_only()
    }

    async fn load(&self, _cancel: CancellationToken) -> Result<(), ReferenceError> {
        self.0.loads.fetch_add(1, Ordering::SeqCst);
        self.0.trace.lock().unwrap().push(Call::Load);
        if self.0.fail_load.swap(false, Ordering::SeqCst) {
            return Err(ReferenceError::failed("injected reference load failure"));
        }
        let writer = self.0.writer.lock().unwrap().clone();
        if let Some(writer) = writer {
            writer
                .inject("written_during_load", json!(true))
                .await
                .map_err(|_| ReferenceError::failed("reference variable callback failed"))?;
        }
        self.0.generation.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        Ok(vec![])
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        self.0.changes.fetch_add(1, Ordering::SeqCst);
        self.0.trace.lock().unwrap().push(Call::Change);
        let generation = self.0.generation.load(Ordering::SeqCst);
        if generation == 0 {
            return Err(ReferenceError::failed("projection requested before load"));
        }
        if self.0.absent.load(Ordering::SeqCst) {
            return Ok(ReferenceProjection::empty());
        }
        // Instrumentation observes calls; reading the full projection never consumes it.
        Ok(ReferenceProjection {
            exact: Some(json!({
                "generation": generation,
                "revision": self.0.revision.load(Ordering::SeqCst),
            })),
            short_model: None,
        })
    }

    async fn expand(
        &self,
        _: &str,
        _cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        Ok(None)
    }
}

#[derive(Default)]
struct Capture(Mutex<Vec<Content>>);
#[async_trait::async_trait]
impl Client for Capture {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.lock().unwrap().push(content.clone());
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

#[derive(Default)]
struct PublicationJournal {
    memory: MemoryJournal,
    publications: AtomicUsize,
}
#[async_trait::async_trait]
impl Journal for PublicationJournal {
    async fn commit(&self, recover: Recover) -> Result<(), Error> {
        self.memory.commit(recover).await
    }

    async fn recovered(&self, recover: Recover) -> Result<(), Error> {
        self.publications.fetch_add(1, Ordering::SeqCst);
        self.memory.recovered(recover).await
    }
}

fn reference(probe: &Arc<Probe>) -> DynReference {
    DynReference::new(LoadedReference(probe.clone())).unwrap()
}

fn runtime() -> Runtime {
    Runtime::new(vec![], Arc::new(DenyAll)).unwrap()
}

struct Observed(Arc<Probe>);
impl Auditor for Observed {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, _: AuditBody) {}
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        ctx: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        if matches!(moment, Moment::SessionCreated | Moment::SessionRestored) {
            let generation = self.0.generation.load(Ordering::SeqCst);
            assert!(
                generation > 0,
                "reference load must precede initialization hook"
            );
            assert_eq!(
                ctx.variables.get("skills"),
                Some(json!({
                    "generation": generation,
                    "revision": self.0.revision.load(Ordering::SeqCst),
                })),
                "hook readers observe the committed reference projection without rereading its source"
            );
            assert_eq!(
                self.0.trace.lock().unwrap().last(),
                Some(&Call::Change),
                "a successful projection refresh must precede the initialization hook"
            );
        }
        self.0.trace.lock().unwrap().push(Call::Hook(moment));
        Box::pin(async { Ok(Injection::default()) })
    }
}

fn projected(items: &[ContentItem], generation: usize, revision: usize) -> bool {
    let expected = json!({"generation": generation, "revision": revision});
    items.iter().any(|item| match item {
        ContentItem::State(StateFrame::StateSnapshot { state }) => {
            state.get("skills") == Some(&expected)
        }
        ContentItem::State(StateFrame::StatePatch { set, .. }) => {
            set.get("skills") == Some(&expected)
        }
        _ => false,
    })
}

fn encoded(value: impl serde::Serialize) -> Value {
    serde_json::to_value(value).unwrap()
}

#[tokio::test]
async fn closing_before_successful_load_does_not_read_unloaded_kinds() {
    for failed_start in [false, true] {
        let probe = Arc::new(Probe::default());
        let session = Session::builder_arc(Arc::new(Capture::default()), runtime())
            .references(vec![reference(&probe)])
            .auditor(Arc::new(Observed(probe.clone())))
            .build()
            .unwrap();
        if failed_start {
            probe.fail_load.store(true, Ordering::SeqCst);
            assert!(matches!(session.start().await, Err(Error::Reference(_))));
        }
        tokio::time::timeout(Duration::from_secs(2), session.close())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            probe.loads.load(Ordering::SeqCst),
            usize::from(failed_start)
        );
        assert_eq!(probe.changes.load(Ordering::SeqCst), 0);
        let mut expected = if failed_start {
            vec![Call::Load]
        } else {
            vec![]
        };
        expected.push(Call::Hook(Moment::SessionTerminated));
        assert_eq!(*probe.trace.lock().unwrap(), expected);
    }
}

#[tokio::test]
async fn direct_first_submit_loads_before_projection_and_only_real_boundaries_call_change() {
    let probe = Arc::new(Probe::default());
    let client = Arc::new(Capture::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .references(vec![reference(&probe)])
        .auditor(Arc::new(Observed(probe.clone())))
        .build()
        .unwrap();

    session
        .submit("first input without explicit start")
        .await
        .unwrap();

    assert_eq!(probe.loads.load(Ordering::SeqCst), 1);
    // The six internal refresh points remain private; only three public hooks
    // occur on a plain turn. Hook observation never invents an Epoch/Turn enum.
    let trace = probe.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&Call::Load));
    assert_eq!(
        &trace[..3],
        &[Call::Load, Call::Change, Call::Hook(Moment::SessionCreated)]
    );
    assert_eq!(
        trace
            .iter()
            .filter_map(|call| match call {
                Call::Hook(moment) => Some(*moment),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![
            Moment::SessionCreated,
            Moment::InputAdmitted,
            Moment::RoundCompleted
        ]
    );
    assert_eq!(probe.changes.load(Ordering::SeqCst), 6);
    assert!(session.state_handle().extract("skills").is_none());
    assert_eq!(
        session.state_view().unwrap()["skills"],
        json!({"generation":1,"revision":0})
    );
    assert_eq!(
        probe.changes.load(Ordering::SeqCst),
        6,
        "reading committed State cannot invoke reference callbacks"
    );
    assert!(projected(&client.0.lock().unwrap()[0].items, 1, 0));
    let trace = probe.trace.lock().unwrap().clone();
    session.start().await.unwrap();
    assert_eq!(*probe.trace.lock().unwrap(), trace);
    session.close().await.unwrap();
}

#[tokio::test]
async fn unchanged_projection_is_not_repeated_and_none_emits_a_context_removal() {
    let probe = Arc::new(Probe::default());
    let session = Session::builder(Capture::default(), runtime())
        .references(vec![reference(&probe)])
        .auditor(Arc::new(Observed(probe.clone())))
        .build()
        .unwrap();
    session.submit("initial projection").await.unwrap();
    let first_len = session.messages().len();
    session.submit("same projection").await.unwrap();
    let unchanged = session.messages();
    assert!(unchanged[first_len..].iter().all(|item| match item {
        ContentItem::State(StateFrame::StateSnapshot { state }) => state.get("skills").is_none(),
        ContentItem::State(StateFrame::StatePatch { set, remove, .. }) => {
            !set.contains_key("skills") && !remove.iter().any(|key| key == "skills")
        }
        _ => true,
    }));
    probe.absent.store(true, Ordering::SeqCst);
    session
        .submit("reference no longer available")
        .await
        .unwrap();
    let removed = session.messages();
    assert!(removed[unchanged.len()..].iter().any(|item| matches!(
        item,
        ContentItem::State(StateFrame::StatePatch { set, remove, .. })
            if !set.contains_key("skills") && remove.iter().any(|key| key == "skills")
    )));
    session.close().await.unwrap();
}

#[tokio::test]
async fn rejected_context_admission_cannot_load_or_advance_reference_projection() {
    let probe = Arc::new(Probe::default());
    let client = Arc::new(Capture::default());
    let journal = Arc::new(MemoryJournal::new());
    let limits = Limits {
        max_context_bytes: 8192,
        ..Default::default()
    };
    let session = Session::builder_arc(client.clone(), runtime())
        .limits(limits)
        .journal_arc(journal.clone())
        .references(vec![reference(&probe)])
        .auditor(Arc::new(Observed(probe.clone())))
        .build()
        .unwrap();
    let state = encoded(session.state());
    // This passes input preparation but exceeds Context's total byte budget.
    let mut rejected = SessionInput::text("x".repeat(16_384));
    rejected
        .variables
        .set
        .insert("uncommitted".into(), json!(true));
    assert!(matches!(
        session.submit(rejected.clone()).await,
        Err(Error::Limit(_))
    ));
    assert!(probe.trace.lock().unwrap().is_empty());
    assert_eq!(encoded(session.state()), state);
    assert!(session.messages().is_empty());
    assert!(journal.latest().is_none());
    assert!(client.0.lock().unwrap().is_empty());
    session.submit("valid initial input").await.unwrap();
    let state = encoded(session.state());
    let messages = session.messages();
    let durable = encoded(journal.latest().unwrap());
    let trace = probe.trace.lock().unwrap().clone();
    probe.revision.store(7, Ordering::SeqCst);
    assert!(matches!(
        session.submit(rejected).await,
        Err(Error::Limit(_))
    ));
    assert_eq!(*probe.trace.lock().unwrap(), trace);
    assert_eq!(encoded(session.state()), state);
    assert_eq!(session.messages(), messages);
    assert_eq!(encoded(journal.latest().unwrap()), durable);
    assert_eq!(client.0.lock().unwrap().len(), 1);
    assert!(session.state_handle().extract("uncommitted").is_none());
    session.submit("valid after rejection").await.unwrap();
    assert!(projected(&client.0.lock().unwrap()[1].items, 1, 7));
    session.close().await.unwrap();
}

#[tokio::test]
async fn load_failure_is_retryable_before_start_or_first_submit_emits_any_boundary() {
    for explicit_start in [true, false] {
        let probe = Arc::new(Probe::default());
        probe.fail_load.store(true, Ordering::SeqCst);
        let client = Arc::new(Capture::default());
        let journal = Arc::new(MemoryJournal::new());
        let session = Session::builder_arc(client.clone(), runtime())
            .journal_arc(journal.clone())
            .references(vec![reference(&probe)])
            .auditor(Arc::new(Observed(probe.clone())))
            .build()
            .unwrap();
        let failure = if explicit_start {
            session.start().await
        } else {
            session.submit("first attempt").await.map(|_| ())
        };
        assert!(matches!(failure, Err(Error::Reference(_))));
        assert_eq!(*probe.trace.lock().unwrap(), vec![Call::Load]);
        assert!(journal.latest().is_none());
        assert!(session.messages().is_empty());
        assert!(client.0.lock().unwrap().is_empty());
        if explicit_start {
            session.start().await.unwrap();
            let trace = probe.trace.lock().unwrap().clone();
            session.start().await.unwrap();
            assert_eq!(*probe.trace.lock().unwrap(), trace);
        }
        session.submit("successful retry").await.unwrap();
        assert_eq!(probe.loads.load(Ordering::SeqCst), 2);
        assert_eq!(probe.generation.load(Ordering::SeqCst), 1);
        assert!(projected(&client.0.lock().unwrap()[0].items, 1, 0));
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn initial_restore_and_live_recover_reload_before_restored_boundary_and_publish_new_projection()
 {
    let probe = Arc::new(Probe::default());
    let journal = Arc::new(PublicationJournal::default());
    let original = Session::builder(Capture::default(), runtime())
        .journal_arc(journal.clone())
        .references(vec![reference(&probe)])
        .auditor(Arc::new(Observed(probe.clone())))
        .build()
        .unwrap();
    original.submit("saved input").await.unwrap();
    let saved = original.checkpoint().await.unwrap();
    original.close().await.unwrap();

    let client = Arc::new(Capture::default());
    let restored = Session::restore(
        Session::builder_arc(client.clone(), runtime())
            .journal_arc(journal.clone())
            .references(vec![reference(&probe)])
            .auditor(Arc::new(Observed(probe.clone()))),
        saved.clone(),
    )
    .await
    .unwrap();
    assert_eq!(probe.loads.load(Ordering::SeqCst), 1);
    probe.trace.lock().unwrap().clear();
    restored.start().await.unwrap();
    assert_eq!(
        *probe.trace.lock().unwrap(),
        vec![
            Call::Load,
            Call::Change,
            Call::Hook(Moment::SessionRestored)
        ]
    );
    restored.submit("after initial restore").await.unwrap();
    assert!(projected(&client.0.lock().unwrap()[0].items, 2, 0));

    probe.trace.lock().unwrap().clear();
    restored.recover(saved.clone()).await.unwrap();
    assert_eq!(journal.publications.load(Ordering::SeqCst), 1);
    assert_eq!(encoded(journal.memory.latest().unwrap()), encoded(saved));
    assert_eq!(probe.loads.load(Ordering::SeqCst), 3);
    assert_eq!(
        *probe.trace.lock().unwrap(),
        vec![
            Call::Load,
            Call::Change,
            Call::Hook(Moment::SessionRestored)
        ]
    );
    restored.submit("after live recovery").await.unwrap();
    assert!(projected(&client.0.lock().unwrap()[1].items, 3, 0));
    restored.close().await.unwrap();
}

#[tokio::test]
async fn live_reload_failure_after_publication_faults_session_and_refuses_next_submit() {
    let probe = Arc::new(Probe::default());
    let client = Arc::new(Capture::default());
    let journal = Arc::new(PublicationJournal::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .journal_arc(journal.clone())
        .references(vec![reference(&probe)])
        .auditor(Arc::new(Observed(probe.clone())))
        .build()
        .unwrap();
    session.start().await.unwrap();
    let saved = session.checkpoint().await.unwrap();
    probe.trace.lock().unwrap().clear();
    probe.fail_load.store(true, Ordering::SeqCst);
    assert!(matches!(
        session.recover(saved.clone()).await,
        Err(Error::Reference(_))
    ));
    assert_eq!(journal.publications.load(Ordering::SeqCst), 1);
    assert_eq!(encoded(journal.memory.latest().unwrap()), encoded(saved));
    assert_eq!(*probe.trace.lock().unwrap(), vec![Call::Load]);
    assert!(matches!(
        session.submit("must be refused").await,
        Err(Error::Config(_))
    ));
    assert!(matches!(session.start().await, Err(Error::Config(_))));
    assert!(client.0.lock().unwrap().is_empty());
    assert_eq!(*probe.trace.lock().unwrap(), vec![Call::Load]);
    session.close().await.unwrap();
}

#[tokio::test]
async fn live_reload_can_await_global_variable_writer_after_recovery_fence_is_released() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let probe = Arc::new(Probe::default());
        let session = Session::builder(Capture::default(), runtime())
            .references(vec![reference(&probe)])
            .auditor(Arc::new(Observed(probe.clone())))
            .build()
            .unwrap();
        session.start().await.unwrap();
        let saved = session.checkpoint().await.unwrap();
        *probe.writer.lock().unwrap() = Some(session.state_handle());
        session.recover(saved).await.unwrap();
        assert_eq!(probe.loads.load(Ordering::SeqCst), 2);
        assert_eq!(
            session.state_handle().extract("written_during_load"),
            Some(json!(true))
        );
        session.submit("writer callback completed").await.unwrap();
        assert!(projected(&session.messages(), 2, 0));
        session.close().await.unwrap();
    })
    .await
    .expect("reference load must not wait behind the recovery variable fence");
}

#[tokio::test]
async fn recovered_custom_variable_cannot_collide_with_installed_kind_or_publish_workspace() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retained.txt");
    let host = Arc::new(
        microcrab_host_local::LocalHost::new(directory.path(), Default::default()).unwrap(),
    );
    let probe = Arc::new(Probe::default());
    let journal = Arc::new(PublicationJournal::default());
    let client = Arc::new(Capture::default());
    let session = Session::builder_arc(client.clone(), runtime())
        .host(host.clone())
        .journal_arc(journal.clone())
        .references(vec![reference(&probe)])
        .auditor(Arc::new(Observed(probe.clone())))
        .build()
        .unwrap();
    session.start().await.unwrap();
    let mut target = session.checkpoint().await.unwrap();
    let mut target_values = target.state.variables.as_value();
    target_values["skills"] = json!("custom collision");
    target.state.variables = microcrab_session::state::Variables::new(target_values).unwrap();
    std::fs::write(&path, b"retain current workspace").unwrap();
    let current = session.checkpoint().await.unwrap();
    let state = encoded(session.state());
    let messages = session.messages();
    let trace = probe.trace.lock().unwrap().clone();
    assert!(matches!(
        session.recover(target).await,
        Err(Error::Config(_))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), b"retain current workspace");
    assert_eq!(journal.publications.load(Ordering::SeqCst), 0);
    assert_eq!(encoded(journal.memory.latest().unwrap()), encoded(&current));
    assert_eq!(encoded(session.state()), state);
    assert_eq!(session.messages(), messages);
    assert_eq!(*probe.trace.lock().unwrap(), trace);

    // Initial restore must reject the same collision even when the workspace matches.
    let mut initial = current.clone();
    let mut initial_values = initial.state.variables.as_value();
    initial_values["skills"] = json!("custom collision");
    initial.state.variables = microcrab_session::state::Variables::new(initial_values).unwrap();
    let initial_probe = Arc::new(Probe::default());
    assert!(matches!(
        Session::restore(
            Session::builder(Capture::default(), runtime())
                .host(host)
                .journal_arc(journal.clone())
                .references(vec![reference(&initial_probe)]),
            initial,
        )
        .await,
        Err(Error::Config(_))
    ));
    assert!(initial_probe.trace.lock().unwrap().is_empty());
    assert_eq!(std::fs::read(&path).unwrap(), b"retain current workspace");
    assert_eq!(journal.publications.load(Ordering::SeqCst), 0);
    assert_eq!(encoded(journal.memory.latest().unwrap()), encoded(current));
    assert_eq!(encoded(session.state()), state);
    assert_eq!(session.messages(), messages);
    session
        .submit("prior session remains usable")
        .await
        .unwrap();
    assert_eq!(client.0.lock().unwrap().len(), 1);
    assert!(session.state_handle().extract("skills").is_none());
    session.close().await.unwrap();
}
