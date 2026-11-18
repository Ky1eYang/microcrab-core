//! Behavioral lifecycle regressions; these cases are added without running them.
use super::*;
use crate::runtime::tool::{AllowTools, Tool, ToolOutput};
use crate::runtime::{BuiltinTool, ToolRegistration};
use crate::{AgentInfo, AgentMode, AgentRole, Client, VariableView};
use microcrab_protocol::{Response, ToolCall, Usage};
use std::sync::Mutex;
use tokio::sync::{Notify, Semaphore, mpsc};

struct Answer;
#[async_trait]
impl Client for Answer {
    async fn complete(
        &self,
        _: &crate::context::Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "child reply".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
fn agent() -> AgentInfo {
    AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "worker".into(),
        modes: vec![AgentMode {
            name: "work".into(),
            description: "Focused worker".into(),
            prompt: "Complete the parent's task.".into(),
            role: AgentRole::SubAgent,
            permissions: vec![],
        }],
        tools: vec![],
        variables: Default::default(),
    }
}
struct FlushGate {
    entered: Notify,
    release: Semaphore,
}
impl Auditor for FlushGate {
    fn is_enabled(&self) -> bool {
        false
    }
    fn publish(&self, _: microcrab_auditor::Moment, _: microcrab_auditor::AuditBody) {}
    fn flush(
        &self,
    ) -> microcrab_auditor::BoxFuture<'_, Result<(), microcrab_auditor::AuditorError>> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release
                .acquire()
                .await
                .map_err(|_| microcrab_auditor::AuditorError("closed fixture".into()))?
                .forget();
            Ok(())
        })
    }
}
struct Factory {
    sent: mpsc::UnboundedSender<Session>,
    audit: Arc<FlushGate>,
}
#[async_trait]
impl ChildFactory for Factory {
    async fn create(
        &self,
        _: Option<&str>,
        _: &str,
        _: Option<&str>,
        _: &str,
        processes: Arc<dyn ProcessControl>,
    ) -> Result<ChildSession, Error> {
        let session = Session::builder(
            Answer,
            crate::runtime::Runtime::new(vec![], Arc::new(AllowTools::new(Vec::<String>::new())))?,
        )
        .session_id("long-lived-child")
        .is_main(false)
        .agent(agent())
        .processes_parent(processes)
        .auditor(self.audit.clone())
        .build()?;
        self.sent.send(session.clone()).map_err(|_| Error::Closed)?;
        struct UsageView;
        impl DelegateUsage for UsageView {
            fn total(&self) -> Usage {
                Usage::default()
            }
        }
        Ok(ChildSession {
            id: "long-lived-child".into(),
            session,
            mode: "work".into(),
            ledger: Arc::new(UsageView),
            auditor: self.audit.clone(),
        })
    }
}
struct ParentScript(Mutex<bool>);
#[async_trait]
impl Client for ParentScript {
    async fn complete(
        &self,
        _: &crate::context::Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let first = std::mem::take(&mut *self.0.lock().unwrap());
        Ok(Response {
            provider_metadata: None,
            content: "parent reply".into(),
            tool_calls: if first {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "delegation".into(),
                    name: "delegate".into(),
                    arguments: json!({"target":"worker","input":"first task"}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
async fn delegated_turns_remain_open_parent_controls_follow_up_and_close_waits_for_ack() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (sent, mut received) = mpsc::unbounded_channel();
        let audit = Arc::new(FlushGate {
            entered: Notify::new(),
            release: Semaphore::new(0),
        });
        let children = Children::default();
        let runtime = crate::runtime::Runtime::new(
            vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],
            Arc::new(AllowTools::new(["delegate"])),
        )
        .unwrap()
        .with_delegate_executor(Arc::new(OwnedDelegateExecutor::new(
            children.clone(),
            Arc::new(Factory {
                sent,
                audit: audit.clone(),
            }),
        )));
        let parent = Session::builder(ParentScript(Mutex::new(true)), runtime)
            .catalog(vec![agent()])
            .build()
            .unwrap();
        parent.submit("delegate").await.unwrap();
        let child = received.recv().await.unwrap();
        let mut activity = child.subscribe_activity();
        while activity.borrow().outcome_sequence < 1 {
            activity.changed().await.unwrap();
        }
        assert_eq!(child.kind(), crate::SessionKind::Delegated);
        assert!(child.submit("direct composer forbidden").await.is_err());
        assert!(child.queue("direct queue forbidden").is_err());
        assert!(
            parent.executions().has_pending(),
            "finishing a child Turn does not close its execution owner"
        );
        assert_eq!(
            parent
                .submit_child("long-lived-child", "worker", "second task")
                .await
                .unwrap()
                .content,
            "child reply"
        );
        assert_eq!(child.subscribe_activity().borrow().outcome_sequence, 2);
        assert!(
            parent
                .submit_child("long-lived-child", "foreign-target", "forbidden")
                .await
                .is_err()
        );
        let closing_parent = parent.clone();
        let closed = tokio::spawn(async move {
            closing_parent
                .close_child("long-lived-child", "worker")
                .await
        });
        audit.entered.notified().await;
        assert!(
            !closed.is_finished(),
            "child close awaits auditor cleanup acknowledgement"
        );
        audit.release.add_permits(4);
        closed.await.unwrap().unwrap();
        parent
            .close_child("long-lived-child", "worker")
            .await
            .unwrap();
        let tasks = parent.executions();
        let mut changed = tasks.subscribe();
        while tasks.has_pending() {
            changed.changed().await.unwrap();
        }
        assert!(!tasks.has_pending());
        assert!(
            parent.inspect_child("long-lived-child", "worker").is_err(),
            "settled child ownership is removed"
        );
        parent.close().await.unwrap();
        children.close().await.unwrap();
    })
    .await
    .unwrap();
}

struct ObserveMode(Arc<Mutex<Vec<Option<String>>>>);
#[async_trait]
impl Tool for ObserveMode {
    fn definition(&self) -> microcrab_protocol::ToolDefinition {
        microcrab_protocol::ToolDefinition {
            name: "observe".into(),
            description: "fixture".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        _: serde_json::Value,
        invocation: ToolContext,
    ) -> Result<ToolOutput, Error> {
        self.0
            .lock()
            .unwrap()
            .push(invocation.state.snapshot().selected_mode);
        Ok(ToolOutput::text("observed"))
    }
}
struct ModeScript {
    first: Mutex<bool>,
    prompts: Arc<Mutex<Vec<Option<serde_json::Value>>>>,
}
#[async_trait]
impl Client for ModeScript {
    async fn complete(
        &self,
        _: &crate::context::Content,
        _: &dyn Auditor,
        variables: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.prompts
            .lock()
            .unwrap()
            .push(variables.get("mode_prompt"));
        let first = std::mem::take(&mut *self.first.lock().unwrap());
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: if first {
                vec![
                    ToolCall {
                        provider_metadata: None,
                        id: "mode".into(),
                        name: "switch_mode".into(),
                        arguments: json!({"mode":"second"}),
                    },
                    ToolCall {
                        provider_metadata: None,
                        id: "observe".into(),
                        name: "observe".into(),
                        arguments: json!({}),
                    },
                ]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
async fn requested_mode_applies_after_the_original_batch_and_before_the_next_request() {
    let observed = Arc::new(Mutex::new(vec![]));
    let prompts = Arc::new(Mutex::new(vec![]));
    let runtime = crate::runtime::Runtime::new(
        vec![
            ToolRegistration::Builtin(BuiltinTool::SwitchMode),
            ToolRegistration::External(Arc::new(ObserveMode(observed.clone()))),
        ],
        Arc::new(AllowTools::new(["switch_mode", "observe"])),
    )
    .unwrap();
    let modes = [
        ("first", "Original instructions"),
        ("second", "Literal {{ instructions }}"),
    ]
    .into_iter()
    .map(|(name, prompt)| AgentMode {
        name: name.into(),
        description: name.into(),
        prompt: prompt.into(),
        role: AgentRole::MainAgent,
        permissions: vec![],
    })
    .collect();
    let session = Session::builder(
        ModeScript {
            first: Mutex::new(true),
            prompts: prompts.clone(),
        },
        runtime,
    )
    .agent(AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "mode-agent".into(),
        modes,
        tools: vec![],
        variables: Default::default(),
    })
    .build()
    .unwrap();
    let frozen = session.context_snapshot().unwrap();
    session.submit("switch then observe").await.unwrap();
    assert_eq!(*observed.lock().unwrap(), vec![Some("first".into())]);
    assert_eq!(
        *prompts.lock().unwrap(),
        vec![
            Some(json!("Original instructions")),
            Some(json!("Literal {{ instructions }}"))
        ]
    );
    assert_eq!(session.state().selected_mode.as_deref(), Some("second"));
    assert_eq!(session.context_snapshot().unwrap().system, frozen.system);
    assert_eq!(session.context_snapshot().unwrap().tools, frozen.tools);
    session.close().await.unwrap();
}

#[tokio::test]
async fn later_user_mode_selection_rejects_an_older_agent_transition() {
    use crate::runtime::tool::switch_mode::SessionControl;
    let runtime =
        crate::runtime::Runtime::new(vec![], Arc::new(AllowTools::new(Vec::<String>::new())))
            .unwrap();
    let modes = ["first", "requested", "user"]
        .into_iter()
        .map(|name| AgentMode {
            name: name.into(),
            description: name.into(),
            prompt: format!("{name} instructions"),
            role: AgentRole::MainAgent,
            permissions: vec![],
        })
        .collect();
    let session = Session::builder(Answer, runtime)
        .agent(AgentInfo {
            tool_media_output_bytes: Default::default(),
            name: "mode-race".into(),
            modes,
            tools: vec![],
            variables: Default::default(),
        })
        .build()
        .unwrap();
    let invocation = ToolContext::new(
        session.state_handle(),
        session.process_control(),
        CancellationToken::new(),
        "switch".into(),
        "switch_mode".into(),
        session.process_control().session_id().into(),
        "mode-race".into(),
        Some("first".into()),
    );
    session
        .inner
        .request_mode("requested", &invocation)
        .unwrap();
    session.select_mode("user").await.unwrap();
    assert!(session.inner.apply_pending_mode().is_err());
    assert_eq!(session.state().selected_mode.as_deref(), Some("user"));
    assert!(!session.is_faulted());
    let mut invocation = invocation;
    invocation.mode = Some("user".into());
    session
        .inner
        .request_mode("requested", &invocation)
        .unwrap();
    session.inner.discard_pending_mode();
    session.inner.apply_pending_mode().unwrap();
    assert_eq!(session.state().selected_mode.as_deref(), Some("user"));
    session.close().await.unwrap();
}

#[tokio::test]
async fn live_limits_apply_only_when_real_owners_can_accept_the_change() {
    let runtime =
        crate::runtime::Runtime::new(vec![], Arc::new(AllowTools::new(Vec::<String>::new())))
            .unwrap();
    let session = Session::builder(Answer, runtime).build().unwrap();
    let mut safe = session.state().limits;
    safe.max_rounds = 7;
    safe.interaction_timeout_ms = 4321;
    session.apply_limits(safe.clone()).await.unwrap();
    assert_eq!(session.state().limits, safe);
    let mut unsafe_change = safe.clone();
    unsafe_change.max_execution_buffers += 1;
    assert!(
        session
            .apply_limits(unsafe_change)
            .await
            .unwrap_err()
            .to_string()
            .contains("rebuild")
    );
    assert_eq!(
        session.state().limits,
        safe,
        "rejected capacity changes cannot lie about actual live owners"
    );
    session.close().await.unwrap();
}

struct ConfiguringFactory {
    factory: Factory,
    entered: Arc<Notify>,
    pending: bool,
}
#[async_trait]
impl ChildFactory for ConfiguringFactory {
    async fn create(
        &self,
        caller: Option<&str>,
        target: &str,
        mode: Option<&str>,
        context: &str,
        processes: Arc<dyn ProcessControl>,
    ) -> Result<ChildSession, Error> {
        self.factory
            .create(caller, target, mode, context, processes)
            .await
    }
    async fn configure(&self, _: &ChildSession) -> Result<(), Error> {
        self.entered.notify_one();
        if self.pending {
            std::future::pending().await
        } else {
            Err(Error::config("child policy restore rejected revision"))
        }
    }
}
#[tokio::test]
async fn failed_or_cancelled_child_configuration_closes_unpublished_prepared_owners() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for pending in [false, true] {
            let (sent, mut received) = mpsc::unbounded_channel();
            let audit = Arc::new(FlushGate {
                entered: Notify::new(),
                release: Semaphore::new(4),
            });
            let entered = Arc::new(Notify::new());
            let children = Children::default();
            let factory = ConfiguringFactory {
                factory: Factory { sent, audit },
                entered: entered.clone(),
                pending,
            };
            let runtime = crate::runtime::Runtime::new(
                vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],
                Arc::new(AllowTools::new(["delegate"])),
            )
            .unwrap()
            .with_delegate_executor(Arc::new(OwnedDelegateExecutor::new(
                children.clone(),
                Arc::new(factory),
            )));
            let parent = Session::builder(ParentScript(Mutex::new(true)), runtime)
                .catalog(vec![agent()])
                .build()
                .unwrap();
            parent.submit("configure child").await.unwrap();
            let child = received.recv().await.unwrap();
            entered.notified().await;
            assert!(
                parent.inspect_child("long-lived-child", "worker").is_err(),
                "configuration cannot publish a live child"
            );
            if pending {
                let id = parent
                    .execution_snapshots()
                    .into_iter()
                    .find(|row| row.tool == "delegate")
                    .unwrap()
                    .execution_id;
                parent.cancel_execution(id).await.unwrap();
            }
            let tasks = parent.executions();
            let mut changed = tasks.subscribe();
            while tasks.has_pending() {
                changed.changed().await.unwrap();
            }
            assert!(
                matches!(child.start().await, Err(Error::Closed)),
                "prepared child close is acknowledged before parent execution settles"
            );
            parent.close().await.unwrap();
            children.close().await.unwrap();
        }
    })
    .await
    .unwrap();
}

struct ControlScript(Mutex<Option<serde_json::Value>>);
#[async_trait]
impl Client for ControlScript {
    async fn complete(
        &self,
        content: &crate::context::Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let arguments = self.0.lock().unwrap().take();
        Ok(Response {
            provider_metadata: None,
            content: "parent settled".into(),
            tool_calls: arguments
                .into_iter()
                .map(|arguments| ToolCall {
                    provider_metadata: None,
                    id: format!("controlled-delegate-{}", content.items.len()),
                    name: "delegate".into(),
                    arguments,
                })
                .collect(),
            usage: Usage::default(),
        })
    }
}
#[tokio::test]
async fn existing_child_controls_do_not_require_start_discovery_and_keep_policy_denials() {
    tokio::time::timeout(std::time::Duration::from_secs(5),async {
        let (sent,mut received)=mpsc::unbounded_channel();
        let children=Children::default();
        let runtime=crate::runtime::Runtime::new(vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],Arc::new(AllowTools::new(["delegate"]))).unwrap().with_delegate_executor(Arc::new(OwnedDelegateExecutor::new(children.clone(),Arc::new(Factory {sent,audit:Arc::new(FlushGate {entered:Notify::new(),release:Semaphore::new(8)})}))));
        let script=Arc::new(ControlScript(Mutex::new(Some(json!({"target":"worker","input":"first task"})))));
        let parent=Session::builder_arc(script.clone(),runtime).catalog(vec![agent()]).build().unwrap();
        parent.submit("start child").await.unwrap();
        let child=received.recv().await.unwrap();
        let mut activity=child.subscribe_activity();
        while activity.borrow().outcome_sequence<1 {activity.changed().await.unwrap();}
        let snapshot=parent.permission_controller().snapshot().unwrap();
        let mut config=snapshot.config;
        config.review=false;
        config.overrides=serde_json::from_value(json!([
            {"tool":{"kind":"exact","value":"delegate"},"action":"deny"},
            {"tool":{"kind":"exact","value":"delegate"},"args":[{"pointer":"/action","matcher":{"kind":"exact","value":"close"}}],"action":"allow"}
        ])).unwrap();
        parent.replace_permissions(snapshot.revision,config).unwrap();
        // A retained child can outlive the next epoch's advertised start catalog.
        // Disable discovery explicitly so the regression cannot accidentally pass
        // through conservative unknown-argument directory eligibility.
        parent.state_handle().install_delegate_availability(false);
        assert!(parent.state_handle().resolve_delegate("worker",None).is_err());
        *script.0.lock().unwrap()=Some(json!({"action":"follow_up","target":"worker","child_id":"long-lived-child","input":"denied"}));
        parent.submit("denied follow-up").await.unwrap();
        assert_eq!(child.subscribe_activity().borrow().outcome_sequence,1,"current policy Deny still blocks follow-up effects");
        for (target,id) in [("worker","foreign-child"),("work","long-lived-child")] {
            *script.0.lock().unwrap()=Some(json!({"action":"close","target":target,"child_id":id}));
            parent.submit("reject foreign control").await.unwrap();
            assert!(parent.inspect_child("long-lived-child","worker").is_ok(),"controls cannot canonicalize aliases or acquire a foreign identity");
        }
        *script.0.lock().unwrap()=Some(json!({"action":"close","target":"worker","child_id":"long-lived-child"}));
        parent.submit("close exact owned child").await.unwrap();
        assert!(parent.inspect_child("long-lived-child","worker").is_err(),"explicitly allowed close does not depend on current start discovery");
        assert!(matches!(child.start().await,Err(Error::Closed)));
        assert!(parent.messages().iter().any(|item|matches!(item,crate::context::ContentItem::Tool {content,..} if serde_json::from_str::<serde_json::Value>(content).is_ok_and(|receipt|receipt["cleanup_acknowledged"]==true))));
        parent.close().await.unwrap();children.close().await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn return_child_publishes_one_reply_and_settles_only_after_cleanup_ack() {
    tokio::time::timeout(std::time::Duration::from_secs(5),async {
        struct ReturnScript(Mutex<bool>);
        #[async_trait]
        impl Client for ReturnScript {
            async fn complete(&self,_:&crate::context::Content,_:&dyn Auditor,_:&dyn VariableView,_:CancellationToken)
                ->Result<Response,microcrab_transport::Error>{
                let first=std::mem::take(&mut*self.0.lock().unwrap());
                Ok(Response{content:"parent".into(),provider_metadata:None,usage:Usage::default(),tool_calls:if first{
                    vec![ToolCall{id:"return-work".into(),name:"delegate".into(),arguments:json!({"target":"worker","input":"focused task","interaction":"return"}),provider_metadata:None}]
                }else{vec![]}})
            }
        }
        let(sent,mut received)=mpsc::unbounded_channel();
        let audit=Arc::new(FlushGate{entered:Notify::new(),release:Semaphore::new(0)});
        let children=Children::default();
        let runtime=crate::runtime::Runtime::new(vec![ToolRegistration::Builtin(BuiltinTool::Delegate)],Arc::new(AllowTools::new(["delegate"]))).unwrap()
            .with_delegate_executor(Arc::new(OwnedDelegateExecutor::new(children.clone(),Arc::new(Factory{sent,audit:audit.clone()}))));
        let parent=Session::builder(ReturnScript(Mutex::new(true)),runtime).catalog(vec![agent()]).build().unwrap();
        parent.submit("run once").await.unwrap();
        let child=received.recv().await.unwrap();
        audit.entered.notified().await;
        assert_eq!(child.communication_identity().unwrap().interaction,crate::state::communication::SessionInteraction::Return);
        assert!(child.submit("forbidden").await.is_err());
        assert!(child.queue("forbidden").is_err());
        assert!(parent.executions().has_pending(),"flush acknowledgement still owns the parent execution");
        audit.release.add_permits(4);
        let mut changes=parent.executions().subscribe();
        while parent.executions().has_pending(){changes.changed().await.unwrap();}
        assert_eq!(child.subscribe_activity().borrow().outcome_sequence,1);
        assert!(child.submit("after close").await.is_err());
        children.close().await.unwrap();
        parent.close().await.unwrap();
    }).await.unwrap();
}
