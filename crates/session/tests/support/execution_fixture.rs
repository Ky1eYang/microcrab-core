//! Test-only admission harness for real Host-backed generic executions.
// Integration targets compile this module separately and use different helper subsets.
#![allow(dead_code)]
use async_trait::async_trait;
use microcrab_host_api::{Host, ShellRequest, process::ProcessHandle};
use microcrab_protocol::{Response, ToolCall, ToolDefinition};
use microcrab_session::{
    Auditor, Client, Error, Limits, Session, VariableView,
    context::{Content, ContentItem},
    runtime::{
        BuiltinTool, Runtime, ToolRegistration,
        execution::{ExecutionId, ExecutionSnapshot, OutputPage, ToolOutput},
        tool::{DenyAll, Tool, ToolContext},
    },
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub struct Model {
    pending: Mutex<Option<ToolCall>>,
}
#[async_trait]
impl Client for Model {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: self.pending.lock().unwrap().take().into_iter().collect(),
            usage: Default::default(),
        })
    }
}
struct Launch {
    handles: Arc<Mutex<Vec<ProcessHandle>>>,
}
#[async_trait]
impl Tool for Launch {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "fixture_run".into(),
            description: "Real Host producer".into(),
            parameters: json!({"type":"object","required":["command"],"properties":{"command":{"type":"string"},"yield_ms":{"type":"integer","minimum":0}},"additionalProperties":false}),
        }
    }
    async fn execute(&self, args: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let handle = context
            .processes
            .launch_shell(
                &context.tool,
                &context.call_id,
                ShellRequest {
                    command: args["command"].as_str().unwrap().into(),
                    cwd: None,
                    // Host capture admits a declared bounded cap; usize::MAX is
                    // not an unbounded-capture sentinel. Session may narrow it.
                    output_bytes: context
                        .state
                        .snapshot()
                        .limits
                        .output_buffer_bytes
                        .min(microcrab_host_local::ShellLimits::default().max_output_bytes),
                },
                context.cancellation.clone(),
            )
            .await?;
        self.handles.lock().unwrap().push(handle);
        let output = context
            .adopt_process_output(handle, vec!["stdout".into(), "stderr".into()], true)
            .await?;
        let window = args["yield_ms"].as_u64().unwrap_or(0);
        if window > 0 {
            let observe = async {
                loop {
                    let page = output.snapshot(65536)?;
                    if page.closed {
                        return Ok::<(), Error>(());
                    }
                    output
                        .wait_for_change(page.revision, context.cancellation.clone())
                        .await?;
                }
            };
            tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => return Err(Error::Cancelled),
                result = observe => result?,
                _ = tokio::time::sleep(Duration::from_millis(window)) => {},
            }
        }
        Ok(ToolOutput::Buffered(output.with_initial_output(true)))
    }
}

pub struct Fixture {
    pub session: Session,
    pub handles: Arc<Mutex<Vec<ProcessHandle>>>,
    pub owner: String,
    model: Arc<Model>,
    next: AtomicU64,
}
impl Fixture {
    pub fn new(owner: &str, host: Arc<dyn Host>, limits: Limits) -> Self {
        let handles = Arc::new(Mutex::new(vec![]));
        let model = Arc::new(Model {
            pending: Mutex::new(None),
        });
        let runtime = Runtime::new(
            vec![
                ToolRegistration::External(Arc::new(Launch {
                    handles: handles.clone(),
                })),
                ToolRegistration::Builtin(BuiltinTool::Job),
            ],
            Arc::new(DenyAll),
        )
        .unwrap();
        let permissions = serde_json::from_value(json!({"overrides":[
            {"tool":{"kind":"exact","value":"fixture_run"},"action":"allow"},
            {"tool":{"kind":"exact","value":"job"},"action":"allow"}
        ]}))
        .unwrap();
        let session = Session::builder_arc(model.clone(), runtime)
            .session_id(owner)
            .host(host)
            .permissions(permissions)
            .limits(limits)
            .build()
            .unwrap();
        Self {
            session,
            handles,
            owner: owner.into(),
            model,
            next: AtomicU64::new(1),
        }
    }
    pub async fn invoke(&self, name: &str, args: Value) -> Result<String, Error> {
        let call_id = format!("fixture-call-{}", self.next.fetch_add(1, Ordering::SeqCst));
        *self.model.pending.lock().unwrap() = Some(ToolCall {
            provider_metadata: None,
            id: call_id.clone(),
            name: name.into(),
            arguments: args,
        });
        self.session.submit("fixture operation").await?;
        self.session
            .messages()
            .into_iter()
            .find_map(|item| match item {
                ContentItem::Tool {
                    call_id: id,
                    content,
                } if id == call_id => Some(content),
                _ => None,
            })
            .ok_or_else(|| Error::Tool("missing fixture result".into()))
    }
    pub async fn start(&self, command: &str, yield_ms: u64) -> Result<Value, Error> {
        let output = self
            .invoke(
                "fixture_run",
                json!({"command":command,"yield_ms":yield_ms}),
            )
            .await?;
        let receipt: Value = serde_json::from_str(&output)?;
        if receipt.get("execution_id").is_none() {
            return Err(Error::Tool(output.into()));
        }
        Ok(receipt)
    }
    pub fn id(&self, address: &str) -> Result<ExecutionId, Error> {
        let (owner, id) = address
            .rsplit_once('/')
            .ok_or_else(|| Error::Tool("exact execution address required".into()))?;
        if owner != self.owner {
            return Err(Error::Denied("execution belongs to another Session".into()));
        }
        let id = id
            .parse::<u64>()
            .ok()
            .filter(|id| *id > 0)
            .ok_or_else(|| Error::Tool("invalid execution id".into()))?;
        Ok(ExecutionId(id))
    }
    pub fn read(&self, address: &str, cursor: u64, maximum: usize) -> Result<OutputPage, Error> {
        self.session
            .read_execution_output(self.id(address)?, cursor, maximum)
    }
    pub async fn action(&self, address: &str, action: &str, args: Value) -> Result<Value, Error> {
        let id = self.id(address)?;
        let registry = self.session.executions();
        Ok(match action {
            "status" => serde_json::to_value(registry.get(id)?)?,
            "read" => serde_json::to_value(registry.read(
                id,
                args["cursor"].as_u64().unwrap_or(0),
                65536,
            )?)?,
            "write" => {
                registry
                    .write(
                        id,
                        args["data"].as_str().unwrap().into(),
                        args["eof"].as_bool().unwrap_or(false),
                        CancellationToken::new(),
                    )
                    .await?;
                json!({"accepted":true})
            }
            "cancel" => {
                self.session.cancel_execution(id).await?;
                serde_json::to_value(registry.get(id)?)?
            }
            "release" => {
                registry.release(id, CancellationToken::new()).await?;
                serde_json::to_value(registry.get(id)?)?
            }
            _ => return Err(Error::Tool("unsupported fixture action".into())),
        })
    }
    pub async fn terminal(&self, address: &str) -> ExecutionSnapshot {
        let id = self.id(address).unwrap();
        let mut changed = self.session.executions().subscribe();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let row = self.session.executions().get(id).unwrap();
                if !row.status.is_pending() {
                    return row;
                }
                changed.changed().await.unwrap();
            }
        })
        .await
        .expect("real producer must reach its reported terminal state")
    }
    pub async fn finish(&self, address: &str, text: &str) -> ExecutionSnapshot {
        self.action(
            address,
            "write",
            json!({"data":format!("{text}\n"),"eof":true}),
        )
        .await
        .unwrap();
        self.terminal(address).await
    }
    pub async fn close(&self) -> Result<(), Error> {
        self.session.close().await
    }
}
pub fn channel(page: &OutputPage, name: &str) -> String {
    page.chunks
        .iter()
        .filter(|chunk| chunk.channel.as_deref() == Some(name))
        .map(|chunk| chunk.content.as_str())
        .collect()
}
pub fn id(receipt: &Value) -> &str {
    receipt["id"].as_str().unwrap()
}
pub fn waiting() -> &'static str {
    if cfg!(windows) {
        "$line = [Console]::ReadLine(); [Console]::Out.WriteLine($line)"
    } else {
        "IFS= read -r line; printf '%s\\n' \"$line\""
    }
}
pub fn sleeping() -> &'static str {
    if cfg!(windows) {
        "Start-Sleep -Seconds 30"
    } else {
        "sleep 30"
    }
}
