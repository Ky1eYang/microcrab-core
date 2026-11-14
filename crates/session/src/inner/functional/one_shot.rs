//! Operation-scoped execution uses Session Context, State and close owners.
#[cfg(test)]
mod tests;
use super::*;
use crate::runtime::handler::RoundRecorder;
use crate::runtime::subagents::OneShotExecutor;
use microcrab_auditor::VariableView;

#[derive(Default)]
pub(crate) struct SessionOneShot {
    pub(crate) children: crate::Children,
}

// Issuer values are read-only inputs. Child framework facts and hook mutations
// come from its own State; no variable endpoint or parent mutation capability.
struct FunctionalVariables {
    owned: State,
    issuer: Arc<dyn VariableView>,
}
impl VariableView for FunctionalVariables {
    fn get(&self, name: &str) -> Option<Value> {
        self.owned.get(name).or_else(|| {
            if crate::state::access::validate_custom_key(name).is_err() {
                None
            } else {
                self.issuer.get(name)
            }
        })
    }
    fn get_all(&self, name: &str) -> Option<Value> {
        self.owned.get_all(name).or_else(|| {
            if crate::state::access::validate_custom_key(name).is_err() {
                None
            } else {
                self.issuer.get_all(name)
            }
        })
    }
}

// Lifecycle observations carry the same functional identity as model usage.
// Mutation remains with the child's BoundAuditor surrounding this raw sink.
struct FunctionalAudit {
    raw: Arc<dyn Auditor>,
    definition: String,
}
impl Auditor for FunctionalAudit {
    fn is_enabled(&self) -> bool {
        self.raw.is_enabled()
    }
    fn publish(&self, moment: microcrab_auditor::Moment, mut body: microcrab_auditor::AuditBody) {
        body.subagent = Some(self.definition.clone());
        if self.raw.is_enabled() {
            self.raw.publish(moment, body);
        }
    }
    fn intervene<'a>(
        &'a self,
        moment: microcrab_auditor::Moment,
        mut context: microcrab_auditor::HookContext<'a>,
    ) -> microcrab_auditor::BoxFuture<
        'a,
        Result<microcrab_auditor::Injection, microcrab_auditor::AuditorError>,
    > {
        context.subagent = Some(self.definition.clone());
        self.raw.intervene(moment, context)
    }
    fn flush(
        &self,
    ) -> microcrab_auditor::BoxFuture<'_, Result<(), microcrab_auditor::AuditorError>> {
        self.raw.flush()
    }
}

#[async_trait::async_trait]
impl OneShotExecutor for SessionOneShot {
    async fn close(&self) -> Result<(), Error> {
        self.children.close().await
    }
    async fn execute(
        &self,
        client: Arc<dyn Client>,
        auditor: Arc<dyn Auditor>,
        variables: Arc<dyn VariableView>,
        request: &crate::context::Content,
        cancellation: &CancellationToken,
    ) -> Result<microcrab_protocol::Response, Error> {
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let definition = request.subagent.clone().ok_or_else(|| {
            Error::config("one-shot execution requires a declared functional request")
        })?;
        let context = Context::from_functional_content(request)?;
        let token = cancellation.child_token();
        let _cancel = CancelOnDrop(token.clone());
        let completion = self.children.register(token.clone()).await?;
        let output_tokens = request.max_output_tokens;
        // A dropped review waiter cancels its operation, while this worker retains
        // actual Context/State and observation until acknowledged cleanup completes.
        tokio::spawn(async move {
            let runtime = Runtime::new(vec![], Arc::new(crate::runtime::tool::DenyAll))?;
            let mut limits = Limits::default();
            limits.max_context_bytes = context.max_bytes();
            limits.max_output_tokens = output_tokens;
            let session = crate::Session::builder_arc(client, runtime)
                .kind(crate::SessionKind::OneShot)
                .limits(limits)
                .auditor(FunctionalAudit {
                    raw: auditor,
                    definition: definition.clone(),
                })
                .build()?;
            session
                .inner
                .state
                .update(|state| state.epoch_id = context.epoch());
            *session
                .inner
                .context
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = context;
            let variables = FunctionalVariables {
                owned: session.inner.state.clone(),
                issuer: variables,
            };
            let result = AssertUnwindSafe(session.inner.execute_one_shot(
                &definition,
                &variables,
                &token,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(Error::lifecycle("one-shot model worker panicked")));
            let cleanup = session.close().await;
            completion.finish(
                cleanup
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| error.diagnostic_reason()),
            );
            cleanup?;
            if token.is_cancelled() {
                return Err(Error::Cancelled);
            }
            result
        })
        .await
        .map_err(|_| Error::lifecycle("one-shot owner failed before cleanup acknowledgement"))?
    }
}
impl SessionInner {
    async fn execute_one_shot(
        self: &Arc<Self>,
        definition: &str,
        variables: &dyn VariableView,
        token: &CancellationToken,
    ) -> Result<microcrab_protocol::Response, Error> {
        if self.kind != crate::SessionKind::OneShot
            || self.one_shot_claimed.swap(true, Ordering::AcqRel)
        {
            return Err(Error::Closed);
        }
        // Independent operation admission; this never acquires the issuing parent
        // foreground lock or enters its user-input loop.
        let _reservation = self.reserve().await?;
        self.register_command_token(token)?;
        let _active = TurnGuard {
            inner: self.clone(),
            clear: false,
        };
        self.initialize(token).await?;
        let mut context = self
            .context
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut request = context.content(self.state().limits.max_output_tokens)?;
        request.subagent = Some(definition.to_owned());
        request.turn_id = None;
        request.round = None;
        let response = tokio::select! { biased;
            _=token.cancelled()=>return Err(Error::Cancelled),
            response=self.client.complete(&request,self.auditor.as_ref(),variables,token.clone())=>response.map_err(Error::from)?,
        };
        if !response.tool_calls.is_empty() {
            return Err(Error::protocol(
                "one-shot functional operation returned tool calls; dispatch is forbidden",
            ));
        }
        tokio::select! {biased;
            _=token.cancelled()=>return Err(Error::Cancelled),
            result=self.auditor.intervene(microcrab_auditor::Moment::RoundCompleted,microcrab_auditor::HookContext {
                epoch:request.epoch,subagent:request.subagent.clone(),turn:None,round:None,call_id:None,request_body:None,max_request_bytes:None,variables,
                payload:microcrab_auditor::HookPayload::RoundCompleted {response:Some(response.clone()),status:None,usage:Some(response.usage.clone())},
            })=>{result.map_err(|e|Error::audit_with_private("functional completion hook failed",e.to_string()))?;}
        }
        context.append_response(&response)?;
        self.observe_context(&context)?;
        *self.context.lock().unwrap_or_else(|e| e.into_inner()) = context;
        Ok(response)
    }
}
