//! Directed exchange reservation stays live; durable accepted carriers survive it.
use super::*;
use crate::runtime::messaging::{AgentDirectory, AgentPeer, Exchange};
use crate::runtime::tool::ToolContext;
use crate::state::communication::inbox::{AcceptedCarrier, QueuedReceipt};
use crate::state::communication::{
    AgentId, Correspondent, CorrespondentRelation, SessionInteraction,
};
use microcrab_transport::{AgentExchange, ExchangeIntent, UserSource};
use sha2::{Digest, Sha256};

impl SessionInner {
    fn communication_caller(
        &self,
        invocation: &ToolContext,
    ) -> Result<ConversationIdentity, Error> {
        self.ensure_model_open()?;
        invocation
            .ensure_admitted(None)
            .map_err(|error| error.into_runtime_error())?;
        if !invocation.state.same_owner(&self.state)
            || invocation.session_id != self.state.processes().session_id()
        {
            return Err(Error::denied(
                "message invocation belongs to another Session",
            ));
        }
        let identity = self
            .communication_identity()
            .ok_or_else(|| Error::denied("legacy Session has no messaging authority"))?;
        if identity.interaction != SessionInteraction::Persistent {
            return Err(Error::denied("return Session has no messaging authority"));
        }
        Ok(identity)
    }
    fn message_operation(&self, invocation: &ToolContext) -> String {
        format!(
            "msg_{:x}",
            Sha256::digest(
                format!(
                    "{}:{}:{}",
                    invocation.session_id,
                    invocation.execution_id.map_or(0, |id| id.0),
                    invocation.call_id
                )
                .as_bytes()
            )
        )
    }
    fn outgoing_carrier(
        &self,
        identity: &ConversationIdentity,
        target: &dyn AgentPeer,
        operation_id: String,
        body: String,
        exchange: Option<AgentExchange>,
    ) -> Result<AcceptedCarrier, Error> {
        crate::state::communication::inbox::validate_body(&body)?;
        let target_identity = target
            .communication_identity()
            .ok_or_else(|| Error::denied("target lacks identity"))?;
        let relation = if identity.parent_agent_id.as_ref() == Some(&target_identity.agent_id) {
            CorrespondentRelation::Child
        } else {
            CorrespondentRelation::Parent
        };
        Ok(AcceptedCarrier {
            operation_id,
            ordinal: 0,
            body,
            source: UserSource::Agent {
                agent_id: identity.agent_id.as_str().into(),
                exchange,
            },
            correspondent: Some(Correspondent {
                agent_id: identity.agent_id.clone(),
                agent: self.agent_name(),
                relation,
                interaction: identity.interaction,
            }),
            speaker: None,
        })
    }
    pub(super) async fn send_message_owned(
        &self,
        arguments: Value,
        invocation: ToolContext,
    ) -> Result<QueuedReceipt, Error> {
        let owner = self.self_weak.upgrade().ok_or(Error::Closed)?;
        tokio::spawn(async move { owner.send_message_transaction(arguments, invocation).await })
            .await
            .map_err(|_| Error::tool("message exchange owner stopped unexpectedly"))?
    }
    async fn send_message_transaction(
        &self,
        arguments: Value,
        invocation: ToolContext,
    ) -> Result<QueuedReceipt, Error> {
        let identity = self.communication_caller(&invocation)?;
        let target = arguments
            .get("agent_id")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::config("send_message requires agent_id"))?;
        let target = AgentId::try_from(target.to_owned())?;
        let intent = arguments
            .get("intent")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::config("send_message requires intent"))?;
        let body = arguments
            .get("body")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::config("send_message requires body"))?
            .to_owned();
        let operation = self.message_operation(&invocation);
        let directory = self.communication_directory();
        let peer = directory.resolve(&identity.agent_id, &target)?;
        let mut ledger = directory.exchanges.lock().await;
        if invocation.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        match intent {
            "send" => {
                if arguments.get("exchange_id").is_some() {
                    return Err(Error::config("proactive send forbids exchange_id"));
                }
                if ledger.exchanges.iter().any(|row| {
                    row.sender == identity.agent_id && row.recipient == target && row.open
                }) {
                    return Err(Error::denied("directed pair already has an open exchange"));
                }
                crate::budget::check(
                    crate::LimitKind::InboxEntries,
                    ledger.exchanges.len().saturating_add(1),
                    4096,
                )?;
                let exchange_id = format!("ex_{}", uuid::Uuid::new_v4().simple());
                let carrier = self.outgoing_carrier(
                    &identity,
                    peer.as_ref(),
                    operation.clone(),
                    body,
                    Some(AgentExchange {
                        exchange_id: exchange_id.clone(),
                        intent: ExchangeIntent::Send,
                    }),
                )?;
                // Hold one owner across durable enqueue: another sender/reply cannot
                // see an approval before the target has acknowledged acceptance.
                let receipt = peer.enqueue_authenticated(carrier).await?;
                ledger.exchanges.push(Exchange {
                    id: exchange_id,
                    sender: identity.agent_id,
                    recipient: target,
                    open: true,
                });
                Ok(receipt)
            }
            "reply" => {
                let exchange_id = arguments
                    .get("exchange_id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| Error::config("reply requires exchange_id"))?;
                let index = ledger
                    .exchanges
                    .iter()
                    .position(|row| {
                        row.id == exchange_id
                            && row.sender == target
                            && row.recipient == identity.agent_id
                            && row.open
                    })
                    .ok_or_else(|| {
                        Error::denied(
                            "reply does not name an open directed exchange for this recipient",
                        )
                    })?;
                let carrier = self.outgoing_carrier(
                    &identity,
                    peer.as_ref(),
                    operation,
                    body,
                    Some(AgentExchange {
                        exchange_id: exchange_id.into(),
                        intent: ExchangeIntent::Reply,
                    }),
                )?;
                let receipt = peer.enqueue_authenticated(carrier).await?;
                ledger.exchanges[index].open = false;
                Ok(receipt)
            }
            _ => Err(Error::config("unknown send_message intent")),
        }
    }
    pub(super) async fn escalate_owned(
        &self,
        arguments: Value,
        invocation: ToolContext,
    ) -> Result<QueuedReceipt, Error> {
        let identity = self.communication_caller(&invocation)?;
        let parent = identity
            .parent_agent_id
            .as_ref()
            .ok_or_else(|| Error::denied("root Session has no escalation target"))?;
        let body = arguments
            .get("body")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::config("escalate requires body"))?
            .to_owned();
        let directory = self.communication_directory();
        let peer = directory.resolve(&identity.agent_id, parent)?;
        let carrier = self.outgoing_carrier(
            &identity,
            peer.as_ref(),
            self.message_operation(&invocation),
            body,
            None,
        )?;
        peer.enqueue_authenticated(carrier).await
    }
}
