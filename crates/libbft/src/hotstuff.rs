use std::{collections::HashMap, net::SocketAddr, time::Duration};

use bytes::Bytes;
use tokio::{sync::mpsc::channel, time::interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Span, warn};

use crate::event::{Emit, EventReceiver, EventSender, EventSenderSlot};

mod core;
mod workers;

pub mod events {
    use crate::event::Event;

    use super::core;

    pub struct HandleCommand;
    impl Event for HandleCommand {
        type Value = core::HotStuffCommand;
    }

    pub struct HandleMessage;
    impl Event for HandleMessage {
        type Value = core::HotStuffMessage;
    }

    pub struct Deliver;
    impl Event for Deliver {
        type Value = core::HotStuffNode;
    }

    pub enum EffectValue {
        BroadcastGeneric(core::HotStuffNode),
        SendVote(core::ReplicaIndex, core::BlockDigest, core::ReplicaIndex), // to, block, voter
    }
    pub struct Effect;
    impl Event for Effect {
        type Value = EffectValue;
    }
}

pub struct HotStuffProtocol {
    core: core::HotStuffCore<HotStuffCoreContextState>,

    command_tx: EventSender<events::HandleCommand>,
    command_rx: EventReceiver<events::HandleCommand>,
    message_tx: EventSender<events::HandleMessage>,
    message_rx: EventReceiver<events::HandleMessage>,
}

struct HotStuffCoreContextState {
    deliver_tx: Option<EventSender<events::Deliver>>,
    effect_tx: Option<EventSender<events::Effect>>,
}

impl HotStuffProtocol {
    pub fn new(config: core::HotStuffCoreConfig) -> Self {
        let (command_tx, command_rx) = channel(1000);
        let (message_tx, message_rx) = channel(1000);
        let context = HotStuffCoreContextState {
            deliver_tx: None,
            effect_tx: None,
        };
        Self {
            core: core::HotStuffCore::new(context, config),
            command_tx,
            command_rx,
            message_tx,
            message_rx,
        }
    }

    pub fn register(
        &mut self,
        mut emit_command: impl Emit<events::HandleCommand>,
        mut emit_message: impl Emit<events::HandleMessage>,
    ) {
        emit_command.install(self.command_tx.clone());
        emit_message.install(self.message_tx.clone());
    }
}

impl EventSenderSlot<events::Deliver> for HotStuffProtocol {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::Deliver>> {
        &mut self.core.context.deliver_tx
    }
}

impl EventSenderSlot<events::Effect> for HotStuffProtocol {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::Effect>> {
        &mut self.core.context.effect_tx
    }
}

impl HotStuffProtocol {
    pub async fn run(&mut self, token: &CancellationToken) {
        let mut interval = interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                () = token.cancelled() => break,
                Some((command, span)) = self.command_rx.recv() => {
                    self.core.on_command(command).instrument(span).await;
                }
                Some((message, span)) = self.message_rx.recv() => {
                    self.core.on_message(message).instrument(span).await;
                }
                now = interval.tick() => {
                    self.core.on_tick(now).await;
                }
            }
        }
    }
}

impl core::HotStuffCoreContext for HotStuffCoreContextState {
    async fn deliver(&mut self, node: core::HotStuffNode) {
        if let Err(err) = self
            .deliver_tx
            .as_ref()
            .unwrap()
            .send((node, Span::current()))
            .await
        {
            warn!("Failed to send deliver event: {err:#}");
        }
    }

    async fn broadcast_generic(&mut self, node: core::HotStuffNode) {
        if let Err(err) = self
            .effect_tx
            .as_ref()
            .unwrap()
            .send((events::EffectValue::BroadcastGeneric(node), Span::current()))
            .await
        {
            warn!("Failed to send broadcast event: {err:#}");
        }
    }

    async fn send_vote(
        &mut self,
        to: core::ReplicaIndex,
        block: core::BlockDigest,
        replica_index: core::ReplicaIndex, // could be `to` for loopback voting
    ) {
        if let Err(err) = self
            .effect_tx
            .as_ref()
            .unwrap()
            .send((
                events::EffectValue::SendVote(to, block, replica_index),
                Span::current(),
            ))
            .await
        {
            warn!("Failed to send vote event: {err:#}");
        }
    }

    async fn make_quorum_cert(
        &mut self,
        block: core::BlockDigest,
        sigs: impl IntoIterator<Item = (core::ReplicaIndex, crate::crypto::PartialSigBytes)>,
    ) {
        //
    }
}

pub struct HotStuffEgressWorker<C> {
    state: workers::HotStuffWorker<C>,
    replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>, // excluding self

    effect_tx: EventSender<events::Effect>,
    effect_rx: EventReceiver<events::Effect>,

    bytes_tx: Option<EventSender<crate::network::events::SendBytes>>,
    message_tx: Option<EventSender<events::HandleMessage>>,
}

impl<C> HotStuffEgressWorker<C> {
    pub fn new(context: C, replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>) -> Self {
        let (effect_tx, effect_rx) = channel(1000);
        Self {
            state: workers::HotStuffWorker::new(context),
            replica_addrs,
            effect_tx,
            effect_rx,
            bytes_tx: None,
            message_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_effect: impl Emit<events::Effect>) {
        emit_effect.install(self.effect_tx.clone());
    }
}

impl<C> EventSenderSlot<crate::network::events::SendBytes> for HotStuffEgressWorker<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<crate::network::events::SendBytes>> {
        &mut self.bytes_tx
    }
}

impl<C> EventSenderSlot<events::HandleMessage> for HotStuffEgressWorker<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::HandleMessage>> {
        &mut self.message_tx
    }
}

impl<C: workers::HotStuffCryptoContext> HotStuffEgressWorker<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some((effect, span))) =
            token.run_until_cancelled(self.effect_rx.recv()).await
        {
            match effect {
                events::EffectValue::BroadcastGeneric(node) => {
                    let (bytes, block) = self.state.egress_generic(&node);
                    let bytes = Bytes::from(bytes);
                    for (&replica_index, &addr) in &self.replica_addrs {
                        if let Err(err) = self
                            .bytes_tx
                            .as_ref()
                            .unwrap()
                            .send(((addr, bytes.clone()), span.clone()))
                            .await
                        {
                            warn!("Failed to broadcast to replica {replica_index}: {err:#}");
                        }
                    }
                    if let Err(err) = self
                        .message_tx
                        .as_ref()
                        .unwrap()
                        .send((core::HotStuffMessage::Generic(block, node), span))
                        .await
                    {
                        warn!("Failed to send loopback generic message: {err:#}");
                    }
                }
                events::EffectValue::SendVote(to, block, replica_index) => {
                    if to == replica_index {
                        let partial_sig = self.state.sign_vote(&block);
                        if let Err(err) = self
                            .message_tx
                            .as_ref()
                            .unwrap()
                            .send((
                                core::HotStuffMessage::Vote(block, replica_index, partial_sig),
                                span,
                            ))
                            .await
                        {
                            warn!("Failed to send loopback vote message: {err:#}");
                        }
                    } else {
                        let bytes = self.state.egress_vote(block, replica_index);
                        if let Err(err) = self
                            .bytes_tx
                            .as_ref()
                            .unwrap()
                            .send(((self.replica_addrs[&to], bytes.into()), span))
                            .await
                        {
                            warn!("Failed to send vote to replica {to}: {err:#}");
                        }
                    }
                }
            }
        }
    }
}

pub struct HotStuffIngressWorker<C> {
    state: workers::HotStuffWorker<C>,

    bytes_tx: EventSender<crate::network::events::HandleBytes>,
    bytes_rx: EventReceiver<crate::network::events::HandleBytes>,

    message_tx: Option<EventSender<events::HandleMessage>>,
}

impl<C> HotStuffIngressWorker<C> {
    pub fn new(context: C) -> Self {
        let (bytes_tx, bytes_rx) = channel(1000);
        Self {
            state: workers::HotStuffWorker::new(context),
            bytes_tx,
            bytes_rx,
            message_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_bytes: impl Emit<crate::network::events::HandleBytes>) {
        emit_bytes.install(self.bytes_tx.clone());
    }
}

impl<C> EventSenderSlot<events::HandleMessage> for HotStuffIngressWorker<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::HandleMessage>> {
        &mut self.message_tx
    }
}

impl<C: workers::HotStuffCryptoContext> HotStuffIngressWorker<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some((bytes, span))) = token.run_until_cancelled(self.bytes_rx.recv()).await
        {
            match self.state.ingress(bytes.as_ref()) {
                Ok(message) => {
                    if let Err(err) = self
                        .message_tx
                        .as_ref()
                        .unwrap()
                        .send((message, span))
                        .await
                    {
                        warn!("Failed to send message: {err:#}");
                    }
                }
                Err(err) => {
                    warn!("Failed to parse message: {err:#}")
                }
            }
        }
    }
}
