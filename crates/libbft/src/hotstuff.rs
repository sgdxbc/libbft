use std::{collections::HashMap, net::SocketAddr, time::Duration};

use bytes::Bytes;
use tokio::{sync::mpsc::channel, time::interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Span, warn};

use crate::event::{Emit, EventReceiver, EventSender, EventSenderSlot};

mod core;
mod workers;

pub mod events {
    use crate::{crypto::PartialSigBytes, event::Event};

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

    pub enum SendMessageValue {
        BroadcastGeneric(core::HotStuffNode),
        Vote(core::ReplicaIndex, core::BlockDigest, core::ReplicaIndex), // to, block, voter
    }
    pub struct SendMessage;
    impl Event for SendMessage {
        type Value = SendMessageValue;
    }

    pub struct MakeQuorumCert;
    impl Event for MakeQuorumCert {
        type Value = (
            core::BlockDigest,
            Vec<(core::ReplicaIndex, PartialSigBytes)>,
        );
    }

    pub struct HandleQuorumCert;
    impl Event for HandleQuorumCert {
        type Value = core::QuorumCert;
    }
}

pub struct HotStuffProtocol {
    core: core::HotStuffCore<HotStuffCoreContextState>,

    command_tx: EventSender<events::HandleCommand>,
    command_rx: EventReceiver<events::HandleCommand>,
    message_tx: EventSender<events::HandleMessage>,
    message_rx: EventReceiver<events::HandleMessage>,
    quorum_cert_tx: EventSender<events::HandleQuorumCert>,
    quorum_cert_rx: EventReceiver<events::HandleQuorumCert>,
}

struct HotStuffCoreContextState {
    deliver_tx: Option<EventSender<events::Deliver>>,
    send_message_tx: Option<EventSender<events::SendMessage>>,
    make_quorum_cert_tx: Option<EventSender<events::MakeQuorumCert>>,
}

impl HotStuffProtocol {
    pub fn new(config: core::HotStuffCoreConfig) -> Self {
        let (command_tx, command_rx) = channel(1000);
        let (message_tx, message_rx) = channel(1000);
        let (quorum_cert_tx, quorum_cert_rx) = channel(1000);
        let context = HotStuffCoreContextState {
            deliver_tx: None,
            send_message_tx: None,
            make_quorum_cert_tx: None,
        };
        Self {
            core: core::HotStuffCore::new(context, config),
            command_tx,
            command_rx,
            message_tx,
            message_rx,
            quorum_cert_tx,
            quorum_cert_rx,
        }
    }

    pub fn register(
        &mut self,
        mut emit_command: impl Emit<events::HandleCommand>,
        mut emit_message: impl Emit<events::HandleMessage>,
        mut emit_quorum_cert: impl Emit<events::HandleQuorumCert>,
    ) {
        emit_command.install(self.command_tx.clone());
        emit_message.install(self.message_tx.clone());
        emit_quorum_cert.install(self.quorum_cert_tx.clone());
    }
}

impl EventSenderSlot<events::Deliver> for HotStuffProtocol {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::Deliver>> {
        &mut self.core.context.deliver_tx
    }
}

impl EventSenderSlot<events::SendMessage> for HotStuffProtocol {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::SendMessage>> {
        &mut self.core.context.send_message_tx
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
                Some((quorum_cert, span)) = self.quorum_cert_rx.recv() => {
                    self.core.on_quorum_cert(quorum_cert).instrument(span).await;
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
            .send_message_tx
            .as_ref()
            .unwrap()
            .send((
                events::SendMessageValue::BroadcastGeneric(node),
                Span::current(),
            ))
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
            .send_message_tx
            .as_ref()
            .unwrap()
            .send((
                events::SendMessageValue::Vote(to, block, replica_index),
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
        if let Err(err) = self
            .make_quorum_cert_tx
            .as_ref()
            .unwrap()
            .send(((block, sigs.into_iter().collect()), Span::current()))
            .await
        {
            warn!("Failed to send make quorum cert event: {err:#}");
        }
    }
}

pub struct HotStuffEgressWorker<C> {
    state: workers::HotStuffWorker<C>,
    replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>, // excluding self

    send_message_tx: EventSender<events::SendMessage>,
    send_message_rx: EventReceiver<events::SendMessage>,

    bytes_tx: Option<EventSender<crate::network::events::SendBytes>>,
    message_tx: Option<EventSender<events::HandleMessage>>,
}

impl<C> HotStuffEgressWorker<C> {
    pub fn new(context: C, replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>) -> Self {
        let (send_message_tx, send_message_rx) = channel(1000);
        Self {
            state: workers::HotStuffWorker::new(context),
            replica_addrs,
            send_message_tx,
            send_message_rx,
            bytes_tx: None,
            message_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_effect: impl Emit<events::SendMessage>) {
        emit_effect.install(self.send_message_tx.clone());
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
            token.run_until_cancelled(self.send_message_rx.recv()).await
        {
            match effect {
                events::SendMessageValue::BroadcastGeneric(node) => {
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
                events::SendMessageValue::Vote(to, block, replica_index) => {
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

pub struct HotStuffQuorumCertWorker<C> {
    context: C,

    make_quorum_cert_tx: EventSender<events::MakeQuorumCert>,
    make_quorum_cert_rx: EventReceiver<events::MakeQuorumCert>,

    quorum_cert_tx: Option<EventSender<events::HandleQuorumCert>>,
}

impl<C> HotStuffQuorumCertWorker<C> {
    pub fn new(context: C) -> Self {
        let (make_quorum_cert_tx, make_quorum_cert_rx) = channel(1000);
        Self {
            context,
            make_quorum_cert_tx,
            make_quorum_cert_rx,
            quorum_cert_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_make_quorum_cert: impl Emit<events::MakeQuorumCert>) {
        emit_make_quorum_cert.install(self.make_quorum_cert_tx.clone());
    }
}

impl<C> EventSenderSlot<events::HandleQuorumCert> for HotStuffQuorumCertWorker<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::HandleQuorumCert>> {
        &mut self.quorum_cert_tx
    }
}

impl<C: workers::HotStuffCryptoContext> HotStuffQuorumCertWorker<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some(((block, sigs), span))) = token
            .run_until_cancelled(self.make_quorum_cert_rx.recv())
            .await
        {
            let sig = match self.context.combine(sigs) {
                Ok(sig) => sig,
                Err(err) => {
                    warn!("Failed to combine quorum cert signatures: {err:#}");
                    continue;
                }
            };
            let quorum_cert = core::QuorumCert { block, sig };
            if let Err(err) = self
                .quorum_cert_tx
                .as_ref()
                .unwrap()
                .send((quorum_cert, span))
                .await
            {
                warn!("Failed to send quorum cert: {err:#}");
            }
        }
    }
}
