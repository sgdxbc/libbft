use std::{collections::HashMap, net::SocketAddr, time::Duration};

use bytes::Bytes;
use tokio::{sync::oneshot, time::interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Span, warn};

use crate::event::{Emit, EventChannel, EventSender, EventSenderSlot};

mod core;
mod workers;

pub use core::{HotStuffCommand, HotStuffCoreConfig, HotStuffParams};

pub mod events {
    use tokio::sync::oneshot;

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
        BroadcastGeneric(core::HotStuffNode, oneshot::Sender<core::BlockDigest>),
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

    pub type SendBytes = crate::network::events::SendBytes;
}

pub struct HotStuffProtocol {
    core: core::HotStuffCore<HotStuffCoreContextState>,

    command: EventChannel<events::HandleCommand>,
    message: EventChannel<events::HandleMessage>,
    quorum_cert: EventChannel<events::HandleQuorumCert>,
}

struct HotStuffCoreContextState {
    deliver_tx: Option<EventSender<events::Deliver>>,
    send_message_tx: Option<EventSender<events::SendMessage>>,
    make_quorum_cert_tx: Option<EventSender<events::MakeQuorumCert>>,
}

impl HotStuffProtocol {
    pub fn new(config: core::HotStuffCoreConfig) -> Self {
        let context = HotStuffCoreContextState {
            deliver_tx: None,
            send_message_tx: None,
            make_quorum_cert_tx: None,
        };
        Self {
            core: core::HotStuffCore::new(context, config),
            command: EventChannel::new(None),
            message: EventChannel::new(None),
            quorum_cert: EventChannel::new(None),
        }
    }

    pub fn register(
        &mut self,
        mut emit_command: impl Emit<events::HandleCommand>,
        mut emit_message: impl Emit<events::HandleMessage>,
        mut emit_quorum_cert: impl Emit<events::HandleQuorumCert>,
    ) {
        emit_command.install(self.command.sender());
        emit_message.install(self.message.sender());
        emit_quorum_cert.install(self.quorum_cert.sender());
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

impl EventSenderSlot<events::MakeQuorumCert> for HotStuffProtocol {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::MakeQuorumCert>> {
        &mut self.core.context.make_quorum_cert_tx
    }
}

impl HotStuffProtocol {
    pub async fn run(&mut self, token: &CancellationToken) {
        let mut interval = interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                () = token.cancelled() => break,
                Some((command, span)) = self.command.recv() => {
                    self.core.on_command(command).instrument(span).await;
                }
                Some((message, span)) = self.message.recv() => {
                    self.core.on_message(message).instrument(span).await;
                }
                Some((quorum_cert, span)) = self.quorum_cert.recv() => {
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
        self.deliver_tx
            .as_ref()
            .unwrap()
            .send(node, Span::current())
            .await;
    }

    async fn broadcast_generic(&mut self, node: core::HotStuffNode) -> Option<core::BlockDigest> {
        let (tx, rx) = oneshot::channel();
        self.send_message_tx
            .as_ref()
            .unwrap()
            .send(
                events::SendMessageValue::BroadcastGeneric(node, tx),
                Span::current(),
            )
            .await;
        rx.await.ok()
    }

    async fn send_vote(
        &mut self,
        to: core::ReplicaIndex,
        block: core::BlockDigest,
        replica_index: core::ReplicaIndex, // could be `to` for loopback voting
    ) {
        self.send_message_tx
            .as_ref()
            .unwrap()
            .send(
                events::SendMessageValue::Vote(to, block, replica_index),
                Span::current(),
            )
            .await;
    }

    async fn make_quorum_cert(
        &mut self,
        block: core::BlockDigest,
        sigs: impl IntoIterator<Item = (core::ReplicaIndex, crate::crypto::PartialSigBytes)>,
    ) {
        self.make_quorum_cert_tx
            .as_ref()
            .unwrap()
            .send((block, sigs.into_iter().collect()), Span::current())
            .await;
    }
}

pub struct HotStuffEgressWorker<C> {
    state: workers::HotStuffWorker<C>,
    replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>, // excluding self

    send_message: EventChannel<events::SendMessage>,

    bytes_tx: Option<EventSender<events::SendBytes>>,
    message_tx: Option<EventSender<events::HandleMessage>>,
}

impl<C> HotStuffEgressWorker<C> {
    pub fn new(context: C, replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>) -> Self {
        Self {
            state: workers::HotStuffWorker::new(context),
            replica_addrs,
            send_message: EventChannel::new(None),
            bytes_tx: None,
            message_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_effect: impl Emit<events::SendMessage>) {
        emit_effect.install(self.send_message.sender());
    }
}

impl<C> EventSenderSlot<events::SendBytes> for HotStuffEgressWorker<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::SendBytes>> {
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
        let bytes_tx = self.bytes_tx.as_ref().unwrap();
        while let Some(Some((effect, span))) =
            token.run_until_cancelled(self.send_message.recv()).await
        {
            match effect {
                events::SendMessageValue::BroadcastGeneric(node, tx) => {
                    let (bytes, block) = span.in_scope(|| self.state.egress_generic(&node));
                    if tx.send(block.clone()).is_err() {
                        warn!("Failed to send block digest back to core");
                    }
                    let bytes = Bytes::from(bytes);
                    for &addr in self.replica_addrs.values() {
                        bytes_tx.send((addr, bytes.clone()), span.clone()).await;
                    }
                    self.message_tx
                        .as_ref()
                        .unwrap()
                        .send(core::HotStuffMessage::Generic(block, node), span)
                        .await;
                }
                events::SendMessageValue::Vote(to, block, replica_index) => {
                    if to == replica_index {
                        let partial_sig = self.state.sign_vote(&block);
                        self.message_tx
                            .as_ref()
                            .unwrap()
                            .send(
                                core::HotStuffMessage::Vote(block, replica_index, partial_sig),
                                span,
                            )
                            .await;
                    } else {
                        let bytes = span.in_scope(|| self.state.egress_vote(block, replica_index));
                        bytes_tx
                            .send((self.replica_addrs[&to], bytes.into()), span)
                            .await;
                    }
                }
            }
        }
    }
}

pub struct HotStuffIngressWorker<C> {
    state: workers::HotStuffWorker<C>,

    bytes: EventChannel<crate::network::events::HandleBytes>,

    message_tx: Option<EventSender<events::HandleMessage>>,
}

impl<C> HotStuffIngressWorker<C> {
    pub fn new(context: C) -> Self {
        Self {
            state: workers::HotStuffWorker::new(context),
            bytes: EventChannel::new(None),
            message_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_bytes: impl Emit<crate::network::events::HandleBytes>) {
        emit_bytes.install(self.bytes.sender());
    }
}

impl<C> EventSenderSlot<events::HandleMessage> for HotStuffIngressWorker<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::HandleMessage>> {
        &mut self.message_tx
    }
}

impl<C: workers::HotStuffCryptoContext> HotStuffIngressWorker<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some((bytes, span))) = token.run_until_cancelled(self.bytes.recv()).await {
            match span.in_scope(|| self.state.ingress(bytes.as_ref())) {
                Ok(message) => {
                    self.message_tx.as_ref().unwrap().send(message, span).await;
                }
                Err(err) => warn!("Failed to parse message: {err:#}"),
            }
        }
    }
}

pub struct HotStuffQuorumCertWorker<C> {
    context: C,

    make_quorum_cert: EventChannel<events::MakeQuorumCert>,

    quorum_cert_tx: Option<EventSender<events::HandleQuorumCert>>,
}

impl<C> HotStuffQuorumCertWorker<C> {
    pub fn new(context: C) -> Self {
        Self {
            context,
            make_quorum_cert: EventChannel::new(None),
            quorum_cert_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_make_quorum_cert: impl Emit<events::MakeQuorumCert>) {
        emit_make_quorum_cert.install(self.make_quorum_cert.sender());
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
            .run_until_cancelled(self.make_quorum_cert.recv())
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
            self.quorum_cert_tx
                .as_ref()
                .unwrap()
                .send(quorum_cert, span)
                .await;
        }
    }
}
