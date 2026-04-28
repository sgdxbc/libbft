use std::{collections::HashMap, net::SocketAddr, time::Duration};

use bytes::Bytes;
use tokio::{select, sync::oneshot, time::interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Span, warn};

use crate::{
    event::{Emit, EventChannel, EventSender, EventSenderSlot},
    narwhal::workers::NarwhalWorker,
};

mod core;
mod workers;

pub use core::{Bullshark, NarwhalCoreConfig, NarwhalParams, NarwhalTxn};

pub mod events {
    use tokio::sync::oneshot;

    use crate::event::Event;

    use super::core;

    pub struct HandleTxn;
    impl Event for HandleTxn {
        type Value = core::NarwhalTxn;
    }

    pub struct HandleMessage;
    impl Event for HandleMessage {
        type Value = core::NarwhalMessage;
    }

    pub struct Deliver;
    impl Event for Deliver {
        type Value = core::NarwhalBlock;
    }

    pub enum SendMessageValue {
        Block(core::NarwhalBlock, oneshot::Sender<core::BlockHash>),
        Ack(
            core::BlockHash,
            core::RoundNum,
            core::ReplicaIndex,
            core::ReplicaIndex,
        ),
        Cert(core::NarwhalCert),
    }
    pub struct SendMessage;
    impl Event for SendMessage {
        type Value = SendMessageValue;
    }

    pub type HandleBytes = crate::network::events::HandleBytes;

    pub type SendBytes = crate::network::events::SendBytes;
}

pub struct NarwhalProtocol<C>
where
    core::NarwhalCore<NarwhalCoreContextState, C>: core::ConsensusProtocol,
{
    core: core::NarwhalCore<NarwhalCoreContextState, C>,

    txn: EventChannel<events::HandleTxn>,
    message: EventChannel<events::HandleMessage>,
}

pub struct NarwhalCoreContextState {
    deliver_tx: Option<EventSender<events::Deliver>>,
    send_message_tx: Option<EventSender<events::SendMessage>>,
}

impl NarwhalProtocol<core::Bullshark> {
    pub fn new(config: core::NarwhalCoreConfig<core::Bullshark>) -> Self {
        let context = NarwhalCoreContextState {
            deliver_tx: None,
            send_message_tx: None,
        };
        Self {
            core: core::NarwhalCore::new(context, config),
            txn: EventChannel::new(None),
            message: EventChannel::new(None),
        }
    }
}

impl<C> NarwhalProtocol<C>
where
    core::NarwhalCore<NarwhalCoreContextState, C>: core::ConsensusProtocol,
{
    pub fn register(
        &self,
        mut emit_txn: impl Emit<events::HandleTxn>,
        mut emit_message: impl Emit<events::HandleMessage>,
    ) {
        emit_txn.install(self.txn.sender());
        emit_message.install(self.message.sender());
    }
}

impl<C> EventSenderSlot<events::Deliver> for NarwhalProtocol<C>
where
    core::NarwhalCore<NarwhalCoreContextState, C>: core::ConsensusProtocol,
{
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::Deliver>> {
        &mut self.core.context.deliver_tx
    }
}

impl<C> EventSenderSlot<events::SendMessage> for NarwhalProtocol<C>
where
    core::NarwhalCore<NarwhalCoreContextState, C>: core::ConsensusProtocol,
{
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::SendMessage>> {
        &mut self.core.context.send_message_tx
    }
}

impl<C> NarwhalProtocol<C>
where
    core::NarwhalCore<NarwhalCoreContextState, C>: core::ConsensusProtocol,
{
    pub async fn run(&mut self, token: &CancellationToken) {
        let mut interval = interval(Duration::from_millis(100));
        self.core.on_init().await;
        loop {
            select! {
                () = token.cancelled() => break,
                Some((txn, span)) = self.txn.recv() => {
                    self.core.on_txn(txn).instrument(span).await;
                }
                Some((message, span)) = self.message.recv() => {
                    self.core.on_message(message).instrument(span).await;
                }
                now = interval.tick() => {
                    self.core.on_tick(now).await;
                }
            }
        }
    }
}

impl core::NarwhalCoreContext for NarwhalCoreContextState {
    async fn deliver(&mut self, block: core::NarwhalBlock) {
        self.deliver_tx
            .as_ref()
            .expect("deliver_tx not set")
            .send(block, Span::current())
            .await;
    }

    async fn broadcast_block(&mut self, block: core::NarwhalBlock) -> Option<core::BlockHash> {
        let (response_tx, response_rx) = oneshot::channel();
        self.send_message_tx
            .as_ref()
            .expect("send_message_tx not set")
            .send(
                events::SendMessageValue::Block(block, response_tx),
                Span::current(),
            )
            .await;
        response_rx.await.ok()
    }

    async fn broadcast_cert(&mut self, cert: core::NarwhalCert) {
        self.send_message_tx
            .as_ref()
            .expect("send_message_tx not set")
            .send(events::SendMessageValue::Cert(cert), Span::current())
            .await;
    }

    async fn ack(
        &mut self,
        block_hash: core::BlockHash,
        round: core::RoundNum,
        replica_index: core::ReplicaIndex,
        signer: core::ReplicaIndex,
    ) {
        self.send_message_tx
            .as_ref()
            .expect("send_message_tx not set")
            .send(
                events::SendMessageValue::Ack(block_hash, round, replica_index, signer),
                Span::current(),
            )
            .await;
    }
}

pub struct NarwhalEgress<C> {
    worker: NarwhalWorker<C>,
    replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>, // exclude self

    send_message: EventChannel<events::SendMessage>,

    bytes_tx: Option<EventSender<events::SendBytes>>,
    message_tx: Option<EventSender<events::HandleMessage>>,
}

impl<C> NarwhalEgress<C> {
    pub fn new(
        context: C,
        params: core::NarwhalParams,
        replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>,
    ) -> Self {
        Self {
            worker: NarwhalWorker::new(context, params),
            replica_addrs,
            send_message: EventChannel::new(None),
            bytes_tx: None,
            message_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_send_message: impl Emit<events::SendMessage>) {
        emit_send_message.install(self.send_message.sender());
    }
}

impl<C> EventSenderSlot<events::SendBytes> for NarwhalEgress<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::SendBytes>> {
        &mut self.bytes_tx
    }
}

impl<C> EventSenderSlot<events::HandleMessage> for NarwhalEgress<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::HandleMessage>> {
        &mut self.message_tx
    }
}

impl<C: workers::NarwhalCryptoContext> NarwhalEgress<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some((message, span))) =
            token.run_until_cancelled(self.send_message.recv()).await
        {
            match message {
                events::SendMessageValue::Block(block, response_tx) => {
                    let (bytes, block_hash) = span.in_scope(|| self.worker.egress_block(block));
                    if response_tx.send(block_hash).is_err() {
                        warn!("Failed to send block hash response");
                    }
                    let bytes = Bytes::from(bytes);
                    for &addr in self.replica_addrs.values() {
                        self.bytes_tx
                            .as_ref()
                            .expect("bytes_tx not set")
                            .send((addr, bytes.clone()), span.clone())
                            .await;
                    }
                }
                events::SendMessageValue::Ack(block_hash, round, replica_index, signer) => {
                    if replica_index == signer {
                        let sig = span
                            .in_scope(|| self.worker.sign_ack(&block_hash, round, replica_index));
                        self.message_tx
                            .as_ref()
                            .expect("message_tx not set")
                            .send(
                                core::NarwhalMessage::Ack(block_hash, replica_index, sig),
                                span,
                            )
                            .await;
                    } else {
                        let bytes = span
                            .in_scope(|| self.worker.egress_ack(block_hash, round, replica_index));
                        self.bytes_tx
                            .as_ref()
                            .expect("bytes_tx not set")
                            .send((self.replica_addrs[&replica_index], bytes.into()), span)
                            .await;
                    }
                }
                events::SendMessageValue::Cert(cert) => {
                    let bytes = span.in_scope(|| self.worker.egress_cert(cert));
                    let bytes = Bytes::from(bytes);
                    for &addr in self.replica_addrs.values() {
                        self.bytes_tx
                            .as_ref()
                            .expect("bytes_tx not set")
                            .send((addr, bytes.clone()), span.clone())
                            .await;
                    }
                }
            }
        }
    }
}

pub struct NarwhalIngress<C> {
    worker: NarwhalWorker<C>,

    bytes: EventChannel<events::HandleBytes>,

    message_tx: Option<EventSender<events::HandleMessage>>,
}

impl<C> NarwhalIngress<C> {
    pub fn new(context: C, params: core::NarwhalParams) -> Self {
        Self {
            worker: NarwhalWorker::new(context, params),
            bytes: EventChannel::new(None),
            message_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_bytes: impl Emit<events::HandleBytes>) {
        emit_bytes.install(self.bytes.sender());
    }
}

impl<C> EventSenderSlot<events::HandleMessage> for NarwhalIngress<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::HandleMessage>> {
        &mut self.message_tx
    }
}

impl<C: workers::NarwhalCryptoContext> NarwhalIngress<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some((bytes, span))) = token.run_until_cancelled(self.bytes.recv()).await {
            match span.in_scope(|| self.worker.ingress(&bytes)) {
                Ok(message) => {
                    self.message_tx
                        .as_ref()
                        .expect("message_tx not set")
                        .send(message, span)
                        .await;
                }
                Err(e) => warn!("Failed to parse incoming message: {e:#}"),
            }
        }
    }
}
