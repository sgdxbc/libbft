use std::{collections::HashMap, net::SocketAddr, time::Duration};

use bytes::Bytes;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, warn};

use crate::event::{Emit, EventChannel, EventSender, EventSenderSlot};

mod core;
#[cfg(test)]
mod tests;
mod workers;

pub use core::{PbftCoreConfig, PbftParams, PbftRequest};

pub mod events {
    use crate::{crypto::SigBytes, event::Event, pbft::core};

    pub struct HandleRequest;
    impl Event for HandleRequest {
        type Value = core::PbftRequest;
    }

    pub enum HandleMessageValue {
        Signed(core::PbftMessage, SigBytes),
        Sync(core::PbftSyncMessage),
    }

    pub struct HandleMessage;
    impl Event for HandleMessage {
        type Value = HandleMessageValue;
    }

    pub enum SendMessageValue {
        Broadcast(core::PbftMessage),
        Sync(core::ReplicaIndex, core::PbftSyncMessage),
    }

    pub struct SendMessage;
    impl Event for SendMessage {
        type Value = SendMessageValue;
    }

    pub struct Deliver;
    impl Event for Deliver {
        type Value = (core::SeqNum, Vec<core::PbftRequest>, core::ViewNum);
    }

    pub struct Snapshot;
    impl Event for Snapshot {
        type Value = (core::SeqNum, core::Digest);
    }

    pub type HandleBytes = crate::network::events::HandleBytes;

    pub type SendBytes = crate::network::events::SendBytes;
}

pub struct PbftProtocol {
    core: core::PbftCore<PbftCoreContextState>,

    request: EventChannel<events::HandleRequest>,
    message: EventChannel<events::HandleMessage>,
    snapshot: EventChannel<events::Snapshot>,
}

struct PbftCoreContextState {
    message_tx: Option<EventSender<events::SendMessage>>,
    deliver_tx: Option<EventSender<events::Deliver>>,
}

impl PbftProtocol {
    pub fn new(config: core::PbftCoreConfig) -> Self {
        let core_context = PbftCoreContextState {
            message_tx: None,
            deliver_tx: None,
        };
        Self {
            core: core::PbftCore::new(core_context, config),
            request: EventChannel::new(None),
            message: EventChannel::new(None),
            snapshot: EventChannel::new(None),
        }
    }

    pub fn register(
        &self,
        mut emit_request: impl Emit<events::HandleRequest>,
        mut emit_message: impl Emit<events::HandleMessage>,
        mut emit_snapshot: impl Emit<events::Snapshot>,
    ) {
        emit_request.install(self.request.sender());
        emit_message.install(self.message.sender());
        emit_snapshot.install(self.snapshot.sender());
    }
}

impl EventSenderSlot<events::SendMessage> for PbftProtocol {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::SendMessage>> {
        &mut self.core.context.message_tx
    }
}

impl EventSenderSlot<events::Deliver> for PbftProtocol {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::Deliver>> {
        &mut self.core.context.deliver_tx
    }
}

impl PbftProtocol {
    pub async fn run(&mut self, token: &CancellationToken) {
        let mut interval = interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                () = token.cancelled() => {
                    break;
                }

                Some((request, span)) = self.request.recv() => {
                    self.core.on_request(request).instrument(span).await;
                }
                Some((value, span)) = self.message.recv() => {
                    match value {
                        events::HandleMessageValue::Signed(message, sig) => {
                            self.core.on_message(message, sig).instrument(span).await
                        }
                        events::HandleMessageValue::Sync(sync_message) => {
                            self.core.on_sync_message(sync_message).instrument(span).await
                        }
                    }
                }
                Some(((seq_num, state_digest), span)) = self.snapshot.recv() => {
                    self.core.on_snapshot(seq_num, state_digest).instrument(span).await;
                }
                now = interval.tick() => {
                    self.core.on_tick(now).await;
                }
            }
        }
    }
}

impl core::PbftCoreContext for PbftCoreContextState {
    async fn send_sync_message(&mut self, to: core::ReplicaIndex, message: core::PbftSyncMessage) {
        self.message_tx
            .as_ref()
            .unwrap()
            // ideally we should use the "lifecycle" span here to create sibling spans for pipeline
            .send(events::SendMessageValue::Sync(to, message), Span::current())
            .await;
    }

    async fn broadcast_message(&mut self, message: core::PbftMessage) {
        self.message_tx
            .as_ref()
            .unwrap()
            .send(
                events::SendMessageValue::Broadcast(message),
                Span::current(),
            )
            .await;
    }

    async fn deliver(
        &mut self,
        seq_num: core::SeqNum,
        requests: Vec<core::PbftRequest>,
        view_num: core::ViewNum,
    ) {
        self.deliver_tx
            .as_ref()
            .unwrap()
            .send((seq_num, requests, view_num), Span::current())
            .await;
    }
}

pub struct PbftEgress<C: workers::PbftCryptoContext> {
    state: workers::PbftWorker<C>,
    replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>, // excluding self

    message: EventChannel<events::SendMessage>,

    bytes_tx: Option<EventSender<events::SendBytes>>, // network
    loopback_tx: Option<EventSender<events::HandleMessage>>, // node
}

impl<C: workers::PbftCryptoContext> PbftEgress<C> {
    pub fn new(
        core_crypto_context: C,
        params: core::PbftParams,
        replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>,
    ) -> Self {
        Self {
            state: workers::PbftWorker::new(core_crypto_context, params),
            replica_addrs,
            message: EventChannel::new(None),
            bytes_tx: None,
            loopback_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_send_message: impl Emit<events::SendMessage>) {
        emit_send_message.install(self.message.sender());
    }
}

impl<C: workers::PbftCryptoContext> EventSenderSlot<events::SendBytes> for PbftEgress<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::SendBytes>> {
        &mut self.bytes_tx
    }
}

impl<C: workers::PbftCryptoContext> EventSenderSlot<events::HandleMessage> for PbftEgress<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::HandleMessage>> {
        &mut self.loopback_tx
    }
}

impl<C: workers::PbftCryptoContext> PbftEgress<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        let bytes_tx = self.bytes_tx.as_ref().unwrap();
        let loopback_tx = self.loopback_tx.as_ref().unwrap();
        while let Some(Some((message, span))) = token.run_until_cancelled(self.message.recv()).await
        {
            match message {
                events::SendMessageValue::Broadcast(mut message) => {
                    let (bytes, sig) = self.state.egress_broadcast(&mut message);
                    let bytes = Bytes::from(bytes);
                    // why there's `Receiver::recv_many` but no `Sender::send_many`?
                    for &addr in self.replica_addrs.values() {
                        bytes_tx.send((addr, bytes.clone()), span.clone()).await;
                    }
                    loopback_tx
                        .send(events::HandleMessageValue::Signed(message, sig), span)
                        .await;
                }
                events::SendMessageValue::Sync(to, message) => {
                    let bytes = self.state.egress_sync(message);
                    bytes_tx
                        .send((self.replica_addrs[&to], bytes.into()), span)
                        .await;
                }
            }
        }
    }
}

pub struct PbftIngress<C: workers::PbftCryptoContext> {
    state: workers::PbftWorker<C>,

    bytes: EventChannel<events::HandleBytes>,

    signed_message_tx: Option<EventSender<events::HandleMessage>>, // node
}

impl<C: workers::PbftCryptoContext> PbftIngress<C> {
    pub fn new(core_crypto_context: C, params: core::PbftParams) -> Self {
        Self {
            state: workers::PbftWorker::new(core_crypto_context, params),
            bytes: EventChannel::new(None),
            signed_message_tx: None,
        }
    }

    pub fn register(&mut self, mut emit_handle_bytes: impl Emit<events::HandleBytes>) {
        emit_handle_bytes.install(self.bytes.sender());
    }
}

impl<C: workers::PbftCryptoContext> EventSenderSlot<events::HandleMessage> for PbftIngress<C> {
    fn sender_slot(&mut self) -> &mut Option<EventSender<events::HandleMessage>> {
        &mut self.signed_message_tx
    }
}

impl<C: workers::PbftCryptoContext> PbftIngress<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some((bytes, span))) = token.run_until_cancelled(self.bytes.recv()).await {
            match span.in_scope(|| self.state.ingress(&bytes)) {
                Ok(value) => {
                    self.signed_message_tx
                        .as_ref()
                        .unwrap()
                        .send(value, span)
                        .await;
                }
                Err(err) => warn!("Failed to decode and verify message: {err:#}"),
            }
        }
    }
}
