use std::{collections::HashMap, net::SocketAddr, time::Duration};

use anyhow::Context as _;
use bytes::{Buf, Bytes};
use tokio::{sync::mpsc::channel, time::interval};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, instrument, warn};

use crate::{
    crypto::SigBytes,
    event::{Emit, EventReceiver, EventSender},
};

mod core;
#[cfg(test)]
mod tests;
mod workers;

pub use core::{PbftCoreConfig, PbftParams, PbftRequest};

pub mod events {
    use crate::{crypto::SigBytes, event::Event, pbft::core};

    pub struct HandleRequest;
    impl Event for HandleRequest {
        type Type = core::PbftRequest;
    }

    pub enum HandleMessageValue {
        Signed(core::PbftMessage, SigBytes),
        Sync(core::PbftSyncMessage),
    }

    pub struct HandleMessage;
    impl Event for HandleMessage {
        type Type = HandleMessageValue;
    }

    pub enum SendMessageValue {
        Broadcast(core::PbftMessage),
        Sync(core::ReplicaIndex, core::PbftSyncMessage),
    }

    pub struct SendMessage;
    impl Event for SendMessage {
        type Type = SendMessageValue;
    }

    pub struct Deliver;
    impl Event for Deliver {
        type Type = (core::SeqNum, Vec<core::PbftRequest>, core::ViewNum);
    }

    pub struct Snapshot;
    impl Event for Snapshot {
        type Type = (core::SeqNum, core::Digest);
    }

    pub type HandleBytes = crate::network::events::HandleBytes;

    pub type SendBytes = crate::network::events::SendBytes;
}

pub struct PbftProtocol {
    core: core::PbftCore<PbftCoreContextState>,

    request_tx: EventSender<events::HandleRequest>,
    request_rx: EventReceiver<events::HandleRequest>,
    message_tx: EventSender<events::HandleMessage>,
    message_rx: EventReceiver<events::HandleMessage>,
    snapshot_tx: EventSender<events::Snapshot>,
    snapshot_rx: EventReceiver<events::Snapshot>,
}

pub struct PbftCoreContextState {
    message_tx: Option<EventSender<events::SendMessage>>,
    deliver_tx: Option<EventSender<events::Deliver>>,
}

impl PbftProtocol {
    pub fn new(config: core::PbftCoreConfig) -> Self {
        let (request_tx, request_rx) = channel(1000);
        let (message_tx, message_rx) = channel(1000);
        let (snapshot_tx, snapshot_rx) = channel(1000);
        let core_context = PbftCoreContextState {
            message_tx: None,
            deliver_tx: None,
        };
        let core = core::PbftCore::new(core_context, config);
        Self {
            core,
            request_tx,
            request_rx,
            message_tx,
            message_rx,
            snapshot_tx,
            snapshot_rx,
        }
    }

    pub fn register(
        &self,
        emit_request: &mut impl Emit<events::HandleRequest>,
        emit_signed_message: &mut impl Emit<events::HandleMessage>,
        emit_loopback_message: &mut impl Emit<events::HandleMessage>,
        emit_snapshot: &mut impl Emit<events::Snapshot>,
    ) {
        emit_request.set_tx(self.request_tx.clone());
        emit_signed_message.set_tx(self.message_tx.clone());
        emit_loopback_message.set_tx(self.message_tx.clone());
        emit_snapshot.set_tx(self.snapshot_tx.clone());
    }
}

impl Emit<events::SendMessage> for PbftProtocol {
    fn tx_slot(&mut self) -> &mut Option<EventSender<events::SendMessage>> {
        &mut self.core.context.message_tx
    }
}

impl Emit<events::Deliver> for PbftProtocol {
    fn tx_slot(&mut self) -> &mut Option<EventSender<events::Deliver>> {
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

                Some((request, span)) = self.request_rx.recv() => {
                    self.core.on_request(request).instrument(span).await;
                }
                Some((value, span)) = self.message_rx.recv() => {
                    match value {
                        events::HandleMessageValue::Signed(message, sig) => {
                            self.core.on_message(message, sig).instrument(span).await
                        }
                        events::HandleMessageValue::Sync(sync_message) => {
                            self.core.on_sync_message(sync_message).instrument(span).await
                        }
                    }
                }
                Some(((seq_num, state_digest), span)) = self.snapshot_rx.recv() => {
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
        if let Err(err) = self
            .message_tx
            .as_ref()
            .unwrap()
            // ideally we should use the "lifecycle" span here to create sibling spans for pipeline
            .send((events::SendMessageValue::Sync(to, message), Span::current()))
            .await
        {
            warn!("Failed to send message to crypto verify worker: {err:#}");
        }
    }

    async fn broadcast_message(&mut self, message: core::PbftMessage) {
        if let Err(err) = self
            .message_tx
            .as_ref()
            .unwrap()
            .send((
                events::SendMessageValue::Broadcast(message),
                Span::current(),
            ))
            .await
        {
            warn!("Failed to send message to crypto verify worker: {err:#}");
        }
    }

    async fn deliver(
        &mut self,
        seq_num: core::SeqNum,
        requests: Vec<core::PbftRequest>,
        view_num: core::ViewNum,
    ) {
        if let Err(err) = self
            .deliver_tx
            .as_ref()
            .unwrap()
            .send(((seq_num, requests, view_num), Span::current()))
            .await
        {
            warn!("Failed to deliver requests: {err:#}");
        }
    }
}

pub struct PbftIngressWorker<C: workers::PbftCryptoContext> {
    state: workers::PbftWorker<C>,

    bytes_tx: EventSender<events::HandleBytes>,
    bytes_rx: EventReceiver<events::HandleBytes>,
    signed_message_tx: Option<EventSender<events::HandleMessage>>, // node
}

impl<C: workers::PbftCryptoContext> PbftIngressWorker<C> {
    pub fn new(core_crypto_context: C, params: core::PbftParams) -> Self {
        let (bytes_tx, bytes_rx) = channel(1000);
        Self {
            state: workers::PbftWorker::new(core_crypto_context, params),
            bytes_tx,
            bytes_rx,
            signed_message_tx: None,
        }
    }

    pub fn register(&mut self, emit_handle_bytes: &mut impl Emit<events::HandleBytes>) {
        emit_handle_bytes.set_tx(self.bytes_tx.clone());
    }
}

impl<C: workers::PbftCryptoContext> Emit<events::HandleMessage> for PbftIngressWorker<C> {
    fn tx_slot(&mut self) -> &mut Option<EventSender<events::HandleMessage>> {
        &mut self.signed_message_tx
    }
}

impl<C: workers::PbftCryptoContext> PbftIngressWorker<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some((bytes, span))) = token.run_until_cancelled(self.bytes_rx.recv()).await
        {
            match span.in_scope(|| self.decode(&bytes)) {
                Ok(value) => {
                    if let Err(err) = self
                        .signed_message_tx
                        .as_ref()
                        .unwrap()
                        .send((value, span))
                        .await
                    {
                        warn!("Failed to send verified message to node: {err:#}");
                    }
                }
                Err(err) => warn!("Failed to decode and verify message: {err:#}"),
            }
        }
    }

    #[instrument(skip_all)]
    fn decode(&self, mut bytes: &[u8]) -> anyhow::Result<events::HandleMessageValue> {
        let value = match bytes.try_get_u8()? {
            0x0 => {
                let Some((sig_bytes, data_bytes)) = bytes.split_at_checked(C::SIG_BYTES_LEN) else {
                    anyhow::bail!("Received bytes too short to contain signature");
                };
                let sig = SigBytes(sig_bytes.into());
                let message = self.state.ingress(data_bytes, &sig)?;
                events::HandleMessageValue::Signed(message, sig)
            }
            0x1 => {
                let data_bytes = bytes;
                let message =
                    borsh::from_slice(data_bytes).context("Failed to deserialize sync message")?;
                events::HandleMessageValue::Sync(message)
            }
            tag => anyhow::bail!("Invalid message: unknown tag {tag:#x}"),
        };
        Ok(value)
    }
}

pub struct PbftEgressWorker<C: workers::PbftCryptoContext> {
    state: workers::PbftWorker<C>,
    replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>, // excluding self

    message_tx: EventSender<events::SendMessage>,
    message_rx: EventReceiver<events::SendMessage>,
    bytes_tx: Option<EventSender<events::SendBytes>>, // network
    loopback_tx: Option<EventSender<events::HandleMessage>>, // node
}

impl<C: workers::PbftCryptoContext> PbftEgressWorker<C> {
    pub fn new(
        core_crypto_context: C,
        params: core::PbftParams,
        replica_addrs: HashMap<core::ReplicaIndex, SocketAddr>,
    ) -> Self {
        let (message_tx, message_rx) = channel(1000);
        Self {
            state: workers::PbftWorker::new(core_crypto_context, params),
            replica_addrs,
            message_tx,
            message_rx,
            bytes_tx: None,
            loopback_tx: None,
        }
    }

    pub fn register(&mut self, emit_send_message: &mut impl Emit<events::SendMessage>) {
        emit_send_message.set_tx(self.message_tx.clone());
    }
}

impl<C: workers::PbftCryptoContext> Emit<events::SendBytes> for PbftEgressWorker<C> {
    fn tx_slot(&mut self) -> &mut Option<EventSender<events::SendBytes>> {
        &mut self.bytes_tx
    }
}

impl<C: workers::PbftCryptoContext> Emit<events::HandleMessage> for PbftEgressWorker<C> {
    fn tx_slot(&mut self) -> &mut Option<EventSender<events::HandleMessage>> {
        &mut self.loopback_tx
    }
}

impl<C: workers::PbftCryptoContext> PbftEgressWorker<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        let bytes_tx = self.bytes_tx.as_ref().unwrap();
        let loopback_tx = self.loopback_tx.as_ref().unwrap();
        while let Some(Some((message, span))) =
            token.run_until_cancelled(self.message_rx.recv()).await
        {
            match message {
                events::SendMessageValue::Broadcast(mut message) => {
                    let (data_bytes, SigBytes(sig_bytes)) = self.state.egress(&mut message);
                    let mut buf = vec![0x0];
                    buf.extend_from_slice(&sig_bytes);
                    buf.extend_from_slice(&data_bytes);
                    let bytes = Bytes::from(buf);

                    // why there's `Receiver::recv_many` but no `Sender::send_many`?
                    for (&index, &addr) in &self.replica_addrs {
                        if let Err(err) = bytes_tx.send(((addr, bytes.clone()), span.clone())).await
                        {
                            warn!("Failed to broadcast message to node {index}: {err:#}");
                        }
                    }
                    if let Err(err) = loopback_tx
                        .send((
                            events::HandleMessageValue::Signed(message, SigBytes(sig_bytes)),
                            span,
                        ))
                        .await
                    {
                        warn!("Failed to loopback signed message: {err:#}");
                    }
                }
                events::SendMessageValue::Sync(to, message) => {
                    let data_bytes = borsh::to_vec(&message).unwrap();
                    let mut buf = vec![0x1];
                    buf.extend_from_slice(&data_bytes);
                    if let Err(err) = bytes_tx
                        .send(((self.replica_addrs[&to], buf.into()), span))
                        .await
                    {
                        warn!("Failed to send sync message to node {to}: {err:#}");
                    }
                }
            }
        }
    }
}
