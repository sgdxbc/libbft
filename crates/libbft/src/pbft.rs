use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use tokio::{
    sync::mpsc::{Receiver, Sender, channel},
    time::interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, instrument, warn};

use crate::{
    crypto::Sig as _,
    event::{Emit, EmitMap},
    pbft::events::Recipient,
};

mod core;
#[cfg(test)]
mod tests;

pub use core::{PbftCoreConfig, PbftParams, PbftRequest};

pub mod events {
    use bytes::Bytes;

    use crate::{event::Event, pbft::core};

    pub struct HandleRequest;
    impl Event for HandleRequest {
        type Type = core::PbftRequest;
    }

    pub struct SignedMessage<S>(std::marker::PhantomData<S>);
    impl<S> Event for SignedMessage<S> {
        type Type = (core::PbftMessage, S);
    }

    pub struct LoopbackMessage<S>(std::marker::PhantomData<S>);
    impl<S> Event for LoopbackMessage<S> {
        type Type = (core::PbftMessage, S);
    }

    pub enum Recipient {
        To(core::ReplicaIndex),
        Broadcast,
    }

    pub struct SendMessage;
    impl Event for SendMessage {
        type Type = (Recipient, core::PbftMessage);
    }

    pub struct Deliver;
    impl Event for Deliver {
        type Type = (Vec<core::PbftRequest>, core::ViewNum);
    }

    pub struct HandleBytes;
    impl Event for HandleBytes {
        type Type = Vec<u8>;
    }

    pub struct SendBytes;
    impl Event for SendBytes {
        type Type = Bytes;
    }
}

pub struct PbftNode<C: core::PbftCoreCryptoContext> {
    core: core::PbftCore<PbftCoreContextState<C>>,

    request_tx: Sender<(core::PbftRequest, Span)>,
    request_rx: Receiver<(core::PbftRequest, Span)>,
    signed_message_tx: Sender<((core::PbftMessage, C::Sig), Span)>,
    signed_message_rx: Receiver<((core::PbftMessage, C::Sig), Span)>,
    loopback_tx: Sender<((core::PbftMessage, C::Sig), Span)>,
    loopback_rx: Receiver<((core::PbftMessage, C::Sig), Span)>,
}

pub struct PbftCoreContextState<C: core::PbftCoreCryptoContext> {
    message_tx: Option<Sender<((Recipient, core::PbftMessage), Span)>>, // crypto verify worker
    deliver_tx: Option<Sender<((Vec<core::PbftRequest>, core::ViewNum), Span)>>, //

    _crypto: std::marker::PhantomData<C>,
}

impl<C: core::PbftCoreCryptoContext> PbftNode<C> {
    pub fn new(config: core::PbftCoreConfig) -> Self {
        let (request_tx, request_rx) = channel(1000);
        let (signed_message_tx, signed_message_rx) = channel(1000);
        let (loopback_tx, loopback_rx) = channel(1000);
        let core_context = PbftCoreContextState {
            message_tx: None,
            deliver_tx: None,
            _crypto: std::marker::PhantomData,
        };
        let core = core::PbftCore::new(core_context, config);
        Self {
            core,
            request_tx,
            request_rx,
            signed_message_tx,
            signed_message_rx,
            loopback_tx,
            loopback_rx,
        }
    }

    pub fn register(
        &self,
        emit_request: &mut impl Emit<events::HandleRequest>,
        emit_signed_message: &mut impl Emit<events::SignedMessage<C::Sig>>,
        emit_loopback_message: &mut impl Emit<events::LoopbackMessage<C::Sig>>,
    ) {
        emit_request.set_tx(self.request_tx.clone());
        emit_signed_message.set_tx(self.signed_message_tx.clone());
        emit_loopback_message.set_tx(self.loopback_tx.clone());
    }
}

impl<C: core::PbftCoreCryptoContext> Emit<events::SendMessage> for PbftNode<C> {
    fn tx_slot(
        &mut self,
    ) -> &mut Option<Sender<(<events::SendMessage as crate::event::Event>::Type, Span)>> {
        &mut self.core.context.message_tx
    }
}

impl<C: core::PbftCoreCryptoContext> Emit<events::Deliver> for PbftNode<C> {
    fn tx_slot(
        &mut self,
    ) -> &mut Option<Sender<(<events::Deliver as crate::event::Event>::Type, Span)>> {
        &mut self.core.context.deliver_tx
    }
}

impl<C: core::PbftCoreCryptoContext> PbftNode<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        let mut interval = interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                () = token.cancelled() => {
                    break;
                }

                Some((request, span)) = self.request_rx.recv() => {
                    self.core.handle_request(request).instrument(span).await;
                }
                Some(((message, sig), span)) = self.signed_message_rx.recv() => {
                    self.core.handle_message(message, sig).instrument(span).await;
                }
                Some(((message, sig), span)) = self.loopback_rx.recv() => {
                    self.core.handle_loopback_message(message, sig).instrument(span).await;
                }
                now = interval.tick() => {
                    self.core.tick(now).await;
                }
            }
        }
    }
}

impl<C: core::PbftCoreCryptoContext> core::PbftCoreContext for PbftCoreContextState<C> {
    async fn send_message(&mut self, to: core::ReplicaIndex, message: core::PbftMessage) {
        if let Err(err) = self
            .message_tx
            .as_ref()
            .unwrap()
            // ideally we should use the "lifecycle" span here to create sibling spans for pipeline
            .send(((Recipient::To(to), message), Span::current()))
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
            .send(((Recipient::Broadcast, message), Span::current()))
            .await
        {
            warn!("Failed to send message to crypto verify worker: {err:#}");
        }
    }

    async fn deliver(&mut self, requests: Vec<core::PbftRequest>, view_num: core::ViewNum) {
        if let Err(err) = self
            .deliver_tx
            .as_ref()
            .unwrap()
            .send(((requests, view_num), Span::current()))
            .await
        {
            warn!("Failed to deliver requests: {err:#}");
        }
    }

    type Sig = C::Sig;
}

pub struct PbftCryptoVerifyWorker<C: core::PbftCoreCryptoContext> {
    core_crypto: core::PbftCoreCrypto<C>,

    bytes_tx: Sender<(Vec<u8>, Span)>,
    bytes_rx: Receiver<(Vec<u8>, Span)>,
    signed_message_tx: Option<Sender<((core::PbftMessage, C::Sig), Span)>>, // node
}

impl<C: core::PbftCoreCryptoContext> PbftCryptoVerifyWorker<C> {
    pub fn new(core_crypto_context: C, params: core::PbftParams) -> Self {
        let (bytes_tx, bytes_rx) = channel(1000);
        Self {
            core_crypto: core::PbftCoreCrypto::new(core_crypto_context, params),
            bytes_tx,
            bytes_rx,
            signed_message_tx: None,
        }
    }

    pub fn register(&mut self, emit_handle_bytes: &mut impl Emit<events::HandleBytes>) {
        emit_handle_bytes.set_tx(self.bytes_tx.clone());
    }
}

impl<C: core::PbftCoreCryptoContext> Emit<events::SignedMessage<C::Sig>>
    for PbftCryptoVerifyWorker<C>
{
    fn tx_slot(
        &mut self,
    ) -> &mut Option<
        Sender<(
            <events::SignedMessage<C::Sig> as crate::event::Event>::Type,
            Span,
        )>,
    > {
        &mut self.signed_message_tx
    }
}

impl<C: core::PbftCoreCryptoContext> PbftCryptoVerifyWorker<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some((bytes, span))) = token.run_until_cancelled(self.bytes_rx.recv()).await
        {
            match span.in_scope(|| self.decode(&bytes)) {
                Ok((message, sig)) => {
                    if let Err(err) = self
                        .signed_message_tx
                        .as_ref()
                        .unwrap()
                        .send(((message, sig), span))
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
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<(core::PbftMessage, C::Sig)> {
        anyhow::ensure!(
            bytes.len() >= 4,
            "Received bytes too short to contain data length"
        );
        let data_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let Some((bytes, piggyback_data)) = &bytes[4..].split_at_checked(data_len) else {
            anyhow::bail!("Received bytes too short to contain data");
        };
        let Some((sig_bytes, data_bytes)) = bytes.split_at_checked(C::Sig::bytes_len()) else {
            anyhow::bail!("Received bytes too short to contain signature");
        };
        let sig = C::Sig::from_bytes(sig_bytes);
        let message = self.core_crypto.verify(data_bytes, &sig, piggyback_data)?;
        Ok((message, sig))
    }
}

pub struct PbftCryptoSignWorker<C: core::PbftCoreCryptoContext> {
    core_crypto: core::PbftCoreCrypto<C>,

    message_tx: Sender<((Recipient, core::PbftMessage), Span)>,
    message_rx: Receiver<((Recipient, core::PbftMessage), Span)>,
    bytes_tx_map: Option<HashMap<core::ReplicaIndex, Sender<(Bytes, Span)>>>, // network
    loopback_tx: Option<Sender<((core::PbftMessage, C::Sig), Span)>>,         // node
}

impl<C: core::PbftCoreCryptoContext> PbftCryptoSignWorker<C> {
    pub fn new(core_crypto_context: C, params: core::PbftParams) -> Self {
        let (message_tx, message_rx) = channel(1000);
        Self {
            core_crypto: core::PbftCoreCrypto::new(core_crypto_context, params),
            message_tx,
            message_rx,
            bytes_tx_map: None,
            loopback_tx: None,
        }
    }

    pub fn register(&mut self, emit_send_message: &mut impl Emit<events::SendMessage>) {
        emit_send_message.set_tx(self.message_tx.clone());
    }
}

impl<C: core::PbftCoreCryptoContext> EmitMap<core::ReplicaIndex, events::SendBytes>
    for PbftCryptoSignWorker<C>
{
    fn tx_map_slot(
        &mut self,
    ) -> &mut Option<
        HashMap<
            core::ReplicaIndex,
            Sender<(<events::SendBytes as crate::event::Event>::Type, Span)>,
        >,
    > {
        &mut self.bytes_tx_map
    }
}

impl<C: core::PbftCoreCryptoContext> Emit<events::LoopbackMessage<C::Sig>>
    for PbftCryptoSignWorker<C>
{
    fn tx_slot(
        &mut self,
    ) -> &mut Option<
        Sender<(
            <events::LoopbackMessage<C::Sig> as crate::event::Event>::Type,
            Span,
        )>,
    > {
        &mut self.loopback_tx
    }
}

impl<C: core::PbftCoreCryptoContext> PbftCryptoSignWorker<C> {
    pub async fn run(&mut self, token: &CancellationToken) {
        while let Some(Some(((recipient, mut message), span))) =
            token.run_until_cancelled(self.message_rx.recv()).await
        {
            let (bytes, sig, piggyback_data) = self.core_crypto.sign(&mut message);
            let data_bytes = [sig.as_bytes(), &bytes].concat();
            let bytes = [
                &(data_bytes.len() as u32).to_le_bytes()[..],
                &data_bytes,
                &piggyback_data,
            ]
            .concat()
            .into();
            match recipient {
                Recipient::To(to) => {
                    if let Err(err) = self.bytes_tx_map.as_ref().unwrap()[&to]
                        .send((bytes, span))
                        .await
                    {
                        warn!("Failed to send message to node {to}: {err:#}");
                    }
                }
                Recipient::Broadcast => {
                    for (to, bytes_tx) in self.bytes_tx_map.as_ref().unwrap() {
                        if let Err(err) = bytes_tx.send((bytes.clone(), span.clone())).await {
                            warn!("Failed to broadcast message to node {to}: {err:#}");
                        }
                    }
                    if let Err(err) = self
                        .loopback_tx
                        .as_ref()
                        .unwrap()
                        .send(((message, sig), span))
                        .await
                    {
                        warn!("Failed to loopback signed message: {err:#}");
                    }
                }
            }
        }
    }
}
