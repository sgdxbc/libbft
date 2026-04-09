use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use libbft_crypto::Sig as _;
use tokio::{
    sync::mpsc::{Receiver, Sender, channel},
    time::interval,
};
use tracing::warn;

mod core;
#[cfg(test)]
mod tests;

pub struct PbftNode<C: core::PbftCoreContext>
where
    C::Sig: Send,
{
    core: core::PbftCore<PbftCoreContextState<C::Sig>>,

    request_tx: Sender<core::PbftRequest>,
    request_rx: Receiver<core::PbftRequest>,
    signed_message_tx: Sender<(core::PbftMessage, C::Sig)>,
    signed_message_rx: Receiver<(core::PbftMessage, C::Sig)>,
    loopback_tx: Sender<(core::PbftMessage, C::Sig)>,
    loopback_rx: Receiver<(core::PbftMessage, C::Sig)>,
}

pub struct PbftCoreContextState<S> {
    message_tx: Option<Sender<(Recipient, core::PbftMessage)>>, // crypto verify worker
    deliver_tx: Option<Sender<(Vec<core::PbftRequest>, core::ViewNum)>>, //

    _sig: std::marker::PhantomData<S>,
}

pub enum Recipient {
    To(core::ReplicaIndex),
    Broadcast,
}

impl<C: core::PbftCoreContext> PbftNode<C> {
    pub fn new(config: core::PbftCoreConfig) -> Self {
        let (request_tx, request_rx) = channel(1000);
        let (signed_message_tx, signed_message_rx) = channel(1000);
        let (loopback_tx, loopback_rx) = channel(1000);
        let core_context = PbftCoreContextState {
            message_tx: None,
            deliver_tx: None,
            _sig: std::marker::PhantomData,
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

    pub fn register<Ctx: core::PbftCoreCryptoContext<Sig = C::Sig>>(
        &self,
        verify_worker: &mut PbftCryptoVerifyWorker<Ctx>,
        sign_worker: &mut PbftCryptoSignWorker<Ctx>,
    ) {
        verify_worker.set_signed_message_tx(self.signed_message_tx.clone());
        sign_worker.set_loopback_tx(self.loopback_tx.clone());
    }

    fn set_message_tx(&mut self, message_tx: Sender<(Recipient, core::PbftMessage)>) {
        let replaced = self.core.context.message_tx.replace(message_tx);
        assert!(replaced.is_none(), "message_tx can only be set once");
    }

    pub fn set_deliver_tx(&mut self, deliver_tx: Sender<(Vec<core::PbftRequest>, core::ViewNum)>) {
        let replaced = self.core.context.deliver_tx.replace(deliver_tx);
        assert!(replaced.is_none(), "deliver_tx can only be set once");
    }

    pub async fn run(&mut self) {
        let mut interval = interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                Some(request) = self.request_rx.recv() => {
                    self.core.handle_request(request).await;
                }
                Some((message, sig)) = self.signed_message_rx.recv() => {
                    self.core.handle_message(message, sig).await;
                }
                Some((message, sig)) = self.loopback_rx.recv() => {
                    self.core.handle_loopback_message(message, sig).await;
                }
                now = interval.tick() => {
                    self.core.tick(now).await;
                }
            }
        }
    }
}

impl<S: Send> core::PbftCoreContext for PbftCoreContextState<S> {
    async fn send_message(&mut self, to: core::ReplicaIndex, message: core::PbftMessage) {
        if let Err(err) = self
            .message_tx
            .as_ref()
            .unwrap()
            .send((Recipient::To(to), message))
            .await
        {
            warn!("Failed to send message to crypto verify worker: {err}");
        }
    }

    async fn broadcast_message(&mut self, message: core::PbftMessage) {
        if let Err(err) = self
            .message_tx
            .as_ref()
            .unwrap()
            .send((Recipient::Broadcast, message))
            .await
        {
            warn!("Failed to send message to crypto verify worker: {err}");
        }
    }

    async fn deliver(&mut self, requests: Vec<core::PbftRequest>, view_num: core::ViewNum) {
        if let Err(err) = self
            .deliver_tx
            .as_ref()
            .unwrap()
            .send((requests, view_num))
            .await
        {
            warn!("Failed to deliver requests: {err}");
        }
    }

    type Sig = S;
}

pub struct PbftCryptoVerifyWorker<C: core::PbftCoreCryptoContext> {
    core_crypto: core::PbftCoreCrypto<C>,

    bytes_tx: Sender<Vec<u8>>,
    bytes_rx: Receiver<Vec<u8>>,
    signed_message_tx: Option<Sender<(core::PbftMessage, C::Sig)>>, // node
}

impl<C: core::PbftCoreCryptoContext> PbftCryptoVerifyWorker<C> {
    pub fn new(core_crypto: core::PbftCoreCrypto<C>) -> Self {
        let (bytes_tx, bytes_rx) = channel(1000);
        Self {
            core_crypto,
            bytes_tx,
            bytes_rx,
            signed_message_tx: None,
        }
    }

    fn set_signed_message_tx(&mut self, signed_message_tx: Sender<(core::PbftMessage, C::Sig)>) {
        let replaced = self.signed_message_tx.replace(signed_message_tx);
        assert!(replaced.is_none(), "signed_message_tx can only be set once");
    }

    pub async fn run(&mut self) {
        while let Some(bytes) = self.bytes_rx.recv().await {
            let Some((message_bytes, sig_bytes)) =
                bytes.split_at_checked(bytes.len() - C::Sig::bytes_len())
            else {
                warn!("Received bytes too short to contain signature");
                continue;
            };
            let sig = C::Sig::from_bytes(sig_bytes);
            match self.core_crypto.verify(message_bytes, &sig) {
                Ok(message) => {
                    if let Err(err) = self
                        .signed_message_tx
                        .as_ref()
                        .unwrap()
                        .send((message, sig))
                        .await
                    {
                        warn!("Failed to send verified message to node: {err}");
                    }
                }
                Err(err) => {
                    warn!("Failed to verify message: {err}");
                }
            }
        }
    }
}

pub struct PbftCryptoSignWorker<C: core::PbftCoreCryptoContext> {
    core_crypto: core::PbftCoreCrypto<C>,

    message_tx: Sender<(Recipient, core::PbftMessage)>,
    message_rx: Receiver<(Recipient, core::PbftMessage)>,
    bytes_tx_map: Option<HashMap<core::ReplicaIndex, Sender<Bytes>>>, // network
    loopback_tx: Option<Sender<(core::PbftMessage, C::Sig)>>,         // node
}

impl<C: core::PbftCoreCryptoContext> PbftCryptoSignWorker<C> {
    pub fn new(core_crypto: core::PbftCoreCrypto<C>) -> Self {
        let (message_tx, message_rx) = channel(1000);
        Self {
            core_crypto,
            message_tx,
            message_rx,
            bytes_tx_map: None,
            loopback_tx: None,
        }
    }

    pub fn register<Ctx: core::PbftCoreContext>(&mut self, node: &mut PbftNode<Ctx>) {
        node.set_message_tx(self.message_tx.clone());
    }

    pub fn set_bytes_tx_map(&mut self, bytes_tx_map: HashMap<core::ReplicaIndex, Sender<Bytes>>) {
        let replaced = self.bytes_tx_map.replace(bytes_tx_map);
        assert!(replaced.is_none(), "bytes_tx_map can only be set once");
    }

    fn set_loopback_tx(&mut self, loopback_tx: Sender<(core::PbftMessage, C::Sig)>) {
        let replaced = self.loopback_tx.replace(loopback_tx);
        assert!(replaced.is_none(), "loopback_tx can only be set once");
    }

    pub async fn run(&mut self) {
        while let Some((recipient, mut message)) = self.message_rx.recv().await {
            let (mut bytes, sig) = self.core_crypto.sign(&mut message);
            bytes.extend_from_slice(sig.as_bytes());
            let bytes = bytes.into();
            match recipient {
                Recipient::To(to) => {
                    if let Err(err) = self
                        .bytes_tx_map
                        .as_ref()
                        .unwrap()
                        .get(&to)
                        .unwrap()
                        .send(bytes)
                        .await
                    {
                        warn!("Failed to send message to node {to}: {err}");
                    }
                }
                Recipient::Broadcast => {
                    for (to, bytes_tx) in self.bytes_tx_map.as_ref().unwrap() {
                        if let Err(err) = bytes_tx.send(bytes.clone()).await {
                            warn!("Failed to broadcast message to node {to}: {err}");
                        }
                    }
                }
            }
            if let Err(err) = self
                .loopback_tx
                .as_ref()
                .unwrap()
                .send((message, sig))
                .await
            {
                warn!("Failed to send signed message to node: {err}");
            }
        }
    }
}
