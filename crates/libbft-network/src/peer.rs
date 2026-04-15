use std::{collections::HashMap, net::SocketAddr};

use futures_util::{SinkExt, StreamExt, future::OptionFuture};
use libbft::event::{Emit, EventReceiver, EventSender};
use tokio::{
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc::channel,
    task::JoinSet,
    try_join,
};
use tokio_util::{
    codec::{Decoder, LengthDelimitedCodec},
    sync::CancellationToken,
};
use tracing::warn;

mod events {
    use bytes::Bytes;
    use libbft::event::Event;

    pub type HandleBytes = libbft::network::events::HandleBytes;

    pub type SendBytes = libbft::network::events::SendBytes;

    pub struct SendConnectionBytes;
    impl Event for SendConnectionBytes {
        type Type = Bytes;
    }
}

pub struct PeerNetwork {
    listener: Option<TcpListener>,
    connection_workers: JoinSet<(SocketAddr, anyhow::Result<()>)>,
    connection_tx_map: HashMap<SocketAddr, EventSender<events::SendConnectionBytes>>,

    send_bytes_tx: EventSender<events::SendBytes>,
    send_bytes_rx: EventReceiver<events::SendBytes>,
    handle_bytes_tx: Option<EventSender<events::HandleBytes>>,
}

impl PeerNetwork {
    pub fn new(listener: impl Into<Option<TcpListener>>) -> Self {
        let (send_bytes_tx, send_bytes_rx) = channel(1000);
        Self {
            listener: listener.into(),
            connection_workers: JoinSet::new(),
            connection_tx_map: HashMap::new(),
            send_bytes_tx,
            send_bytes_rx,
            handle_bytes_tx: None,
        }
    }

    pub fn register(&self, emit_send_bytes: &mut impl Emit<events::SendBytes>) {
        emit_send_bytes.set_tx(self.send_bytes_tx.clone());
    }
}

impl Emit<events::HandleBytes> for PeerNetwork {
    fn tx_slot(&mut self) -> &mut Option<EventSender<events::HandleBytes>> {
        &mut self.handle_bytes_tx
    }
}

impl PeerNetwork {
    // currently exposing non-blocking connect, which will automatically connect to multiple peers
    // concurrently
    // can switch to blocking async method if causality or timely error reporting is desired
    pub fn connect(&mut self, remote_addr: SocketAddr) {
        self.spawn_worker(Remote::Connect(remote_addr));
    }

    fn spawn_worker(&mut self, remote: Remote) {
        let remote_addr = match remote {
            Remote::Connected(_, addr) => addr,
            Remote::Connect(addr) => addr,
        };
        let mut worker = ConnectionWorker::new(remote);
        Emit::<events::HandleBytes>::set_tx(&mut worker, self.handle_bytes_tx.clone().unwrap());

        if !self.connection_tx_map.contains_key(&remote_addr)
        // the case of simultaneous mutual connection attempts. this only happens when spawning
        // workers for accepted connections, so we can `unwrap` on the listener
        // tie breaking is done by preserving the connection initiated by the peer with smaller
        // address, that is, local addr (server) > remote addr (client)
        // say two peers with A and B, where A < B, connect to each other at the same time. at this
        // time, A inserts A -> B to the map, and B inserts B -> A
        // 1. A -> B accepted by B. B enters if branch, replaces the tx in the map and drop the old
        //    one
        // 2. B -> A accepted by A. A enters else branch and drops the new tx
        // before tie breaking completes, if B have sent messages to A (via B -> A), those messages
        // will successfully depart from B, as the channel will remain open until it's empty, even
        // if the tx has been replaced from the map. those messages will also be delivered by A, as
        // the ingress tasks always outlive the paired remote egress tasks
        //
        // nonetheless, it is recommended to apply random jitter to the protocol if its nodes
        // perform symmetric broadcasts
            || self.listener.as_ref().unwrap().local_addr().unwrap() > remote_addr
        {
            let replaced = self
                .connection_tx_map
                .insert(remote_addr, worker.send_bytes_tx.clone());
            if replaced.is_some() {
                warn!(
                    "Simultaneous connection attempt with peer {remote_addr}: replaced existing connection"
                );
            }
        } else {
            warn!(
                "Simultaneous connection attempt with peer {remote_addr}: dropped new connection"
            );
        }
        self.connection_workers
            .spawn(async move { (remote_addr, worker.run().await) });
    }

    pub async fn run(&mut self, token: &CancellationToken) -> anyhow::Result<()> {
        loop {
            enum Select<A, S, W> {
                Accept(A),
                SendBytes(S),
                Worker(W),
            }
            match select! {
                () = token.cancelled() => break,
                Some(accept) = OptionFuture::from(self.listener.as_mut().map(|l| l.accept())) => {
                    Select::Accept(accept?)
                }
                Some((remote_addr, bytes)) = self.send_bytes_rx.recv() => {
                    Select::SendBytes((remote_addr, bytes))
                }
                Some(res) = self.connection_workers.join_next() => Select::Worker(res.unwrap()),
            } {
                Select::Accept((stream, remote_addr)) => {
                    self.spawn_worker(Remote::Connected(stream, remote_addr));
                }
                Select::SendBytes(((remote_addr, bytes), span)) => {
                    if !self.connection_tx_map.contains_key(&remote_addr) {
                        self.connect(remote_addr);
                    }
                    if let Err(err) = self.connection_tx_map[&remote_addr]
                        .send((bytes, span))
                        .await
                    {
                        warn!("Failed to send bytes to peer {remote_addr}: {err:#}");
                    }
                }
                Select::Worker((remote_addr, res)) => {
                    if let Err(err) = res {
                        warn!("Connection worker for peer {remote_addr} failed: {err:#}");
                    }
                    self.connection_tx_map.remove(&remote_addr);
                }
            }
        }
        self.connection_tx_map.clear();
        while let Some(res) = self.connection_workers.join_next().await {
            if let (remote_addr, Err(err)) = res.unwrap() {
                warn!("Connection worker for peer {remote_addr} failed: {err:#}");
            }
        }
        Ok(())
    }
}

enum Remote {
    Connected(TcpStream, SocketAddr),
    Connect(SocketAddr),
}

pub struct ConnectionWorker {
    remote: Remote,

    send_bytes_tx: EventSender<events::SendConnectionBytes>,
    send_bytes_rx: EventReceiver<events::SendConnectionBytes>,
    handle_bytes_tx: Option<EventSender<events::HandleBytes>>,
}

impl ConnectionWorker {
    fn new(remote: Remote) -> Self {
        let (send_bytes_tx, send_bytes_rx) = channel(1000);
        Self {
            remote,
            send_bytes_tx,
            send_bytes_rx,
            handle_bytes_tx: None,
        }
    }
}

impl Emit<events::HandleBytes> for ConnectionWorker {
    fn tx_slot(&mut self) -> &mut Option<EventSender<events::HandleBytes>> {
        &mut self.handle_bytes_tx
    }
}

impl ConnectionWorker {
    async fn run(mut self) -> anyhow::Result<()> {
        let stream = match self.remote {
            Remote::Connected(stream, _) => stream,
            Remote::Connect(remote_addr) => TcpStream::connect(remote_addr).await?,
        };
        let handle_bytes_tx = self.handle_bytes_tx.unwrap();
        drop(self.send_bytes_tx);
        let (mut write, mut read) = LengthDelimitedCodec::new().framed(stream).split();
        let ingress = async {
            while let Some(bytes) = read.next().await.transpose()? {
                if let Err(err) = handle_bytes_tx
                    .send((bytes.to_vec(), tracing::Span::none()))
                    .await
                {
                    warn!("Failed to send received bytes to handler: {err:#}");
                }
            }
            anyhow::Ok(())
        };
        let egress = async {
            while let Some((bytes, _span)) = self.send_bytes_rx.recv().await {
                write.send(bytes).await?
            }
            anyhow::Ok(())
        };
        try_join!(ingress, egress)?;
        Ok(())
    }
}
