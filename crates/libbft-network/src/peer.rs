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
    use std::net::SocketAddr;

    use bytes::Bytes;
    use libbft::event::Event;

    pub struct SendBytes;
    impl Event for SendBytes {
        type Type = (SocketAddr, Bytes);
    }

    pub struct HandleBytes;
    impl Event for HandleBytes {
        type Type = Vec<u8>;
    }

    pub struct SendConnectionBytes;
    impl Event for SendConnectionBytes {
        type Type = Bytes;
    }
}

pub struct PeerNetwork {
    listener: Option<TcpListener>,
    connection_workers: JoinSet<(SocketAddr, anyhow::Result<()>)>,
    send_connection_bytes_tx_map: HashMap<SocketAddr, EventSender<events::SendConnectionBytes>>,

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
            send_connection_bytes_tx_map: HashMap::new(),
            send_bytes_tx,
            send_bytes_rx,
            handle_bytes_tx: None,
        }
    }

    pub fn register(&self, emit_send_bytes: &mut impl Emit<events::SendBytes>) {
        emit_send_bytes.set_tx(self.send_bytes_tx.clone());
    }

    // currently exposing non-blocking connect, which will automatically connect to multiple peers
    // concurrently
    // can switch to blocking async method if causality or timely error reporting is desired
    pub fn connect(&mut self, remote_addr: SocketAddr) {
        self.spawn_worker(Remote::Connect(remote_addr));
    }

    fn spawn_worker(&mut self, remote: Remote) {
        let remote_addr = match &remote {
            &Remote::Connected(_, addr) => addr,
            &Remote::Connect(addr) => addr,
        };
        let mut worker = ConnectionWorker::new(remote);
        Emit::<events::HandleBytes>::set_tx(&mut worker, self.handle_bytes_tx.clone().unwrap());
        self.send_connection_bytes_tx_map
            .insert(remote_addr, worker.send_bytes_tx.clone());
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
                    if !self.send_connection_bytes_tx_map.contains_key(&remote_addr) {
                        self.connect(remote_addr);
                    }
                    if let Err(err) = self.send_connection_bytes_tx_map[&remote_addr]
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
                    self.send_connection_bytes_tx_map.remove(&remote_addr);
                }
            }
        }
        self.send_connection_bytes_tx_map.clear();
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
        let token = CancellationToken::new();
        let ingress = async {
            while let Some(bytes) = token
                .run_until_cancelled(read.next())
                .await
                .flatten()
                .transpose()?
            {
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
                write.send(bytes.into()).await?
            }
            token.cancel();
            anyhow::Ok(())
        };
        try_join!(ingress, egress)?;
        Ok(())
    }
}
