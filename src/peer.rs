use crate::persistent_marking_lb::{InnerExchange, RuntimeOrder, RuntimeOrderTxChannel};
use std::fmt::{Debug, Display, Error, Formatter};
use std::net::SocketAddr;
use tokio::net::TcpStream;

use futures::StreamExt;
use tokio::runtime::Runtime;
use tokio::stream::StreamExt as TStreamExt;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio_util::codec::{Framed, LinesCodec};
use uuid::Uuid;

pub trait FrontEndPeer {}
pub trait BackEndPeer {}

pub type PeerTxChannel = mpsc::Sender<InnerExchange<String>>;
pub type PeerRxChannel = mpsc::Receiver<InnerExchange<String>>;

#[derive(Debug)]
pub struct Peer {
    pub uuid: Uuid,
    // Useful when the Peer need to contact the runtime (peer terminated
    // connection)
    // Note: when the runtime need to communicate with the peer, it will
    // go through the associated halve rx channel
    pub runtime_tx: RuntimeOrderTxChannel,

    pub stream_channel_tx: PeerTxChannel,
    pub stream_channel_rx: PeerRxChannel,

    pub sink_channel_tx: PeerTxChannel,
    pub sink_channel_rx: PeerRxChannel,
}

#[derive(Clone)]
pub struct PeerMetadata {
    pub uuid: Uuid,
    pub socket_addr: SocketAddr,
}

impl Debug for PeerMetadata {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.debug_struct("PeerMetadata")
            .field("uuid", &self.uuid)
            .field("socket_addr", &self.socket_addr)
            .finish()
    }
}

impl Display for PeerMetadata {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.write_fmt(format_args!(
            "Peer's metadata \
            UUID: {}\
            SocketAddr: {}\
        ",
            self.uuid, self.socket_addr
        ))
    }
}

pub struct PeerHalve {
    pub metadata: PeerMetadata,
    runtime_tx: RuntimeOrderTxChannel,
    pub rx: PeerRxChannel,
    pub tx: PeerTxChannel,
}

pub struct StreamPeerHalve {
    pub halve: PeerHalve,
    pub tcp_stream: futures::stream::SplitStream<Framed<TcpStream, LinesCodec>>,
}

pub struct SinkPeerHalve {
    pub halve: PeerHalve,
    pub tcp_sink: futures::stream::SplitSink<Framed<TcpStream, LinesCodec>, String>,
}

pub enum PeerError {
    SocketAddrNotFound,
}

/// Create sink (write) & stream (read) halves for the
/// given TcpStream
pub fn create_peer_halves(
    tcp_stream: TcpStream,
    socket_addr: SocketAddr,
    runtime_tx: RuntimeOrderTxChannel,
) -> (SinkPeerHalve, StreamPeerHalve) {
    let frame = Framed::new(tcp_stream, LinesCodec::new());
    let (tcp_sink, tcp_stream) = frame.split::<String>();
    let uuid = Uuid::new_v4();

    let peer_sink: SinkPeerHalve =
        SinkPeerHalve::new(tcp_sink, uuid, runtime_tx.clone(), socket_addr.clone());
    let peer_stream: StreamPeerHalve =
        StreamPeerHalve::new(tcp_stream, uuid, runtime_tx.clone(), socket_addr.clone());

    (peer_sink, peer_stream)
}

pub trait PeerHalveRuntime {
    fn start(self);
}

impl PeerHalveRuntime for StreamPeerHalve {
    fn start(mut self) {
        let read_task = move || async move {
            info!(
                "Spawning read task for the client {}",
                self.halve.metadata.uuid
            );
            loop {
                let line = futures::stream::StreamExt::next(&mut self.tcp_stream).await;
                match line {
                    Some(line) => {
                        debug!("Got a new line : {:?}", line);
                    }
                    None => {
                        debug!("Peer terminated connection, notifying runtime");
                        if let Err(err) = self
                            .halve
                            .runtime_tx
                            .send(RuntimeOrder::PeerTerminatedConnection(self.halve.metadata))
                            .await
                        {
                            error!(
                                "Could not send the termination of the \
                            peer to the runtime via channel, reason : {}",
                                err
                            );
                        }
                        break;
                    }
                }
            }
        };
        tokio::task::spawn(read_task());
    }
}

impl PeerHalveRuntime for SinkPeerHalve {
    fn start(mut self) {
        let write_task = move || async move {
            info!(
                "Spawning writing task for the client {}",
                self.halve.metadata.uuid
            );
            loop {
                // self.tcp_sink.
                if let Some(sink_order) = self.halve.rx.recv().await {
                    info!("Got order from another task to sink");
                    match sink_order {
                        InnerExchange::Start => {
                            info!("Starting the writing");
                        }
                        InnerExchange::Pause => {
                            info!("Pause the writing");
                        }
                        InnerExchange::Write(_payload) => {
                            info!("WRITING PAYLOADDDD");
                            // tcp_sink.send_all(&mut futures::stream::once(futures::future::ok(payload)));
                        }
                        InnerExchange::FromRuntime(runtime_order) => {
                            info!("Got an order from runtime : {:?}", runtime_order);
                        }
                    }
                } else {
                    info!("Weird got nothing");
                }
            }
        };
        tokio::task::spawn(write_task());
    }
}

impl SinkPeerHalve {
    pub fn new(
        tcp_sink: futures::stream::SplitSink<Framed<TcpStream, LinesCodec>, String>,
        uuid: Uuid,
        runtime_tx: RuntimeOrderTxChannel,
        socket_addr: SocketAddr,
    ) -> Self {
        SinkPeerHalve {
            halve: PeerHalve::new(uuid, runtime_tx, socket_addr),
            tcp_sink,
        }
    }
}

impl StreamPeerHalve {
    pub fn new(
        tcp_stream: futures::stream::SplitStream<Framed<TcpStream, LinesCodec>>,
        uuid: Uuid,
        runtime_tx: RuntimeOrderTxChannel,
        socket_addr: SocketAddr,
    ) -> Self {
        StreamPeerHalve {
            halve: PeerHalve::new(uuid, runtime_tx, socket_addr),
            tcp_stream,
        }
    }
}

impl PeerHalve {
    pub fn new(uuid: Uuid, runtime_tx: RuntimeOrderTxChannel, socket_addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::channel::<InnerExchange<String>>(1000);
        PeerHalve {
            metadata: PeerMetadata { uuid, socket_addr },
            runtime_tx,
            rx,
            tx,
        }
    }
}

impl Peer {
    pub fn new(runtime_tx: RuntimeOrderTxChannel) -> Self {
        let (stream_channel_tx, stream_channel_rx) = mpsc::channel::<InnerExchange<String>>(1000);
        let (sink_channel_tx, sink_channel_rx) = mpsc::channel::<InnerExchange<String>>(1000);

        Peer {
            uuid: Uuid::new_v4(),
            runtime_tx,
            stream_channel_rx,
            stream_channel_tx,
            sink_channel_rx,
            sink_channel_tx,
        }
    }
}
