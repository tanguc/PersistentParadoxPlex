use crate::persistent_marking_lb::{InnerExchange, RuntimeOrder};
use std::fmt::Debug;
use std::net::SocketAddr;
use tokio::net::TcpStream;

use futures::StreamExt;
use tokio::stream::StreamExt as TStreamExt;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio_util::codec::{Framed, LinesCodec};
use uuid::Uuid;

pub trait FrontEndPeer {}
pub trait BackEndPeer {}

#[derive(Debug)]
pub struct Peer {
    pub uuid: Uuid,
    pub runtime_rx: watch::Receiver<RuntimeOrder>,
    // pub frame: Framed<TcpStream, LinesCodec>,
    // pub sink: futures::stream::SplitSink<
    //     futures::stream::SplitSink<String, Error = ()> + Unpin + Send,
    //     String,
    // >,
    // pub stream: futures::stream::SplitStream<String>,
    pub stream_channel_rx: mpsc::Receiver<InnerExchange<String>>,
    pub stream_channel_tx: mpsc::Sender<InnerExchange<String>>,

    pub sink_channel_rx: mpsc::Receiver<InnerExchange<String>>,
    pub sink_channel_tx: mpsc::Sender<InnerExchange<String>>,
}

// pub struct PeerHalve {
//     pub runtime_rx: watch::Receiver<RuntimeOrder>,
//     pub rx: T<InnerExchange<String>>,
//     pub tx: T<InnerExchange<String>>,
// }

pub struct PeerMetadata {
    pub uuid: Uuid,
    pub socket_addr: SocketAddr,
}

pub struct PeerHalve {
    pub metadata: PeerMetadata,
    runtime_rx: watch::Receiver<RuntimeOrder>,
    rx: mpsc::Receiver<InnerExchange<String>>,
    pub tx: mpsc::Sender<InnerExchange<String>>,
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
pub fn peer_halves(
    tcp_stream: TcpStream,
    socket_addr: SocketAddr,
    runtime_rx: watch::Receiver<RuntimeOrder>,
) -> (SinkPeerHalve, StreamPeerHalve) {
    let frame = Framed::new(tcp_stream, LinesCodec::new());
    let (tcp_sink, tcp_stream) = frame.split::<String>();

    let peer_sink: SinkPeerHalve =
        SinkPeerHalve::new(tcp_sink, runtime_rx.clone(), socket_addr.clone());
    let peer_stream: StreamPeerHalve =
        StreamPeerHalve::new(tcp_stream, runtime_rx.clone(), socket_addr.clone());

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
                ///
                /// impossible
                // let should_not = self.halve.rx.recv().await;
                // debug!("PeerHalveStream result from rx : {:?}", should_not);
                ///
                ///
                let line = futures::stream::StreamExt::next(&mut self.tcp_stream).await;
                // let line = tokio::stream::StreamExt::next(&mut tcp_stream).await;

                match line {
                    Some(line) => {
                        debug!("Got a new line : {:?}", line);
                    }
                    None => {
                        debug!("Peer terminated connection");
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
                        InnerExchange::START => {
                            info!("Starting the writing");
                        }
                        InnerExchange::PAUSE => {
                            info!("Pause the writing");
                        }
                        InnerExchange::WRITE(_payload) => {
                            info!("WRITING PAYLOADDDD");
                            // tcp_sink.send_all(&mut futures::stream::once(futures::future::ok(payload)));
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
        runtime_rx: watch::Receiver<RuntimeOrder>,
        socket_addr: SocketAddr,
    ) -> Self {
        SinkPeerHalve {
            halve: PeerHalve::new(runtime_rx, socket_addr),
            tcp_sink,
        }
    }
}

impl StreamPeerHalve {
    pub fn new(
        tcp_stream: futures::stream::SplitStream<Framed<TcpStream, LinesCodec>>,
        runtime_rx: watch::Receiver<RuntimeOrder>,
        socket_addr: SocketAddr,
    ) -> Self {
        StreamPeerHalve {
            halve: PeerHalve::new(runtime_rx, socket_addr),
            tcp_stream,
        }
    }
}

impl PeerHalve {
    pub fn new(runtime_rx: watch::Receiver<RuntimeOrder>, socket_addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::channel::<InnerExchange<String>>(1000);
        PeerHalve {
            metadata: PeerMetadata {
                uuid: Uuid::new_v4(),
                socket_addr,
            },
            runtime_rx,
            rx,
            tx,
        }
    }
}

impl Peer {
    pub fn new(runtime_rx: watch::Receiver<RuntimeOrder>) -> Self {
        let (stream_channel_tx, stream_channel_rx) = mpsc::channel::<InnerExchange<String>>(1000);
        let (sink_channel_tx, sink_channel_rx) = mpsc::channel::<InnerExchange<String>>(1000);

        Peer {
            uuid: Uuid::new_v4(),
            runtime_rx,
            stream_channel_rx,
            stream_channel_tx,
            sink_channel_rx,
            sink_channel_tx,
        }
    }
}
