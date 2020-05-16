use crate::persistent_marking_lb::{InnerExchange, RuntimeOrder, RuntimeOrderTxChannel};
use std::fmt::{Debug, Display, Error, Formatter};
use std::net::SocketAddr;
use tokio::net::TcpStream;

use futures::StreamExt;

use tokio::sync::mpsc;

use tokio_util::codec::{Framed, LinesCodec};
use uuid::Uuid;
use crate::UpstreamPeerMetadata;

pub trait FrontEndPeer {}
pub trait BackEndPeer {}

pub type PeerTxChannel = mpsc::Sender<InnerExchange<String>>;
pub type PeerRxChannel = mpsc::Receiver<InnerExchange<String>>;

#[derive(Clone, PartialEq)]
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
            SocketAddr: {}",
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

pub struct DownstreamPeerStreamHalve {
    pub halve: PeerHalve,
    pub tcp_stream: futures::stream::SplitStream<Framed<TcpStream, LinesCodec>>,
}

pub struct DownstreamPeerSinkHalve {
    pub halve: PeerHalve,
    pub tcp_sink: futures::stream::SplitSink<Framed<TcpStream, LinesCodec>, String>,
}

pub struct UpstreamPeerStreamHalve {
    pub halve: PeerHalve
}

pub enum PeerError {
    SocketAddrNotFound,
}

/// Create stream halve for upstream peers (GRPC)
/// Only stream halve is created, mainly because a
/// dedicated task handle the received messages.
/// The sink itself is a simple channel (mpsc) which will
/// be retrievable from runtime (to send messages)
pub fn create_upstream_halves(
    metadata: UpstreamPeerMetadata,
    runtime_tx: RuntimeOrderTxChannel,
) -> UpstreamPeerStreamHalve {
    debug!("Creating upstream peer halves");
    let uuid = Uuid::new_v4();

    let stream_halve =
        PeerHalve::new(uuid, runtime_tx.clone(), metadata.into());

    UpstreamPeerStreamHalve {
        halve: stream_halve
    }
}

/// Create sink (write) & stream (read) halves for the
/// given TcpStream
/// Either the stream or the sink halve, are under the same UUID (same client)
pub fn create_peer_halves(
    tcp_stream: TcpStream,
    socket_addr: SocketAddr,
    runtime_tx: RuntimeOrderTxChannel,
) -> (DownstreamPeerSinkHalve, DownstreamPeerStreamHalve) {
    let frame = Framed::new(tcp_stream, LinesCodec::new());
    let (tcp_sink, tcp_stream) = frame.split::<String>();
    let uuid = Uuid::new_v4();

    let peer_sink: DownstreamPeerSinkHalve =
        DownstreamPeerSinkHalve::new(tcp_sink, uuid, runtime_tx.clone(), socket_addr.clone());
    let peer_stream: DownstreamPeerStreamHalve =
        DownstreamPeerStreamHalve::new(tcp_stream, uuid, runtime_tx.clone(), socket_addr.clone());

    (peer_sink, peer_stream)
}

pub trait PeerHalveRuntime {
    fn start(self);
}

impl PeerHalveRuntime for DownstreamPeerStreamHalve {
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

                        // choice a back peer (downstream)
                        // send via the channel the string(buffer)
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

impl PeerHalveRuntime for DownstreamPeerSinkHalve {
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

impl DownstreamPeerSinkHalve {
    pub fn new(
        tcp_sink: futures::stream::SplitSink<Framed<TcpStream, LinesCodec>, String>,
        uuid: Uuid,
        runtime_tx: RuntimeOrderTxChannel,
        socket_addr: SocketAddr,
    ) -> Self {
        DownstreamPeerSinkHalve {
            halve: PeerHalve::new(uuid, runtime_tx, socket_addr),
            tcp_sink,
        }
    }
}

impl DownstreamPeerStreamHalve {
    pub fn new(
        tcp_stream: futures::stream::SplitStream<Framed<TcpStream, LinesCodec>>,
        uuid: Uuid,
        runtime_tx: RuntimeOrderTxChannel,
        socket_addr: SocketAddr,
    ) -> Self {
        DownstreamPeerStreamHalve {
            halve: PeerHalve::new(uuid, runtime_tx, socket_addr),
            tcp_stream,
        }
    }
}

impl PeerHalve {
    pub fn new(uuid: Uuid, runtime_tx: RuntimeOrderTxChannel, socket_addr: SocketAddr) -> Self {
        let (tx, rx) =
            mpsc::channel::<InnerExchange<String>>(1000);
        PeerHalve {
            metadata: PeerMetadata { uuid, socket_addr },
            runtime_tx,
            rx,
            tx,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::persistent_marking_lb::RuntimeOrder;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;
    use tokio::time::{delay_for, Duration};

    #[tokio::test]
    async fn test_create_peer_halves() {
        let addr = String::from("127.0.0.1:59403");
        let tcp_listener = TcpListener::bind(addr.clone());
        let mock_tcp_client_task = async move {
            delay_for(Duration::from_secs(1)).await;
            let _tcp_stream = TcpStream::connect(addr.clone()).await.unwrap();
        };
        tokio::task::spawn(mock_tcp_client_task);
        let (tcp_socket, socket_addr) = tcp_listener.await.unwrap().accept().await.unwrap();
        debug!("Client connected");
        let (runtime_tx, _) = mpsc::channel::<RuntimeOrder>(1);

        let peer_halves = super::create_peer_halves(tcp_socket, socket_addr, runtime_tx);
        assert_eq!(
            peer_halves.0.halve.metadata, peer_halves.1.halve.metadata,
            "Halves of the same peer do not have same UUID"
        );
    }
}
