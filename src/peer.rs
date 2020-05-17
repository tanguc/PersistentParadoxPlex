use std::fmt::{Debug, Display, Error, Formatter};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use futures::{StreamExt};
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LinesCodec};
use uuid::Uuid;
use crate::persistent_marking_lb::{PeerEvent, RuntimeEvent, RuntimeOrderTxChannel};
use crate::UpstreamPeerMetadata;
use crate::backend;
use futures_util::TryFutureExt;

pub trait FrontEndPeer {}
pub trait BackEndPeer {}

pub type PeerTxChannel = mpsc::Sender<PeerEvent<String>>;
pub type PeerRxChannel = mpsc::Receiver<PeerEvent<String>>;

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

/// Only stream runtime is stored
/// The sink is only a simple channel and his runtime
/// is created by the GRPC runtime
pub struct UpstreamPeerHalve<T> {
    pub stream_halve: PeerHalve,
    // to write data to the client
    pub grpc_tx_channel: tokio::sync::mpsc::UnboundedSender<T>,
    // not available for us (passed to tonic)
    pub grpc_rx_channel: tokio::sync::mpsc::UnboundedReceiver<T>
}

pub enum PeerError {
    SocketAddrNotFound,
}

/// Create stream halve for upstream peers (GRPC)
/// Only stream halve is created, mainly because a
/// dedicated task handle the received messages.
/// The sink itself is a simple channel (mpsc) which will
/// be retrievable from runtime (to send messages)
pub fn create_upstream_halves<T>(
    metadata: UpstreamPeerMetadata,
    runtime_tx: RuntimeOrderTxChannel,
) -> UpstreamPeerHalve<T> {
    debug!("Creating upstream peer halves");
    let uuid = Uuid::new_v4();

    let stream_halve =
        PeerHalve::new(uuid, runtime_tx.clone(), metadata.host);

    let (message_tx, message_rx) =
        tokio::sync::mpsc::unbounded_channel::<T>();

    UpstreamPeerHalve {
        stream_halve,
        grpc_tx_channel: message_tx,
        grpc_rx_channel: message_rx
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
                            .send(RuntimeEvent::PeerTerminatedConnection(self.halve.metadata))
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

impl PeerHalveRuntime for UpstreamPeerHalve<backend::InputStreamRequest> {
    fn start(self) {
        debug!("Starting upstream stream halve");
        tokio::spawn(async {

            let mut connect_client =
                backend::backend_peer_service_client::BackendPeerServiceClient::connect(
                    format!("tcp://{}", self.stream_halve.metadata.socket_addr.to_string())
                ).map_err(|err| {
                    debug!("Cannot connect to the GRPC server [{:?}]", err);
                    tonic::Status::aborted("Cannot connect to the GRPC server")
                }).await?;

            let call_method =
                connect_client.bidirectional_streaming(
                    tonic::Request::new(self.grpc_rx_channel)).await?;

            if let mut method_stream = call_method.into_inner() {
                debug!("Been able to call the method");

                if let Some(message) = method_stream.message().await? {
                    debug!("Got a new message : [{:?}]", message);
                }

            } else {
                debug!("Cannot call method: because");
            }
            debug!("Handle GRPC4");

            Ok::<(), tonic::Status>(())
        });
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
                        PeerEvent::Start => {
                            info!("Starting the writing");
                        }
                        PeerEvent::Pause => {
                            info!("Pause the writing");
                        }
                        PeerEvent::Write(_payload) => {
                            info!("WRITING PAYLOADDDD");
                            // tcp_sink.send_all(&mut futures::stream::once(futures::future::ok(payload)));
                        },
                        PeerEvent::Stop => {
                            info!("Stopping peer sink");
                            break;
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
            mpsc::channel::<PeerEvent<String>>(1000);
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
    use crate::persistent_marking_lb::RuntimeEvent;
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
        let (runtime_tx, _) = mpsc::channel::<RuntimeEvent>(1);

        let peer_halves = super::create_peer_halves(tcp_socket, socket_addr, runtime_tx);
        assert_eq!(
            peer_halves.0.halve.metadata, peer_halves.1.halve.metadata,
            "Halves of the same peer do not have same UUID"
        );
    }
}
