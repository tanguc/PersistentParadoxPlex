use crate::backend;
use crate::runtime::{PeerEvent, RuntimeEvent, RuntimeOrderTxChannel};
use crate::UpstreamPeerMetadata;
use futures::StreamExt;
use futures_util::TryFutureExt;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display, Error, Formatter};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LinesCodec};
use uuid::Uuid;

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
    pub grpc_rx_channel: tokio::sync::mpsc::UnboundedReceiver<T>,
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Peer got a broken pipe")
    }
}

impl std::fmt::Debug for PeerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Peer got a broken pipe")
    }
}

impl std::convert::From<tonic::Status> for Box<PeerError> {
    fn from(status: tonic::Status) -> Self {
        Box::new(PeerError::BrokenPipe)
    }
}

impl std::error::Error for PeerError {
    fn description(&self) -> &str {
        "Peer got a broken pipe"
    }

    fn cause(&self) -> Option<&(dyn std::error::Error)> {
        None
    }
}

pub enum PeerError {
    SocketAddrNotFound,
    BrokenPipe,
    ServerClosed,
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

    let stream_halve = PeerHalve::new(uuid, runtime_tx.clone(), metadata.host);

    let (message_tx, message_rx) = tokio::sync::mpsc::unbounded_channel::<T>();

    UpstreamPeerHalve {
        stream_halve,
        grpc_tx_channel: message_tx,
        grpc_rx_channel: message_rx,
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

type BoxError = Result<(), Box<PeerError>>;

pub trait PeerHalveRuntime {
    fn start(self) -> JoinHandle<BoxError>;
}

fn prepare_upstream_sink_request(payload: String) -> backend::InputStreamRequest {
    let address = String::from("127.0.0.1");
    let time = String::from("14:12:44");

    let request = backend::InputStreamRequest {
        header: Option::Some(backend::Header { address, time }),
        payload,
    };

    return request;
}

async fn get_upstream_tx_channel(
    mut runtime_tx: RuntimeOrderTxChannel,
) -> Option<tokio::sync::mpsc::UnboundedSender<backend::InputStreamRequest>> {
    debug!("Finding an upstream channel available");

    let oneshot_answer = tokio::sync::oneshot::channel();

    let order = RuntimeEvent::GetUpstreamPeer(oneshot_answer.0);
    match runtime_tx.send(order).await {
        Ok(_) => {
            debug!("Asked for an upstream peer to the runtime");
            debug!("Waiting for an answer");

            match tokio::join!(oneshot_answer.1).0 {
                Ok(answer) => match answer {
                    Some(upstream_grpc_tx) => {
                        debug!("Got the channel of the upstream peer");
                        Option::Some(upstream_grpc_tx)
                    }
                    None => Option::None,
                },
                Err(_) => {
                    error!("Failed when received answer from runtime about the upstream peer");
                    Option::None
                }
            }
        }
        Err(err) => {
            error!("Failed when asked for an upstream stream, \n :{:?}", err);
            Option::None
        }
    }
}

impl PeerHalveRuntime for DownstreamPeerStreamHalve {
    fn start(mut self) -> JoinHandle<BoxError> {
        let read_task = move || async move {
            info!(
                "Spawning read task for the client {}",
                self.halve.metadata.uuid
            );
            loop {
                let line = futures::stream::StreamExt::next(&mut self.tcp_stream).await;
                match line {
                    Some(line) => {
                        match line {
                            Ok(line) => {
                                debug!("Got a new line : {:?}", line);
                                let upstream_tx_channel =
                                    get_upstream_tx_channel(self.halve.runtime_tx.clone()).await;

                                match upstream_tx_channel {
                                    Some(tx_channel) => {
                                        debug!("Writing to the upstream peer");
                                        let request = prepare_upstream_sink_request(line);
                                        if let Err(err) = tx_channel.send(request) {
                                            error!("Failed to send an input request to the upstream: {:?}", err);
                                        }
                                    }
                                    None => {
                                        debug!(
                                            "Didnt find any channel available to send from downstream"
                                        );
                                    }
                                }
                            }
                            Err(codec_error) => {
                                error!(
                                    "Got a codec error when received downstream payload: {:?}",
                                    codec_error
                                );
                            }
                        }
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
            Ok::<(), Box<PeerError>>(())
        };
        tokio::task::spawn(read_task())
    }
}

impl PeerHalveRuntime for UpstreamPeerHalve<backend::InputStreamRequest> {
    fn start(mut self) -> JoinHandle<BoxError> {
        debug!("Starting upstream stream halve");
        tokio::spawn(async {
            let mut connect_client =
                backend::backend_peer_service_client::UpstreamPeerServiceClient::connect(format!(
                    "tcp://{}",
                    self.stream_halve.metadata.socket_addr.to_string()
                ))
                .map_err(|err| {
                    debug!("Cannot connect to the GRPC server [{:?}]", err);
                    return Box::new(PeerError::ServerClosed);
                })
                .await?;

            debug!("calling method");
            let call_method = connect_client
                .bidirectional_streaming(tonic::Request::new(self.grpc_rx_channel))
                .await?;

            let mut read_stream_loop = call_method.into_inner();
            loop {
                debug!("Starting reading loop from upstream..");
                match read_stream_loop.message().await {
                    Ok(stream_res) => match stream_res {
                        Some(message) => {
                            debug!("Got message from upstream: [{:?}]", message);
                            let order = RuntimeEvent::MessageToDownstreamPeer(message);
                            if let Err(err) = self.stream_halve.runtime_tx.send(order).await {
                                error!("Failed to send the order MessageToDownstreamPeer to the runtime [{:?}]", err);
                            }
                        }
                        None => {
                            error!(
                                "Received NONE from upstream, weird, please contact the developer"
                            );
                        }
                    },
                    Err(err) => {
                        error!("The error code [{:?}]", err.code() as u8);
                        error!("The error message [{:?}]", err);
                        debug!("Notifying the runtime about upstream termination...");
                        match self
                            .stream_halve
                            .runtime_tx
                            .send(RuntimeEvent::PeerTerminatedConnection(
                                self.stream_halve.metadata.clone(),
                            ))
                            .await
                        {
                            Ok(_) => {
                                debug!("Successfully notified the runtime");
                            }
                            Err(err) => {
                                error!("Failed to send the upstream termination to the runtime");
                                error!("{:?}", err);
                            }
                        }
                        return Err(Box::new(PeerError::BrokenPipe));
                    }
                }
            }
        })
    }
}

fn prepare_downstream_sink_request(payload: std::string::String) -> backend::InputStreamRequest {
    debug!("Preparing downstream peer input request");

    let request = backend::InputStreamRequest {
        header: Option::Some(backend::Header {
            time: "92:398:329".to_string(),
            address: "127.43.49.30".to_string(),
        }),
        payload: payload,
    };

    return request;
}

// async fn get_downstream_peer(
//     mut runtime_tx: RuntimeOrderTxChannel,
//     client_uuid: &str,
// ) -> Option<tokio::sync::mpsc::Sender<backend::InputStreamRequest>> {
//     debug!(
//         "Finding the downstream client with UUID [{:?}]",
//         client_uuid
//     );

//     let oneshot_channel = tokio::sync::oneshot::channel::<
//         Option<tokio::sync::mpsc::Sender<backend::InputStreamRequest>>,
//     >();

//     let order = RuntimeEvent::MessageToDownstreamPeer((oneshot_channel.0, client_uuid.to_string()));
//     let mut downstream_peer_tx = Option::None;

//     match runtime_tx.send(order).await {
//         Ok(_) => match tokio::join!(oneshot_channel.1).0 {
//             Ok(runtime_downstream_peer_tx) => {
//                 debug!("Received answer to GetDownstreamPeer order");
//             }
//             Err(err) => {
//                 error!("Failed to receive GetDownstreamPeer answer from the runtime (oneshot channel) [{:?}]", err);
//             }
//         },
//         Err(err) => {
//             error!("Failed to send GetDownstreamPeer order to the runtime");
//         }
//     }

//     return downstream_peer_tx;
// }

impl PeerHalveRuntime for DownstreamPeerSinkHalve {
    fn start(mut self) -> JoinHandle<BoxError> {
        let write_task = move || async move {
            info!(
                "Spawning writing task for the client {}",
                self.halve.metadata.uuid
            );
            loop {
                // self.tcp_sink.
                if let Some(event) = self.halve.rx.recv().await {
                    info!("Got order from another task to sink");
                    match event {
                        PeerEvent::Start => {
                            info!("Starting the writing");
                        }
                        PeerEvent::Pause => {
                            info!("Pause the writing");
                        }
                        PeerEvent::Write(_payload) => {
                            info!("WRITING PAYLOADDDD");

                            let downstream_message = prepare_downstream_sink_request(_payload);

                            // tcp_sink.send_all(&mut futures::stream::once(futures::future::ok(payload)));
                        }
                        PeerEvent::Stop => {
                            info!("Stopping peer sink");
                            break;
                        }
                    }
                } else {
                    info!("Weird got nothing");
                }
            }
            Ok::<(), Box<PeerError>>(())
        };
        tokio::task::spawn(write_task())
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
        let (tx, rx) = mpsc::channel::<PeerEvent<String>>(1000);
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
    use crate::runtime::RuntimeEvent;
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
