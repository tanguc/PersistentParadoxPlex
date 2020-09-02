use crate::{
    runtime::{PeerEvent, RuntimeEvent, RuntimeOrderTxChannel},
    upstream::get_upstream_tx_channel,
    upstream_proto::{Header, InputStreamRequest},
};
use async_trait::async_trait;
use futures::StreamExt;
use std::fmt::{Debug, Display, Error, Formatter};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LinesCodec};
use uuid::Uuid;

pub type PeerEventTxChannel = mpsc::Sender<PeerEvent<String>>;
pub type PeerEventRxChannel = mpsc::Receiver<PeerEvent<String>>;

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
    pub runtime_tx: RuntimeOrderTxChannel,
    pub rx: PeerEventRxChannel,
    pub tx: PeerEventTxChannel,
}

pub struct DownstreamPeerSinkHalve {
    halve: PeerHalve,
    tcp_sink: futures::stream::SplitSink<Framed<TcpStream, LinesCodec>, String>,
}

pub struct DownstreamPeerStreamHalve {
    halve: PeerHalve,
    tcp_stream: futures::stream::SplitStream<Framed<TcpStream, LinesCodec>>,
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

pub type BoxError = Result<(), Box<PeerError>>;

pub struct DownstreamPeer {
    pub metadata: PeerMetadata,
    pub sink: DownstreamPeerSinkHalve,
    pub stream: DownstreamPeerStreamHalve,
}

impl DownstreamPeer {
    pub fn new(
        tcp_stream: TcpStream,
        socket_addr: SocketAddr,
        runtime_tx: RuntimeOrderTxChannel,
    ) -> Self {
        let frame = Framed::new(tcp_stream, LinesCodec::new());
        let (tcp_sink, tcp_stream) = frame.split::<String>();
        let uuid = Uuid::new_v4();

        let peer_sink: DownstreamPeerSinkHalve =
            DownstreamPeerSinkHalve::new(tcp_sink, uuid, runtime_tx.clone(), socket_addr.clone());
        let peer_stream: DownstreamPeerStreamHalve = DownstreamPeerStreamHalve::new(
            tcp_stream,
            uuid,
            runtime_tx.clone(),
            socket_addr.clone(),
        );

        Self {
            metadata: PeerMetadata {
                socket_addr: socket_addr.clone(),
                uuid: uuid.clone(),
            },
            sink: peer_sink,
            stream: peer_stream,
        }
    }
}

#[async_trait]
pub trait PeerRuntime {
    type Output;

    async fn start(mut self) -> Result<Self::Output, PeerError>; // TODO should return the tx of the runtime of the peer (to handle runtiome event)
}

pub struct DownstreamPeerFinalState {
    pub metadata: PeerMetadata,
    pub sink_tx: PeerEventTxChannel,
    pub stream_tx: PeerEventTxChannel,
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

#[async_trait]
impl PeerRuntime for DownstreamPeer {
    type Output = DownstreamPeerFinalState;

    async fn start(mut self) -> Result<Self::Output, PeerError> {
        let sink_tx = self.sink.start();
        let stream_tx = self.stream.start();

        Ok(DownstreamPeerFinalState {
            metadata: self.metadata,
            sink_tx,
            stream_tx,
        })
    }
}

impl DownstreamPeerStreamHalve {
    fn start(mut self) -> PeerEventTxChannel {
        let tx = self.halve.tx.clone();
        let task = move || async move {
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
                                    Some(mut tx_channel) => {
                                        debug!("Writing to the upstream peer");
                                        // let request = prepare_upstream_sink_request(line);

                                        if let Err(err) =
                                            tx_channel.send(PeerEvent::Write(line)).await
                                        {
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
        tokio::task::spawn(task());

        tx
    }
}

impl DownstreamPeerSinkHalve {
    fn start(mut self) -> PeerEventTxChannel {
        let tx = self.halve.tx.clone();
        let task = move || async move {
            info!(
                "Spawning writing task for the client {}",
                self.halve.metadata.uuid
            );
            let mut paused = false;
            loop {
                // self.tcp_sink.
                if let Some(event) = self.halve.rx.recv().await {
                    match event {
                        PeerEvent::Start => {
                            info!("[Downstream sink ORDER] Start");
                        }
                        PeerEvent::Pause => {
                            paused = true;
                            info!("[Downstream sink ORDER] Pause -");
                        }
                        PeerEvent::Write(_payload) => {
                            info!("[Downstream sink ORDER] Write -");
                            //todo check if it's paused
                            let downstream_message = prepare_downstream_sink_request(_payload);

                            // tcp_sink.send_all(&mut futures::stream::once(futures::future::ok(payload)));
                        }
                        PeerEvent::Stop => {
                            info!("[Downstream sink ORDER] Stop");
                            break;
                        }
                        PeerEvent::Resume => {
                            trace!(
                                "Resume the downstream sink [{}]",
                                self.halve.metadata.uuid.clone()
                            );
                        }
                    }
                } else {
                    info!("Weird got nothing");
                }
            }
            Ok::<(), Box<PeerError>>(())
        };
        tokio::task::spawn(task());

        tx
    }
}

fn prepare_downstream_sink_request(payload: std::string::String) -> InputStreamRequest {
    debug!("Preparing downstream peer input request");

    let request = InputStreamRequest {
        header: Option::Some(Header {
            client_uuid: String::from("totoierz"),
            time: "92:398:329".to_string(),
            address: "127.43.49.30".to_string(),
        }),
        payload: payload,
    };

    return request;
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

        let downstream_peer = super::DownstreamPeer::new(tcp_socket, socket_addr, runtime_tx);
        // let peer_halves = super::create_downstream_peer_halves(tcp_socket, socket_addr, runtime_tx);
        assert_eq!(
            downstream_peer.sink.halve.metadata, downstream_peer.stream.halve.metadata,
            "Halves of the same peer do not have same UUID"
        );
    }
}

// async fn get_downstream_peer(
//     mut runtime_tx: RuntimeOrderTxChannel,
//     client_uuid: &str,
// ) -> Option<tokio::sync::mpsc::Sender<InputStreamRequest>> {
//     debug!(
//         "Finding the downstream client with UUID [{:?}]",
//         client_uuid
//     );

//     let oneshot_channel = tokio::sync::oneshot::channel::<
//         Option<tokio::sync::mpsc::Sender<InputStreamRequest>>,
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
