use crate::runtime::{
    send_message_to_runtime, PeerEvent, PeerMetadata, RuntimeError, RuntimeEvent,
    RuntimeEventDownstream, RuntimeOrderTxChannel,
};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{
    sink::SinkExt,
    stream::{SplitSink, SplitStream, StreamExt},
};
use std::{fmt::Formatter, net::SocketAddr};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::{BytesCodec, Framed};
use uuid::Uuid;

pub type DownstreamPeerEventTx = tokio::sync::mpsc::Sender<PeerEvent<(Bytes, ())>>;
pub type DownstreamPeerEventRx = tokio::sync::mpsc::Receiver<PeerEvent<(Bytes, ())>>;

pub type DownstreamTcpSink = futures::stream::SplitSink<Framed<TcpStream, BytesCodec>, Bytes>;
pub type DownstreamTcpStream = futures::stream::SplitStream<Framed<TcpStream, BytesCodec>>;

pub struct DownstreamPeerHalve {
    pub metadata: PeerMetadata,
    pub runtime_tx: RuntimeOrderTxChannel,
    pub tx: DownstreamPeerEventTx,
    pub rx: DownstreamPeerEventRx,
}

pub struct DownstreamPeerSinkHalve {
    halve: DownstreamPeerHalve,
    tcp_sink: DownstreamTcpSink,
}

pub struct DownstreamPeerStreamHalve {
    halve: DownstreamPeerHalve,
    tcp_stream: DownstreamTcpStream,
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
        let frame = Framed::new(tcp_stream, BytesCodec::new());
        let (tcp_sink, tcp_stream): (
            SplitSink<Framed<TcpStream, BytesCodec>, bytes::Bytes>,
            SplitStream<Framed<TcpStream, BytesCodec>>,
        ) = frame.split();
        let uuid = Uuid::new_v4().to_string();

        let peer_sink: DownstreamPeerSinkHalve = DownstreamPeerSinkHalve::new(
            tcp_sink,
            uuid.clone(),
            runtime_tx.clone(),
            socket_addr.clone(),
        );
        let peer_stream: DownstreamPeerStreamHalve = DownstreamPeerStreamHalve::new(
            tcp_stream,
            uuid.clone(),
            runtime_tx.clone(),
            socket_addr.clone(),
        );

        Self {
            metadata: PeerMetadata {
                socket_addr: socket_addr.clone(),
                uuid,
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
    pub sink_tx: DownstreamPeerEventTx,
    pub stream_tx: DownstreamPeerEventTx,
}

impl DownstreamPeerSinkHalve {
    pub fn new(
        tcp_sink: DownstreamTcpSink,
        uuid: String,
        runtime_tx: RuntimeOrderTxChannel,
        socket_addr: SocketAddr,
    ) -> Self {
        DownstreamPeerSinkHalve {
            halve: DownstreamPeerHalve::new(uuid, runtime_tx, socket_addr),
            tcp_sink,
        }
    }
}

impl DownstreamPeerStreamHalve {
    pub fn new(
        tcp_stream: DownstreamTcpStream,
        uuid: String,
        runtime_tx: RuntimeOrderTxChannel,
        socket_addr: SocketAddr,
    ) -> Self {
        DownstreamPeerStreamHalve {
            halve: DownstreamPeerHalve::new(uuid, runtime_tx, socket_addr),
            tcp_stream,
        }
    }
}

impl DownstreamPeerHalve {
    pub fn new(uuid: String, runtime_tx: RuntimeOrderTxChannel, socket_addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        DownstreamPeerHalve {
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
    fn start(mut self) -> DownstreamPeerEventTx {
        let tx = self.halve.tx.clone();
        let task = move || async move {
            info!(
                "Spawning read task for the client {}",
                self.halve.metadata.uuid
            );
            loop {
                let line = futures::stream::StreamExt::next(&mut self.tcp_stream).await;
                match line {
                    Some(line) => match line {
                        Ok(line) => {
                            debug!("Got a new line : {:?}", line);
                            let runtime_order =
                                RuntimeEvent::Downstream(RuntimeEventDownstream::Message(
                                    line.into(),
                                    self.halve.metadata.uuid.clone().to_string(),
                                ));

                            if let Err(err) = send_message_to_runtime(
                                self.halve.runtime_tx.clone(),
                                self.halve.metadata.clone(),
                                runtime_order,
                            )
                            .await
                            {
                                error!("Failed to send runtime error, analyzing the error.");
                                match err {
                                    RuntimeError::Closed => {
                                        error!("Failed to send message to runtime, because it has been closed, exiting downstream [{:?}] peer streaming runtime", self.halve.metadata.uuid.clone());
                                        break;
                                    }
                                    _ => warn!(
                                        "Failed to send message to runtime, cause [{:?}]",
                                        err
                                    ),
                                }
                            }
                        }
                        Err(codec_error) => {
                            error!(
                                "Got a codec error when received downstream payload: {:?}",
                                codec_error
                            );
                        }
                    },
                    None => {
                        debug!("[Peer terminated connection] notifying runtime");
                        let runtime_order =
                            RuntimeEvent::Downstream(RuntimeEventDownstream::TerminatedConnection(
                                self.halve.metadata.clone(),
                            ));

                        if let Err(err) = send_message_to_runtime(
                            self.halve.runtime_tx.clone(),
                            self.halve.metadata.clone(),
                            runtime_order,
                        )
                        .await
                        {
                            error!("[Failed to send message to runtime], aborting anyway...");
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
    fn start(mut self) -> DownstreamPeerEventTx {
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
                        PeerEvent::Write((payload, _)) => {
                            info!("[Downstream sink ORDER] Write -");
                            //todo check if it's paused
                            // let downstream_message = prepare_downstream_sink_request(payload);
                            if let Err(send_err) = self.tcp_sink.send(payload).await {
                                error!(
                                    "Failed to send the payload to the downstream [{:?}], cause [{:?}]",
                                    self.halve.metadata.uuid.clone(), send_err
                                );
                            }
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
