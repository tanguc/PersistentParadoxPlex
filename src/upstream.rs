use crate::backend;
use crate::backend::backend_peer_service_client::UpstreamPeerServiceClient;
use crate::peer::BoxError;
use crate::peer::{PeerError, PeerHalve, PeerMetadata, PeerRuntime, PeerTxChannel};
use crate::runtime::{Runtime, RuntimeEvent, RuntimeOrderTxChannel};
use async_trait::async_trait;
use futures::TryFutureExt;
use std::net::SocketAddr;
use tokio;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use uuid::Uuid;

type UpstreamPeerInputRequest = backend::InputStreamRequest;
type UpstreamPeerOuputRequest = backend::OutputStreamRequest;

pub enum UpstreamPeerMetadataError {
    HostInvalid,
}

pub enum UpstreamPeerError {
    HalvesCreationError,
    CannotConnect,
    CannotListen,
}

// /// Only stream runtime is stored
// /// The sink is only a simple channel and his runtime
// /// is created by the GRPC runtime
// /// T type of sending request to clients
// /// U type of receiving request from clients
// pub struct UpstreamPeer {
//     pub stream_halve: PeerHalve,
//     // to write data to the client
//     pub grpc_tx_channel: tokio::sync::mpsc::UnboundedSender<UpstreamPeerOuputRequest>,
//     // not available for us (passed to tonic)
//     pub grpc_rx_channel: tokio::sync::mpsc::UnboundedReceiver<UpsteamPeerInputRequest>,
// }

pub struct UpstreamPeerPending {
    pub stream_halve: PeerHalve,
    // to write data to the client
    pub grpc_tx_channel: tokio::sync::mpsc::UnboundedSender<UpstreamPeerInputRequest>,
    // not available for us (passed to tonic)
    pub grpc_rx_channel: tokio::sync::mpsc::UnboundedReceiver<UpstreamPeerInputRequest>,
}

pub struct UpstreamPeerConnect {
    pub stream_halve: PeerHalve,
    pub grpc_tx_channel: tokio::sync::mpsc::UnboundedSender<UpstreamPeerInputRequest>,
    // not available for us (passed to tonic)
    pub grpc_rx_channel: tokio::sync::mpsc::UnboundedReceiver<UpstreamPeerInputRequest>,
    pub client: UpstreamPeerServiceClient<tonic::transport::Channel>,
}
pub struct UpstreamPeerReadyToListen {
    pub stream_halve: PeerHalve,
    pub stream: tonic::Streaming<backend::OutputStreamRequest>,
    pub grpc_tx_channel: tokio::sync::mpsc::UnboundedSender<UpstreamPeerInputRequest>,
}

pub struct UpstreamPeer<T>
where
    T: UpstreamPeerStateTransition,
{
    state: T,
}

pub struct UpstreamPeerMetadata {
    // metadata: PeerMetadata,
    host: SocketAddr,
    // runtime_tx: RuntimeOrderTxChannel,
}

#[async_trait]
pub trait UpstreamPeerStateTransition {
    type NextState;
    async fn next(self) -> Option<Self::NextState>;
}

#[async_trait]
impl UpstreamPeerStateTransition for UpstreamPeerPending {
    type NextState = UpstreamPeerConnect;
    async fn next(self) -> Option<Self::NextState> {
        match backend::backend_peer_service_client::UpstreamPeerServiceClient::connect(format!(
            "tcp://{}",
            self.stream_halve.metadata.socket_addr.to_string()
        ))
        .await
        {
            Ok(client) => {
                debug!("UpstreamPeer connected via gRPC");
                Some(UpstreamPeerConnect {
                    stream_halve: self.stream_halve,
                    client,
                    grpc_tx_channel: self.grpc_tx_channel,
                    grpc_rx_channel: self.grpc_rx_channel,
                })
            }
            Err(err) => {
                debug!("Cannot connect to the GRPC server [{:?}]", err);
                // return Box::new(PeerError::ServerClosed);
                None
            }
        }
    }
}

#[async_trait]
impl UpstreamPeerStateTransition for UpstreamPeerConnect {
    type NextState = UpstreamPeerReadyToListen;
    async fn next(mut self) -> Option<Self::NextState> {
        match self
            .client
            .bidirectional_streaming(tonic::Request::new(self.grpc_rx_channel))
            .await
        {
            Ok(stream) => {
                self.grpc_tx_channel.clone();
                debug!("[Upstream init] Succeed to init the gRPC method.");
                Some(UpstreamPeerReadyToListen {
                    stream_halve: self.stream_halve,
                    grpc_tx_channel: self.grpc_tx_channel,
                    stream: stream.into_inner(),
                })
            }
            Err(err) => {
                error!(
                    "[UpstreamPeer start] Failed to get the stream, error [{:?}]",
                    err
                );
                None
            }
        }
    }
}

impl UpstreamPeer<UpstreamPeerPending> {
    fn new(metadata: UpstreamPeerMetadata, runtime_tx: RuntimeOrderTxChannel) -> Self {
        debug!("Creating upstream peer halves");
        let uuid = Uuid::new_v4();

        let stream_halve = PeerHalve::new(uuid, runtime_tx.clone(), metadata.host);

        let (message_tx, message_rx) =
            tokio::sync::mpsc::unbounded_channel::<UpstreamPeerInputRequest>();

        UpstreamPeer {
            state: UpstreamPeerPending {
                stream_halve,
                grpc_tx_channel: message_tx,
                grpc_rx_channel: message_rx,
            },
        }
    }
}

pub struct UpstreamPeerListening {
    pub sink_tx: PeerTxChannel,
    pub stream_tx: PeerTxChannel,
    pub metadata: PeerMetadata,
}

impl UpstreamPeerReadyToListen {
    fn start(mut self) {
        debug!("[UpstreamPeerReadyToListen] starting...");
        let task = async {
            // let mut read_stream_loop = call_method.into_inner();
            // loop {
            //     debug!("Starting reading loop from upstream..");
            //     match read_stream_loop.message().await {
            //         Ok(stream_res) => match stream_res {
            //             Some(message) => {
            //                 debug!("Got message from upstream: [{:?}]", message);
            //                 let order = RuntimeEvent::MessageToDownstreamPeer(message);
            //                 if let Err(err) = self.stream_halve.runtime_tx.send(order).await {
            //                     error!("Failed to send the order MessageToDownstreamPeer to the runtime [{:?}]", err);
            //                 }
            //             }
            //             None => {
            //                 error!(
            //                     "Received NONE from upstream, weird, please contact the developer"
            //                 );
            //             }
            //         },
            //         Err(err) => {
            //             error!("The error code [{:?}]", err.code() as u8);
            //             error!("The error message [{:?}]", err);
            //             debug!("Notifying the runtime about upstream termination...");
            //             match self
            //                 .stream_halve
            //                 .runtime_tx
            //                 .send(RuntimeEvent::PeerTerminatedConnection(
            //                     self.stream_halve.metadata.clone(),
            //                 ))
            //                 .await
            //             {
            //                 Ok(_) => {
            //                     debug!("Successfully notified the runtime");
            //                 }
            //                 Err(err) => {
            //                     error!("Failed to send the upstream termination to the runtime");
            //                     error!("{:?}", err);
            //                 }
            //             }
            //             return Err(Box::new(PeerError::BrokenPipe));
            //         }
            //     }
            // }
        };

        tokio::spawn(task);
        todo!();
    }
}

#[async_trait]
impl PeerRuntime for UpstreamPeer<UpstreamPeerPending> {
    type Output = UpstreamPeerListening;

    async fn start(mut self) -> Result<Self::Output, PeerError> {
        debug!("[UpstreamingPeer - UpstreamPeerPending] starting...");

        let metadata = self.state.stream_halve.metadata.clone();
        let connected = self
            .state
            .next()
            .await
            .ok_or_else(|| PeerError::BrokenPipe)?;
        let listening = connected
            .next()
            .await
            .ok_or_else(|| PeerError::BrokenPipe)?;

        let tx = listening.stream_halve.tx.clone();
        listening.start();

        //TODO create a task which will listen Events from runtime in order to sink to the GRPC clients
        // dont forget to get the stream tx
        // sink.start()

        Ok(UpstreamPeerListening {
            sink_tx: tx.clone(),
            stream_tx: tx.clone(),
            metadata,
        })
    }
}

// ///should be in the runtime
pub async fn register_upstream_peers(mut runtime: Runtime) {
    debug!("Registering upstream peers");
    // TODO only for debugging purposes
    let upstream_peer_metadata = get_upstream_peers();

    for upstream_peer_metadata in upstream_peer_metadata {
        let upstream_peer = UpstreamPeer::new(upstream_peer_metadata, runtime.tx.clone());

        match upstream_peer.start().await {
            Ok(upstream_peer) => {
                runtime
                    .add_upstream_peer_halves(
                        upstream_peer.sink_tx.clone(),
                        upstream_peer.stream_tx.clone(),
                        upstream_peer.metadata.clone(),
                    )
                    .await;
            }
            Err(err) => {
                error!("Failed to register the upstream peer to the runtime");
            }
        }

        // {
        //     tokio::spawn(async move {
        //         let mut interval = tokio::time::interval(Duration::from_secs(20));
        //         loop {
        //             interval.tick().await;
        //             debug!("Send a debug client request");
        //             let body = backend::InputStreamRequest {
        //                 header: Option::Some(backend::Header {
        //                     address: "823.12938I.3291833.".to_string(),
        //                     time: "12:32:12".to_string(),
        //                 }),
        //                 payload: "Task spawn - client send fake data".to_string(),
        //             };
        //             if let Err(err) = upstream_stream_tx.send(body) {
        //                 error!("Cannot send message from client (spawn task): [{:?}]", err);
        //                 error!(
        //                     "Looks like the halve channel has closed or dropped, aborting the task"
        //                 );
        //                 return;
        //             } else {
        //                 debug!("Message sent");
        //             }
        //             debug!("Tick - new message to client, expecting a message from server task");
        //         }
        //     });
        // }
    }
}

// // impl PeerRuntime for UpstreamPeer<backend::InputStreamRequest> {
// //     fn start(mut self) -> JoinHandle<BoxError> {
// // debug!("Starting upstream stream halve");
// // tokio::spawn(async {
// //     let mut connect_client =
// //         backend::backend_peer_service_client::UpstreamPeerServiceClient::connect(format!(
// //             "tcp://{}",
// //             self.stream_halve.metadata.socket_addr.to_string()
// //         ))
// //         .map_err(|err| {
// //             debug!("Cannot connect to the GRPC server [{:?}]", err);
// //             return Box::new(PeerError::ServerClosed);
// //         })
// //         .await?;

// //     debug!("calling method");
// //     let call_method = connect_client
// //         .bidirectional_streaming(tonic::Request::new(self.grpc_rx_channel))
// //         .await?;

// //     let mut read_stream_loop = call_method.into_inner();
// //     loop {
// //         debug!("Starting reading loop from upstream..");
// //         match read_stream_loop.message().await {
// //             Ok(stream_res) => match stream_res {
// //                 Some(message) => {
// //                     debug!("Got message from upstream: [{:?}]", message);
// //                     let order = RuntimeEvent::MessageToDownstreamPeer(message);
// //                     if let Err(err) = self.stream_halve.runtime_tx.send(order).await {
// //                         error!("Failed to send the order MessageToDownstreamPeer to the runtime [{:?}]", err);
// //                     }
// //                 }
// //                 None => {
// //                     error!(
// //                         "Received NONE from upstream, weird, please contact the developer"
// //                     );
// //                 }
// //             },
// //             Err(err) => {
// //                 error!("The error code [{:?}]", err.code() as u8);
// //                 error!("The error message [{:?}]", err);
// //                 debug!("Notifying the runtime about upstream termination...");
// //                 match self
// //                     .stream_halve
// //                     .runtime_tx
// //                     .send(RuntimeEvent::PeerTerminatedConnection(
// //                         self.stream_halve.metadata.clone(),
// //                     ))
// //                     .await
// //                 {
// //                     Ok(_) => {
// //                         debug!("Successfully notified the runtime");
// //                     }
// //                     Err(err) => {
// //                         error!("Failed to send the upstream termination to the runtime");
// //                         error!("{:?}", err);
// //                     }
// //                 }
// //                 return Err(Box::new(PeerError::BrokenPipe));
// //             }
// //         }
// //     }
// // })
// //     }
// // }

fn get_upstream_peers() -> Vec<UpstreamPeerMetadata> {
    debug!("Creating backend peers [DEBUGGING PURPOSES]");

    let mut upstream_peers = vec![];

    upstream_peers.push(UpstreamPeerMetadata {
        host: "127.0.0.1:4770"
            .parse()
            .map_err(|err| {
                error!("Could not parse addr: [{:?}]", err);
            })
            .unwrap(),
    });

    upstream_peers
}

pub fn prepare_upstream_sink_request(payload: String) -> backend::InputStreamRequest {
    let address = String::from("127.0.0.1");
    let time = String::from("14:12:44");

    let request = backend::InputStreamRequest {
        header: Option::Some(backend::Header { address, time }),
        payload,
    };

    return request;
}

pub async fn get_upstream_tx_channel(
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
