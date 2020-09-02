use crate::downstream::{
    BoxError, PeerError, PeerEventRxChannel, PeerEventTxChannel, PeerHalve, PeerMetadata,
    PeerRuntime,
};
use crate::runtime::{PeerEvent, Runtime, RuntimeEvent, RuntimeOrderTxChannel};
use crate::upstream_proto::{
    upstream_peer_service_client::UpstreamPeerServiceClient, Header, InputStreamRequest,
    OutputStreamRequest, ReadyRequest, ReadyResult,
};
use async_trait::async_trait;
use enclose::enclose;
use std::net::SocketAddr;
use tokio;
use uuid::Uuid;

type UpstreamPeerInputRequest = InputStreamRequest;
type UpstreamPeerOuputRequest = OutputStreamRequest;

pub enum UpstreamPeerMetadataError {
    HostInvalid,
}

pub enum UpstreamPeerError {
    HalvesCreationError,
    CannotConnect,
    CannotListen,
}

pub struct UpstreamPeerPending {
    pub stream_halve: PeerHalve,
    pub sink_halve: PeerHalve,
}

/// Once the client has passed the Ready request of gRPC
pub struct UpstreamPeerWaitReadiness {
    pub stream_halve: PeerHalve,
    pub sink_halve: PeerHalve,
    pub client: UpstreamPeerServiceClient<tonic::transport::Channel>,
}

type UpstreamBiStreamingSender = tokio::sync::mpsc::Sender<UpstreamPeerInputRequest>;

pub struct UpstreamPeerReady {
    pub stream_halve: PeerHalve,
    pub sink_halve: PeerHalve,
    pub client: UpstreamPeerServiceClient<tonic::transport::Channel>, // useful to receive data
}

pub struct UpstreamPeer<T>
where
    T: UpstreamPeerStateTransition,
{
    state: T,
}

pub struct UpstreamPeerMetadata {
    host: SocketAddr,
}

#[async_trait]
pub trait UpstreamPeerStateTransition {
    type NextState;
    async fn next(self) -> Option<Self::NextState>;
}

#[async_trait]
impl UpstreamPeerStateTransition for UpstreamPeerPending {
    type NextState = UpstreamPeerWaitReadiness;
    async fn next(self) -> Option<Self::NextState> {
        match UpstreamPeerServiceClient::connect(format!(
            "tcp://{}",
            self.stream_halve.metadata.socket_addr.to_string()
        ))
        .await
        {
            Ok(client) => {
                trace!("UpstreamPeer connected via gRPC");
                Some(UpstreamPeerWaitReadiness {
                    stream_halve: self.stream_halve,
                    sink_halve: self.sink_halve,
                    client,
                })
            }
            Err(err) => {
                error!("Cannot connect to the GRPC server [{:?}]", err);
                None
            }
        }
    }
}

#[async_trait]
impl UpstreamPeerStateTransition for UpstreamPeerWaitReadiness {
    type NextState = UpstreamPeerReady;
    async fn next(mut self) -> Option<Self::NextState> {
        let ready_req = tonic::Request::new(ReadyRequest {
            header: Some(Header {
                address: String::from("127.293.23.302"),
                time: String::from("09:38:93"),
                client_uuid: String::from("8374-32KE-394U2-ZKND"),
            }),
        });

        match self.client.ready(ready_req).await {
            Ok(is_ready) => {
                if is_ready.get_ref().ready {
                    return Some(UpstreamPeerReady {
                        stream_halve: self.stream_halve,
                        sink_halve: self.sink_halve,
                        client: self.client,
                    });
                } else {
                    todo!("Retry again until limit");
                }
            }
            Err(err) => {
                error!(
                    "Failed to send a ready request to the upstream [{}], reason: [{:?}]",
                    self.stream_halve.metadata.uuid.clone(),
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
        let stream_halve = PeerHalve::new(uuid.clone(), runtime_tx.clone(), metadata.host);
        let sink_halve = PeerHalve::new(uuid.clone(), runtime_tx.clone(), metadata.host);

        UpstreamPeer {
            state: UpstreamPeerPending {
                stream_halve,
                sink_halve,
            },
        }
    }
}

pub struct UpstreamPeerStarted {
    pub sink_tx: PeerEventTxChannel,
    pub stream_tx: PeerEventTxChannel,
    pub metadata: PeerMetadata,
}

impl UpstreamPeerReady {
    async fn start(mut self) {
        debug!("[UpstreamPeerReadyToListen] starting...");

        let (bi_stream_tx, bi_stream_rx) = tokio::sync::mpsc::channel(100);
        match self
            .client
            .bidirectional_streaming(tonic::Request::new(bi_stream_rx))
            .await
        {
            Ok(response) => {
                debug!("[Upstream init] Succeed to init the gRPC method.");

                upstream_start_sink_runtime(
                    self.sink_halve.metadata,
                    self.sink_halve.runtime_tx,
                    self.sink_halve.rx,
                    bi_stream_tx,
                )
                .await;
                upstream_start_stream_runtime(
                    self.stream_halve.metadata,
                    self.stream_halve.rx,
                    self.stream_halve.runtime_tx,
                    response.into_inner(),
                )
                .await;
            }
            Err(err) => {
                error!(
                    "[UpstreamPeer start] Failed to get the stream, error [{:?}]",
                    err
                );
            }
        };
    }
}

#[async_trait]
impl PeerRuntime for UpstreamPeer<UpstreamPeerPending> {
    type Output = UpstreamPeerStarted;

    async fn start(mut self) -> Result<Self::Output, PeerError> {
        debug!("[UpstreamingPeer - UpstreamPeerPending] starting...");

        let metadata = self.state.stream_halve.metadata.clone();
        let connected = self
            .state
            .next()
            .await
            .ok_or_else(|| PeerError::BrokenPipe)?;
        let active = connected
            .next()
            .await
            .ok_or_else(|| PeerError::BrokenPipe)?;

        let sink_tx = active.sink_halve.tx.clone();
        let stream_tx = active.stream_halve.tx.clone();
        tokio::spawn(active.start());

        Ok(UpstreamPeerStarted {
            sink_tx,
            stream_tx,
            metadata,
        })
    }
}

async fn upstream_start_stream_runtime(
    metadata: PeerMetadata,
    rx: PeerEventRxChannel,
    runtime_tx: RuntimeOrderTxChannel,
    bi_stream_rx: tonic::codec::Streaming<OutputStreamRequest>,
) {
    debug!("[Starting upstream stream runtime...]");

    let task = || async {
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

    tokio::spawn(task());
}

async fn upstream_start_sink_runtime(
    metadata: PeerMetadata,
    runtime_tx: RuntimeOrderTxChannel,
    mut rx: PeerEventRxChannel,
    mut bi_stream_tx: UpstreamBiStreamingSender,
) {
    info!("[Starting upstream sink runtime]");
    let mut paused = false;

    loop {
        match rx.recv().await {
            Some(runtime_order) => {
                trace!("Upstream sink - got order {:?} from runtime", runtime_order);
                match runtime_order {
                    PeerEvent::Stop => {
                        trace!(
                            "Stopping the upstream sink for client [{}]",
                            metadata.uuid.clone()
                        );
                    }
                    PeerEvent::Pause => {
                        trace!(
                            "Pausing the upstream sink for client [{}]",
                            metadata.uuid.clone()
                        );
                        paused = true;
                    }
                    PeerEvent::Resume => {
                        trace!(
                            "Resume the upstream sink for client [{}]",
                            metadata.uuid.clone()
                        );
                    }
                    PeerEvent::Write(data) => {
                        if !paused {
                            //TODO would be much better to change the state of the sink loop
                            let size = data.len();
                            //TODO would be better to trace only with cfg(debug) mode
                            match bi_stream_tx.send(prepare_upstream_sink_request(data)).await {
                                Ok(_) => {
                                    trace!(
                                        "Wrote data of length [{}] to the client [{}]",
                                        size,
                                        metadata.uuid.clone()
                                    );
                                }
                                Err(err) => {
                                    error!(
                                        "Failed to write data of length [{}], to the client [{}], cause [{:?}], closing the upstream sink",
                                        size,
                                        metadata.uuid.clone(),
                                        err
                                    );
                                    return;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            None => {
                warn!("Failed to receive an order from runtime");
            }
        }
    }
}

// ///should be in the runtime
/// Register all upstream peers from a source
/// All wrapped into a separate task
pub fn register_upstream_peers(mut runtime: Runtime) {
    let task = async move {
        debug!("Registering upstream peers");
        // TODO only for debugging purposes
        let upstream_peer_metadata = get_upstream_peers();

        for upstream_peer_metadata in upstream_peer_metadata {
            let upstream_task = enclose!((mut runtime) move || async move {
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
            });
            tokio::spawn(upstream_task());

            // {
            //     tokio::spawn(async move {
            //         let mut interval = tokio::time::interval(Duration::from_secs(20));
            //         loop {
            //             interval.tick().await;
            //             debug!("Send a debug client request");
            //             let body = InputStreamRequest {
            //                 header: Option::Some(Header {
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
    };
    tokio::spawn(task);
}

// // impl PeerRuntime for UpstreamPeer<InputStreamRequest> {
// //     fn start(mut self) -> JoinHandle<BoxError> {
// // debug!("Starting upstream stream halve");
// // tokio::spawn(async {
// //     let mut connect_client =
// //         backend_peer_service_client::UpstreamPeerServiceClient::connect(format!(
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

pub fn prepare_upstream_sink_request(payload: String) -> InputStreamRequest {
    let address = String::from("127.0.0.1");
    let time = String::from("14:12:44");

    let request = InputStreamRequest {
        header: Option::Some(Header {
            client_uuid: String::from("totoierz"), //TODO change it correctly ....
            address,
            time,
        }),
        payload,
    };

    return request;
}

pub async fn get_upstream_tx_channel(
    mut runtime_tx: RuntimeOrderTxChannel,
) -> Option<PeerEventTxChannel> {
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
