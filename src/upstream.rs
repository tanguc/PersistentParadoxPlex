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
use std::{borrow::BorrowMut, fmt::Debug, net::SocketAddr};
use tokio;
use tonic::Status;
use uuid::Uuid;

type UpstreamPeerInputRequest = InputStreamRequest;
type UpstreamPeerOuputRequest = OutputStreamRequest;

pub enum UpstreamPeerMetadataError {
    HostInvalid,
}

#[derive(Debug)]
pub enum UpstreamPeerError {
    HalvesCreationError,
    CannotConnect,
    CannotListen,
    RuntimeClosed,
    HeaderMissingOrCorrupted,
    RuntimeAbortOrder,
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
            header: None,
            ready: String::from("false"),
        });

        match self.client.ready(ready_req).await {
            Ok(is_ready) => {
                if is_ready.get_ref().ready {
                    debug!("[Upstream is ready]");
                    return Some(UpstreamPeerReady {
                        stream_halve: self.stream_halve,
                        sink_halve: self.sink_halve,
                        client: self.client,
                    });
                } else {
                    todo!("[Upstream readiness] Retry again until limit");
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

/// state where the upstream has been correctly created
/// but still need to init the sink & stream connection
/// due to gRPC constraints, we force to start the sink runtime first
/// then wait an order from another source to write then start the runtime for stream
/// //TODO document about the stream (channel rx) which might be not available until the gRPC init has passed  
impl UpstreamPeerReady {
    async fn start(mut self) {
        debug!("[UpstreamPeerReady] starting...");

        let (bi_stream_tx, bi_stream_rx) = tokio::sync::mpsc::channel(100);
        // We need to start the sink before to stream,
        // mainly because gRPC method is waiting from someone something to send
        tokio::task::spawn(upstream_start_sink_runtime(
            self.sink_halve.metadata,
            self.sink_halve.runtime_tx,
            self.sink_halve.rx,
            bi_stream_tx,
        ));
        match self
            .client
            .bidirectional_streaming(tonic::Request::new(bi_stream_rx))
            .await
        {
            Ok(response) => {
                debug!("[Upstream init] Succeed to init the gRPC method.");
                // Until we haven't initiated the connection to the upstream (sink task above)
                // we cannot stream from him
                tokio::task::spawn(upstream_start_stream_runtime(
                    self.stream_halve.metadata,
                    self.stream_halve.rx,
                    self.stream_halve.runtime_tx,
                    response.into_inner(),
                ));
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

async fn send_message_to_runtime(
    mut runtime_tx: RuntimeOrderTxChannel,
    metadata: PeerMetadata,
    payload: RuntimeEvent,
) -> Result<(), UpstreamPeerError> {
    //TODO better to send enum

    if let Err(err) = runtime_tx.send(payload).await {
        error!(
            "Failed to send the order to the runtime [{:?}] from upstream [{}], terminating the upstream streaming runtime",
            err,
            metadata.uuid.clone()
        );
        Ok(())
    } else {
        Err(UpstreamPeerError::RuntimeClosed)
    }
}

async fn upstream_start_stream_runtime(
    metadata: PeerMetadata,
    mut rx: PeerEventRxChannel,
    mut runtime_tx: RuntimeOrderTxChannel,
    mut bi_stream_rx: tonic::codec::Streaming<OutputStreamRequest>,
) {
    let mut paused = false;

    // Decide if the upstream should pause/resume,
    // <bool>true is returned if the runtime has closed or if the upstream should close (order coming from runtime)
    let runtime_order_resolution = enclose!(
        (mut runtime_tx, metadata)
        move |runtime_order: Option<PeerEvent<String>>, paused: &mut bool| -> Result<(), UpstreamPeerError> {
        if let Some(order) = runtime_order {
            trace!("Received runtime order [{:?}] for upstream stream [{}]", order, metadata.uuid.clone());
            match order {
                PeerEvent::Pause => {
                    trace!("Pausing upstream stream [{}]", metadata.uuid.clone());
                    *paused = true;
                    Ok(())
                }
                PeerEvent::Resume => {
                    trace!("Resuming upstream stream [{}]", metadata.uuid.clone());
                    *paused = false;
                    Ok(())
                }
                PeerEvent::Stop => {
                    trace!("Stopping upstream stream [{}]", metadata.uuid.clone());
                    Err(UpstreamPeerError::RuntimeAbortOrder)
                }
                _ => {Ok(())}
            }
        } else {
            trace!("Looks like runtime of upstream stream [{}] has been closed, terminating", metadata.uuid.clone());
            Err(UpstreamPeerError::RuntimeClosed)
        }
    });

    let stream_resolution = enclose!(
            (metadata)
            move |bi_stream_rx_resp: Result<Option<OutputStreamRequest>, Status>, mut runtime_tx: RuntimeOrderTxChannel| {
                let metadata = metadata.clone();
                async move {
                    debug!("[Resolution of received message...]");

                    match bi_stream_rx_resp {
                        Ok(stream_res) => {
                            if let Some(req_message) = stream_res {
                                trace!(
                                    "Upstream[{}] - message [{:?}]",
                                    metadata.uuid.clone(),
                                    req_message
                                );
                                if let Some(header) = req_message.header {
                                    let runtime_event = RuntimeEvent::MessageToDownstreamPeer(req_message.payload, header);
                                    send_message_to_runtime(runtime_tx, metadata, runtime_event).await
                                } else {
                                    warn!(
                                        "Upstream stream [{:?}] did not send any header in the message, skipping...",
                                        metadata.uuid.clone()
                                    );
                                    Ok(())
                                }
                            } else {
                                error!("Received NONE from upstream, weird, please contact the developer");
                                Ok(())
                            }
                        }
                        Err(err) => {
                            warn!(
                                "Failed to read from upstream && error code [{:?}] && error [{:?}",
                                err.code() as u8,
                                err
                            );
                            let runtime_event = RuntimeEvent::UptreamPeerStatusFail(metadata.clone());
                            send_message_to_runtime(runtime_tx, metadata, runtime_event).await
                        }
                    }
                }
            }
    );

    let mut abort = false;
    while !abort {
        tokio::select! {
            runtime_order = (rx.recv()), if paused => {
                debug!("[Upstream stream runtime] got runtime order");
                if let Err(err) = runtime_order_resolution(runtime_order, &mut paused) {
                    error!("[Aborting Upstream [{:?}] stream runtime, cause [{:?}]", metadata.uuid.clone(), err);
                    abort = true;
                }
            },
            stream_resp = (bi_stream_rx.message()) => {
                debug!("[Upstream stream runtime] got line");
                if let Err(err) = stream_resolution(stream_resp, runtime_tx.clone()).await {
                    error!("[Aborting Upstream [{:?}] stream runtime, cause [{:?}]", metadata.uuid.clone(), err);
                    abort = true;
                }
            }
        }
    }
    debug!("[Upstream [{:?}] aborted]", metadata.uuid.clone());
}

async fn upstream_start_sink_runtime(
    metadata: PeerMetadata,
    runtime_tx: RuntimeOrderTxChannel,
    mut rx: PeerEventRxChannel,
    mut bi_stream_tx: UpstreamBiStreamingSender,
) {
    debug!("[Starting upstream sink runtime]");
    let mut paused = false;

    loop {
        match rx.recv().await {
            Some(runtime_order) => {
                trace!(
                    "[Upstream sink] - got order {:?} from runtime",
                    runtime_order
                );
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
                        debug!("[Upstream Write Event]");
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
                    _ => {
                        debug!("[Upstream sink] Got another event");
                    }
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
/// Each upstream peer initialization are also wrapped into a different task
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
                        debug!("Registering upstream client [{:?}]", upstream_peer.metadata.uuid.clone());
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
