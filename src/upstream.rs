use crate::upstream_proto::{
    upstream_peer_service_client::UpstreamPeerServiceClient, Header, InputStreamRequest,
    OutputStreamRequest,
};
use crate::{conf, downstream::PeerRuntime};
use crate::{
    downstream::PeerError,
    runtime::{
        send_message_to_runtime, PeerEvent, PeerMetadata, Runtime, RuntimeEvent,
        RuntimeEventUpstream, RuntimeOrderTxChannel,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use enclose::enclose;
use futures::Stream;
use std::{fmt::Debug, net::SocketAddr, pin::Pin};
use tokio::{self, sync::mpsc::Receiver, sync::mpsc::Sender, task::JoinHandle, time};
use tonic::Status;
use uuid::Uuid;

type UpstreamPeerInputRequest = InputStreamRequest;
type UpstreamPeerOuputRequest = OutputStreamRequest;

type UpstreamBiStreamingSender = tokio::sync::mpsc::Sender<UpstreamPeerInputRequest>;

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

type UpstreamPeerEvent = PeerEvent<(Bytes, Header)>;
pub type UpstreamPeerEventTx = Sender<UpstreamPeerEvent>;
pub type UpstreamPeerEventRx = Receiver<UpstreamPeerEvent>;

pub struct UpstreamPeerHalve {
    pub metadata: PeerMetadata,
    pub runtime_tx: RuntimeOrderTxChannel,
    pub rx: UpstreamPeerEventRx,
    pub tx: UpstreamPeerEventTx,
}

impl UpstreamPeerHalve {
    pub fn new(uuid: String, runtime_tx: RuntimeOrderTxChannel, socket_addr: SocketAddr) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        Self {
            metadata: PeerMetadata { uuid, socket_addr },
            runtime_tx,
            rx,
            tx,
        }
    }
}

pub struct UpstreamPeerPending {
    pub stream_halve: UpstreamPeerHalve,
    pub sink_halve: UpstreamPeerHalve,
    pub ready_timeout: u32,
    pub alive_timeout: u32,
}

/// Once the client has passed the Ready request of gRPC
pub struct UpstreamPeerWaitReadiness {
    pub stream_halve: UpstreamPeerHalve,
    pub sink_halve: UpstreamPeerHalve,
    pub client: UpstreamPeerServiceClient<tonic::transport::Channel>,
    pub ready_timeout: u32,
    pub alive_timeout: u32,
}
pub struct UpstreamPeerReady {
    pub stream_halve: UpstreamPeerHalve,
    pub sink_halve: UpstreamPeerHalve,
    pub client: UpstreamPeerServiceClient<tonic::transport::Channel>, // useful to receive data
    pub alive_timeout: u32,
}

pub struct UpstreamPeer<T>
where
    T: UpstreamPeerStateTransition,
{
    state: T,
}
#[derive(Debug)]
pub struct UpstreamPeerMetadata {
    pub host: SocketAddr,
    pub alive_timeout: u32,
    pub ready_timeout: u32,
}

impl UpstreamPeerMetadata {
    pub fn from(from: &conf::Upstream) -> anyhow::Result<Self> {
        let addr: SocketAddr = format!("{}:{}", from.host, from.port).parse::<SocketAddr>()?;
        Ok(Self {
            host: addr,
            alive_timeout: from.alive_timeout,
            ready_timeout: from.ready_timeout,
        })
    }
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
                    alive_timeout: self.alive_timeout,
                    ready_timeout: self.ready_timeout,
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
        let timeout = time::timeout(
            tokio::time::Duration::from_secs(self.ready_timeout.into()),
            self.client.ready(()),
        )
        .await;

        match timeout {
            Ok(value) => match value {
                Ok(is_ready) => {
                    if is_ready.get_ref().ready {
                        debug!("[Upstream is ready]");
                        return Some(UpstreamPeerReady {
                            stream_halve: self.stream_halve,
                            sink_halve: self.sink_halve,
                            client: self.client,
                            alive_timeout: self.alive_timeout,
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
            },
            Err(err) => {
                error!(
                    "Upstream [{:?}] failed to send his readiness in [{}] seconds",
                    &self.sink_halve.metadata.uuid, self.ready_timeout
                );
                None
            }
        }
    }
}

impl UpstreamPeer<UpstreamPeerPending> {
    pub fn new(metadata: &UpstreamPeerMetadata, runtime_tx: RuntimeOrderTxChannel) -> Self {
        debug!("Creating upstream peer halves");
        let uuid = Uuid::new_v4().to_string();
        let stream_halve =
            UpstreamPeerHalve::new(uuid.clone(), runtime_tx.clone(), metadata.host.clone());
        let sink_halve = UpstreamPeerHalve::new(uuid, runtime_tx.clone(), metadata.host.clone());

        UpstreamPeer {
            state: UpstreamPeerPending {
                stream_halve,
                sink_halve,
                alive_timeout: metadata.alive_timeout,
                ready_timeout: metadata.ready_timeout,
            },
        }
    }
}

pub struct UpstreamPeerStarted {
    pub sink_tx: UpstreamPeerEventTx,
    pub stream_tx: UpstreamPeerEventTx,
    pub metadata: PeerMetadata,
}

impl Debug for UpstreamPeerStarted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return f
            .debug_struct("UpstreamPeerStarted")
            .field("uuid", &self.metadata.uuid)
            .finish();
    }
}

/// state where the upstream has been correctly created
/// but still need to init the sink & stream connection
/// due to gRPC constraints, we force to start the sink runtime first
/// then wait an order from another source to write then start the runtime for stream
/// //TODO document about the stream (channel rx) which might be not available until the gRPC init has passed  
impl UpstreamPeerReady {
    async fn start(mut self) -> JoinHandle<Result<(), tonic::Status>> {
        debug!("[UpstreamPeerReady] starting...");

        let task = move || async move {
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
                    let my_own_stream = MyStreamClientGRPC {
                        0: response.into_inner(),
                    };
                    debug!("[Upstream init] Succeed to init the gRPC method.");
                    // Until we haven't initiated the connection to the upstream (sink task above)
                    // we cannot stream from him
                    tokio::task::spawn(upstream_start_stream_runtime(
                        self.stream_halve.metadata,
                        self.stream_halve.rx,
                        self.stream_halve.runtime_tx,
                        my_own_stream,
                    ));
                    Ok(())
                }
                Err(err) => {
                    error!(
                        "[UpstreamPeer start] Failed to get the stream, error [{:?}]",
                        &err
                    );
                    Err(err)
                }
            }
        };
        tokio::task::spawn(task())
    }
}

#[async_trait]
impl PeerRuntime for UpstreamPeer<UpstreamPeerPending> {
    type Output = UpstreamPeerStarted;

    /// Create a new task and transit between states
    /// until the upstream is completely effective
    async fn start(mut self) -> Result<Self::Output, PeerError> {
        debug!("[UpstreamingPeer - UpstreamPeerPending] starting...");

        let task = move || async move {
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

            Ok::<Self::Output, PeerError>(UpstreamPeerStarted {
                sink_tx,
                stream_tx,
                metadata,
            })
        };
        match tokio::task::spawn(task())
            .await
            .map_err(|err| PeerError::BrokenPipe)?
        {
            Ok(res) => Ok(res),
            Err(_) => Err(PeerError::BrokenPipe),
        }
    }
}

pub struct MyStreamClientGRPC(tonic::codec::Streaming<OutputStreamRequest>);

impl Stream for MyStreamClientGRPC {
    type Item = Result<OutputStreamRequest, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl Drop for MyStreamClientGRPC {
    fn drop(&mut self) {
        info!("The upstream has been dropped");
    }
}

async fn upstream_start_stream_runtime(
    metadata: PeerMetadata,
    mut rx: UpstreamPeerEventRx,
    runtime_tx: RuntimeOrderTxChannel,
    mut bi_stream_rx: MyStreamClientGRPC,
) {
    let mut paused = false;

    // Decide if the upstream should pause/resume,
    // <bool>true is returned if the runtime has closed or if the upstream should close (order coming from runtime)
    let runtime_order_resolution = enclose!(
        (mut runtime_tx, metadata)
        move |runtime_order: Option<UpstreamPeerEvent>, paused: &mut bool| -> Result<(), UpstreamPeerError> {
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
            move |bi_stream_rx_resp: Option<OutputStreamRequest>, runtime_tx: RuntimeOrderTxChannel| {
                let metadata = metadata.clone();
                async move {
                    if let Some(req_message) = bi_stream_rx_resp {
                        trace!(
                            "Upstream[{}] - message [{:?}]",
                            metadata.uuid.clone(),
                            req_message
                        );
                        if let Some(header) = req_message.header {
                            let runtime_event = RuntimeEvent::Upstream(
                                RuntimeEventUpstream::Message(
                                    req_message.payload.into(), header.client_uuid.clone()
                                )
                            );
                            send_message_to_runtime(runtime_tx, metadata, runtime_event)
                            .await
                            .map_err(|err| () )
                        } else {
                            warn!(
                                "Upstream stream [{:?}] did not send any header in the message, skipping...",
                                metadata.uuid.clone()
                            );
                            Ok(())
                        }
                    } else {
                        error!("Received NONE from upstream, weird, resuming the runtime, please contact the developer");
                        Ok(())
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
            stream_result = (bi_stream_rx.0.message()) => {
                debug!("[Upstream stream runtime] got line");
                match stream_result {
                    Ok(message) => {
                        if let Err(_) =  stream_resolution(message, runtime_tx.clone()).await {
                            warn!("Aborting Upstream [{:?}] stream runtime", metadata.uuid.clone());
                            abort = true;
                        }
                    }
                    Err(err) => {
                        debug!(
                            "Failed to read from upstream && error code [{:?}] && error [{:?}] && details [{:?}]",
                            err.code() as u8,
                            err,
                            err.details()
                        );
                        let runtime_order = RuntimeEvent::Upstream(
                            RuntimeEventUpstream::TerminatedConnection(
                                metadata.clone()
                            )
                        );
                        let _ = send_message_to_runtime(runtime_tx.clone(), metadata.clone(), runtime_order).await;

                        warn!("Aborting Upstream [{:?}] stream runtime, cause [{:?}]", metadata.uuid.clone(), err);
                        abort = true;
                    }
                }
            }
        }
    }
    debug!("[Upstream [{:?}] aborted]", metadata.uuid.clone());
}

async fn upstream_start_sink_runtime(
    metadata: PeerMetadata,
    runtime_tx: RuntimeOrderTxChannel,
    mut rx: UpstreamPeerEventRx,
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
                    PeerEvent::Write((payload, from)) => {
                        debug!("[Upstream Write Event]");
                        if !paused {
                            //TODO would be much better to change the state of the sink loop
                            let size = payload.len();
                            //TODO would be better to trace only with cfg(debug) mode
                            match bi_stream_tx
                                .send(prepare_upstream_sink_request(payload, from.clone()))
                                .await
                            {
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
                error!(
                    "All tx pipelines (sink) for my peer [{:?}] have been dropped, aborting.",
                    metadata.uuid
                );
                return;
            }
        }
    }
}

/// should be in the runtime
/// Register all upstream peers from a source
/// All wrapped into a separate task
/// Each upstream peer initialization are also wrapped into a different task
pub fn register_upstream_peers(
    mut runtime_tx: RuntimeOrderTxChannel,
    upstreams: Vec<conf::Upstream>,
) {
    let task = move || async move {
        for upstream in upstreams {
            match UpstreamPeerMetadata::from(&upstream) {
                Ok(upstream) => {
                    let runtime_order =
                        RuntimeEvent::Upstream(RuntimeEventUpstream::Register(upstream));

                    if let Err(err) = runtime_tx.send(runtime_order).await {
                        error!("Failed to register upstream peers, cause : [{:?}]", err);
                    }
                }
                Err(err) => {
                    error!(
                        "Failed to register the following upstream : [{:?}], cause: [{:?}]",
                        &upstream, err
                    );
                }
            }
        }
    };
    tokio::spawn(task());
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

// fn get_upstream_peers() -> Vec<UpstreamPeerMetadata> {
//     debug!("Creating backend peers [DEBUGGING PURPOSES]");

//     let mut upstream_peers = vec![];

//     upstream_peers.push(UpstreamPeerMetadata {
//         host: "127.0.0.1:45888"
//             .parse()
//             .map_err(|err| {
//                 error!("Could not parse addr: [{:?}]", err);
//             })
//             .unwrap(),
//         alive_timeout: 30,
//         ready_timeout: 30,
//     });

//     upstream_peers.push(UpstreamPeerMetadata {
//         host: "127.0.0.1:45887"
//             .parse()
//             .map_err(|err| {
//                 error!("Fialed to parse addr [{:?}]", err);
//             })
//             .unwrap(),
//         alive_timeout: 30,
//         ready_timeout: 30,
//     });

//     upstream_peers
// }

pub fn prepare_upstream_sink_request(payload: Bytes, header: Header) -> InputStreamRequest {
    let request = InputStreamRequest {
        header: Option::Some(header),
        payload: (*payload).into(),
    };

    return request;
}
