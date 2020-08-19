use crate::backend;
use crate::peer::{
    DownstreamPeerSinkHalve, DownstreamPeerStreamHalve, PeerMetadata, PeerTxChannel,
    UpstreamPeerHalve,
};

use std::collections::HashMap;
use std::net::SocketAddr;

use std::fmt::Debug;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch, Mutex};
use uuid::Uuid;

pub type PersistentMarkingLBRuntime = Arc<Mutex<Runtime>>;

/// Channel to send/receive runtime orders
pub type RuntimeOrderTxChannel = mpsc::Sender<RuntimeEvent>;
pub type RuntimeOrderRxChannel = watch::Receiver<RuntimeEvent>;

type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Clone, Debug, PartialEq)]
pub enum PeerEvent<T> {
    Start,
    Pause,
    Stop,
    Write(T),
}

#[derive(Debug)]
pub enum RuntimeEvent {
    NoOrder,
    ShutdownPeer,
    PausePeer,
    GotMessageFromUpstreamPeer(String),
    GotMessageFromDownstream(String),
    PeerTerminatedConnection(PeerMetadata),
    GetUpstreamPeer(
        tokio::sync::oneshot::Sender<
            Option<tokio::sync::mpsc::UnboundedSender<backend::InputStreamRequest>>,
        >,
    ),
    MessageToDownstreamPeer(backend::OutputStreamRequest),
}

#[derive(Debug)]
pub struct RuntimePeersPool {
    // Used only for runtime orders
    pub downstream_peers_stream_tx: HashMap<Uuid, PeerTxChannel>,
    pub upstream_peers_stream_tx: HashMap<Uuid, PeerTxChannel>,

    // Used to send data to write
    pub downstream_peers_sink_tx: HashMap<Uuid, PeerTxChannel>,
    pub upstream_peers_sink_tx: HashMap<Uuid, mpsc::UnboundedSender<backend::InputStreamRequest>>,

    pub peers_addr_uuids: HashMap<SocketAddr, Uuid>,
}

#[derive(Clone)]
pub struct Runtime {
    pub tx: RuntimeOrderTxChannel,
    pub peers_pool: Arc<Mutex<RuntimePeersPool>>,
}

enum RuntimeError {
    PeerReferenceNotFound(PeerMetadata),
    PeerHalveDown(PeerMetadata),
    PeerChannelCommunicationError(PeerMetadata),
}

impl Runtime {
    pub fn new() -> Runtime {
        let (tx, rx) = mpsc::channel::<RuntimeEvent>(1000);
        let runtime = Runtime {
            tx,
            peers_pool: Arc::new(Mutex::new(RuntimePeersPool {
                downstream_peers_stream_tx: HashMap::new(),
                downstream_peers_sink_tx: HashMap::new(),
                upstream_peers_stream_tx: HashMap::new(),
                upstream_peers_sink_tx: HashMap::new(),
                peers_addr_uuids: HashMap::new(),
            })),
        };
        runtime.clone().start(rx);

        runtime
    }

    fn start(mut self, mut rx: mpsc::Receiver<RuntimeEvent>) {
        let runtime_task = async move {
            debug!("Starting runtime of PersistentMarkingLB");
            loop {
                match rx.recv().await {
                    Some(runtime_event) => {
                        info!("Got order from a client");
                        match runtime_event {
                            RuntimeEvent::NoOrder => {
                                debug!("NoOrder");
                            }
                            RuntimeEvent::ShutdownPeer => {
                                debug!("Peer shutdown");
                            }
                            RuntimeEvent::PausePeer => {
                                debug!("Peer paused");
                            }
                            RuntimeEvent::PeerTerminatedConnection(peer_metadata) => {
                                {
                                    let scope_lock = self.peers_pool.lock().await;
                                    debug!(
                                        "Before termination hashmap: \n\
                                    {:?}\
                                    \n\
                                    {:?}",
                                        scope_lock.downstream_peers_stream_tx,
                                        scope_lock.downstream_peers_sink_tx,
                                    );
                                }
                                self.handle_peer_termination(peer_metadata).await;
                            }
                            RuntimeEvent::GotMessageFromUpstreamPeer(_) => {
                                debug!("GotMessageFromUpstreamPeer")
                            }
                            RuntimeEvent::GotMessageFromDownstream(_) => {
                                debug!("GotMessageFromDownstream")
                            }
                            RuntimeEvent::GetUpstreamPeer(oneshot_answer) => {
                                debug!("GetUpstreamPeer order");
                                let upstreams = self.peers_pool.lock().await;
                                let mut upstream_tx = Option::None;
                                if !upstreams.upstream_peers_sink_tx.is_empty() {
                                    // TODO this one shouldnt act like that but find the best peer (by round robin)
                                    for upstream_tx_channel in
                                        upstreams.upstream_peers_sink_tx.iter()
                                    {
                                        upstream_tx = Option::Some(upstream_tx_channel.1.clone());
                                    }
                                }
                                match oneshot_answer.send(upstream_tx) {
                                    Ok(_) => {
                                        debug!("Successfully sent upstream peer tx");
                                    }
                                    Err(err) => {
                                        error!(
                                            "Failed to send the upstream peer one short answer : {:?}",
                                            err
                                        );
                                    }
                                };
                            }
                            RuntimeEvent::MessageToDownstreamPeer(metadata) => {
                                debug!("Runtime - GetDownstreamPeer order");
                                debug!(
                                    "Trying to find the downstream peer with UUID [{:?}]",
                                    metadata.header
                                );

                                let peers_pool = &*self.peers_pool.lock().await;
                                let mut downstream_peer = Option::None;
                                // TODO should be choosen by uuid and not randomly
                                if !peers_pool.downstream_peers_sink_tx.is_empty() {
                                    for downstream_peer_sink_tx in
                                        peers_pool.downstream_peers_sink_tx.iter()
                                    {
                                        downstream_peer =
                                            Option::Some(downstream_peer_sink_tx.clone());
                                    }
                                } else {
                                    error!(
                                        "No downstream peer has been found for the UUID [{:?}] ",
                                        metadata.header
                                    );
                                }

                                if let Some(downstream_peer) = downstream_peer {
                                    debug!(
                                        "Sending the Writing order to the downstream peer [{:?}]",
                                        downstream_peer.0
                                    );
                                    let downstream_peer_event = PeerEvent::Write(metadata.payload);

                                    if let Err(err) =
                                        downstream_peer.1.send(downstream_peer_event).await
                                    {
                                        error!("Failed to send the Writing order to the downstream peer tx channel with UUID [{:?}]", err);
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        debug!(
                            "Looks like all senders halves of runtime have \
                            been dropped"
                        );
                    }
                }
            }
        };
        debug!("Dropping the runtime task");

        tokio::task::spawn(runtime_task);
    }

    /// Remove reference of the front peer
    /// and returns the Sender channels of each tasks related to (Sink &
    /// Stream)
    async fn remove_downstream_peer(
        &mut self,
        peer_metadata: PeerMetadata,
    ) -> RuntimeResult<(Option<PeerTxChannel>, Option<PeerTxChannel>)> {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        let peer_sink_tx;
        let peer_stream_txt;
        if locked_peers_pool
            .downstream_peers_sink_tx
            .contains_key(&peer_metadata.uuid)
            && locked_peers_pool
                .downstream_peers_stream_tx
                .contains_key(&peer_metadata.uuid)
        {
            peer_sink_tx = locked_peers_pool
                .downstream_peers_sink_tx
                .remove(&peer_metadata.uuid);
            peer_stream_txt = locked_peers_pool
                .downstream_peers_stream_tx
                .remove(&peer_metadata.uuid);
            Ok((peer_sink_tx, peer_stream_txt))
        } else {
            warn!(
                "The sink or stream channels have not been found for the \
                    following peer: {}",
                peer_metadata
            );
            Err(RuntimeError::PeerReferenceNotFound(peer_metadata))
        }
    }

    /// When a peer is down (notified from stream halve usually)
    /// Notifying the sink halve to stop right now his runtime
    async fn handle_peer_termination(&mut self, peer_metadata: PeerMetadata) -> RuntimeResult<()> {
        debug!("Handle peer terminated connection");

        let (peer_sink_tx, _) = self.remove_downstream_peer(peer_metadata.clone()).await?;
        let mut peer_sink_tx =
            peer_sink_tx.ok_or(RuntimeError::PeerHalveDown(peer_metadata.clone()))?;

        peer_sink_tx.send(PeerEvent::Stop).await.map_err(|_| {
            error!(
                "Cannot send a termination order to the sink task \
                    of the peer : {}",
                peer_metadata
            );
            RuntimeError::PeerChannelCommunicationError(peer_metadata.clone())
        })
    }

    pub async fn add_upstream_peer_halves(
        &mut self,
        peer_halve: &UpstreamPeerHalve<backend::InputStreamRequest>,
    ) {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        locked_peers_pool.upstream_peers_stream_tx.insert(
            peer_halve.stream_halve.metadata.uuid,
            peer_halve.stream_halve.tx.clone(),
        );

        locked_peers_pool.upstream_peers_sink_tx.insert(
            peer_halve.stream_halve.metadata.uuid,
            peer_halve.grpc_tx_channel.clone(),
        );

        locked_peers_pool.peers_addr_uuids.insert(
            peer_halve.stream_halve.metadata.socket_addr.clone(),
            peer_halve.stream_halve.metadata.uuid,
        );
    }

    pub async fn add_downstream_peer_halves(
        &mut self,
        peer_sink_halve: &DownstreamPeerSinkHalve,
        peer_stream_halve: &DownstreamPeerStreamHalve,
    ) {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        locked_peers_pool.downstream_peers_stream_tx.insert(
            peer_stream_halve.halve.metadata.uuid,
            peer_stream_halve.halve.tx.clone(),
        );
        locked_peers_pool.downstream_peers_sink_tx.insert(
            peer_sink_halve.halve.metadata.uuid,
            peer_sink_halve.halve.tx.clone(),
        );
        locked_peers_pool.peers_addr_uuids.insert(
            peer_sink_halve.halve.metadata.socket_addr,
            peer_sink_halve.halve.metadata.uuid,
        );
    }
}
