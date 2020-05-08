use crate::peer::{PeerMetadata, PeerTxChannel, SinkPeerHalve, StreamPeerHalve};

use std::collections::HashMap;
use std::net::SocketAddr;

use std::fmt::Debug;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex, watch};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub enum InnerExchange<T> {
    Start,
    Pause,
    Write(T),
    FromRuntime(RuntimeOrder),
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeOrder {
    NoOrder,
    ShutdownPeer,
    PausePeer,
    GotMessageFromUpstreamPeer(String),
    GotMessageFromDownstream(String),
    PeerTerminatedConnection(PeerMetadata),
}

pub type PersistentMarkingLBRuntime = Arc<Mutex<PersistentMarkingLB>>;
pub type RuntimeOrderTxChannel = mpsc::Sender<RuntimeOrder>;
pub type RuntimeOrderRxChannel = watch::Receiver<RuntimeOrder>;

#[derive(Debug)]
pub struct PersistentMarkingPeersPool {
    // Used only for runtime orders
    pub front_peers_stream_tx: HashMap<Uuid, PeerTxChannel>,
    pub back_peers_stream_tx: HashMap<Uuid, PeerTxChannel>,

    // Used to send data to write
    pub front_peers_sink_tx: HashMap<Uuid, PeerTxChannel>,
    pub back_peers_sink_tx: HashMap<Uuid, PeerTxChannel>,

    pub peers_socket_addr_uuids: HashMap<SocketAddr, Uuid>,
}

#[derive(Clone)]
pub struct PersistentMarkingLB {
    pub tx: RuntimeOrderTxChannel,
    pub peers_pool: Arc<Mutex<PersistentMarkingPeersPool>>,
}

pub enum PersistentMarkingLBError {
    CannotHandleClient,
}

enum RuntimeError {
    PeerReferenceNotFound(PeerMetadata),
    PeerHalveDown(PeerMetadata),
    PeerChannelCommunicationError(PeerMetadata),
}

type RuntimeResult<T> = Result<T, RuntimeError>;

impl PersistentMarkingLB {
    pub fn new() -> PersistentMarkingLB {
        let (tx, rx) = mpsc::channel::<RuntimeOrder>(1000);
        let runtime = PersistentMarkingLB {
            tx,
            peers_pool: Arc::new(Mutex::new(PersistentMarkingPeersPool {
                front_peers_stream_tx: HashMap::new(),
                front_peers_sink_tx: HashMap::new(),
                back_peers_stream_tx: HashMap::new(),
                back_peers_sink_tx: HashMap::new(),
                peers_socket_addr_uuids: HashMap::new(),
            })),
        };
        runtime.clone().start2(rx);

        runtime
    }

    fn start2(mut self, mut rx: mpsc::Receiver<RuntimeOrder>) {
        let runtime_task = async move {
            debug!("Starting runtime of PersistentMarkingLB");
            match rx.recv().await {
                Some(runtime_order) => {
                    info!("Got order from a client");
                    match runtime_order {
                        RuntimeOrder::NoOrder => {
                            debug!("NoOrder");
                        }
                        RuntimeOrder::ShutdownPeer => {
                            debug!("Peer have been shutdown");
                        }
                        RuntimeOrder::PausePeer => {
                            debug!("Peer paused");
                        }
                        RuntimeOrder::PeerTerminatedConnection(peer_metadata) => {
                            {
                                let scope_lock = self.peers_pool.lock().await;
                                debug!(
                                    "Before termination hashmap: \n\
                                {:?}\
                                \n\
                                {:?}",
                                    scope_lock.front_peers_stream_tx,
                                    scope_lock.front_peers_sink_tx,
                                );
                            }
                            self.handle_peer_termination(peer_metadata).await;
                        }
                        RuntimeOrder::GotMessageFromUpstreamPeer(_) => {
                            debug!("GotMessageFromUpstreamPeer")
                        }
                        RuntimeOrder::GotMessageFromDownstream(_) => {
                            debug!("GotMessageFromDownstream")
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
        };

        tokio::task::spawn(runtime_task);
    }

    /// Remove reference of the front peer
    /// and returns the Sender channels of each tasks related to (Sink &
    /// Stream)
    async fn remove_front_peer(
        &mut self,
        peer_metadata: PeerMetadata,
    ) -> RuntimeResult<(Option<PeerTxChannel>, Option<PeerTxChannel>)> {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        let peer_sink_tx;
        let peer_stream_txt;
        if locked_peers_pool
            .front_peers_sink_tx
            .contains_key(&peer_metadata.uuid)
            && locked_peers_pool
                .front_peers_stream_tx
                .contains_key(&peer_metadata.uuid)
        {
            peer_sink_tx = locked_peers_pool
                .front_peers_sink_tx
                .remove(&peer_metadata.uuid);
            peer_stream_txt = locked_peers_pool
                .front_peers_stream_tx
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

    async fn handle_peer_termination(&mut self, peer_metadata: PeerMetadata) -> RuntimeResult<()> {
        debug!("Handle peer terminated connection");

        let (peer_sink_tx, _) = self.remove_front_peer(peer_metadata.clone()).await?;
        let mut peer_sink_tx =
            peer_sink_tx.ok_or(RuntimeError::PeerHalveDown(peer_metadata.clone()))?;

        peer_sink_tx
            .send(InnerExchange::FromRuntime(RuntimeOrder::ShutdownPeer))
            .await
            .map_err(|_| {
                error!(
                    "Cannot send a termination order to the sink task \
                    of the peer : {}",
                    peer_metadata
                );
                RuntimeError::PeerChannelCommunicationError(peer_metadata.clone())
            })
    }

    pub async fn add_peer_halves(
        &mut self,
        peer_sink_halve: &SinkPeerHalve,
        peer_stream_halve: &StreamPeerHalve,
    ) {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        locked_peers_pool.front_peers_stream_tx.insert(
            peer_stream_halve.halve.metadata.uuid,
            peer_stream_halve.halve.tx.clone(),
        );
        locked_peers_pool.front_peers_sink_tx.insert(
            peer_sink_halve.halve.metadata.uuid,
            peer_sink_halve.halve.tx.clone(),
        );
        locked_peers_pool.peers_socket_addr_uuids.insert(
            peer_sink_halve.halve.metadata.socket_addr,
            peer_sink_halve.halve.metadata.uuid,
        );
    }
}
