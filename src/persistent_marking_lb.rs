use crate::peer::{PeerMetadata, PeerRxChannel, PeerTxChannel, SinkPeerHalve, StreamPeerHalve};

use std::collections::HashMap;
use std::net::SocketAddr;

use std::borrow::BorrowMut;
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, watch, Mutex};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum InnerExchange<T> {
    Start,
    Pause,
    Write(T),
    FromRuntime(RuntimeOrder),
}

#[derive(Clone, Debug)]
pub enum RuntimeOrder {
    NoOrder,
    ShutdownPeer,
    PausePeer,
    PeerTerminatedConnection(PeerMetadata),
}

pub type PersistentMarkingLBRuntime = Arc<Mutex<PersistentMarkingLB>>;
pub type RuntimeOrderTxChannel = mpsc::Sender<RuntimeOrder>;

#[derive(Debug)]
pub struct PersistentMarkingLB {
    pub tx: RuntimeOrderTxChannel,

    // Used only for runtime orders
    pub front_peers_stream_tx: HashMap<Uuid, PeerTxChannel>,
    pub back_peers_stream_tx: HashMap<Uuid, PeerTxChannel>,

    // Used to send data to write
    pub front_peers_sink_tx: HashMap<Uuid, PeerTxChannel>,
    pub back_peers_sink_tx: HashMap<Uuid, PeerTxChannel>,

    pub peers_socket_addr_uuids: HashMap<SocketAddr, Uuid>,
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
    pub fn new() -> PersistentMarkingLBRuntime {
        let (tx, rx) = mpsc::channel::<RuntimeOrder>(1000);
        let runtime = Arc::new(Mutex::new(PersistentMarkingLB {
            front_peers_stream_tx: HashMap::new(),
            front_peers_sink_tx: HashMap::new(),
            back_peers_stream_tx: HashMap::new(),
            back_peers_sink_tx: HashMap::new(),
            tx,
            peers_socket_addr_uuids: HashMap::new(),
        }));
        Self::start(runtime.clone(), rx);

        runtime
    }

    fn start(runtime: PersistentMarkingLBRuntime, mut rx: mpsc::Receiver<RuntimeOrder>) {
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
                            Self::handle_peer_termination(runtime, peer_metadata).await;
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
        runtime: PersistentMarkingLBRuntime,
        peer_metadata: PeerMetadata,
    ) -> RuntimeResult<(Option<PeerTxChannel>, Option<PeerTxChannel>)> {
        let mut locked_runtime = runtime.lock().await;

        let peer_sink_tx;
        let peer_stream_txt;
        if locked_runtime
            .front_peers_sink_tx
            .contains_key(&peer_metadata.uuid)
            && locked_runtime
                .front_peers_stream_tx
                .contains_key(&peer_metadata.uuid)
        {
            peer_sink_tx = locked_runtime
                .front_peers_sink_tx
                .remove(&peer_metadata.uuid);
            peer_stream_txt = locked_runtime
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

    async fn handle_peer_termination(
        runtime: PersistentMarkingLBRuntime,
        peer_metadata: PeerMetadata,
    ) -> RuntimeResult<()> {
        debug!("Handle peer terminated connection");

        let (peer_sink_tx, _) = Self::remove_front_peer(runtime, peer_metadata.clone()).await?;
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
        runtime: PersistentMarkingLBRuntime,
        peer_sink_halve: &SinkPeerHalve,
        peer_stream_halve: &StreamPeerHalve,
    ) {
        let mut runtime_locked = runtime.lock().await;

        runtime_locked.front_peers_stream_tx.insert(
            peer_stream_halve.halve.metadata.uuid,
            peer_stream_halve.halve.tx.clone(),
        );
        runtime_locked.front_peers_sink_tx.insert(
            peer_sink_halve.halve.metadata.uuid,
            peer_sink_halve.halve.tx.clone(),
        );
        runtime_locked.peers_socket_addr_uuids.insert(
            peer_sink_halve.halve.metadata.socket_addr,
            peer_sink_halve.halve.metadata.uuid,
        );
    }
}
