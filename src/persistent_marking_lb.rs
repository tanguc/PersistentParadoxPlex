use crate::peer::{PeerMetadata, PeerRxChannel, PeerTxChannel, SinkPeerHalve, StreamPeerHalve};

use std::collections::HashMap;
use std::net::SocketAddr;

use futures::lock::Mutex;
use std::borrow::BorrowMut;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
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

impl PersistentMarkingLB {
    pub fn new() -> Arc<Mutex<Self>> {
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

    fn start(runtime: Arc<Mutex<PersistentMarkingLB>>, mut rx: mpsc::Receiver<RuntimeOrder>) {
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

    async fn handle_peer_termination(
        runtime: Arc<Mutex<PersistentMarkingLB>>,
        peer_metadata: PeerMetadata,
    ) {
        debug!("Peer terminated connection");
        let mut locked_runtime = runtime.lock().await;
        locked_runtime
            .front_peers_stream_tx
            .remove(&peer_metadata.uuid);

        if let Some(peer_sink) = locked_runtime
            .front_peers_sink_tx
            .get_mut(&peer_metadata.uuid)
        {
            debug!(
                "Removing sink for the peer : {}, \
                                sending an order via channel",
                peer_metadata
            );
            if let Err(err) = peer_sink
                .send(InnerExchange::FromRuntime(RuntimeOrder::ShutdownPeer))
                .await
            {
                error!(
                    "Cannot send a termination order to the sink task \
                    of the peer : {}",
                    peer_metadata
                );
            }
        } else {
            warn!(
                "The sink channel has not been found for the following \
            peer: {}",
                peer_metadata
            );
        }
    }

    pub fn add_peer_halves(
        &mut self,
        peer_sink_halve: &SinkPeerHalve,
        peer_stream_halve: &StreamPeerHalve,
    ) {
        self.front_peers_stream_tx.insert(
            peer_stream_halve.halve.metadata.uuid,
            peer_stream_halve.halve.tx.clone(),
        );
        self.front_peers_sink_tx.insert(
            peer_sink_halve.halve.metadata.uuid,
            peer_sink_halve.halve.tx.clone(),
        );
        self.peers_socket_addr_uuids.insert(
            peer_sink_halve.halve.metadata.socket_addr,
            peer_sink_halve.halve.metadata.uuid,
        );
    }
}
