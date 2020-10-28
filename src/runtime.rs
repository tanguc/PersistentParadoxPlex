use crate::{upstream::UpstreamPeerStarted, utils::round_robin};

use crate::downstream::{DownstreamPeerSinkChannelTx, DownstreamPeerStreamChannelTx};
use crate::{
    upstream::{UpstreamPeerSinkChannelTx, UpstreamPeerStreamChannelTx},
    upstream_proto::Header,
};

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
};
use std::{
    fmt::{Debug, Display, Error, Formatter},
    hash::Hash,
    hash::Hasher,
};

use linked_hash_set::LinkedHashSet;

use std::sync::Arc;

use tokio::sync::{mpsc, mpsc::error::TrySendError, watch, Mutex};
use uuid::Uuid;

use evmap::{shallow_copy, ReadHandle, WriteHandle};

pub type PersistentMarkingLBRuntime = Arc<Mutex<Runtime>>;

/// Channel to send/receive runtime orders
pub type RuntimeOrderTxChannel = mpsc::Sender<RuntimeEvent>;
pub type RuntimeOrderRxChannel = watch::Receiver<RuntimeEvent>;

type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, PartialEq)]
pub enum PeerEvent<T = ()> {
    Start,
    Pause,
    Stop,
    Write(T),
    Resume,
}

#[derive(Debug)]
pub enum RuntimeEvent {
    NoOrder,
    ShutdownPeer,
    PausePeer,
    DownstreamPeerTerminatedConnection(PeerMetadata),
    UpstreamPeerTerminatedConnection(PeerMetadata),
    // GetUpstreamPeer(tokio::sync::oneshot::Sender<Option<PeerEventTxChannel>>),
    MessageFromDownstreamPeer(String, String), // .1 uuid of the incoming downstream client
    MessageFromUpstreamPeer(String, String),   // .1 uuid of the target downstream client
    NewUpstream(UpstreamPeerStarted),
}

// pub struct UpstreamPeerAvailablePool(UpstreamPeerStreamChannelTx);

// impl std::cmp::Eq for UpstreamPeerAvailablePool {}

// impl std::cmp::PartialEq for UpstreamPeerAvailablePool {
//     fn eq(&self, other: &Self) -> bool {
//         false
//     }

//     fn ne(&self, other: &Self) -> bool {
//         false
//     }
// }

// impl std::hash::Hash for UpstreamPeerAvailablePool {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         let repr = format!("{:?}", self.0);
//         state.write(repr.as_bytes());
//     }
// }

// impl evmap::shallow_copy::ShallowCopy for UpstreamPeerAvailablePool {
//     unsafe fn shallow_copy(&self) -> std::mem::ManuallyDrop<Self> {
//         std::mem::ManuallyDrop::new(*self)
//     }
// }

// impl Debug for UpstreamPeerAvailablePool {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         self.0.fmt(f)
//     }
// }

#[derive(Debug)]
pub struct RuntimePeersPool {
    // Used only for runtime orders
    pub downstream_peers_stream_tx: HashMap<Uuid, DownstreamPeerStreamChannelTx>,
    pub upstream_peers_stream_tx: HashMap<Uuid, UpstreamPeerStreamChannelTx>,
    // pub upstream_peers_stream_tx_read: evmap::WriteHandle<Uuid, UpstreamPeerAvailablePool>,
    // pub upstream_peers_stream_tx_write: evmap::ReadHandle<Uuid, UpstreamPeerAvailablePool>,
    // pub round_robin_context: crate::utils::RoundRobin::RoundRobinContext<Uuid>,

    // Used to send data to write
    pub downstream_peers_sink_tx: HashMap<Uuid, DownstreamPeerSinkChannelTx>,
    pub upstream_peers_sink_tx: HashMap<Uuid, UpstreamPeerSinkChannelTx>,

    pub peers_addr_uuids: HashMap<SocketAddr, Uuid>,
}

pub type PeerEventTxChannel<T> = mpsc::Sender<PeerEvent<(String, T)>>; // first param (payload) and second for uuid of the target peer
pub type PeerEventRxChannel<T> = mpsc::Receiver<PeerEvent<(String, T)>>;

pub struct PeerHalve<T> {
    pub metadata: PeerMetadata,
    pub runtime_tx: RuntimeOrderTxChannel,
    pub rx: PeerEventRxChannel<T>,
    pub tx: PeerEventTxChannel<T>,
}

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

#[derive(Clone)]
pub struct Runtime {
    pub tx: RuntimeOrderTxChannel,
    pub peers_pool: Arc<Mutex<RuntimePeersPool>>,
}

#[derive(Debug)]
pub enum RuntimeError {
    PeerReferenceNotFound(PeerMetadata),
    PeerHalveDown(PeerMetadata),
    PeerChannelCommunicationError(PeerMetadata),
    Closed,
}

impl Runtime {
    pub fn new() -> Runtime {
        let (tx, rx) = mpsc::channel::<RuntimeEvent>(1000);

        // let (upstream_peers_sink_tx_write, upstream_peers_sink_tx_read) =
        //     evmap::new::<Uuid, UpstreamPeerAvailablePool>();

        // let upstream_peers_sink_tx_available_ll = std::collections::LinkedList::new();
        // upstream_peers_sink_tx_available_ll.append(Uuid::new_v4());

        // let cursor_upstream_available = upstream_peers_sink_tx_available_ll.cursor_front();

        let runtime = Runtime {
            tx,
            peers_pool: Arc::new(Mutex::new(RuntimePeersPool {
                downstream_peers_stream_tx: HashMap::new(),
                upstream_peers_stream_tx: HashMap::new(),
                downstream_peers_sink_tx: HashMap::new(),
                upstream_peers_sink_tx: HashMap::new(),
                peers_addr_uuids: HashMap::new()
                // upstream_peers_stream_tx_read: upstream_peers_sink_tx_read,
                // upstream_peers_stream_tx_write: upstream_peers_sink_tx_write,
            })),
        };
        runtime.clone().start(rx);

        runtime
    }

    fn start(mut self, mut rx: mpsc::Receiver<RuntimeEvent>) {
        let runtime_task = async move {
            debug!("Starting runtime of PersistentMarkingLB");
            let mut round_robin_context = round_robin::RoundRobinContext::new();

            loop {
                match rx.recv().await {
                    Some(runtime_event) => {
                        info!("Got order from a client");
                        match runtime_event {
                            RuntimeEvent::NoOrder => {
                                debug!("NO ORDER");
                                unimplemented!();
                            }
                            RuntimeEvent::ShutdownPeer => {
                                debug!("SHUTDOWN ORDER");
                                unimplemented!();
                            }
                            RuntimeEvent::PausePeer => {
                                debug!("PAUSE ORDER");
                                unimplemented!();
                            }
                            RuntimeEvent::NewUpstream(peer) => {
                                debug!("REGISTER NEW UPSTREAM ORDER");
                                let peer_uuid = peer.metadata.uuid.clone();
                                self.add_upstream_peer_halves(
                                    peer.sink_tx,
                                    peer.stream_tx,
                                    peer.metadata,
                                )
                                .await;
                                round_robin_context.add(peer_uuid);
                            }
                            /// Only for STREAM runtime of upstream peers
                            RuntimeEvent::UpstreamPeerTerminatedConnection(peer_metadata) => {
                                debug!(
                                    "TERMINATION ORDER - UPSTREAM PEER [{:?}]",
                                    peer_metadata.uuid.clone()
                                );
                                if let Ok(upstream_peer) =
                                    self.remove_upstream_peer(peer_metadata.clone()).await
                                {
                                    round_robin_context.delete(&peer_metadata.uuid);
                                    if let Some(peer_sink_tx) = upstream_peer.0 {
                                        let _ = send_termination_peer(peer_metadata, peer_sink_tx);
                                    }
                                }
                            }
                            /// Only for STREAM runtime of downstream peers
                            RuntimeEvent::DownstreamPeerTerminatedConnection(peer_metadata) => {
                                debug!(
                                    "TERMINATION ORDER - DOWNSTREAM PEER [{:?}]",
                                    peer_metadata.uuid.clone()
                                );

                                if let Ok(downstream_peer) =
                                    self.remove_downstream_peer(peer_metadata.clone()).await
                                {
                                    trace!("TERMINATION ORDER - Upstream [{:?}] found, sending termination", peer_metadata.uuid.clone());
                                    if let Some(peer_sink_tx) = downstream_peer.0 {
                                        let _ = send_termination_peer(peer_metadata, peer_sink_tx);
                                    }
                                }
                            }
                            RuntimeEvent::MessageFromDownstreamPeer(payload, uuid) => {
                                debug!("ORDER - MESSAGE FROM DOWNSTREAM PEER");
                                if let Some(next_rr_upstream) = round_robin_context.next() {
                                    let next_upstream =
                                        self.get_upstream_sink_tx_by_uuid(&next_rr_upstream).await;

                                    match next_upstream {
                                        Some(mut tx) => {
                                            let _ = Header {
                                                address: "127.0.0.1".into(), // TODO fill with the current host address
                                                client_uuid: uuid.clone().to_string(), //
                                                time: "03:53:39".into(),     // TODO current time
                                            };
                                            let event = PeerEvent::Write((
                                                payload,
                                                uuid.clone().to_string(),
                                            ));

                                            if let Err(err) = tx.try_send(event) {
                                                // try send_timeout looks better
                                                warn!("[Failed to send message to the upstream [{:?}], veryfying why...", uuid.clone());
                                                match err {
                                                    TrySendError::Closed(_) => {
                                                        //should try another upstream peer to send the message
                                                        error!("[Upstream peer [{:?}] has closed, cannot send, trying to send to another one]", uuid.clone());
                                                    }
                                                    TrySendError::Full(_) => {
                                                        warn!("[Upstream peer [{:?}] channel is full, cannot send, retrying in few seconds]", uuid.clone());
                                                    }
                                                }
                                            }
                                        }
                                        None => {
                                            warn!("[Failed to find an available upstream peer]");
                                        }
                                    }
                                }
                            }
                            RuntimeEvent::MessageFromUpstreamPeer(payload, uuid) => {
                                debug!("ORDER - MESSAGE FROM UPSTREAM PEER ORDER");
                                debug!(
                                    "Trying to find the downstream peer with UUID [{:?}]",
                                    uuid.clone()
                                );
                                if let Some(mut sink_tx) =
                                    self.get_downstream_sink_tx_by_str(uuid.as_str()).await
                                {
                                    debug!(
                                        "Sending the Writing order to the downstream peer [{:?}]",
                                        uuid.clone()
                                    );
                                    let write_order = PeerEvent::Write((payload, ()));

                                    if let Err(send_err) = sink_tx.send(write_order).await {
                                        error!("Failed to send the Writing order to the downstream peer tx channel with UUID [{:?}]", send_err);
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

        tokio::task::spawn(runtime_task);
    }

    async fn get_downstream_sink_tx_by_str(
        &mut self,
        uuid: &str,
    ) -> Option<DownstreamPeerSinkChannelTx> {
        if let Ok(parsed_uuid) = Uuid::parse_str(uuid) {
            return self.get_downstream_sink_tx_by_uuid(&parsed_uuid).await;
        } else {
            error!("Failed to parse the following UUID = [{:?}]", uuid);
            return None;
        }
    }

    async fn get_downstream_sink_tx_by_uuid(
        &mut self,
        uuid: &Uuid,
    ) -> Option<DownstreamPeerSinkChannelTx> {
        let peers_pool = &*self.peers_pool.lock().await;
        // let mut downstream_peer = Result::Err();

        // for downstream_peers in peers_pool.downstream_peers_sink_tx.iter() {
        //     trace!("Downstream peer [{:?}]", downstream_peers.0)
        // }

        if !peers_pool.downstream_peers_sink_tx.is_empty()
            && peers_pool.downstream_peers_sink_tx.contains_key(uuid)
        {
            return peers_pool.downstream_peers_sink_tx.get(uuid).cloned();
        } else {
            error!(
                "No downstream peer has been found for the UUID [{:?}]",
                uuid.clone()
            );
            return None;
        }
    }

    async fn get_upstream_sink_tx_by_uuid(
        &mut self,
        uuid: &Uuid,
    ) -> Option<UpstreamPeerSinkChannelTx> {
        let peers_pool = &*self.peers_pool.lock().await;

        if !peers_pool.upstream_peers_sink_tx.is_empty() {
            if !peers_pool.upstream_peers_sink_tx.contains_key(uuid) {
                return peers_pool.upstream_peers_sink_tx.get(uuid).cloned();
            } else {
                error!("Failed to retrieve the upstream sink tx [{:?}]", &uuid);
                return None;
            }
        } else {
            error!("The upstream sink tx list is empty");
            return None;
        }
    }

    async fn get_upstream_sink_tx_by_str(
        &mut self,
        uuid: &str,
    ) -> Option<UpstreamPeerSinkChannelTx> {
        if let Ok(parsed_uuid) = Uuid::parse_str(uuid) {
            return self.get_upstream_sink_tx_by_uuid(&parsed_uuid).await;
        } else {
            error!("Failed to parse the following UUID [{:?}]", uuid);
            return None;
        }
    }

    async fn remove_upstream_peer(
        &mut self,
        peer_metadata: PeerMetadata,
    ) -> RuntimeResult<(
        Option<UpstreamPeerSinkChannelTx>,
        Option<UpstreamPeerStreamChannelTx>,
    )> {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        let peer_sink_tx;
        let peer_stream_tx;

        if locked_peers_pool
            .upstream_peers_sink_tx
            .contains_key(&peer_metadata.uuid)
            && locked_peers_pool
                .upstream_peers_sink_tx
                .contains_key(&peer_metadata.uuid)
        {
            peer_sink_tx = locked_peers_pool
                .upstream_peers_sink_tx
                .remove(&peer_metadata.uuid);
            peer_stream_tx = locked_peers_pool
                .upstream_peers_stream_tx
                .remove(&peer_metadata.uuid);

            // // notify the round robin about it
            // locked_peers_pool
            //     .round_robin_context
            //     .delete_candidat(&peer_metadata.uuid.clone());

            Ok((peer_sink_tx, peer_stream_tx))
        } else {
            warn!(
                "Failed to find and remove the upstream peer [{:?}]",
                peer_metadata.clone()
            );
            Err(RuntimeError::PeerReferenceNotFound(peer_metadata.clone()))
        }
    }

    /// Remove reference of the front peer
    /// and returns the Sender channels of each tasks related to (Sink &
    /// Stream)
    async fn remove_downstream_peer(
        &mut self,
        peer_metadata: PeerMetadata,
    ) -> RuntimeResult<(
        Option<DownstreamPeerSinkChannelTx>,
        Option<DownstreamPeerStreamChannelTx>,
    )> {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        trace!(
            "Before termination hashmap: \n\
        {:?}\
        \n\
        {:?}",
            locked_peers_pool.downstream_peers_stream_tx,
            locked_peers_pool.downstream_peers_sink_tx,
        );

        let peer_sink_tx;
        let peer_stream_tx;
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
            peer_stream_tx = locked_peers_pool
                .downstream_peers_stream_tx
                .remove(&peer_metadata.uuid);
            Ok((peer_sink_tx, peer_stream_tx))
        } else {
            warn!(
                "Failed to find and remove the downstream peer [{:?}]",
                peer_metadata
            );
            Err(RuntimeError::PeerReferenceNotFound(peer_metadata))
        }
    }

    pub async fn add_upstream_peer_halves(
        &mut self,
        sink_tx: UpstreamPeerSinkChannelTx,
        stream_tx: UpstreamPeerStreamChannelTx,
        metadata: PeerMetadata,
    ) {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        locked_peers_pool
            .upstream_peers_stream_tx
            .insert(metadata.uuid.clone(), stream_tx);

        locked_peers_pool
            .upstream_peers_sink_tx
            .insert(metadata.uuid.clone(), sink_tx);

        locked_peers_pool
            .peers_addr_uuids
            .insert(metadata.socket_addr.clone(), metadata.uuid.clone());

        // notify the round robin context about it
        // locked_peers_pool
        //     .round_robin_context
        //     .new_candidat(metadata.uuid.clone());
    }

    pub async fn add_downstream_peer_halves(
        &mut self,
        metadata: PeerMetadata,
        sink_tx: DownstreamPeerSinkChannelTx,
        stream_tx: DownstreamPeerStreamChannelTx,
    ) {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        locked_peers_pool
            .downstream_peers_stream_tx
            .insert(metadata.uuid.clone(), stream_tx);
        locked_peers_pool
            .downstream_peers_sink_tx
            .insert(metadata.uuid.clone(), sink_tx);
        locked_peers_pool
            .peers_addr_uuids
            .insert(metadata.socket_addr.clone(), metadata.uuid.clone());
    }
}

/// New task which will try to send the STOP order to the given peer
/// Runtime don't care anymore about this peer because it's not more referenced by this last
async fn send_termination_peer<T>(peer_metadata: PeerMetadata, mut peer_tx: PeerEventTxChannel<T>)
where
    T: Send + 'static,
{
    let task = async move {
        trace!(
            "Sending termination for the peer [{:?}]",
            peer_metadata.clone()
        );

        let stop_order: PeerEvent<()> = PeerEvent::Stop;
        if let Err(peer_tx_err) = peer_tx.send(PeerEvent::Stop).await {
            error!(
                "Failed to send stop order to peer [{}]",
                peer_metadata.clone()
            );
            Err(RuntimeError::PeerChannelCommunicationError(
                peer_metadata.clone(),
            ))
        } else {
            Ok(())
        }
    };
    tokio::spawn(task);
}

pub async fn send_message_to_runtime(
    mut runtime_tx: RuntimeOrderTxChannel,
    metadata: PeerMetadata,
    payload: RuntimeEvent,
) -> Result<(), RuntimeError> {
    //TODO better to send enum

    if let Err(err) = runtime_tx.send(payload).await {
        error!(
            "Failed to send the order to the runtime [{:?}] from upstream [{}], terminating the upstream streaming runtime",
            err,
            metadata.uuid.clone()
        );
        Err(RuntimeError::Closed)
    } else {
        Ok(())
    }
}
