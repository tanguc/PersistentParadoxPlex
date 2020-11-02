use crate::{
    downstream::PeerRuntime,
    upstream::{UpstreamPeer, UpstreamPeerEventTx, UpstreamPeerMetadata, UpstreamPeerStarted},
    utils::round_robin,
};

use crate::upstream_proto::Header;

use crate::downstream::DownstreamPeerEventTx;
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
};
use std::{
    fmt::{Debug, Display, Error, Formatter},
    hash::Hash,
    hash::Hasher,
};

use std::sync::Arc;

use tokio::sync::{mpsc, mpsc::error::TrySendError, watch, Mutex};
use uuid::Uuid;

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
pub enum RuntimeEventUpstream {
    Register(UpstreamPeerMetadata),
    TerminatedConnection(PeerMetadata),
    Message(String, String),
}
#[derive(Debug)]
pub enum RuntimeEventDownstream {
    Register,
    TerminatedConnection(PeerMetadata),
    Message(String, String),
}
#[derive(Debug)]
pub enum RuntimeEventAdmin {
    NoOrder,
    ShutdownPeer,
    PausePeer,
}

#[derive(Debug)]
pub enum RuntimeEvent {
    Upstream(RuntimeEventUpstream),
    Downstream(RuntimeEventDownstream),
    Admin(RuntimeEventAdmin),
}

#[derive(Debug)]
pub struct RuntimePeersPool {
    // Used only for runtime orders
    pub downstream_peers_stream_tx: HashMap<String, DownstreamPeerEventTx>,
    pub upstream_peers_stream_tx: HashMap<String, UpstreamPeerEventTx>,
    // pub upstream_peers_stream_tx_read: evmap::WriteHandle<Uuid, UpstreamPeerAvailablePool>,
    // pub upstream_peers_stream_tx_write: evmap::ReadHandle<Uuid, UpstreamPeerAvailablePool>,
    // pub round_robin_context: crate::utils::RoundRobin::RoundRobinContext<Uuid>,

    // Used to send data to write
    pub downstream_peers_sink_tx: HashMap<String, DownstreamPeerEventTx>,
    pub upstream_peers_sink_tx: HashMap<String, UpstreamPeerEventTx>,

    pub peers_uuid_by_addr: HashMap<SocketAddr, String>,
    pub peers_addr_by_uuid: HashMap<String, SocketAddr>,
}

// pub type PeerEventTxChannel<T> = mpsc::Sender<PeerEvent<(String, T)>>; // first param (payload) and second for uuid of the target peer
// pub type PeerEventRxChannel<T> = mpsc::Receiver<PeerEvent<(String, T)>>;

// pub type DownstreamPeerEventTxChannel = mpsc::Sender<PeerEvent<(String, ())>>;
// pub type DownstreamPeerEventRxChannel = mpsc::Receiver<PeerEvent<(String, ())>>;

// pub struct PeerHalve<U, T: mpsc::Sender<U>> {
// }

#[derive(Clone, PartialEq)]
pub struct PeerMetadata {
    pub uuid: String,
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
                peers_uuid_by_addr: HashMap::new(),
                peers_addr_by_uuid: HashMap::new(),
                // upstream_peers_stream_tx_read: upstream_peers_sink_tx_read,
                // upstream_peers_stream_tx_write: upstream_peers_sink_tx_write,
            })),
        };
        runtime.clone().start(rx);

        runtime
    }

    /// Clone via `Arc` the internal peers pool
    /// which contains all upstreams and downstreams peers
    pub fn get_peers_pool(&mut self) -> Arc<Mutex<RuntimePeersPool>> {
        self.peers_pool.clone()
    }

    async fn handle_admin_orders(&mut self, order: RuntimeEventAdmin) {
        debug!("HANDLE ADMIN ORDERS");

        match order {
            RuntimeEventAdmin::NoOrder => {
                debug!("NO ORDER");
                unimplemented!();
            }
            RuntimeEventAdmin::ShutdownPeer => {
                debug!("SHUTDOWN ORDER");
                unimplemented!();
            }
            RuntimeEventAdmin::PausePeer => {
                debug!("PAUSE ORDER");
                unimplemented!();
            }
        }
    }

    /// Register a new upstream with the given metadata
    /// Create a new task for each register.
    /// Until the upstream peer is effective, it has to pass multiple
    /// state to be defined as ready.
    // async fn register_upstream_peer(&self, metadata: UpstreamPeerMetadata) {
    //     debug!("Registering a new upstream peer");
    //     trace!("new upstream peer metadata [{:?}]", &metadata);

    // }

    async fn handle_upstream_orders(
        &mut self,
        order: RuntimeEventUpstream,
        round_robin_context: &mut round_robin::RoundRobinContext<String>,
    ) {
        debug!("HANDLE UPSTREAM ORDERS");

        match order {
            RuntimeEventUpstream::Register(metadata) => {
                debug!("REGISTER NEW UPSTREAM ORDER");

                let ready_upstream = UpstreamPeer::new(&metadata, self.tx.clone()).start().await;
                if let Ok(ready_upstream) = ready_upstream {
                    let peer_uuid = ready_upstream.metadata.uuid.clone();
                    self.add_upstream_peer_halves(
                        ready_upstream.sink_tx,
                        ready_upstream.stream_tx,
                        ready_upstream.metadata,
                    )
                    .await;
                    round_robin_context.add(peer_uuid);
                } else {
                    error!("Failed to register the upstream peer [{:?}]", &metadata);
                }

                // let upstream_ready = self.register_upstream_peer(metadata).await;
            }
            /// Only for STREAM runtime of upstream peers
            RuntimeEventUpstream::TerminatedConnection(peer_metadata) => {
                debug!(
                    "TERMINATION ORDER - UPSTREAM PEER [{:?}]",
                    peer_metadata.uuid.clone()
                );
                if let Ok(upstream_peer) = self.remove_upstream_peer(peer_metadata.clone()).await {
                    round_robin_context.delete(&peer_metadata.uuid);
                    if let Some(peer_sink_tx) = upstream_peer.0 {
                        let _ = send_event_to_peer(peer_metadata, peer_sink_tx, PeerEvent::Stop);
                    }
                }
            }
            RuntimeEventUpstream::Message(payload, uuid) => {
                debug!("ORDER - MESSAGE FROM UPSTREAM PEER ORDER");
                debug!(
                    "Trying to find the downstream peer with UUID [{:?}]",
                    uuid.clone()
                );
                if let Some(mut sink_tx) = self.get_downstream_sink_tx_by_uuid(uuid.as_str()).await
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

    async fn handle_downstream_orders(
        &mut self,
        order: RuntimeEventDownstream,
        round_robin_context: &mut round_robin::RoundRobinContext<String>,
    ) {
        debug!("HANDLE UPSTREAM ORDERS");
        match order {
            /// Only for STREAM runtime of downstream peers
            RuntimeEventDownstream::TerminatedConnection(peer_metadata) => {
                debug!(
                    "TERMINATION ORDER - DOWNSTREAM PEER [{:?}]",
                    peer_metadata.uuid.clone()
                );

                if let Ok(downstream_peer) =
                    self.remove_downstream_peer(peer_metadata.clone()).await
                {
                    trace!(
                        "TERMINATION ORDER - Upstream [{:?}] found, sending termination",
                        peer_metadata.uuid.clone()
                    );
                    if let Some(peer_sink_tx) = downstream_peer.0 {
                        let _ = send_event_to_peer(peer_metadata, peer_sink_tx, PeerEvent::Stop);
                    }
                }
            }
            RuntimeEventDownstream::Message(payload, uuid) => {
                debug!("ORDER - MESSAGE FROM DOWNSTREAM PEER");
                if let Some(next_rr_upstream) = round_robin_context.next() {
                    let next_upstream = self.get_upstream_sink_tx_by_uuid(&next_rr_upstream).await;
                    let downstream_addr = self.get_peer_addr_by_uuid(&uuid).await;

                    match next_upstream {
                        Some(mut tx) => {
                            let header = Header {
                                address: format!("{:?}", downstream_addr), // TODO fill with the current host address
                                client_uuid: uuid.clone().to_string(),     //
                                time: chrono::offset::Utc::now().to_rfc3339(), // TODO current time
                            };
                            let event = PeerEvent::Write((payload, header));

                            // TODO if the channel is full, should reconsider to send to another upstream peer
                            if let Err(err) = tx.try_send(event) {
                                // try send_timeout looks better
                                warn!("[Failed to send message to the upstream [{:?}], verifying why...", uuid.clone());
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
            RuntimeEventDownstream::Register => {
                unimplemented!(
                    "RuntimeEventDownstream::Register is not implemented for downstream peers"
                );
            }
        }
    }

    fn start(mut self, mut rx: mpsc::Receiver<RuntimeEvent>) {
        let runtime_task = async move {
            debug!("Starting runtime of PersistentMarkingLB");
            let mut round_robin_context = round_robin::RoundRobinContext::new();

            loop {
                debug!("WAITING ORDER FOR RUNTIME");
                match rx.recv().await {
                    Some(runtime_event) => {
                        info!("Got order from a client");

                        match runtime_event {
                            RuntimeEvent::Upstream(order) => {
                                self.handle_upstream_orders(order, &mut round_robin_context)
                                    .await;
                            }
                            RuntimeEvent::Downstream(order) => {
                                self.handle_downstream_orders(order, &mut round_robin_context)
                                    .await;
                            }
                            RuntimeEvent::Admin(order) => {
                                self.handle_admin_orders(order).await;
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

    async fn get_peer_addr_by_uuid(&mut self, uuid: &str) -> Option<SocketAddr> {
        let peers_pool = &*self.peers_pool.lock().await;

        let uuid = &uuid.to_string();
        if peers_pool.peers_addr_by_uuid.contains_key(uuid) {
            return peers_pool.peers_addr_by_uuid.get(uuid).cloned();
        } else {
            error!("Failed to retrieve the addr of the peer UUID [{:?}]", &uuid);
            None
        }
    }

    async fn get_downstream_sink_tx_by_uuid(
        &mut self,
        uuid: &str,
    ) -> Option<DownstreamPeerEventTx> {
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

    async fn get_upstream_sink_tx_by_uuid(&mut self, uuid: &str) -> Option<UpstreamPeerEventTx> {
        let peers_pool = &*self.peers_pool.lock().await;

        trace!(
            "Upstream sink tx list = {:?}",
            &peers_pool.upstream_peers_sink_tx
        );
        if !peers_pool.upstream_peers_sink_tx.is_empty() {
            if peers_pool
                .upstream_peers_sink_tx
                .contains_key(&uuid.to_string())
            {
                return peers_pool.upstream_peers_sink_tx.get(uuid).cloned();
            } else {
                error!(
                    "Failed to get upstream sink tx [{:?}], it does not exist",
                    &uuid
                );
                return None;
            }
        } else {
            error!("The upstream sink tx list is empty");
            return None;
        }
    }

    async fn remove_upstream_peer(
        &mut self,
        peer_metadata: PeerMetadata,
    ) -> RuntimeResult<(Option<UpstreamPeerEventTx>, Option<UpstreamPeerEventTx>)> {
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
            // TODO delete from hashmap of uuid<->socketaddr and socketaddr<->uuid

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
    ) -> RuntimeResult<(Option<DownstreamPeerEventTx>, Option<DownstreamPeerEventTx>)> {
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
            // TODO delete from hashmap of uuid<->socketaddr and socketaddr<->uuid

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
        sink_tx: UpstreamPeerEventTx,
        stream_tx: UpstreamPeerEventTx,
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
            .peers_uuid_by_addr
            .insert(metadata.socket_addr.clone(), metadata.uuid.clone());

        locked_peers_pool
            .peers_addr_by_uuid
            .insert(metadata.uuid.clone(), metadata.socket_addr.clone());

        // notify the round robin context about it
        // locked_peers_pool
        //     .round_robin_context
        //     .new_candidat(metadata.uuid.clone());
    }

    pub async fn add_downstream_peer_halves(
        &mut self,
        metadata: PeerMetadata,
        sink_tx: DownstreamPeerEventTx,
        stream_tx: DownstreamPeerEventTx,
    ) {
        let mut locked_peers_pool = self.peers_pool.lock().await;

        locked_peers_pool
            .downstream_peers_stream_tx
            .insert(metadata.uuid.clone(), stream_tx);
        locked_peers_pool
            .downstream_peers_sink_tx
            .insert(metadata.uuid.clone(), sink_tx);
        locked_peers_pool
            .peers_uuid_by_addr
            .insert(metadata.socket_addr.clone(), metadata.uuid.clone());
        locked_peers_pool
            .peers_addr_by_uuid
            .insert(metadata.uuid.clone(), metadata.socket_addr.clone());
    }
}

/// New task which will try to send the STOP order to the given peer
/// Runtime don't care anymore about this peer because it's not more referenced by this last
async fn send_event_to_peer<T>(peer_metadata: PeerMetadata, mut peer_tx: mpsc::Sender<T>, event: T)
where
    T: Send + 'static + std::fmt::Debug,
{
    let task = async move {
        trace!(
            "Sending termination for the peer [{:?}]",
            peer_metadata.clone()
        );

        if let Err(peer_tx_err) = peer_tx.send(event).await {
            error!(
                "Failed to send stop order to peer [{}], cause [{:?}]",
                peer_metadata.clone(),
                peer_tx_err
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
