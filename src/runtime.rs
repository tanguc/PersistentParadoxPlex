use crate::downstream::{DownstreamPeerSinkChannelTx, DownstreamPeerStreamChannelTx};
use crate::{
    upstream::{UpstreamPeerSinkChannelTx, UpstreamPeerStreamChannelTx},
    upstream_proto::Header,
};

use std::fmt::{Debug, Display, Error, Formatter};
use std::{collections::HashMap, net::SocketAddr};

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
pub enum RuntimeEvent {
    NoOrder,
    ShutdownPeer,
    PausePeer,
    DownstreamPeerTerminatedConnection(PeerMetadata),
    UpstreamPeerTerminatedConnection(PeerMetadata),
    // GetUpstreamPeer(tokio::sync::oneshot::Sender<Option<PeerEventTxChannel>>),
    MessageFromDownstreamPeer(String, String), // .1 uuid of the incoming downstream client
    MessageFromUpstreamPeer(String, String),   // .1 uuid of the target downstream client
}

#[derive(Debug)]
pub struct RuntimePeersPool {
    // Used only for runtime orders
    pub downstream_peers_stream_tx: HashMap<Uuid, DownstreamPeerStreamChannelTx>,
    pub upstream_peers_stream_tx: HashMap<Uuid, UpstreamPeerStreamChannelTx>,

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
                            /// Only for STREAM runtime of upstream peers
                            RuntimeEvent::UpstreamPeerTerminatedConnection(peer_metadata) => {
                                debug!(
                                    "TERMINATION ORDER - UPSTREAM PEER [{:?}]",
                                    peer_metadata.uuid.clone()
                                );
                                if let Ok(upstream_peer) =
                                    self.remove_upstream_peer(peer_metadata.clone()).await
                                {
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
                                let upstreams = self.peers_pool.lock().await;
                                let mut upstream_tx = Option::None;
                                if !upstreams.upstream_peers_sink_tx.is_empty() {
                                    // TODO this one shouldnt act like that but find the best peer (by round robin)
                                    for upstream_tx_channel in
                                        upstreams.upstream_peers_sink_tx.iter()
                                    {
                                        upstream_tx = Option::Some((
                                            upstream_tx_channel.0.clone(),
                                            upstream_tx_channel.1.clone(),
                                        ));
                                    }
                                }

                                match upstream_tx {
                                    Some((_, mut tx)) => {
                                        let _ = Header {
                                            address: "127.0.0.1".into(), // TODO fill with the current host address
                                            client_uuid: uuid.clone().to_string(), //
                                            time: "03:53:39".into(),     // TODO current time
                                        };
                                        let event =
                                            PeerEvent::Write((payload, uuid.clone().to_string()));

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
                            RuntimeEvent::MessageFromUpstreamPeer(payload, uuid) => {
                                debug!("ORDER - MESSAGE FROM UPSTREAM PEER ORDER");
                                debug!(
                                    "Trying to find the downstream peer with UUID [{:?}]",
                                    uuid.clone()
                                );
                                if let Some(mut sink_tx) =
                                    self.get_downstream_sink_tx(uuid.clone()).await
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
            debug!("Dropping the runtime task");
        };

        tokio::task::spawn(runtime_task);
    }

    async fn get_downstream_sink_tx(
        &mut self,
        uuid: String,
    ) -> Option<DownstreamPeerSinkChannelTx> {
        if let Ok(parsed_uuid) = Uuid::parse_str(uuid.as_str()) {
            let peers_pool = &*self.peers_pool.lock().await;
            // let mut downstream_peer = Result::Err();

            for downstream_peers in peers_pool.downstream_peers_sink_tx.iter() {
                trace!("Downstream peer [{:?}]", downstream_peers.0)
            }

            if !peers_pool.downstream_peers_sink_tx.is_empty()
                && peers_pool
                    .downstream_peers_sink_tx
                    .contains_key(&parsed_uuid.clone())
            {
                return peers_pool
                    .downstream_peers_sink_tx
                    .get(&parsed_uuid)
                    .cloned();
            } else {
                error!(
                    "No downstream peer has been found for the UUID [{:?}]",
                    uuid.clone()
                );
                return None;
            }
        } else {
            error!("Failed to parse the following UUID = [{:?}]", uuid);
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
