use crate::downstream::{
    DownstreamPeerSinkHalve, DownstreamPeerStreamHalve, PeerEventRxChannel, PeerEventTxChannel,
    PeerHalve, PeerMetadata,
};
use crate::upstream_proto::{Header, InputStreamRequest, OutputStreamRequest};

use std::collections::HashMap;
use std::net::SocketAddr;

use std::fmt::Debug;
use std::sync::Arc;

use tokio::sync::{mpsc, mpsc::error::TrySendError, oneshot, watch, Mutex};
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
    MessageFromDownstreamPeer(String, Header),
    MessageFromUpstreamPeer(String, String), //1st message && 2nd for uuid
}

#[derive(Debug)]
pub struct RuntimePeersPool {
    // Used only for runtime orders
    pub downstream_peers_stream_tx: HashMap<Uuid, PeerEventTxChannel>,
    pub upstream_peers_stream_tx: HashMap<Uuid, PeerEventTxChannel>,

    // Used to send data to write
    pub downstream_peers_sink_tx: HashMap<Uuid, PeerEventTxChannel>,
    pub upstream_peers_sink_tx: HashMap<Uuid, PeerEventTxChannel>,

    pub peers_addr_uuids: HashMap<SocketAddr, Uuid>,
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
                                        send_termination_peer(peer_metadata, peer_sink_tx);
                                    }
                                }
                                unimplemented!();
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
                                    if let Some(peer_sink_tx) = downstream_peer.0 {
                                        send_termination_peer(peer_metadata, peer_sink_tx);
                                    }
                                }
                            }
                            RuntimeEvent::MessageFromUpstreamPeer(payload, uuid) => {
                                debug!("MESSAGE TO UPSTREAM PEER ORDER");
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
                                    Some((uuid, mut tx)) => {
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
                            RuntimeEvent::MessageFromDownstreamPeer(payload, header) => {
                                debug!("MESSAGE TO DOWNSTREAM PEER ORDER");
                                debug!(
                                    "Trying to find the downstream peer with UUID [{:?}]",
                                    header
                                );

                                let peers_pool = &*self.peers_pool.lock().await;
                                let mut downstream_peer = Option::None;
                                // TODO should be choosen by uuid and not randomly
                                if !peers_pool.downstream_peers_sink_tx.is_empty() {
                                    for downstream_peer_sink_tx in
                                        peers_pool.downstream_peers_sink_tx.iter()
                                    {
                                        downstream_peer =
                                            Option::Some(downstream_peer_sink_tx.1.clone());
                                    }
                                } else {
                                    error!(
                                        "No downstream peer has been found for the UUID [TODO PUT UUID of the client here] "
                                    );
                                }

                                if let Some(mut downstream_peer) = downstream_peer {
                                    debug!(
                                        "Sending the Writing order to the downstream peer [TODO PUT UUID of the client here]"
                                    );
                                    let downstream_peer_event =
                                        PeerEvent::Write((payload, header.client_uuid));

                                    if let Err(err) =
                                        downstream_peer.send(downstream_peer_event).await
                                    {
                                        error!("Failed to send the Writing order to the downstream peer tx channel with UUID [{:?}]", err);
                                    }
                                }
                            }
                            RuntimeEvent::MessageFromUpstreamPeer(payload, uuid) => {
                                debug!("[MessageToUpstreamPeer] Payload length [{}] && Header target client UUID [{}]", payload.len(), uuid.clone());
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

    async fn remove_upstream_peer(
        &mut self,
        peer_metadata: PeerMetadata,
    ) -> RuntimeResult<(Option<PeerEventTxChannel>, Option<PeerEventTxChannel>)> {
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
    ) -> RuntimeResult<(Option<PeerEventTxChannel>, Option<PeerEventTxChannel>)> {
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
        sink_tx: PeerEventTxChannel,
        stream_tx: PeerEventTxChannel,
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
        sink_tx: PeerEventTxChannel,
        stream_tx: PeerEventTxChannel,
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
async fn send_termination_peer(peer_metadata: PeerMetadata, mut peer_tx: PeerEventTxChannel) {
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
