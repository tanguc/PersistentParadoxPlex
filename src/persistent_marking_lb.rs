use crate::peer::{Peer, SinkPeerHalve, StreamPeerHalve};
use futures::{Stream, TryStream};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
use tokio::sync::mpsc;
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum InnerExchange<T> {
    START,
    PAUSE,
    WRITE(T),
}

#[derive(Clone, Debug)]
pub enum RuntimeOrder {
    NO_ORDER,
    SHUTDOWN_PEER,
    PAUSE_PEER,
}

type PeerSenderChannel = mpsc::Sender<InnerExchange<String>>;
type PeerReceiverChannel = mpsc::Sender<InnerExchange<String>>;

#[derive(Debug)]
pub struct PersistentMarkingLB {
    // pub back_end_sink_channel: HashMap<Uuid, mpsc::Sender<InnerExchange<String>>>,
    // pub back_end_addr: HashMap<SocketAddr, Uuid>,
    pub self_rx: watch::Receiver<RuntimeOrder>,
    pub self_tx: watch::Sender<RuntimeOrder>,

    // Used only for runtime orders
    pub front_peers_stream_channels: HashMap<Uuid, PeerSenderChannel>,
    pub back_peers_stream_channels: HashMap<Uuid, PeerSenderChannel>,

    // Used to send data to write
    pub front_peers_sink_channels: HashMap<Uuid, PeerSenderChannel>,
    pub back_peers_sink_channels: HashMap<Uuid, PeerSenderChannel>,

    pub peers_socket_addr_uuids: HashMap<SocketAddr, Uuid>,
    // dummy_peers: Vec<Peer>,
}

pub enum PersistentMarkingLBError {
    CannotHandleClient,
}

impl PersistentMarkingLB {
    pub fn new() -> Self {
        let (self_tx, self_rx) = watch::channel(RuntimeOrder::NO_ORDER);
        PersistentMarkingLB {
            front_peers_stream_channels: HashMap::new(),
            front_peers_sink_channels: HashMap::new(),
            back_peers_stream_channels: HashMap::new(),
            back_peers_sink_channels: HashMap::new(),
            self_rx,
            self_tx,
            peers_socket_addr_uuids: HashMap::new(),
        }
    }

    pub fn add_peer_halves(
        &mut self,
        peer_sink_halve: &SinkPeerHalve,
        peer_stream_halve: &StreamPeerHalve,
    ) -> Result<(), PersistentMarkingLBError> {
        self.front_peers_stream_channels.insert(
            peer_stream_halve.halve.metadata.uuid,
            peer_stream_halve.halve.tx.clone(),
        );
        self.front_peers_sink_channels.insert(
            peer_sink_halve.halve.metadata.uuid,
            peer_sink_halve.halve.tx.clone(),
        );
        self.peers_socket_addr_uuids.insert(
            peer_sink_halve.halve.metadata.socket_addr,
            peer_sink_halve.halve.metadata.uuid,
        );
        Ok(())
    }

    // pub fn add_peer(
    //     &mut self,
    //     peer: &Peer,
    //     peer_addr: SocketAddr,
    // ) -> Result<(), PersistentMarkingLBError> {
    //     self.front_end_sink_channel
    //         .insert(peer.uuid.clone(), peer.sink_channel_tx.clone());
    //     self.front_end_addr.insert(peer_addr, peer.uuid.clone());
    //     Ok(())
    // }
    //
    // pub fn add_peer_test(&mut self, peer: Peer) {
    //     self.dummy_peers.push(peer);
    // }
}
