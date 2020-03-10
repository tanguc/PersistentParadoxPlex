use crate::peer::Peer;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
use tokio::sync::mpsc;
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum InnerExchange {
    START,
    PAUSE,
}

#[derive(Debug)]
pub struct PersistentMarkingLB {
    pub peers_channels: HashMap<Uuid, mpsc::Sender<InnerExchange>>,
    pub peers_addr: HashMap<SocketAddr, Uuid>,
    pub self_rx: watch::Receiver<InnerExchange>,
    pub self_tx: watch::Sender<InnerExchange>,
}

pub enum PersistentMarkingLBError {
    CANNOT_HANDLE_CLIENT,
}

impl PersistentMarkingLB {
    pub fn new() -> Self {
        let (self_tx, self_rx) = watch::channel(InnerExchange::START);
        PersistentMarkingLB {
            peers_channels: HashMap::new(),
            peers_addr: HashMap::new(),
            self_rx,
            self_tx,
        }
    }

    pub fn add_peer(
        &mut self,
        peer: &Peer,
        peer_addr: SocketAddr,
    ) -> Result<(), PersistentMarkingLBError> {
        self.peers_channels.insert(peer.uuid, peer.self_tx.clone());
        self.peers_addr.insert(peer_addr, peer.uuid.clone());

        Ok(())
    }
}
