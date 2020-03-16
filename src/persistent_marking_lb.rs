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
    pub front_end_sink_channel: HashMap<Uuid, mpsc::Sender<InnerExchange>>,
    pub front_end_addr: HashMap<SocketAddr, Uuid>,

    pub back_end_sink_channel: HashMap<Uuid, mpsc::Sender<InnerExchange>>,
    pub back_end_addr: HashMap<SocketAddr, Uuid>,

    pub self_rx: watch::Receiver<InnerExchange>,
    pub self_tx: watch::Sender<InnerExchange>,
}

pub enum PersistentMarkingLBError {
    CannotHandleClient,
}

impl PersistentMarkingLB {
    pub fn new() -> Self {
        let (self_tx, self_rx) = watch::channel(InnerExchange::START);
        PersistentMarkingLB {
            front_end_sink_channel: HashMap::new(),
            front_end_addr: HashMap::new(),
            back_end_sink_channel: HashMap::new(),
            back_end_addr: HashMap::new(),
            self_rx,
            self_tx,
        }
    }

    pub fn add_peer(
        &mut self,
        peer: &Peer,
        peer_addr: SocketAddr,
    ) -> Result<(), PersistentMarkingLBError> {
        self.front_end_sink_channel
            .insert(peer.uuid.clone(), peer.sink_channel_tx.clone());
        self.front_end_addr.insert(peer_addr, peer.uuid.clone());
        Ok(())
    }
}
