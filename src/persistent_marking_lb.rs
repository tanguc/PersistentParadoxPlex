use crate::{InnerExchange, Shared};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub struct PersistentMarkingLB {
    pub shared: Shared,
    pub inner_rx: watch::Receiver<InnerExchange>,
    pub inner_tx: watch::Sender<InnerExchange>,
}

impl PersistentMarkingLB {
    pub fn new() -> Self {
        let mut shared: Shared = Shared {
            peers: HashMap::new(),
            peers_by_uuid: HashMap::new(),
        };
        let (inner_tx, inner_rx) = watch::channel(InnerExchange::START);

        PersistentMarkingLB {
            shared,
            inner_rx,
            inner_tx,
        }
    }
}
