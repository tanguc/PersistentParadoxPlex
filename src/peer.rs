use crate::persistent_marking_lb::InnerExchange;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Debug)]
pub struct Peer {
    pub uuid: Uuid,
    runtime_rx: watch::Receiver<InnerExchange>,
    pub self_rx: mpsc::Receiver<InnerExchange>,
    pub self_tx: mpsc::Sender<InnerExchange>,
    tcp_stream: TcpStream,
}

pub enum PeerError {
    SocketAddrNotFound,
}

impl Peer {
    pub fn new(tcp_stream: TcpStream, runtime_rx: watch::Receiver<InnerExchange>) -> Self {
        let (self_tx, self_rx) = mpsc::channel(1000);
        Peer {
            uuid: Uuid::new_v4(),
            runtime_rx,
            self_rx,
            self_tx,
            tcp_stream,
        }
    }
}
