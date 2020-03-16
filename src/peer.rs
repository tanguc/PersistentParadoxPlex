use crate::persistent_marking_lb::InnerExchange;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_util::codec::{Framed, LinesCodec};
use uuid::Uuid;

pub trait FrontEndPeer {}
pub trait BackEndPeer {}

#[derive(Debug)]
pub struct Peer {
    pub uuid: Uuid,
    pub runtime_rx: watch::Receiver<InnerExchange>,
    pub frame: Framed<TcpStream, LinesCodec>,

    pub stream_channel_rx: mpsc::Receiver<InnerExchange>,
    pub stream_channel_tx: mpsc::Sender<InnerExchange>,

    pub sink_channel_rx: mpsc::Receiver<InnerExchange>,
    pub sink_channel_tx: mpsc::Sender<InnerExchange>,
}

pub enum PeerError {
    SocketAddrNotFound,
}

impl Peer {
    pub fn new(tcp_stream: TcpStream, runtime_rx: watch::Receiver<InnerExchange>) -> Self {
        let (stream_channel_tx, stream_channel_rx) = mpsc::channel::<InnerExchange>(1000);
        let (sink_channel_tx, sink_channel_rx) = mpsc::channel::<InnerExchange>(1000);

        Peer {
            uuid: Uuid::new_v4(),
            runtime_rx,
            stream_channel_rx,
            stream_channel_tx,
            sink_channel_rx,
            sink_channel_tx,
            frame: Framed::new(tcp_stream, LinesCodec::new()),
        }
    }
}
