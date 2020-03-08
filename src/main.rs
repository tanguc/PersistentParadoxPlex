extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate uuid;

pub mod persistent_marking_lb;
use persistent_marking_lb::PersistentMarkingLB;

use futures::executor;
use futures::prelude::*;
use futures::stream::{SplitSink, SplitStream};
use futures::task::Context;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task;
use tokio_util::codec::{Framed, LinesCodec};
use uuid::Uuid;

async fn process_socket(peer: Peer) {
    debug!("Processing socket");

    loop {
        debug!("Processing loop");
    }
}

type PersistentMarkingLBRuntime = Arc<Mutex<PersistentMarkingLB>>;

struct UpstreamServerPool {}
struct RedisCache {}

#[derive(Clone, Debug)]
struct Peer {
    uuid: Uuid,
    inner_exchange_rx: watch::Receiver<InnerExchange>,
    tcp_stream: TcpStream,
}

enum PeerError {
    SocketAddrNotFound,
}

impl Peer {
    fn new(tcp_stream: TcpStream, channel_rx: watch::Receiver<InnerExchange>) -> Self {
        Peer {
            uuid: Uuid::new_v4(),
            inner_exchange_rx: channel_rx,
            tcp_stream,
        }
    }

    fn get_socket_addr(&self) -> Result<SocketAddr, PeerError> {
        match self.tcp_stream.local_addr() {
            Ok(socket_addr) => Ok(socket_addr),
            Err(err) => {
                error!("Not able to retrieve client socket address");
                Err(PeerError::SocketAddrNotFound)
            }
        }
    }
}

struct Shared {
    peers: HashMap<SocketAddr, Peer>,
    peers_by_uuid: HashMap<Uuid, Peer>,
}

#[derive(Clone, Debug)]
enum InnerExchange {
    START,
    PAUSE,
}

async fn handle_new_client(runtime: PersistentMarkingLBRuntime, client: TcpStream) {
    info!("New client connected");
    let peer = Peer::new(client, runtime.lock().unwrap().inner_rx.clone());
    shared.peers.insert(peer.get_socket_addr()?, peer.clone());
    shared
        .peers_by_nickname
        .insert(peer.uuid.clone(), peer.clone());
    task::spawn(process_socket(peer));
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    debug!("Starting listener!");

    let mut runtime: PersistentMarkingLBRuntime =
        Arc::new(Mutex::new(persistent_marking_lb::PersistentMarkingLB::new()));
    let mut listener_res = TcpListener::bind("127.0.0.1:7999");
    let lb_server = async move {
        match listener_res.await {
            Ok(mut listener_res) => loop {
                let mut incoming_streams = listener_res.incoming();

                while let Some(client_stream) = incoming_streams.next().await {
                    handle_new_client(runtime.clone(), client_stream?);
                }
            },
            Err(_) => {
                error!("Cannot TCP listen");
            }
        }
    };

    info!("Server running");

    //let login_server = future::select(Box::pin(login_server), future::ready(Ok::<_, ()>(())));

    lb_server.await;
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_add() {
        assert_eq!(add(1, 2), 3);
    }

    #[test]
    fn test_bad_add() {
        // This assert would fire and test will fail.
        // Please note, that private functions can be tested too!
        assert_eq!(bad_add(1, 2), 3);
    }
}
