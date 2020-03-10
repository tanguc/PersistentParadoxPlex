extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate uuid;

pub mod peer;
pub mod persistent_marking_lb;
pub mod utils;

use crate::persistent_marking_lb::PersistentMarkingLBError;
use futures::executor;
use futures::prelude::*;
use futures::stream::{SplitSink, SplitStream};
use futures::task::Context;
use peer::Peer;
use peer::PeerError;
use persistent_marking_lb::PersistentMarkingLB;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task;
use tokio_util::codec::{Framed, LinesCodec};
use uuid::Uuid;

type PersistentMarkingLBRuntime = Arc<Mutex<PersistentMarkingLB>>;

async fn process_socket(peer: Peer) {
    debug!("Processing socket");

    loop {
        debug!("Processing loop");
    }
}

async fn handle_new_client(runtime: PersistentMarkingLBRuntime, client: TcpStream) {
    info!("New client connected");

    match client.peer_addr() {
        Ok(peer_addr) => {
            let peer = Peer::new(client, runtime.lock().unwrap().self_rx.clone());

            runtime.lock().unwrap().add_peer(&peer, peer_addr.clone());

            task::spawn(process_socket(peer));
        }
        Err(err) => {
            error!("Not able to retrieve client socket address");
        }
    }
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
                    match client_stream {
                        Ok(client_stream) => {
                            handle_new_client(runtime.clone(), client_stream);
                        }
                        Err(err) => {
                            error!("Cannot process the incoming client stream");
                        }
                    }
                }
            },
            Err(_) => {
                error!("Cannot TCP listen");
            }
        }
    };

    info!("Server running");
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
