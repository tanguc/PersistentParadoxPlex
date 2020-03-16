extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate uuid;

pub mod peer;
pub mod persistent_marking_lb;
pub mod utils;

use futures::prelude::*;
use peer::Peer;
use persistent_marking_lb::{InnerExchange, PersistentMarkingLB};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

type PersistentMarkingLBRuntime = Arc<Mutex<PersistentMarkingLB>>;

async fn process_socket_io(peer: Peer) {
    debug!("Processing socket");

    let (mut tcp_sink, mut tcp_stream) = peer.frame.split::<String>();

    let peer_uuid = peer.uuid.clone();
    let read_task = move || async move {
        info!("Spawning read task for the client {}", peer_uuid);
        loop {
            let line = tcp_stream.next().await;

            match line {
                Some(line) => {
                    debug!("Got a new line : {:?}", line);
                }
                None => {
                    debug!("Peer terminated connection");
                    break;
                }
            }
        }
    };

    let peer_uuid = peer.uuid.clone();
    let mut write_task_runtime_rx = peer.runtime_rx.clone();
    let write_task = move || async move {
        info!("Spawning writing task for the client {}", peer_uuid);

        loop {
            match write_task_runtime_rx.recv().await {
                Some(runtime_order) => match runtime_order {
                    InnerExchange::START => {
                        info!("Starting the writing");
                    }
                    InnerExchange::PAUSE => {
                        info!("Pause the writing");
                    }
                },
                None => {
                    error!("The sender half (runtime) has dropped, it should not.");
                    break;
                }
            }
        }

        // let test_send_all_data = vec!["Salut", "Je", "m'appel", "toto", "et", "c'est", "finis"];
        // let owned_str = test_send_all_data.into_iter().map(|str| String::from(str));
        // let mut stream_data = futures::stream::iter(test_send_all_data);

        // let mut toto = futures::stream::once(async { stream_data });
        // tcp_sink.send("Toto".to_string());
        // tcp_sink.send_all(&mut toto).await;
    };

    task::spawn(write_task());
    task::spawn(read_task());
}

async fn handle_new_client(runtime: PersistentMarkingLBRuntime, client: TcpStream) {
    info!("New client connected");

    match client.peer_addr() {
        Ok(peer_addr) => {
            debug!("Got client addr");
            let peer = Peer::new(client, runtime.lock().unwrap().self_rx.clone());

            runtime.lock().unwrap().add_peer(&peer, peer_addr.clone());
            process_socket_io(peer).await;
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

    let addr = "127.0.0.1:7999";

    let mut listener_res = TcpListener::bind(addr);
    let lb_server = async move {
        match listener_res.await {
            Ok(mut listener_res) => loop {
                info!("Listening on {}", addr);
                let mut incoming_streams = listener_res.incoming();
                while let Some(client_stream) = incoming_streams.next().await {
                    match client_stream {
                        Ok(client_stream) => {
                            handle_new_client(runtime.clone(), client_stream).await;
                        }
                        Err(_err) => {
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
    fn test_add() {}

    #[test]
    fn test_bad_add() {
        // This assert would fire and test will fail.
        // Please note, that private functions can be tested too!
    }
}
