extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate uuid;

pub mod peer;
pub mod persistent_marking_lb;
pub mod utils;

use crate::peer::{peer_halves, PeerHalveRuntime};

use crate::persistent_marking_lb::InnerExchange;
use futures::StreamExt;
use persistent_marking_lb::{PersistentMarkingLB, PersistentMarkingLBRuntime};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{delay_for, Duration};

async fn dummy_task_for_writing(
    mut peer_sink_tx_channel: tokio::sync::mpsc::Sender<InnerExchange<String>>,
) {
    info!("Starting the dummy data exchanger (to the sink peer)");

    let mut i = 0;
    loop {
        delay_for(Duration::from_secs(2)).await;
        let dummy_message = format!("DUMMY ANSWER {}", i);
        let res = peer_sink_tx_channel
            .send(InnerExchange::Write(dummy_message))
            .await;
        debug!("Sent message from dummy, res : {:?}", res);
        i = i + 1;
    }
}

async fn handle_new_client(runtime: PersistentMarkingLBRuntime, tcp_stream: TcpStream) {
    info!("New client connected");

    match tcp_stream.peer_addr() {
        Ok(peer_addr) => {
            debug!("Got client addr");
            // let mut peer = Peer::new(runtime.lock().unwrap().self_rx.clone());

            let (peer_sink, peer_stream) =
                peer_halves(tcp_stream, peer_addr, runtime.lock().await.tx.clone());

            PersistentMarkingLB::add_peer_halves(runtime, &peer_sink, &peer_stream);

            //debug purposes
            tokio::spawn(dummy_task_for_writing(peer_sink.halve.tx.clone()));

            peer_sink.start();
            peer_stream.start();
        }
        Err(_err) => {
            error!("Not able to retrieve client socket address");
        }
    }
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    debug!("Starting listener!");

    let runtime = PersistentMarkingLB::new();

    let addr = "127.0.0.1:7999";

    let listener_res = TcpListener::bind(addr);
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
