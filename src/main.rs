extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate uuid;

pub mod peer;
pub mod persistent_marking_lb;
pub mod utils;

use crate::peer::{peer_halves, PeerHalveRuntime, SinkPeerHalve, StreamPeerHalve};
use futures::prelude::*;
use peer::Peer;
use persistent_marking_lb::{InnerExchange, PersistentMarkingLB, RuntimeOrder};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch::Receiver;
use tokio::task;
use tokio::time::{delay_for, Duration};
use tokio_util::codec::{Framed, LinesCodec};

type PersistentMarkingLBRuntime = Arc<Mutex<PersistentMarkingLB>>;

// async fn process_socket_io(mut peer: Peer, frame: Framed<TcpStream, LinesCodec>) {
//     debug!("Processing socket");

// let mut runtime_channel_rx = peer.runtime_rx.clone();
// let runtime_orders = move || async move {
//     let mut started = false;
//     loop {
//         match peer.runtime_rx.recv().await {
//             Some(runtime_order) => match runtime_order {
//                 RuntimeOrder::NO_ORDER => {
//                     started = true;
//                     info!("NO order from runtime");
//                 }
//                 RuntimeOrder::PAUSE_PEER => {
//                     info!("Got order to pause peer");
//                 }
//                 RuntimeOrder::SHUTDOWN_PEER => {
//                     info!("Got order to shutdown peer");
//                 }
//             },
//             None => {
//                 error!("The sender half (runtime) has dropped, it should not.");
//                 break;
//             }
//         };
//     }
// };

// task::spawn(runtime_orders());

// let mut for_dummy_write_task_sink_channel_tx = peer.sink_channel_tx.clone();
// task::spawn(dummy_task_for_writing(for_dummy_write_task_sink_channel_tx));
// }

// async fn dummy_task_for_writing(mut peer_writing_task_channel_tx: Sender<InnerExchange<String>>) {
//     info!("Starting the dummy data exchanger (to the sink peer)");
//
//     let mut i = 0;
//     loop {
//         delay_for(Duration::from_secs(2)).await;
//         let dummy_message = format!("DUMMY ANSWER {}", i);
//         peer_writing_task_channel_tx.send(InnerExchange::WRITE(dummy_message));
//         debug!("Sent message from dummy");
//         i = i + 1;
//     }
// }

async fn handle_new_client(runtime: PersistentMarkingLBRuntime, tcp_stream: TcpStream) {
    info!("New client connected");

    match tcp_stream.peer_addr() {
        Ok(peer_addr) => {
            debug!("Got client addr");
            // let mut peer = Peer::new(runtime.lock().unwrap().self_rx.clone());

            let (peer_sink, peer_stream) = peer_halves(
                tcp_stream,
                peer_addr,
                runtime.lock().unwrap().self_rx.clone(),
            );

            runtime
                .lock()
                .unwrap()
                .add_peer_halves(&peer_sink, &peer_stream);

            peer_sink.start();
            peer_stream.start();
            // peer_stream
            //     .runtime
            //     .lock()
            //     .unwrap()
            //     .add_peer(&peer, peer_addr.clone());

            // process_socket_io(peer, frame).await;
            // runtime.lock().unwrap().add_peer_test(peer);
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
                while let Some(client_stream) =
                    tokio::stream::StreamExt::next(&mut incoming_streams).await
                {
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
