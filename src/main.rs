#![feature(trace_macros)]
trace_macros!(false);

extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate enclose;
extern crate uuid;

pub mod downstream;
pub mod runtime;
pub mod upstream;
pub mod upstream_proto;
pub mod utils;
use crate::runtime::PeerEvent;
use downstream::{DownstreamPeer, PeerEventTxChannel, PeerRuntime};
use futures::StreamExt;
use runtime::Runtime;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{delay_for, Duration};
use upstream::register_upstream_peers;

/// should be in the downstream scope
async fn dummy_task_for_writing_as_downstream_clients(
    mut peer_sink_tx_channel: PeerEventTxChannel,
) {
    info!("Starting the dummy data exchanger (to the sink peer)");

    let mut i = 0;
    loop {
        delay_for(Duration::from_secs(2)).await;
        let dummy_message = format!("DUMMY ANSWER {}", i);
        let res = peer_sink_tx_channel
            .send(PeerEvent::Write((
                dummy_message,
                String::from("73928-329842-293842-23984823"),
            )))
            .await;
        debug!("Sent message from dummy, res : {:?}", res);
        i = i + 1;
    }
}

/// should be in the runtime
async fn handle_new_downstream_client(runtime: &mut Runtime, tcp_stream: TcpStream) {
    info!("New downstream client connected");

    match tcp_stream.peer_addr() {
        Ok(peer_addr) => {
            debug!("Got client addr");
            let downstream_peer = DownstreamPeer::new(tcp_stream, peer_addr, runtime.tx.clone());
            match <DownstreamPeer as PeerRuntime>::start(downstream_peer).await {
                Ok(started) => {
                    let sink_tx = started.sink_tx.clone();
                    runtime
                        .add_downstream_peer_halves(
                            started.metadata.clone(),
                            started.sink_tx,
                            started.stream_tx,
                        )
                        .await;

                    let mut simulate_downstream_clients_tasks = false;
                    if let Ok(_) = std::env::var("SIMULATE_DOWNSTREAM_CLIENTS") {
                        debug!("Activating dummy downstream client");
                        //debug purposes
                        tokio::spawn(dummy_task_for_writing_as_downstream_clients(sink_tx));
                        simulate_downstream_clients_tasks = true;
                    }
                    debug!(
                        "Simulation of downstream client is [{}]",
                        if simulate_downstream_clients_tasks {
                            "ON"
                        } else {
                            "OFF"
                        }
                    );
                }
                Err(_) => {
                    error!("Failed to start downstream peer runtime");
                }
            }
        }
        Err(_err) => {
            error!("Not able to retrieve client socket address");
        }
    }
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    // Uncomment only to compile protos
    // upstream_proto::compile_protos();

    let mut runtime = Runtime::new();
    register_upstream_peers(runtime.clone());

    let port = 7999;
    let uri = "127.0.0.1";
    let addr = format!("{}:{}", uri, port);
    debug!("[URI] = {} && [PORT] = {}", &uri, port);

    let listener_res = TcpListener::bind(addr.clone());
    let lb_server = async move {
        match listener_res.await {
            Ok(mut listener_res) => loop {
                info!("Listening on {}", addr);
                let mut incoming_streams = listener_res.incoming();
                while let Some(client_stream) = incoming_streams.next().await {
                    match client_stream {
                        Ok(client_stream) => {
                            handle_new_downstream_client(&mut runtime, client_stream).await;
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
