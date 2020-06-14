#![feature(trace_macros)]
extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate uuid;

pub mod upstream_peer;
pub mod peer;
pub mod runtime;
pub mod utils;
pub mod backend;
use crate::peer::{create_peer_halves, PeerHalveRuntime, create_upstream_halves, UpstreamPeerHalve, PeerError};
use crate::runtime::PeerEvent;
use std::net::{SocketAddr};
use futures::StreamExt;
use runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{delay_for, Duration};
use tokio::task::JoinHandle;

async fn dummy_task_for_writing(
    mut peer_sink_tx_channel: tokio::sync::mpsc::Sender<PeerEvent<String>>,
) {
    info!("Starting the dummy data exchanger (to the sink peer)");

    let mut i = 0;
    loop {
        delay_for(Duration::from_secs(2)).await;
        let dummy_message = format!("DUMMY ANSWER {}", i);
        let res = peer_sink_tx_channel
            .send(PeerEvent::Write(dummy_message))
            .await;
        debug!("Sent message from dummy, res : {:?}", res);
        i = i + 1;
    }
}

async fn handle_new_client(runtime: &mut Runtime, tcp_stream: TcpStream) {
    info!("New client connected");

    match tcp_stream.peer_addr() {
        Ok(peer_addr) => {
            debug!("Got client addr");
            let (peer_sink, peer_stream) =
                create_peer_halves(tcp_stream, peer_addr, runtime.tx.clone());

            runtime.add_downstream_peer_halves(&peer_sink, &peer_stream).await;

            //debug purposes
            tokio::spawn(dummy_task_for_writing(peer_sink.halve.tx.clone()));

            let peer_sink_handle = peer_sink.start();
            let peer_stream_handle = peer_stream.start();
        }
        Err(_err) => {
            error!("Not able to retrieve client socket address");
        }
    }
}

pub struct UpstreamPeerMetadata {
    pub host: SocketAddr
}

pub enum UpstreamPeerMetadataError {
    HostInvalid
}

fn get_upstream_peers() -> Vec<UpstreamPeerMetadata> {
    debug!("Creating backend peers [DEBUGGING PURPOSES]");

    let mut back_peers = vec![];

    back_peers.push(UpstreamPeerMetadata {
        host: "127.0.0.1:4770".parse().map_err(|err| {
            error!("Could not parse addr: [{:?}]", err);
        }).unwrap(),
    });

    back_peers
}

pub async fn register_upstream_peers(mut runtime: Runtime) {
    debug!("Registering upstream peers");
    // TODO only for debugging purposes
    let upstream_peer_metadata = get_upstream_peers();

    for upstream_peer_metadata in upstream_peer_metadata {
        let upstream_peer: UpstreamPeerHalve<backend::InputStreamRequest> =
            create_upstream_halves(
                upstream_peer_metadata, runtime.tx.clone()
            );
        let upstream_stream_tx =
            upstream_peer.grpc_tx_channel.clone();

        runtime.add_upstream_peer_halves(&upstream_peer).await;
        upstream_peer.start();
        {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(20));
                loop {
                    interval.tick().await;
                    debug!("Send a debug client request");
                    let body = backend::InputStreamRequest {
                        header: Option::Some(backend::Header {
                            address: "823.12938I.3291833.".to_string(),
                            time: "12:32:12".to_string()
                        }),
                        payload: "Task spawn - client send fake data".to_string()
                    };
                    if let Err(err) = upstream_stream_tx.send(body) {
                        error!("Cannot send message from client (spawn task): [{:?}]", err);
                        error!("Looks like the halve channel has closed or dropped, aborting the task");
                        return;
                    } else {
                        debug!("Message sent");
                    }
                    debug!("Tick - new message to client, expecting a message from server task");
                }
            });
        }
    }
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    // upstream_peer::compile_protos();

    debug!("Starting listener!");
    let mut runtime = Runtime::new();
    register_upstream_peers(runtime.clone()).await;

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
                            handle_new_client(&mut runtime, client_stream).await;
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
