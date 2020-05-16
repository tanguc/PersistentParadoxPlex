#![feature(trace_macros)]
extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate uuid;

use std::net::Ipv4Addr;

pub mod upstream_peer;
pub mod peer;
pub mod persistent_marking_lb;
pub mod utils;
pub mod backend;

use crate::peer::{create_peer_halves, PeerHalveRuntime, create_upstream_halves};

use crate::persistent_marking_lb::InnerExchange;
use futures::StreamExt;
use persistent_marking_lb::PersistentMarkingLB;

use tokio::net::{TcpListener, TcpStream};

use tokio::time::{delay_for, Duration};
use crate::upstream_peer::backend_peer::InputStreamRequest;

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

async fn handle_new_client(runtime: &mut PersistentMarkingLB, tcp_stream: TcpStream) {
    info!("New client connected");

    match tcp_stream.peer_addr() {
        Ok(peer_addr) => {
            debug!("Got client addr");
            let (peer_sink, peer_stream) =
                create_peer_halves(tcp_stream, peer_addr, runtime.tx.clone());

            runtime.add_peer_halves(&peer_sink, &peer_stream).await;

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

pub struct UpstreamPeerMetadata {
    pub ip: Ipv4Addr,
    pub port: u16,
}

async fn handle_grcp() -> Result<(), Box<dyn std::error::Error>> {
    debug!("Handle GRPC");
    let mut connectClient =
        backend::backend_peer_service_client::BackendPeerServiceClient::connect(
            "tcp://:4770"
        ).await?;
    debug!("Handle GRPC1");

    let (mut message_tx, message_rx) =
        tokio::sync::mpsc::unbounded_channel::<backend::InputStreamRequest>();

    debug!("Handle GRPC2");

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let mut body = backend::InputStreamRequest {
                header: Option::Some(backend::Header {
                    address: "823.12938I.3291833.".to_string(),
                    time: "12:32:12".to_string()
                }),
                payload: "Task spawn - client send fake data".to_string()
            };
            if let Err(err) = message_tx.send(body) {
                debug!("Cannot send message from client (spawn task): [{:?}]", err);
            } else {
                debug!("Message sent");
            }
            debug!("Tick - new message to client, expecting a message from server task");
        }
    });
    debug!("Handle GRPC5");

    let call_method = connectClient.bidirectional_streaming(
        tonic::Request::new(message_rx)
    ).await?;

    debug!("Handle GRPC3");

    if let mut methodStream = call_method.into_inner() {
        debug!("Been able to call the method");
        tokio::spawn(async move {



            if let Some(message) = methodStream.message().await? {
                debug!("Got a new message : [{:?}]", message);
            }

            Ok::<(), tonic::Status>(())
        });
    } else {
        debug!("Cannot call method: because");
    }
    debug!("Handle GRPC4");

    Ok(())
    // Ok::<(), tonic::transport::Error>(())
}


fn get_upstream_peers() -> Vec<UpstreamPeerMetadata> {
    debug!("Creating backend peers [DEBUGGING PURPOSES]");

    let mut back_peers = vec![];

    back_peers.push(UpstreamPeerMetadata {
        ip: "localhost".parse().unwrap(),
        port: 4770,
    });

    back_peers
}

pub fn register_upstream_peers(mut runtime: PersistentMarkingLB) {
    debug!("Registering upstream peers");
    // TODO only for debugging purposes
    let upstream_peer_metadata = get_upstream_peers();

    for upstream_peer_metadata in upstream_peer_metadata {
        let upstream_peer = create_upstream_halves(
            upstream_peer_metadata,
            runtime.tx.clone()
        );
        upstream_peer.start();
        // runtime.add_upstream_peer(upstream_peer);
    }
    // tokio::spawn(async {
    //     debug!("fuck it");
    //     if let Err(err) = handle_grcp().await {
    //         error!("Error from GRPC : [{:?}]", err);
    //     }
    // });

}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    // upstream_peer::compile_protos();

    debug!("Starting listener!");
    let mut runtime = PersistentMarkingLB::new();

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

    register_upstream_peers(runtime.clone());

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
