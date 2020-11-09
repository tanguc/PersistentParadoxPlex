#![feature(linked_list_cursors)]
#![feature(trace_macros)]
#![feature(decl_macro)]

trace_macros!(false);

extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;
extern crate anyhow;
extern crate enclose;
extern crate uuid;

pub mod admin_management_server;
pub mod conf;
pub mod downstream;
pub mod runtime;
pub mod upstream;
pub mod upstream_proto;
// pub mod upstream_proto;
pub mod utils;
use crate::runtime::PeerEvent;
use admin_management_server::start_http_management_server;
use anyhow::{anyhow, Result};
use conf::load_config;
use downstream::{DownstreamPeer, DownstreamPeerEventTx, PeerRuntime};
use futures::StreamExt;
use runtime::{PeerMetadata, Runtime, RuntimePeersPool};
use std::{net::SocketAddr, sync::Arc};
use tokio::task::JoinHandle;
use tokio::time::{delay_for, Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use upstream::register_upstream_peers;

/// should be in the downstream scope
async fn dummy_task_for_writing_as_downstream_clients(
    mut peer_sink_tx_channel: DownstreamPeerEventTx,
) {
    info!("Starting a dummy downstream client (sink)");

    let mut i = 0;
    loop {
        delay_for(Duration::from_secs(2)).await;
        let dummy_message = format!("DUMMY ANSWER {}", i);

        let res = peer_sink_tx_channel
            .send(PeerEvent::Write((dummy_message.into(), ())))
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
                        // this has to be done via sending an order via channel and not accessing it like this.......
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

fn init_logging() -> anyhow::Result<()> {
    let debug_level;
    if let Some(env_var_level) = std::env::var_os("RUST_LOG") {
        debug_level = env_var_level;
    } else {
        debug_level = "INFO".into();
    }

    if let Ok(debug_level) = debug_level.into_string() {
        // let env_filter = Ok::<_, ()>(tracing_subscriber::EnvFilter::from_default_env());
        let env_filter = tracing_subscriber::EnvFilter::try_new(format!(
            "persistent_marking_lb={},tonic=trace,tokio=trace",
            debug_level.clone()
        ));
        match env_filter {
            Ok(filter) => {
                tracing_subscriber::fmt()
                    .with_env_filter(filter)
                    .try_init()
                    .map_err(|err| {
                        eprintln!("Failed to init with env filter, aborting: {:?}", err);
                        anyhow!(err)
                    })?;
                Ok(())
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                eprintln!("Failed to parse filters for tracing/logging");
                eprintln!("persistent_marking_lb={}", debug_level.clone());
                Ok(())
            }
        }
    } else {
        let err = String::from("Can't convert OS string");
        eprintln!("Error: {}", err.clone());
        Err(anyhow!(err))
    }
}

#[tokio::main]
async fn main() {
    init_logging().unwrap();

    let mut runtime = Runtime::new();
    let conf = load_config().unwrap();

    register_upstream_peers(runtime.tx.clone(), conf.upstreams.to_owned());
    start_http_management_server(
        runtime.tx.clone(),
        &conf.management_server,
        conf.management_server_tls.as_ref(),
    );

    let addr = format!("{}:{}", conf.server.host, conf.server.port);
    debug!("Listening on [{:?}]", &addr);

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
                error!("Failed to bind the TCP connection");
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
