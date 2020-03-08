extern crate pretty_env_logger;
extern crate tokio;
extern crate tokio_util;
#[macro_use]
extern crate log;

use futures::executor;
use futures::prelude::*;
use futures::stream::SplitStream;
use futures::task::Context;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;
use tokio_util::codec::{Framed, LinesCodec};

async fn process_socket(tcp_stream: TcpStream, nickname: String, shared: Arc<Mutex<Shared>>) {
    debug!("Processing socket");

    match tcpStream.local_addr() {
        Ok(socket_addr) => {
            let stream_framed = Framed::new(tcpStream, LinesCodec::new());
            let peer = Peer {
                tcpSocket: stream_framed,
            };
            shared.lock().unwrap().peers.insert(socket_addr, peer);

            loop {
                debug!("Processing loop");
            }
        }
        Err(err) => {}
    }
}

struct Peer {
    tx: Receiver,
    rx: SplitStream,
    tcpSocket: Framed,
    nickname: String,
}

/*
impl Stream for Peer {
    type Item = Peer;

    fn poll_next(&mut self, cx: &mut Context) -> Result<Async<Option<Self::Item>>, Self::Error> {}
}
*/

struct Shared<'a> {
    peers: HashMap<SocketAddr, Peer>,
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    debug!("Starting listener!");
    let i = 0;

    let mut shared: Arc<Mutex<Shared>> = Arc::new(Mutex::new(Shared {
        peers: HashMap::new(),
    }));
    let mut listener_res = TcpListener::bind("127.0.0.1:7999");
    let login_server = async move {
        match listener_res.await {
            Ok(mut listener_res) => loop {
                let mut incoming_streams = listener_res.incoming();

                let (login_channel_tx, login_channel_rx) =
                    tokio::sync::watch::channel("login_handler");

                while let Some(client_stream) = incoming_streams.next().await {
                    match client_stream {
                        Ok(client_stream) => {
                            println!("New client connected");
                            let shared = shared.clone();
                            task::spawn(async {
                                process_socket(client_stream, format!("Nickname-{}", i++), shared)
                                    .await
                            });
                        }
                        Err(_) => {
                            error!("Got error during fetch of the client streaming");
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

    //let login_server = future::select(Box::pin(login_server), future::ready(Ok::<_, ()>(())));

    login_server.await;
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
