use crate::{runtime::RuntimeOrderTxChannel, RuntimePeersPool};
use rocket::routes;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod upstream {

    use std::net::SocketAddr;

    use rocket::{get, post, State};
    use tokio::sync::mpsc::error::TrySendError;

    pub mod request {

        use serde::Deserialize;

        #[derive(Debug, Deserialize)]
        pub struct AddUpstream {
            pub host: String,
            pub port: usize,
            pub alive_timeout: usize,
        }
    }
    use crate::{
        runtime::{RuntimeEvent, RuntimeEventUpstream, RuntimeOrderTxChannel},
        upstream::{register_upstream_peers, UpstreamPeerMetadata},
    };
    use request::AddUpstream;
    use rocket_contrib::json::Json;

    #[post("/add", format = "application/json", data = "<metadata>")]
    pub fn add_upstream(metadata: Json<AddUpstream>, runtime_tx: State<'_, RuntimeOrderTxChannel>) {
        trace!("Adding a new upstream with data [{:?}]", &metadata);
        // let socket_addr = SocketAddr::new(
        //     IpAddr::V4(
        //         Ipv4Addr::new()
        //     )
        // )

        let addr = format!("{}:{}", metadata.host, metadata.port);
        if let Ok(addr_parsed) = addr.parse::<SocketAddr>() {
            let runtime_order =
                RuntimeEvent::Upstream(RuntimeEventUpstream::Register(UpstreamPeerMetadata {
                    host: addr_parsed,
                    alive_timeout: metadata.alive_timeout,
                }));
            if let Err(err) = runtime_tx.inner().clone().try_send(runtime_order) {
                match err {
                    TrySendError::Full(_) => {
                        error!("Failed to send the order, because the runtime is busy");
                    }
                    TrySendError::Closed(_) => {
                        panic!("Closing the http server, the core runtime has been closed");
                    }
                }
            }
        } else {
            error!(
                "Failed to parse the host [{:?}] & the port [{:?}]",
                metadata.host, metadata.port
            );
        }
    }

    #[post("/delete/<upstream_uuid>")]
    pub fn delete_upstream(upstream_uuid: String) {
        unimplemented!()
    }

    #[get("/")]
    pub fn get_upstreams() {
        unimplemented!()
    }
}

pub mod downstream {}
pub mod admin {}

/// Start a warp web server for administration purposes
/// Administration management includes:
/// - CRUD on upstream peers
pub fn start_http_management_server(runtime_tx: RuntimeOrderTxChannel) {
    debug!("Starting http management server");

    rocket::ignite()
        .mount(
            "/upstream",
            routes![
                upstream::add_upstream,
                upstream::delete_upstream,
                upstream::get_upstreams
            ],
        )
        .mount("/downstream", routes![])
        .mount("/admin", routes![])
        .manage(runtime_tx)
        .launch();
}
