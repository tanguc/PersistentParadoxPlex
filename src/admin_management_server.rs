use crate::{conf, runtime::RuntimeOrderTxChannel, RuntimePeersPool};
use rocket::routes;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod upstream {
    use rocket::{get, post, State};
    use std::net::SocketAddr;
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
pub fn start_http_management_server(
    runtime_tx: RuntimeOrderTxChannel,
    conf: &conf::ManagementServer,
    tls_conf: Option<&conf::ManagementServerTls>,
) {
    debug!("Starting http management server");

    let default_rocket_config =
        rocket::config::Config::active().expect("Failed to load default rocket config, leaving");
    let mut rocket_config = rocket::config::Config::new(default_rocket_config.environment);

    rocket_config.set_port(conf.port);
    rocket_config.set_address(conf.host.clone());

    if let Some(log_level) = conf.log_level.as_ref() {
        debug!("Setting log level for Rocket");
        if let Ok(log_level) = log_level.parse::<rocket::config::LoggingLevel>() {
            rocket_config.set_log_level(log_level);
        } else {
            panic!("Failed to parse the log level")
        }
    }

    if let Some(tls_conf) = tls_conf {
        debug!("Setting tls for rocket");
        if let Err(err) = rocket_config.set_tls(&tls_conf.cert_path, &tls_conf.key_path) {
            panic!("Failed to configure TLS for Rocket, {:?}", err);
        }
    }
    // rocket_config.set_tls(, )

    let task = || async move {
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
    };
    tokio::task::spawn(task());
}
