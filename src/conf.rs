use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::fs::read_to_string;

#[derive(Clone, Debug, Deserialize)]
pub struct Upstream {
    pub host: String,
    pub port: u16,

    #[serde(default = "default_upstream_alive_timeout")]
    pub alive_timeout: u32,
}
fn default_upstream_alive_timeout() -> u32 {
    60
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManagementServer {
    #[serde(default = "default_management_server_host")]
    pub host: String,

    #[serde(default = "default_management_server_port")]
    pub port: u16,

    pub log_level: Option<String>,
}
fn default_management_server_host() -> String {
    "127.0.0.1".to_string()
}
fn default_management_server_port() -> u16 {
    8000
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManagementServerTls {
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Server {
    #[serde(default = "default_server_host")]
    pub host: String,

    #[serde(default = "default_server_port")]
    pub port: u16,
}

fn default_server_host() -> String {
    "127.0.0.1".to_string()
}
fn default_server_port() -> u16 {
    7999
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub server: Server,

    #[serde(rename(deserialize = "upstream"))]
    pub upstreams: std::vec::Vec<Upstream>,

    pub management_server: ManagementServer,
    pub management_server_tls: Option<ManagementServerTls>,
}

pub fn load_config() -> Result<Config> {
    let file = read_to_string("config.toml").map_err(|err| {
        return anyhow!("Could not load configuration file [{:?}]", err);
    })?;
    debug!("Configuration file loaded");
    trace!("Configuration file content : [{:?}]", &file);

    let config: Config = toml::from_str(&file).map_err(|err| {
        return anyhow!(
            "Failed to deserialize the content of the configuration file, cause [{:?}]",
            err
        );
    })?;
    trace!("Configuration struct loaded : [{:?}]", &config);

    Ok(config)
}
