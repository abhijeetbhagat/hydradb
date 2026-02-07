pub mod app;
pub mod log_store;
pub mod network;
pub mod state_machine;

use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::HttpServer;
use openraft::Config;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

pub type NodeId = u64;
pub type Raft = openraft::Raft<TypeConfig>;
pub type LogStore = log_store::LogStore;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Put { key: String, value: String },
    Del { key: String },
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Put { key, value, .. } => {
                write!(f, "Put {{ key: {}, value: {} }}", key, value)
            }
            Request::Del { key } => {
                write!(f, "Del {{ key: {} }}", key)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    Put { prev_value: Option<String> },
    Get { value: Option<String> },
    Del { existed: bool },
    List { keys: Vec<String> },
    Mem { value: Option<String> },
    Blank { value: Option<String> },
}

pub mod typ {
    use openraft::BasicNode;

    use super::NodeId;
    use super::TypeConfig;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
);

pub async fn start_raft_node(node_id: NodeId, port: u16, namespace: String) -> anyhow::Result<()> {
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    // Create a instance of where the Raft logs will be stored.
    let log_store = LogStore::default();
    // Create a instance of where the Raft data will be stored.
    let state_machine_store = Arc::new(state_machine::StateMachineStore::new(namespace)?);

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let client = reqwest::Client::new();
    let network = network::Network::new(client);

    // Create a local raft instance.
    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store.clone(),
        state_machine_store.clone(),
    )
    .await
    .unwrap();

    // Create an application that will store all the instances created above, this will
    // later be used on the actix-web services.
    let http_addr = format!("localhost:{port}");
    let app_data = Data::new(app::App {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        log_store,
        state_machine_store,
        config,
    });

    // Start the actix-web server.
    let server = HttpServer::new(move || {
        actix_web::App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app_data.clone())
            // raft internal RPC
            .service(network::raft_rpc::append)
            .service(network::raft_rpc::snapshot)
            .service(network::raft_rpc::vote)
            // admin API
            .service(network::management::init)
            .service(network::management::add_learner)
            .service(network::management::change_membership)
            .service(network::management::metrics)
            // application API
            .service(network::api::write)
            .service(network::api::read)
            .service(network::api::consistent_read)
    });

    let x = server.bind(http_addr)?;

    Ok(x.run().await?)
}
