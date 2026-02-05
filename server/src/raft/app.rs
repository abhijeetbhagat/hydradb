use std::sync::Arc;

use super::log_store::LogStore;
use super::state_machine::StateMachineStore;
use super::{NodeId, Raft};

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub log_store: LogStore,
    pub state_machine_store: Arc<StateMachineStore>,
    pub config: Arc<openraft::Config>,
}
