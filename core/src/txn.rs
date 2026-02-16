use std::u64;

#[derive(Debug)]
pub(crate) enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Snapshot,
    Serializable,
}

#[derive(Debug, Clone)]
pub(crate) enum TxnState {
    InProgress,
    Aborted,
    Committed,
}

#[derive(Debug)]
pub(crate) struct Txn {
    // todo: should be 64 bits?
    id: u32,
    isolation_level: IsolationLevel,
    state: TxnState,
}

impl Txn {
    pub fn new(id: u32, isolation_level: IsolationLevel) -> Self {
        Self {
            id,
            isolation_level,
            state: TxnState::InProgress,
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn set_state(&mut self, state: TxnState) {
        self.state = state;
    }
}
