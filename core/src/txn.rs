use std::u64;

#[derive(Debug, Clone, Default)]
pub enum IsolationLevel {
    #[default]
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Snapshot,
    Serializable,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TxnState {
    InProgress,
    Aborted,
    Committed,
}

#[derive(Debug)]
pub struct Txn {
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

    #[inline]
    pub fn id(&self) -> u32 {
        self.id
    }

    #[inline]
    pub fn set_state(&mut self, state: TxnState) {
        self.state = state;
    }

    #[inline]
    pub fn isolation_level(&self) -> &IsolationLevel {
        &self.isolation_level
    }
}
