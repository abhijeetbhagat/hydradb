use std::{collections::BTreeSet, u64};

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
    inprogress_txns: BTreeSet<u32>,
}

impl Txn {
    pub fn new(id: u32, isolation_level: IsolationLevel) -> Self {
        Self {
            id,
            isolation_level,
            state: TxnState::InProgress,
            inprogress_txns: BTreeSet::new(),
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

    #[inline]
    pub fn set_inprogress_txns(&mut self, txns: BTreeSet<u32>) {
        self.inprogress_txns = txns;
    }

    #[inline]
    pub fn get_inprogress_txns(&self) -> &BTreeSet<u32> {
        &self.inprogress_txns
    }
}
