use bytes::Bytes;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::txnal_hydradb::TxnalHydraDBInner;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    write_set: BTreeSet<Bytes>,
    read_set: BTreeSet<Bytes>,
    db: Arc<TxnalHydraDBInner>,
}

impl Txn {
    pub fn new(id: u32, isolation_level: IsolationLevel, db: Arc<TxnalHydraDBInner>) -> Self {
        Self {
            id,
            isolation_level,
            state: TxnState::InProgress,
            inprogress_txns: BTreeSet::new(),
            write_set: BTreeSet::new(),
            read_set: BTreeSet::new(),
            db,
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

    #[inline]
    pub fn add_to_write_set(&mut self, k: Bytes) {
        self.write_set.insert(k);
    }

    #[inline]
    pub fn get_write_set(&self) -> &BTreeSet<Bytes> {
        &self.write_set
    }
}
