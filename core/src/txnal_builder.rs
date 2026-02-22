use crate::error::HydraDBResult;
use crate::txn::IsolationLevel;
use crate::txnal_hydradb::TxnalHydraDB;

#[derive(Default)]
pub struct TxnalHydraDBBuilder {
    max_file_size_threshold: u64,
    cask: Option<String>,
    cache_size: u64,
    isolation_level: IsolationLevel,
}

impl TxnalHydraDBBuilder {
    pub fn new() -> Self {
        Self {
            max_file_size_threshold: 1048576,
            cask: None,
            cache_size: 10,
            isolation_level: IsolationLevel::ReadUncommitted,
        }
    }

    pub fn with_file_limit(mut self, l: u64) -> Self {
        self.max_file_size_threshold = l;
        self
    }

    pub fn with_cache_size(mut self, n: u64) -> Self {
        self.cache_size = n;
        self
    }

    pub fn with_cask<T: Into<String>>(mut self, cask: T) -> Self {
        self.cask = Some(cask.into());
        self
    }

    pub fn with_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    pub fn build(self) -> HydraDBResult<TxnalHydraDB> {
        TxnalHydraDB::new(
            self.cask.unwrap(),
            self.max_file_size_threshold,
            self.cache_size,
            self.isolation_level,
        )
    }
}
