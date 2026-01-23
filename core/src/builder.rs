use crate::hydradb::HydraDB;
use anyhow::Result;

#[derive(Default)]
pub struct HydraDBBuilder {
    max_file_size_threshold: u64,
    cask: Option<String>,
    cache_size: usize,
}

impl HydraDBBuilder {
    pub fn new() -> Self {
        Self {
            max_file_size_threshold: 1048576,
            cask: None,
            cache_size: 10,
        }
    }

    pub fn with_file_limit(mut self, l: u64) -> Self {
        self.max_file_size_threshold = l;
        self
    }

    pub fn with_cache_size(mut self, n: usize) -> Self {
        self.cache_size = n;
        self
    }

    pub fn with_cask<T: Into<String>>(mut self, cask: T) -> Self {
        self.cask = Some(cask.into());
        self
    }

    pub fn build(self) -> Result<HydraDB> {
        HydraDB::new(
            self.cask.unwrap(),
            self.max_file_size_threshold,
            self.cache_size,
        )
    }
}
