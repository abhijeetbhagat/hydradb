use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct InMemEntry {
    pub file_id: usize,
    pub val_sz: u32,
    pub val_pos: usize,
    pub tstamp: u32,
}

impl InMemEntry {
    pub fn new(file_id: usize, val_sz: u32, val_pos: usize, tstamp: u32) -> Self {
        Self {
            file_id,
            val_sz,
            val_pos,
            tstamp,
        }
    }
}

//TODO: concurrency required
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct InMemKVStore {
    kv_store: HashMap<Bytes, InMemEntry>,
}

impl InMemKVStore {
    /// constructs a new in-mem store
    pub fn new() -> Self {
        Self {
            kv_store: HashMap::new(),
        }
    }

    /// puts the key-value pair in the store
    pub fn put(&mut self, k: impl Into<Bytes>, v: InMemEntry) {
        self.kv_store.insert(k.into(), v);
    }

    /// gets the value for given key `k`
    pub fn get(&self, k: impl AsRef<[u8]>) -> Option<InMemEntry> {
        self.kv_store.get(k.as_ref()).cloned()
    }

    /// deletes the given key `k`
    pub fn del(&mut self, k: impl AsRef<[u8]>) {
        self.kv_store.remove(k.as_ref());
    }

    /// checks if the given key `k` is present
    pub fn has_key(&self, k: impl AsRef<[u8]>) -> bool {
        self.kv_store.contains_key(k.as_ref())
    }

    /// returns all the keys in the in-mem store
    pub fn keys(&self) -> Option<Vec<Bytes>> {
        if self.kv_store.is_empty() {
            None
        } else {
            Some(self.kv_store.keys().cloned().collect())
        }
    }

    /// returns the num of entries in the in-mem store
    pub fn len(&self) -> usize {
        self.kv_store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.kv_store.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemEntry, InMemKVStore};

    #[test]
    fn put_test() {
        let mut store = InMemKVStore::new();
        store.put("abhi", InMemEntry::new(1, 5, 1, 0));
        store.put("pads", InMemEntry::new(1, 9, 2, 0));
        store.put("ashu", InMemEntry::new(1, 5, 3, 0));
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn del_test() {
        let mut store = InMemKVStore::new();
        store.put("abhi", InMemEntry::new(1, 5, 1, 0));
        store.put("pads", InMemEntry::new(1, 9, 2, 0));
        store.del("abhi");
        store.put("ashu", InMemEntry::new(1, 5, 3, 0));
        assert_eq!(store.len(), 2);
    }
}
