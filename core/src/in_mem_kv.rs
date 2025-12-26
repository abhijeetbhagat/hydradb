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
    kv_store: HashMap<String, InMemEntry>,
}

impl InMemKVStore {
    pub fn new() -> Self {
        Self {
            kv_store: HashMap::new(),
        }
    }

    pub fn put(&mut self, k: String, v: InMemEntry) {
        self.kv_store.insert(k, v);
    }

    pub fn get(&self, k: &str) -> Option<InMemEntry> {
        self.kv_store.get(k).cloned()
    }

    pub fn del(&mut self, k: &str) {
        self.kv_store.remove(k);
    }

    pub fn has_key(&self, k: &str) -> bool {
        self.kv_store.contains_key(k)
    }

    pub fn len(&self) -> usize {
        self.kv_store.len()
    }
}

mod tests {
    use super::{InMemEntry, InMemKVStore};

    #[test]
    fn put_test() {
        let mut store = InMemKVStore::new();
        store.put("abhi".to_owned(), InMemEntry::new(1, 5, 1, 0));
        store.put("pads".to_owned(), InMemEntry::new(1, 9, 2, 0));
        store.put("ashu".to_owned(), InMemEntry::new(1, 5, 3, 0));
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn del_test() {
        let mut store = InMemKVStore::new();
        store.put("abhi".to_owned(), InMemEntry::new(1, 5, 1, 0));
        store.put("pads".to_owned(), InMemEntry::new(1, 9, 2, 0));
        store.del("abhi");
        store.put("ashu".to_owned(), InMemEntry::new(1, 5, 3, 0));
        assert_eq!(store.len(), 2);
    }
}
