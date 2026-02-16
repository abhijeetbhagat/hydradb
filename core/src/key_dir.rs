use bytes::Bytes;
use dashmap::DashMap;

#[derive(Debug, Clone)]
pub struct KeyDirEntry {
    pub file_id: usize,
    pub val_sz: u32,
    pub val_pos: u64,
    pub tstamp: u32,
    pub txn_start_id: u32,
    pub txn_end_id: u32,
}

impl KeyDirEntry {
    pub fn new(
        file_id: usize,
        val_sz: u32,
        val_pos: u64,
        tstamp: u32,
        txn_start_id: u32,
        txn_end_id: u32,
    ) -> Self {
        Self {
            file_id,
            val_sz,
            val_pos,
            tstamp,
            txn_start_id,
            txn_end_id,
        }
    }
}

#[derive(Debug)]
pub struct KeyDir {
    kv_store: DashMap<Bytes, Vec<KeyDirEntry>>,
}

impl Default for KeyDir {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyDir {
    /// constructs a new in-mem store
    pub fn new() -> Self {
        Self {
            kv_store: DashMap::new(),
        }
    }

    /// puts the key-value pair in the store
    pub fn put(&self, k: impl Into<Bytes>, v: KeyDirEntry) {
        self.kv_store
            .entry(k.into())
            .and_modify(|vals| vals.push(v.clone()))
            .or_insert(vec![v]);
    }

    /// gets the value for given key `k`
    pub fn get(&self, k: impl AsRef<[u8]>) -> Option<Vec<KeyDirEntry>> {
        self.kv_store.get(k.as_ref()).map(|entry| entry.clone())
    }

    /// deletes the given key `k`
    pub fn del(&self, k: impl AsRef<[u8]>) {
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
            Some(
                self.kv_store
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect(),
            )
        }
    }

    /// returns an iterator over all kv pairs in the dashmap
    pub fn entries(&self) -> dashmap::iter::Iter<'_, Bytes, Vec<KeyDirEntry>> {
        self.kv_store.iter()
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
    use super::{KeyDir, KeyDirEntry};

    #[test]
    fn put_test() {
        let store = KeyDir::new();
        store.put("abhi", KeyDirEntry::new(1, 5, 1, 0, 1, 1));
        store.put("pads", KeyDirEntry::new(1, 9, 2, 0, 2, 2));
        store.put("ashu", KeyDirEntry::new(1, 5, 3, 0, 3, 2));
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn del_test() {
        let store = KeyDir::new();
        store.put("abhi", KeyDirEntry::new(1, 5, 1, 0, 1, 1));
        store.put("pads", KeyDirEntry::new(1, 9, 2, 0, 2, 2));
        store.del("abhi");
        store.put("ashu", KeyDirEntry::new(1, 5, 3, 0, 3, 3));
        assert_eq!(store.len(), 2);
    }
}
