use std::collections::HashMap;

struct Entry {
    file_id: usize,
    val_sz: usize,
    val_pos: usize,
    tstamp: usize,
}

impl Entry {
    pub fn new(file_id: usize, val_sz: usize, val_pos: usize, tstamp: usize) -> Self {
        Self {
            file_id,
            val_sz,
            val_pos,
            tstamp,
        }
    }
}

struct InMemKVStore {
    kv_store: HashMap<String, Entry>,
}

impl InMemKVStore {
    pub fn new() -> Self {
        Self {
            kv_store: HashMap::new(),
        }
    }

    pub fn put(&mut self, k: String, v: Entry) {
        self.kv_store.insert(k, v);
    }

    pub fn del(&mut self, k: &str) {
        self.kv_store.remove(k);
    }

    pub fn len(&self) -> usize {
        self.kv_store.len()
    }
}

mod tests {
    use super::{Entry, InMemKVStore};

    #[test]
    fn put_test() {
        let mut store = InMemKVStore::new();
        store.put("abhi".to_owned(), Entry::new(1, 5, 1, 0));
        store.put("pads".to_owned(), Entry::new(1, 9, 2, 0));
        store.put("ashu".to_owned(), Entry::new(1, 5, 3, 0));
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn del_test() {
        let mut store = InMemKVStore::new();
        store.put("abhi".to_owned(), Entry::new(1, 5, 1, 0));
        store.put("pads".to_owned(), Entry::new(1, 9, 2, 0));
        store.del("abhi");
        store.put("ashu".to_owned(), Entry::new(1, 5, 3, 0));
        assert_eq!(store.len(), 2);
    }
}
