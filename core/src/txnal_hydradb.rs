use crate::data_file_iter::{DataFileEntry, OptimizedDataFileIterator};
use crate::error::{HydraDBError, HydraDBResult};
use crate::txn::Txn;
use crate::txn::{IsolationLevel, TxnState};
use crate::utils::{
    calc_crc, calc_crc_txn, sets_share_item, to_db_entry, to_db_entry_txn, to_hint_entry_txn,
};
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::one::RefMut;
use log::debug;
use mini_moka::sync::Cache;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::fs;
use std::fs::{DirBuilder, File};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct TxnalKeyDirEntry {
    pub file_id: usize,
    pub val_sz: u32,
    pub val_pos: u64,
    pub tstamp: u32,
    pub txn_start_id: u32,
    pub txn_end_id: u32,
}

impl TxnalKeyDirEntry {
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
struct TxnalKeyDir {
    kv_store: DashMap<Bytes, Vec<TxnalKeyDirEntry>>,
}

impl TxnalKeyDir {
    fn new() -> Self {
        Self {
            kv_store: DashMap::new(),
        }
    }

    fn put(&self, k: impl Into<Bytes>, v: TxnalKeyDirEntry) {
        self.kv_store
            .entry(k.into())
            .and_modify(|vals| vals.push(v.clone()))
            .or_insert(vec![v]);
    }

    fn get(&self, k: impl AsRef<[u8]>) -> Option<Vec<TxnalKeyDirEntry>> {
        self.kv_store.get(k.as_ref()).map(|entry| entry.clone())
    }

    fn get_mut(&self, k: impl AsRef<[u8]>) -> Option<RefMut<'_, Bytes, Vec<TxnalKeyDirEntry>>> {
        self.kv_store.get_mut(k.as_ref())
    }

    fn del(&self, k: impl AsRef<[u8]>) {
        self.kv_store.remove(k.as_ref());
    }

    fn has_key(&self, k: impl AsRef<[u8]>) -> bool {
        self.kv_store.contains_key(k.as_ref())
    }

    fn keys(&self) -> Option<Vec<Bytes>> {
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

    fn len(&self) -> usize {
        self.kv_store.len()
    }
}

#[derive(Debug)]
struct WriterState {
    writer: BufWriter<File>,
    last_val_offset: u64,
    cur_file_size: u64,
}

#[derive(Debug)]
pub(crate) struct TxnalHydraDBInner {
    /// name of the cask (folder/namespace)
    cur_cask: String,

    /// current file id increases monotonically
    cur_id: AtomicUsize,

    /// in-mem kv map
    key_dir: TxnalKeyDir,

    /// max file size after which a new one gets created
    max_file_size_threshold: u64,

    /// file writer
    writer: Mutex<WriterState>,

    /// for caching files during reads
    file_cache: Cache<usize, Arc<File>>,

    /// monotonically increasing txn id
    cur_txn_id: AtomicU32,

    /// track txn states
    txn_states: DashMap<u32, TxnState>,

    /// isolation level of the db
    isolation_level: IsolationLevel,

    txn_write_sets: DashMap<u32, BTreeSet<Bytes>>,
}

/// Transactional version of HydraDB with MVCC support.
#[derive(Debug)]
pub struct TxnalHydraDB {
    inner: Arc<TxnalHydraDBInner>,
}

impl TxnalHydraDB {
    pub fn new<T: Into<String> + Debug>(
        namespace: T,
        max_file_size_threshold: u64,
        cache_size: u64,
        isolation_level: IsolationLevel,
    ) -> HydraDBResult<Self> {
        let namespace = namespace.into();

        let cur_id;
        let cur_file_size;
        let last_val_offset;

        if !fs::exists(format!("./{namespace}"))? {
            let dir_builder = DirBuilder::new();
            dir_builder.create(format!("./{}", &namespace))?;
            cur_id = 0;
            cur_file_size = 0;
            last_val_offset = 0;
        } else {
            let mut mx = 0;
            for entry in fs::read_dir(format!("./{}", &namespace))? {
                let entry = entry?;
                let path = entry.path();

                if path.is_file()
                    && let Some(path) = path.file_name()
                    && let Some(path) = path.to_str()
                    && path != "hint"
                    && path != "temp"
                {
                    debug!("path is {path}");
                    mx = std::cmp::max(mx, path.parse::<usize>()?)
                }
            }

            cur_id = mx;

            let path = format!("./{namespace}/{cur_id}");
            cur_file_size = if Path::new(&path).exists() {
                fs::metadata(path)?.len()
            } else {
                0
            };
            last_val_offset = cur_file_size;
        }

        let file = File::options()
            .create(true)
            .append(true)
            .open(format!("./{}/{}", namespace, cur_id))?;

        let inner = TxnalHydraDBInner {
            cur_cask: namespace,
            cur_id: cur_id.into(),
            key_dir: TxnalKeyDir::new(),
            max_file_size_threshold,
            writer: Mutex::new(WriterState {
                writer: BufWriter::new(file),
                last_val_offset,
                cur_file_size,
            }),
            file_cache: Cache::new(cache_size),
            cur_txn_id: AtomicU32::new(0),
            txn_states: DashMap::new(),
            isolation_level,
            txn_write_sets: DashMap::new(),
        };

        // TODO: build_key_dir for txnal format

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// gets all in-progress txns
    fn get_inprogress_txns(&self) -> BTreeSet<u32> {
        let v = self
            .inner
            .txn_states
            .iter()
            .filter(|txn| *txn.value() == TxnState::InProgress)
            .map(|entry| entry.key().clone());
        BTreeSet::from_iter(v)
    }

    // todo: accept isolation level or set default during db creation
    pub fn begin_txn(&self) -> Txn {
        // SAFETY: no reordering affects the increment
        let new_txn_id = self
            .inner
            .cur_txn_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;

        let mut txn = Txn::new(
            new_txn_id,
            self.inner.isolation_level.clone(),
            Arc::clone(&self.inner),
        );
        txn.set_inprogress_txns(self.get_inprogress_txns());

        self.inner
            .txn_states
            .insert(new_txn_id, TxnState::InProgress);

        txn
    }

    pub fn commit(&self, txn: &mut Txn) -> HydraDBResult<()> {
        self.complete_txn(txn, TxnState::Committed)
    }

    pub fn abort(&self, txn: &mut Txn) -> HydraDBResult<()> {
        self.complete_txn(txn, TxnState::Aborted)
    }

    fn complete_txn(&self, txn: &mut Txn, state: TxnState) -> HydraDBResult<()> {
        if state == TxnState::Committed
            && txn.isolation_level() == &IsolationLevel::Snapshot
            && self.has_conflict(txn)
        {
            txn.set_state(TxnState::Aborted);
            return Err(HydraDBError::WriteWriteConflict);
        }

        txn.set_state(state.clone());
        self.inner.txn_states.insert(txn.id(), state);
        Ok(())
    }

    fn has_conflict(&self, txn: &Txn) -> bool {
        for tid in txn.get_inprogress_txns() {
            if let Some(t) = self.inner.txn_states.get(tid)
                && *t.value() == TxnState::Committed
                && let Some(other_write_set) = self.inner.txn_write_sets.get(tid)
                && sets_share_item(txn.get_write_set(), &other_write_set)
            {
                return true;
            }
        }

        let max_txn_id = self
            .inner
            .cur_txn_id
            .load(std::sync::atomic::Ordering::Relaxed);
        for tid in txn.id()..=max_txn_id {
            if let Some(t) = self.inner.txn_states.get(&tid)
                && *t.value() == TxnState::Committed
                && let Some(other_write_set) = self.inner.txn_write_sets.get(&tid)
                && sets_share_item(txn.get_write_set(), &other_write_set)
            {
                return true;
            }
        }

        false
    }

    fn get_txn_state(&self, id: u32) -> Option<TxnState> {
        self.inner.txn_states.get(&id).map(|v| v.clone())
    }

    fn is_visible(txn_states: &DashMap<u32, TxnState>, txn: &Txn, val: &TxnalKeyDirEntry) -> bool {
        match txn.isolation_level() {
            IsolationLevel::ReadUncommitted => val.txn_end_id == 0,
            IsolationLevel::ReadCommitted => {
                // val being used by some other txn not committed yet
                if val.txn_start_id != txn.id()
                    && let Some(state) = txn_states.get(&val.txn_start_id)
                    && *state != TxnState::Committed
                {
                    return false;
                }

                // val deleted by current txn
                if val.txn_end_id == txn.id() {
                    return false;
                }

                // val deleted by some other txn
                if val.txn_end_id > 0
                    && let Some(state) = txn_states.get(&val.txn_end_id)
                    && *state == TxnState::Committed
                {
                    return false;
                }

                true
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Snapshot => {
                // val being used by txn started after the current one
                if val.txn_start_id > txn.id() {
                    return false;
                }

                // val is being used by an inprogress txn
                if txn.get_inprogress_txns().contains(&val.txn_start_id) {
                    return false;
                }

                // same checks as read committed
                // val being used by some other txn not committed yet
                if val.txn_start_id != txn.id()
                    && let Some(state) = txn_states.get(&val.txn_start_id)
                    && *state != TxnState::Committed
                {
                    return false;
                }

                // val deleted by current txn
                if val.txn_end_id == txn.id() {
                    return false;
                }

                // val deleted by some other committed txn before this txn
                if val.txn_end_id > 0
                    && val.txn_end_id < txn.id()
                    && let Some(state) = txn_states.get(&val.txn_end_id)
                    && *state == TxnState::Committed
                {
                    return false;
                }

                true
            }
            _ => true,
        }
    }

    /// reads and validates a value from the data file for the given key entry
    fn read_value_from_file(
        &self,
        txn: &Txn,
        k: &[u8],
        val_entries: &[TxnalKeyDirEntry],
    ) -> HydraDBResult<Option<Bytes>> {
        let val = None;

        for entry in val_entries.iter().rev() {
            println!("entry {:?}", entry);

            if !Self::is_visible(&self.inner.txn_states, txn, entry) {
                continue;
            }

            let file = if let Some(arcd_file) = self.inner.file_cache.get(&entry.file_id) {
                arcd_file.clone()
            } else {
                self.inner.file_cache.insert(
                    entry.file_id,
                    Arc::new(
                        File::options()
                            .read(true)
                            .open(format!("./{}/{}", self.inner.cur_cask, entry.file_id))?,
                    ),
                );
                println!("file inserted in cache");
                self.inner.file_cache.get(&entry.file_id).unwrap().clone()
            };

            let entry_len = 21 + k.len() + entry.val_sz as usize;
            println!("k len {} entry.val_sz {}", k.len(), entry.val_sz);
            let mut buf = vec![0; entry_len];
            let _ = file.read_exact_at(&mut buf, entry.val_pos);

            println!("val read from file: {:?}", buf);

            let entry_crc = u32::from_be_bytes(buf[1..=4].try_into().unwrap());
            let tstamp = u32::from_be_bytes(buf[5..=8].try_into().unwrap());
            let txn_id = u32::from_be_bytes(buf[9..=12].try_into().unwrap());
            let ksz = u32::from_be_bytes(buf[13..=16].try_into().unwrap());
            let vsz = u32::from_be_bytes(buf[17..=20].try_into().unwrap());

            let k_start = 21usize;
            let k_end = k_start + ksz as usize;
            let v_end = k_end + vsz as usize;
            println!("reading val from val read from {k_end} to {v_end}");
            let crc = calc_crc_txn(
                tstamp,
                txn_id,
                ksz,
                vsz,
                &buf[k_start..k_end],
                &buf[k_end..v_end],
            );

            println!(
                "val read from {k_end} to {v_end} {}",
                str::from_utf8(&buf[k_end..v_end]).unwrap()
            );

            if entry_crc != crc {
                debug!("crc mismatch");
                return Err(HydraDBError::FileCorruptionError(
                    entry.file_id,
                    entry_crc,
                    crc,
                ));
            }

            return Ok(Some(Bytes::copy_from_slice(&buf[k_end..v_end])));
        }

        Ok(val)
    }

    /// gets the value, if present, for the given key `k`
    pub fn get(&self, txn: &Txn, k: impl AsRef<[u8]>) -> HydraDBResult<Option<Bytes>> {
        if let Some(in_mem_entry) = self.inner.key_dir.get(&k) {
            let val = self.read_value_from_file(txn, k.as_ref(), &in_mem_entry)?;
            debug!("val is '{:?}'", val);
            Ok(val)
        } else {
            debug!("val is");
            Ok(None)
        }
    }

    /// puts the key `k` & value `v` pair
    pub fn put(
        &self,
        txn: &mut Txn,
        k: impl Into<Bytes>,
        v: impl Into<Bytes>,
    ) -> HydraDBResult<()> {
        let k = k.into();
        let v = v.into();

        // allow only one writer at a time
        let mut writer = self.inner.writer.lock().unwrap();

        // first mark all entries for this key `k` as deleted by the current txn
        if let Some(mut entries) = self.inner.key_dir.get_mut(&k) {
            for entry in entries.iter_mut().rev() {
                if Self::is_visible(&self.inner.txn_states, txn, entry) {
                    entry.txn_end_id = txn.id();
                }
            }
        }

        debug!("cur file size {}", writer.cur_file_size);
        let cur_id = if (21 + k.len() as u64 + v.len() as u64 + writer.cur_file_size)
            >= self.inner.max_file_size_threshold
        {
            // SAFETY: it is safe to use relaxed ordering here since we are locking
            // the writer at the beginning of this method. therefore, everything after
            // will be sequential execution
            let old_cur_id = self
                .inner
                .cur_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let new_cur_id = old_cur_id + 1;

            let file = File::options()
                .create(true)
                .append(true)
                .open(format!("./{}/{}", self.inner.cur_cask, new_cur_id))?;

            *writer = WriterState {
                writer: BufWriter::new(file),
                last_val_offset: 0,
                cur_file_size: 0,
            };
            new_cur_id
        } else {
            self.inner.cur_id.load(std::sync::atomic::Ordering::Relaxed)
        };

        let file_id = cur_id;
        let ksz = k.len() as u32;
        let val_pos = writer.last_val_offset;
        let vsz = v.len() as u32;
        writer.last_val_offset += 21 + ksz as u64 + vsz as u64;
        let tstamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;
        let crc = calc_crc_txn(tstamp, txn.id(), ksz, vsz, &k, &v);

        let entry = to_db_entry_txn(0, crc, tstamp, txn.id(), &k, &v);

        writer.writer.write_all(&entry)?;
        writer.writer.write_all(&k)?;
        writer.writer.write_all(&v)?;
        writer.writer.flush()?;

        writer.cur_file_size += 21u64 + k.len() as u64 + v.len() as u64;

        // then append to entries for the current key `k`
        self.inner.key_dir.put(
            k.to_owned(),
            TxnalKeyDirEntry::new(file_id, vsz, val_pos, tstamp, txn.id(), 0),
        );

        txn.add_to_write_set(k.clone());
        self.inner
            .txn_write_sets
            .entry(txn.id())
            .and_modify(|set| {
                set.insert(k.clone());
            })
            .or_insert_with(|| {
                let mut set = BTreeSet::new();
                set.insert(k);
                set
            });

        Ok(())
    }

    /// deletes the given key
    pub fn del(&self, txn: &mut Txn, k: impl AsRef<[u8]>) -> HydraDBResult<bool> {
        let k = k.as_ref();
        let k_exists = self.mark_deleted(txn, k)?;
        Ok(k_exists)
    }

    fn mark_deleted(&self, txn: &mut Txn, k: &[u8]) -> HydraDBResult<bool> {
        // allow only one writer at a time
        let mut writer = self.inner.writer.lock().unwrap();

        if let Some(mut entries) = self.inner.key_dir.get_mut(k) {
            for entry in entries.iter_mut().rev() {
                if Self::is_visible(&self.inner.txn_states, txn, entry) {
                    entry.txn_end_id = txn.id();
                }
            }
        }

        let k_exists = self.inner.key_dir.has_key(k);
        if k_exists {
            debug!("cur file size {}", writer.cur_file_size);
            if (21u64 + k.len() as u64 + writer.cur_file_size) >= self.inner.max_file_size_threshold
            {
                let old_cur_id = self
                    .inner
                    .cur_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let new_cur_id = old_cur_id + 1;

                let file = File::options()
                    .create(true)
                    .append(true)
                    .open(format!("./{}/{}", self.inner.cur_cask, new_cur_id))?;

                *writer = WriterState {
                    writer: BufWriter::new(file),
                    last_val_offset: 0,
                    cur_file_size: 0,
                };
                new_cur_id
            } else {
                self.inner.cur_id.load(std::sync::atomic::Ordering::Relaxed)
            };

            let ksz = k.len() as u32;
            writer.last_val_offset += 21 + ksz as u64;
            let tstamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;
            let crc = calc_crc(tstamp, ksz, 0, k, &[]);

            let entry = to_db_entry(1, crc, tstamp, k, &[]);

            writer.writer.write_all(&entry)?;
            writer.writer.write_all(k)?;
            writer.writer.write_all(&[])?;
            writer.writer.flush()?;

            writer.cur_file_size += 21u64 + k.len() as u64;

            self.inner.key_dir.del(k.clone());
            let k = Bytes::copy_from_slice(k);
            txn.add_to_write_set(k.clone());
            self.inner
                .txn_write_sets
                .entry(txn.id())
                .and_modify(|s| {
                    s.insert(k.clone());
                })
                .or_insert_with(|| {
                    let mut set = BTreeSet::new();
                    set.insert(k);
                    set
                });
        }

        Ok(k_exists)
    }

    /// lists all the keys in the store
    pub fn list_all(&self) -> Option<Vec<Bytes>> {
        self.inner.key_dir.keys()
    }
}

#[cfg(test)]
mod tests {
    use crate::error::HydraDBError;
    use crate::txn::IsolationLevel;
    use crate::txnal_builder::TxnalHydraDBBuilder;
    use std::fs;

    #[test]
    fn test_read_uncommitted() {
        let _ = env_logger::builder().is_test(true).try_init();

        let db = TxnalHydraDBBuilder::new()
            .with_cask("ruc_test")
            .with_file_limit(100)
            .with_cache_size(5)
            .build()
            .unwrap();
        let mut t1 = db.begin_txn();
        let _ = db.put(&mut t1, "abhi", "rust");
        assert_eq!(db.inner.key_dir.len(), 1);

        let t2 = db.begin_txn();
        let val = db.get(&t2, "abhi");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert!(val.is_some());
        let val = val.unwrap();
        assert_eq!(val, "rust");

        let _ = fs::remove_dir_all("./ruc_test");
    }

    #[test]
    fn test_rc_reading_val_used_by_uncommitted_txn() {
        let _ = env_logger::builder().is_test(true).try_init();

        let db = TxnalHydraDBBuilder::new()
            .with_cask("rc_reading_val_used_by_uncommitted_txn")
            .with_file_limit(100)
            .with_cache_size(5)
            .with_isolation_level(IsolationLevel::ReadCommitted)
            .build()
            .unwrap();

        let mut t1 = db.begin_txn();
        let _ = db.put(&mut t1, "abhi", "rust");
        assert_eq!(db.inner.key_dir.len(), 1);

        let t2 = db.begin_txn();
        let val = db.get(&t2, "abhi");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert!(val.is_none());

        let _ = fs::remove_dir_all("./rc_reading_val_used_by_uncommitted_txn");
    }

    #[test]
    fn test_rc_reading_val_deleted_by_other_txn() {
        let _ = env_logger::builder().is_test(true).try_init();

        let db = TxnalHydraDBBuilder::new()
            .with_cask("rc_reading_val_deleted_by_other_txn")
            .with_file_limit(100)
            .with_cache_size(5)
            .with_isolation_level(IsolationLevel::ReadCommitted)
            .build()
            .unwrap();

        let mut t1 = db.begin_txn();
        let _ = db.put(&mut t1, "abhi", "rust");
        assert_eq!(db.inner.key_dir.len(), 1);
        db.commit(&mut t1);

        let mut t1 = db.begin_txn();
        let _ = db.del(&mut t1, "abhi");
        assert_eq!(db.inner.key_dir.len(), 0);
        db.commit(&mut t1);

        let t2 = db.begin_txn();
        let val = db.get(&t2, "abhi");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert!(val.is_none());

        let _ = fs::remove_dir_all("./rc_reading_val_deleted_by_other_txn");
    }

    #[test]
    fn test_rc_reading_val_deleted_by_self() {
        let _ = env_logger::builder().is_test(true).try_init();

        let db = TxnalHydraDBBuilder::new()
            .with_cask("rc_reading_val_deleted_by_self")
            .with_file_limit(100)
            .with_cache_size(5)
            .with_isolation_level(IsolationLevel::ReadCommitted)
            .build()
            .unwrap();

        let mut t1 = db.begin_txn();
        let _ = db.put(&mut t1, "abhi", "rust");
        assert_eq!(db.inner.key_dir.len(), 1);
        db.commit(&mut t1);

        let mut t2 = db.begin_txn();
        let _ = db.del(&mut t2, "abhi");
        assert_eq!(db.inner.key_dir.len(), 0);

        let val = db.get(&t2, "abhi");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert!(val.is_none());

        let _ = fs::remove_dir_all("./rc_reading_val_deleted_by_self");
    }

    #[test]
    fn test_rr() {
        let _ = env_logger::builder().is_test(true).try_init();

        let db = TxnalHydraDBBuilder::new()
            .with_cask("rr_test")
            .with_file_limit(100)
            .with_cache_size(5)
            .with_isolation_level(IsolationLevel::RepeatableRead)
            .build()
            .unwrap();

        let mut t1 = db.begin_txn();
        let t2 = db.begin_txn();

        let _ = db.put(&mut t1, "abhi", "rust");
        let v = db.get(&t1, "abhi");
        assert!(v.is_ok());
        let v = v.unwrap();
        assert_eq!(v, Some("rust".into()));

        let v = db.get(&t2, "abhi");
        assert!(v.is_ok());
        assert_eq!(v.unwrap(), None);

        db.commit(&mut t1);

        let v = db.get(&t2, "abhi");
        assert!(v.is_ok());
        assert_eq!(v.unwrap(), None);

        let t3 = db.begin_txn();
        let v = db.get(&t3, "abhi");
        assert!(v.is_ok());
        let v = v.unwrap();
        assert_eq!(v, Some("rust".into()));

        let _ = fs::remove_dir_all("./rr_test");
    }

    #[test]
    fn test_snapshot_isolation() {
        let _ = env_logger::builder().is_test(true).try_init();

        let db = TxnalHydraDBBuilder::new()
            .with_cask("si_test")
            .with_file_limit(100)
            .with_cache_size(5)
            .with_isolation_level(IsolationLevel::Snapshot)
            .build()
            .unwrap();

        let mut t1 = db.begin_txn();

        let mut t2 = db.begin_txn();

        let mut t3 = db.begin_txn();

        let _ = db.put(&mut t1, "abhi", "rust");
        let _ = db.commit(&mut t1);

        let _ = db.put(&mut t2, "abhi", "rust");
        assert_eq!(db.commit(&mut t2), Err(HydraDBError::WriteWriteConflict));

        let _ = db.put(&mut t3, "ashu", "java");
        let _ = db.commit(&mut t3);

        let _ = fs::remove_dir_all("./si_test");
    }
}
