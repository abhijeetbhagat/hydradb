use crate::data_file_iter::{DataFileEntry, DataFileIterator, OptimizedDataFileIterator};
use crate::key_dir::{KeyDir, KeyDirEntry};
use crate::restore::*;
use crate::utils::calc_crc;
use anyhow::Result;
use bytes::Bytes;
use core::str;
use dashmap::DashMap;
use log::debug;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs;
use std::io::{BufWriter, Read, Write};
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::{
    fs::{DirBuilder, File},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::field::debug;

/// returns a raw db entry to persist from the given data
#[inline]
fn to_db_entry(crc: u32, tstamp: u32, k: &[u8], v: &[u8]) -> Vec<u8> {
    // crc + tstamp + ksz + vsz + key + val
    let mut o = Vec::with_capacity(4 + 4 + 4 + 4 + k.len() + v.len());

    let kl = k.len() as u32;
    let vl = v.len() as u32;

    o.extend_from_slice(&crc.to_be_bytes());
    o.extend_from_slice(&tstamp.to_be_bytes());
    o.extend_from_slice(&kl.to_be_bytes());
    o.extend_from_slice(&vl.to_be_bytes());
    o.extend_from_slice(k);
    o.extend_from_slice(v);
    o
}

#[inline]
fn to_hint_entry(tstamp: u32, k: &[u8], v: &[u8], val_pos: u64) -> Vec<u8> {
    // tstamp + ksz + vsz + val_pos + key
    let mut o = Vec::with_capacity(4 + 4 + 4 + 8 + k.len());

    let kl = k.len() as u32;
    let vl = v.len() as u32;

    o.extend_from_slice(&tstamp.to_be_bytes());
    o.extend_from_slice(&kl.to_be_bytes());
    o.extend_from_slice(&vl.to_be_bytes());
    o.extend_from_slice(&val_pos.to_be_bytes());
    o.extend_from_slice(k);
    o
}

#[derive(Debug, Default)]
struct WriterState {
    writer: Option<BufWriter<File>>,
    // tracks the val positions in a data file
    // so that we avoid expensive seek operations to calculate them
    last_val_offset: u64,
    cur_file_size: u64,
}

/// the main bitcask storage engine
#[derive(Serialize, Deserialize, Debug)]
pub struct HydraDB {
    cur_cask: String,    // dir name of the cur_cask
    cur_id: AtomicUsize, // needs to be atomic
    key_dir: KeyDir,
    max_file_size_threshold: u64,
    // cur_file_size: AtomicU64,
    // #[serde(skip)]
    // writer: Mutex<Option<BufWriter<File>>>,
    // tracks the val positions in a data file
    // so that we avoid expensive seek operations to calculate them
    // last_val_offset: AtomicU64,
    #[serde(skip)]
    writer: Mutex<WriterState>,

    #[serde(skip)]
    file_cache: DashMap<usize, Arc<File>>,
}

impl Default for HydraDB {
    fn default() -> Self {
        HydraDBBuilder::new().with_cask("default").build().unwrap()
    }
}

impl HydraDB {
    /// creates an instance of `HydraDB` with the given `namespace`
    pub fn new<T: Into<String> + Debug>(
        namespace: T,
        max_file_size_threshold: u64,
        cache_size: usize,
    ) -> Result<Self> {
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

        let mut db = Self {
            cur_cask: namespace,
            cur_id: cur_id.into(),
            key_dir: KeyDir::new(),
            max_file_size_threshold,
            writer: Mutex::new(WriterState {
                writer: Some(BufWriter::new(file)),
                last_val_offset,
                cur_file_size,
            }),
            file_cache: DashMap::with_capacity(cache_size),
        };

        let _ = db.build_key_dir();

        Ok(db)
    }

    fn get_active_file(&self) -> usize {
        self.cur_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// builds the in-mem store by scanning the data files
    fn build_key_dir(&mut self) -> Result<()> {
        let restorer: Box<dyn Restore> = if Path::new(&format!("{}/hint", self.cur_cask)).exists() {
            Box::new(HintFileRestore)
        } else {
            Box::new(DataFileRestore)
        };

        restorer.restore(
            "./",
            &self.cur_cask,
            self.cur_id.load(std::sync::atomic::Ordering::Relaxed),
            &mut self.key_dir,
        )
    }

    /// gets the value, if present, for the given key `k`
    pub fn get(&self, k: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        if let Some(in_mem_entry) = self.key_dir.get(k) {
            let KeyDirEntry {
                file_id,
                val_sz,
                val_pos,
                tstamp: _,
            } = in_mem_entry;
            // debug!("val_pos is {val_pos} val sz {val_sz}");

            // debug!("reading from ./{}/{}", self.cur_cask, file_id);
            let file = if let Some(arcd_file) = self.file_cache.get(&file_id) {
                arcd_file.clone()
            } else {
                self.file_cache.insert(
                    file_id,
                    Arc::new(
                        File::options()
                            .read(true)
                            .open(format!("./{}/{}", self.cur_cask, file_id))?,
                    ),
                );
                self.file_cache.get(&file_id).unwrap().clone()
            };

            // file.seek(SeekFrom::Start(val_pos))?;
            // debug!("file pos is {:?}", file.stream_position());

            let mut v = vec![0; val_sz as usize];
            file.read_exact_at(&mut v, val_pos)?;
            // let mut f = file.take(val_sz as u64);

            // f.read_to_end(&mut v)?;
            // debug!("value is {}", str::from_utf8(&v).unwrap());

            Ok(Some(v.into()))
        } else {
            Ok(None)
        }
    }

    /// puts the given key-value pair under the set namespace
    pub fn put(&self, k: impl Into<Bytes>, v: impl Into<Bytes>) -> Result<()> {
        let k = k.into();
        let v = v.into();

        let entry = self.put_with_file_size_check(&k, &v)?;

        // then write to im
        self.key_dir.put(k, entry);

        Ok(())
    }

    fn put_with_file_size_check(&self, k: &[u8], v: &[u8]) -> Result<KeyDirEntry> {
        let mut writer = self.writer.lock().unwrap();

        debug!("cur file size {}", writer.cur_file_size);
        if (16u64 + k.len() as u64 + v.len() as u64 + writer.cur_file_size)
            >= self.max_file_size_threshold
        {
            self.cur_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let file = File::options().create(true).append(true).open(format!(
                "./{}/{}",
                self.cur_cask,
                self.cur_id.load(std::sync::atomic::Ordering::Relaxed)
            ))?;

            *writer = WriterState {
                writer: Some(BufWriter::new(file)),
                last_val_offset: 0,
                cur_file_size: 0,
            };

            let file_id = self.cur_id.load(std::sync::atomic::Ordering::Relaxed);
            let ksz = k.len() as u32;
            let val_pos = writer.last_val_offset + 16 + ksz as u64; // 16 bytes header size
            let vsz = v.len() as u32;
            writer.last_val_offset += 16 + ksz as u64 + vsz as u64; // 16 bytes header size
            let tstamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u32;
            let crc = calc_crc(tstamp, ksz, vsz, k, v);

            let entry = to_db_entry(crc, tstamp, k, v);

            let _ = writer.writer.as_mut().unwrap().write_all(&entry);
            let _ = writer.writer.as_mut().unwrap().flush();

            writer.cur_file_size += 16u64 + k.len() as u64 + v.len() as u64;

            Ok(KeyDirEntry::new(file_id, vsz, val_pos, tstamp))
        } else {
            let file_id = self.cur_id.load(std::sync::atomic::Ordering::Relaxed);
            let ksz = k.len() as u32;
            let val_pos = writer.last_val_offset + 16 + ksz as u64; // 16 bytes header size
            let vsz = v.len() as u32;
            writer.last_val_offset += 16 + ksz as u64 + vsz as u64; // 16 bytes header size
            let tstamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u32;
            let crc = calc_crc(tstamp, ksz, vsz, k, v);

            let entry = to_db_entry(crc, tstamp, k, v);

            let _ = writer.writer.as_mut().unwrap().write_all(&entry);
            let _ = writer.writer.as_mut().unwrap().flush();

            writer.cur_file_size += 16u64 + k.len() as u64 + v.len() as u64;

            Ok(KeyDirEntry::new(file_id, vsz, val_pos, tstamp))
        }

        // let entry = self.persist(k, v)?;

        // self.cur_file_size += 16u64 + k.len() as u64 + v.len() as u64;

        // Ok(entry)
    }

    // fn persist(&self, k: &[u8], v: &[u8]) -> Result<KeyDirEntry> {
    //     let writer = self.writer.clone().unwrap();
    //     let mut writer = writer.lock().unwrap();
    //     let file_id = self.cur_id.load(std::sync::atomic::Ordering::Relaxed);
    //     let ksz = k.len() as u32;
    //     let val_pos = self.last_val_offset + 16 + ksz as u64; // 16 bytes header size
    //     let vsz = v.len() as u32;
    //     self.last_val_offset += 16 + ksz as u64 + vsz as u64; // 16 bytes header size
    //     let tstamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u32;
    //     let crc = calc_crc(tstamp, ksz, vsz, k, v);

    //     let entry = to_db_entry(crc, tstamp, k, v);

    //     let _ = writer.write_all(&entry);
    //     let _ = writer.flush();

    //     Ok(KeyDirEntry::new(file_id, vsz, val_pos, tstamp))
    // }

    /// deletes the given key
    pub fn del(&self, k: impl AsRef<[u8]>) -> Result<bool> {
        // TODO if file almost full, then create new file, bump id
        let k = k.as_ref();
        let k_exists = self.key_dir.has_key(k);
        if k_exists {
            // mark entry as deleted
            let _ = self.put_with_file_size_check(k, b"TOMBSTONE")?;

            // then del from im
            self.key_dir.del(k);
        }

        Ok(k_exists)
    }

    /// merges old files into a single file & generates a hint file
    pub fn merge(&self) -> Result<()> {
        // the goal of merge is to create a hint file.
        // it shoudn't modify/delete any old files until the hint file is completed.
        // it shouldn't modify/delete the active file.
        // it should refer to the current keydir when building the hint file.
        // after the hint file is created, all old files should be deleted.
        //

        // no merging if no old files
        if self.cur_id.load(std::sync::atomic::Ordering::Relaxed) == 0 {
            return Ok(());
        }

        // get all the files in the current cask
        let mut files: Vec<usize> = fs::read_dir(format!("./{}", &self.cur_cask))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                if path.is_file() {
                    if let Some(path) = path.file_name() {
                        if let Some(path) = path.to_str() {
                            path.parse::<usize>().ok()
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // sort them in increasing order starting with file lowest number
        // (an already merged file may also exist)
        files.sort();

        // open a temp file for storing merged data
        let mut temp_file = BufWriter::new(
            File::options()
                .create(true)
                .append(true)
                .open(format!("./{}/temp", self.cur_cask))?,
        );
        let mut temp_file_has_data = false;

        // open a temp file for storing merged data
        let mut hint_file = BufWriter::new(
            File::options()
                .create(true)
                .append(true)
                .open(format!("./{}/hint", self.cur_cask))?,
        );

        let mut cur_val_offset = 0;
        let mut file_entry = DataFileEntry::new();

        // merge all files except the last one (active file)
        for file_id in &files[..files.len() - 1] {
            let mut file_iter =
                OptimizedDataFileIterator::new(format!("./{}/{}", self.cur_cask, file_id))?;

            // for DataFileEntry {
            //     crc,
            //     tstamp,
            //     ksz: _ksz,
            //     vsz: _vsz,
            //     key,
            //     val,
            //     val_pos,
            // } in file_iter.flatten()
            while file_iter.next_into(&mut file_entry).is_some() {
                if let Some(entry) = self.key_dir.get(&file_entry.key) {
                    // key present in keydir

                    // check if the current old file has the valid record verified by presence of
                    // entry in the keydir
                    if entry.file_id == *file_id && entry.val_pos == file_entry.val_pos {
                        // if yes, then the entry is latest and can be recorded in the hint file
                        // and the merged file
                        let entry = to_db_entry(
                            file_entry.crc,
                            file_entry.tstamp,
                            &file_entry.key,
                            &file_entry.val,
                        );
                        let _ = temp_file.write_all(&entry);
                        temp_file_has_data = true;

                        let val_pos = cur_val_offset + 16 + file_entry.key.len() as u64;
                        cur_val_offset += entry.len() as u64;
                        let entry = to_hint_entry(
                            file_entry.tstamp,
                            &file_entry.key,
                            &file_entry.val,
                            val_pos,
                        );
                        let _ = hint_file.write_all(&entry);

                        self.key_dir.put(
                            file_entry.key.clone(),
                            KeyDirEntry {
                                file_id: self.cur_id.load(std::sync::atomic::Ordering::Relaxed) - 1,
                                val_sz: file_entry.val.len() as u32,
                                val_pos,
                                tstamp: file_entry.tstamp,
                            },
                        );
                    } else {
                        // key could be present in the active file or a newer old file getting
                        // processed in future iterations of this loop.
                        // so do nothing.
                    }
                } else {
                    // key deleted so skip processing it
                }
            }
        }

        for file_id in &files[..files.len() - 1] {
            debug!("rming ./{}/{}", self.cur_cask, file_id);

            fs::remove_file(format!("./{}/{}", self.cur_cask, file_id))?;
        }

        debug!(
            "cur id {}",
            self.cur_id.load(std::sync::atomic::Ordering::Relaxed)
        );

        if temp_file_has_data {
            let _res = fs::rename(
                format!("{}/temp", self.cur_cask),
                format!(
                    "{}/{}",
                    self.cur_cask,
                    self.cur_id.load(std::sync::atomic::Ordering::Relaxed) - 1
                ),
            );
        } else {
            fs::remove_file(format!("{}/temp", self.cur_cask))?;
            fs::remove_file(format!("{}/hint", self.cur_cask))?;
        }

        Ok(())
    }

    /// lists all the keys in the store
    pub fn list_all(&self) -> Option<Vec<Bytes>> {
        self.key_dir.keys()
    }
}

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

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::hydradb::HydraDBBuilder;
    use env_logger;
    use log::debug;
    use rand::Rng;

    #[test]
    fn test_del() {
        let mut db = HydraDBBuilder::new()
            .with_cask("del")
            .with_file_limit(60)
            .build()
            .unwrap();
        db.put("pooja", "kalyaninagar").unwrap();
        db.put("abhi", "baner").unwrap();
        db.del("pooja").unwrap();

        assert_eq!(db.key_dir.len(), 1);

        let _ = fs::remove_dir_all("./del");
    }

    #[test]
    fn test_logging_and_reading() {
        let _ = env_logger::builder()
            .is_test(true) // Ensures output is captured by cargo test
            .try_init();
        let mut db = HydraDBBuilder::new()
            .with_cask("names-to-addresses")
            .build()
            .unwrap();
        db.put("pooja", "kalyaninagar").unwrap();
        db.put("abhi", "baner").unwrap();
        db.put("pads", "hinjewadi").unwrap();
        db.put("ashu", "baner").unwrap();
        db.put("swap", "usa").unwrap();
        db.put("jane", "mk").unwrap();

        assert_eq!(db.key_dir.len(), 6);
        let e = db.key_dir.get("pooja").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 21);

        let val = db.get("pooja");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("kalyaninagar".into()));

        let val = db.get("jane");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("mk".into()));

        let val = db.get("ashu");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("baner".into()));

        let _ = fs::remove_dir_all("./names-to-addresses");
    }

    #[test]
    fn test_restore() {
        let db = HydraDBBuilder::new()
            .with_cask("test")
            .with_file_limit(60)
            .build()
            .unwrap();
        assert_eq!(db.key_dir.len(), 6);
        let e = db.key_dir.get("pooja").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 21);
        let e = db.key_dir.get("abhi").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 53);
        let e = db.key_dir.get("pads").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 78);
        let e = db.key_dir.get("jane").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 155);

        let val = db.get("pooja");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("kalyaninagar".into()));
    }

    #[test]
    fn test_list_keys() {
        let mut db = HydraDBBuilder::new()
            .with_cask("names-to-addresses")
            .with_file_limit(60)
            .build()
            .unwrap();
        db.put("pooja", "kalyaninagar").unwrap();
        db.put("abhi", "baner").unwrap();
        db.put("pads", "hinjewadi").unwrap();
        db.put("ashu", "baner").unwrap();
        db.put("swap", "usa").unwrap();
        db.put("jane", "mk").unwrap();
        let keys = db.list_all();
        assert!(keys.is_some());
        let keys = keys.unwrap();
        assert_eq!(keys.len(), 6);

        let _ = fs::remove_dir_all("./names-to-addresses");
    }

    #[test]
    fn test_active_file() {
        let db = HydraDBBuilder::new()
            .with_cask("active_file_test")
            .with_file_limit(60)
            .build()
            .unwrap();
        assert_eq!(db.get_active_file(), 2)
    }

    #[test]
    fn test_split_file() {
        let mut db = HydraDBBuilder::new()
            .with_cask("split_test")
            .with_file_limit(60)
            .build()
            .unwrap();
        db.put("abhi", "rust").unwrap();
        db.put("pads", "java").unwrap();
        assert_eq!(db.get_active_file(), 0);

        db.put("swap", ".net").unwrap();
        assert_eq!(db.get_active_file(), 1);

        let val = db.get("abhi");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("rust".into()));
        let _ = fs::remove_dir_all("./split_test");
    }

    #[test]
    fn test_merge() {
        let _ = env_logger::builder()
            .is_test(true) // Ensures output is captured by cargo test
            .try_init();

        let mut db = HydraDBBuilder::new()
            .with_cask("merge_test")
            .with_file_limit(60)
            .build()
            .unwrap();
        db.put("abhi", "rust").unwrap();
        db.put("pads", "java").unwrap();
        assert_eq!(db.get_active_file(), 0);

        db.put("swap", ".net").unwrap();
        db.put("pooj", "pyth").unwrap();
        assert_eq!(db.get_active_file(), 1);

        db.put("jane", "scal").unwrap();
        db.put("zigg", "blac").unwrap();
        assert_eq!(db.get_active_file(), 2);

        db.put("ashu", "java").unwrap();
        db.put("muma", "mlsp").unwrap();
        assert_eq!(db.get_active_file(), 3);

        db.put("bran", "basi").unwrap();
        db.put("darc", "dlng").unwrap();
        assert_eq!(db.get_active_file(), 4);

        db.put("laur", "lisp").unwrap();
        db.put("dave", "dlng").unwrap();
        assert_eq!(db.get_active_file(), 5);

        db.put("ashu", "scal").unwrap();
        db.put("zigg", "blac").unwrap();
        assert_eq!(db.get_active_file(), 6);

        db.put("nisc", "soli").unwrap();
        db.put("conr", "java").unwrap();
        assert_eq!(db.get_active_file(), 7);

        db.put("jane", "jula").unwrap();
        db.put("bran", "cppl").unwrap();
        assert_eq!(db.get_active_file(), 8);

        db.put("pooj", "dops").unwrap();
        db.put("darn", "rlan").unwrap();
        assert_eq!(db.get_active_file(), 9);

        let val = db.get("abhi");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("rust".into()));

        // merge
        let result = db.merge();
        assert!(result.is_ok());

        let files: Vec<_> = fs::read_dir("./merge_test").unwrap().collect();
        assert_eq!(files.len(), 3);

        let val = db.get("pooj");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("dops".into()));

        let val = db.get("jane");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("jula".into()));

        let _ = fs::remove_dir_all("./merge_test");
    }

    #[test]
    fn test_hint_file_restore() {
        {
            let mut db = HydraDBBuilder::new()
                .with_cask("hint_file_restore_test")
                .with_file_limit(60)
                .build()
                .unwrap();
            db.put("abhi", "rust").unwrap();
            db.put("pads", "java").unwrap();
            assert_eq!(db.get_active_file(), 0);

            db.put("swap", ".net").unwrap();
            db.put("pooj", "pyth").unwrap();
            assert_eq!(db.get_active_file(), 1);

            let val = db.get("abhi");
            assert!(val.is_ok());
            let val = val.unwrap();
            assert_eq!(val, Some("rust".into()));

            let result = db.merge();
            assert!(result.is_ok());
        }
        // the keydir should be now gone

        // restore from hint file
        let _ = HydraDBBuilder::new()
            .with_cask("hint_file_restore_test")
            .with_file_limit(60)
            .build()
            .unwrap();

        let _ = fs::remove_dir_all("./hint_file_restore_test");
    }
}
