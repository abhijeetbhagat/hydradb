use crate::in_mem_kv::{InMemEntry, InMemKVStore};
use crate::merger::*;
use crate::utils::calc_crc;
use anyhow::Result;
use bytes::Bytes;
use core::str;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::{
    fs::{DirBuilder, File},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(not(test))]
const MAX_FILE_SIZE_THRESHOLD: u64 = 1048576;

#[cfg(test)]
const MAX_FILE_SIZE_THRESHOLD: u64 = 60;

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

/// the main bitcask storage engine
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SotraDB {
    cur_cask: String, // dir name of the cur_cask
    cur_id: usize,    // needs to be atomic
    im_store: InMemKVStore,
    cur_file_size: u64,
}

impl SotraDB {
    /// creates an instance of `SotraDB` with the given `namespace`
    pub fn new<T: Into<String> + Debug>(namespace: T) -> Result<Self> {
        let namespace = namespace.into();

        let cur_id;
        let cur_file_size;

        if !fs::exists(format!("./{namespace}"))? {
            let dir_builder = DirBuilder::new();
            dir_builder.create(format!("./{}", &namespace))?;
            cur_id = 0;
            cur_file_size = 0;
        } else {
            let mut mx = 0;
            for entry in fs::read_dir(format!("./{}", &namespace))? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    if let Some(path) = path.file_name() {
                        if let Some(path) = path.to_str() {
                            mx = std::cmp::max(mx, path.parse::<usize>()?)
                        }
                    }
                }
            }

            cur_id = mx;

            let path = format!("./{namespace}/{cur_id}");
            cur_file_size = if Path::new(&path).exists() {
                fs::metadata(path)?.len()
            } else {
                0
            };
        }

        let mut db = Self {
            cur_cask: namespace,
            cur_id,
            im_store: InMemKVStore::new(),
            cur_file_size,
        };

        let _ = db.build_key_dir();

        Ok(db)
    }

    fn get_active_file(&self) -> usize {
        self.cur_id
    }

    /// builds the in-mem store by scanning the data files
    fn build_key_dir(&mut self) -> Result<()> {
        // TODO check for hint file later
        let path;
        let restorer: Box<dyn Restore> = if Path::new(&format!("{}/hint", self.cur_cask)).exists() {
            path = format!("{}/{}", self.cur_cask, "hint");
            Box::new(HintFileRestore)
        } else {
            path = format!("{}/{}", self.cur_cask, self.cur_id);
            Box::new(DataFileRestore)
        };

        restorer.restore(&path, &mut self.im_store)
    }

    /// gets the value, if present, for the given key `k`
    pub fn get(&self, k: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        if let Some(in_mem_entry) = self.im_store.get(k) {
            let InMemEntry {
                file_id,
                val_sz,
                val_pos,
                tstamp: _,
            } = in_mem_entry;
            // println!("val_pos is {val_pos} val sz {val_sz}");

            let mut file = File::options()
                .read(true)
                .open(format!("./{}/{}", self.cur_cask, file_id))?;

            file.seek(SeekFrom::Current(val_pos as i64))?;
            // println!("file pos is {}", file.stream_pos);

            let mut v = Vec::with_capacity(val_sz as usize);
            let mut f = file.take(val_sz as u64);

            f.read_to_end(&mut v)?;

            Ok(Some(v.into()))
        } else {
            Ok(None)
        }
    }

    /// puts the given key-value pair under the set namespace
    pub fn put(&mut self, k: impl Into<Bytes>, v: impl Into<Bytes>) -> Result<()> {
        // first write to cur file
        // TODO if file almost full, then create new file, bump id
        let k = k.into();
        let v = v.into();

        if (16u64 + k.len() as u64 + v.len() as u64 + self.cur_file_size) > MAX_FILE_SIZE_THRESHOLD
        {
            self.cur_id += 1;
            self.cur_file_size = 0;
        }

        let entry = self.persist(&k, &v)?;
        println!("entry inserted: {:?}", entry);

        self.cur_file_size += 16u64 + k.len() as u64 + v.len() as u64;

        // then write to im
        self.im_store.put(k, entry);

        Ok(())
    }

    /// deletes the given key
    pub fn del(&mut self, k: impl AsRef<[u8]>) -> Result<bool> {
        // TODO if file almost full, then create new file, bump id
        let k = k.as_ref();
        let k_exists = self.im_store.has_key(k);
        if k_exists {
            // mark entry as deleted
            let _entry = self.persist(k, b"TOMBSTONE")?;

            // then del from im
            self.im_store.del(k);
        }

        Ok(k_exists)
    }

    /// merges old files into a single file & generates a hint file
    pub fn merge(&mut self) -> Result<()> {
        // the goal of merge is to create a hint file.
        // it shoudn't modify/delete any old files until the hint file is completed.
        // it shouldn't modify/delete the active file.
        // it should refer to the current keydir when building the hint file.
        //
        //

        // no merging if no old files
        if self.cur_id == 0 {
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
        let mut temp_file = File::options()
            .create(true)
            .append(true)
            .open(format!("./{}/temp", self.cur_cask))?;

        // open a temp file for storing merged data
        let mut hint_file = File::options()
            .create(true)
            .append(true)
            .open(format!("./{}/hint", self.cur_cask))?;

        // merge all files except the last one
        for file_id in &files[..files.len() - 1] {
            let file = File::options()
                .read(true)
                .open(format!("./{}/{}", self.cur_cask, file_id))?;
            println!("reading from file ./{}/{}", self.cur_cask, file_id);

            let mut reader = BufReader::new(file);
            let mut buf = [0; 4 + 4 + 4 + 4]; // 4 crc + 4 tstamp + 4 ksz + 4 vsz
            let mut i = 0;
            let mut j = 4;

            while reader.read_exact(&mut buf).is_ok() {
                println!("buf read: {:?}", buf);
                let crc = u32::from_be_bytes(buf[i..j].try_into().unwrap());
                i = j;
                j += 4;
                let tstamp = u32::from_be_bytes(buf[i..j].try_into().unwrap());
                i = j;
                j += 4;
                let ksz = u32::from_be_bytes(buf[i..j].try_into().unwrap());
                i = j;
                j += 4;
                let vsz = u32::from_be_bytes(buf[i..j].try_into().unwrap());
                // read key using ksz, val using vsz
                let mut key = vec![0; ksz as usize];
                reader.read_exact(&mut key)?;

                let val_pos = reader.stream_position().unwrap();

                let mut val = vec![0; vsz as usize];
                reader.read_exact(&mut val)?;

                if let Some(entry) = self.im_store.get(&key) {
                    // key present in keydir

                    // check if the current old file has the valid record verified by presence of
                    // entry in the keydir
                    if entry.file_id == *file_id && entry.val_pos as u64 == val_pos {
                        let entry = to_db_entry(crc, tstamp, &key, &val);
                        let _ = temp_file.write_all(&entry);

                        let entry = to_hint_entry(tstamp, &key, &val, val_pos);
                        let _ = hint_file.write_all(&entry);
                    } else {
                        // key could be present in the active file or a newer old file getting
                        // processed in future iterations of this loop.
                        // so do nothing.
                    }
                } else {
                    // key deleted so skip processing it
                }

                i = 0;
                j = 4;
            }
        }

        // todo rename the temp file to cur_id - 1

        Ok(())
    }

    /// lists all the keys in the store
    pub fn list_all(&self) -> Option<Vec<Bytes>> {
        self.im_store.keys()
    }

    fn persist(&mut self, k: &[u8], v: &[u8]) -> Result<InMemEntry> {
        let mut file = File::options()
            .create(true)
            .append(true)
            .open(format!("./{}/{}", self.cur_cask, self.cur_id))?;
        let file_id = self.cur_id;
        let ksz = k.len() as u32;
        let val_pos = file.seek(SeekFrom::End(0))? + 4 + 4 + 4 + 4 + ksz as u64;
        let vsz = v.len() as u32;
        let tstamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u32;
        let crc = calc_crc(tstamp, ksz, vsz, k, v);

        let entry = to_db_entry(crc, tstamp, k, v);
        let _ = file.write_all(&entry);

        Ok(InMemEntry::new(file_id, vsz, val_pos, tstamp))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::sotradb::SotraDB;

    #[test]
    fn test_del() {
        let mut db = SotraDB::new("del").unwrap();
        db.put("pooja", "kalyaninagar").unwrap();
        db.put("abhi", "baner").unwrap();
        db.del("pooja").unwrap();

        assert_eq!(db.im_store.len(), 1);

        let _ = fs::remove_dir_all("./del");
    }

    #[test]
    fn test_logging_and_reading() {
        let mut db = SotraDB::new("names-to-addresses").unwrap();
        db.put("pooja", "kalyaninagar").unwrap();
        db.put("abhi", "baner").unwrap();
        db.put("pads", "hinjewadi").unwrap();
        db.put("ashu", "baner").unwrap();
        db.put("swap", "usa").unwrap();
        db.put("jane", "mk").unwrap();

        assert_eq!(db.im_store.len(), 6);
        let e = db.im_store.get("pooja").unwrap();
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

        let _ = fs::remove_dir_all("./names-to-addresses");
    }

    #[test]
    fn test_restore() {
        let db = SotraDB::new("test").unwrap();
        assert_eq!(db.im_store.len(), 6);
        let e = db.im_store.get("pooja").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 21);
        let e = db.im_store.get("abhi").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 53);
        let e = db.im_store.get("pads").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 78);
        let e = db.im_store.get("jane").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 155);

        let val = db.get("pooja");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("kalyaninagar".into()));
    }

    #[test]
    fn test_list_keys() {
        let mut db = SotraDB::new("names-to-addresses").unwrap();
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
        let db = SotraDB::new("active_file_test").unwrap();
        assert_eq!(db.get_active_file(), 2)
    }

    #[test]
    fn test_split_file() {
        let mut db = SotraDB::new("split_test").unwrap();
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
        let mut db = SotraDB::new("merge_test").unwrap();
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

        db.merge();

        let _ = fs::remove_dir_all("./merge_test");
    }
}
