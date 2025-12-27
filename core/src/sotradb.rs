use crate::in_mem_kv::{InMemEntry, InMemKVStore};
use crate::utils::calc_crc;
use anyhow::Result;
use core::str;
use std::fmt::Debug;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::io::{Seek, SeekFrom};
use std::{
    fs::{DirBuilder, File},
    time::{SystemTime, UNIX_EPOCH},
};
use serde::{Serialize, Deserialize};

struct DBEntry {
    crc: u32,
    tstamp: u32,
    ksz: u32,
    vsz: u32,
    key: Vec<u8>,
    val: Vec<u8>,
}

/// returns a raw db entry to persist from the given data
fn to_db_entry(crc: u32, tstamp: u32, k: &[u8], v: &[u8]) -> Vec<u8> {
    let mut o = Vec::with_capacity(4 + 4 + k.len() + v.len());
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

/// the main bitcask storage engine
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SotraDB {
    cur_cask: String, // dir name of the cur_cask
    cur_id: usize,    // needs to be atomic
    im_store: InMemKVStore,
}

impl SotraDB {
    /// creates an instance of `SotraDB` with the given `namespace`
    pub fn new<T: Into<String> + Debug>(namespace: T) -> Result<Self> {
        let namespace = namespace.into();

        if !fs::exists(format!("./{namespace}"))? {
            let dir_builder = DirBuilder::new();
            dir_builder.create(format!("./{}", &namespace))?;
        }

        let mut db = Self {
            cur_cask: namespace,
            cur_id: 0,
            im_store: InMemKVStore::new(),
        };

        db.restore();

        Ok(db)
    }

    /// builds the in-mem store by scanning the data files
    fn restore(&mut self) -> Result<()> {
        // TODO check for hint file later
        let file = File::options()
            .read(true)
            .open(format!("./{}/{}", self.cur_cask, self.cur_id))?;

        let mut reader = BufReader::new(file);
        let mut buf = [0; 4 + 4 + 4 + 4]; // 4 crc + 4 tstamp + 4 ksz + 4 vsz
        let mut i = 4;
        let mut j = 7;

        while let Ok(size) = reader.read(&mut buf) && size != 0 {
            let tstamp = u32::from_be_bytes(buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;
            let ksz = u32::from_be_bytes(buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;
            let vsz = u32::from_be_bytes(buf[i..=j].try_into().unwrap());
            // read key using ksz, val using vsz
            let mut key = vec![0; ksz as usize];
            reader.read(&mut key)?;
            let val_pos = reader.stream_position().unwrap();
            let mut val = vec![0; vsz as usize];
            reader.read(&mut val)?;

            let entry = InMemEntry::new(self.cur_id, vsz, val_pos as usize, tstamp);
            self.im_store.put(String::from_utf8(key).unwrap(), entry);
            i = 4;
            j = 7;
        }

        Ok(())
    }

    /// gets the value, if present, for the given key `k`
    pub fn get(&self, k: &str) -> Result<Option<String>> {
        if let Some(in_mem_entry) = self.im_store.get(k) {
            let InMemEntry {
                file_id: _,
                val_sz,
                val_pos,
                tstamp: _,
            } = in_mem_entry;
            // println!("val_pos is {val_pos} val sz {val_sz}");

            let mut file = File::options()
                .read(true)
                .open(format!("./{}/{}", self.cur_cask, self.cur_id))?;
            file.seek(SeekFrom::Current(val_pos as i64))?;
            // println!("file pos is {}", file.stream_pos);

            let mut v = Vec::with_capacity(val_sz as usize);
            let mut f = file.take(val_sz as u64);

            f.read_to_end(&mut v)?;

            Ok(Some(str::from_utf8(&v).unwrap().to_string()))
        } else {
            Ok(None)
        }
    }

    /// puts the given key-value pair under the set namespace
    pub fn put<S: Into<String>>(&mut self, k: S, v: S) -> Result<()> {
        // first write to cur file
        // TODO if file almost full, then create new file, bump id
        let k = k.into();
        let entry = self.persist(k.as_bytes(), v.into().as_bytes())?;

        // then write to im
        self.im_store.put(k, entry);

        Ok(())
    }

    /// deletes the given key
    pub fn del<S: Into<String>>(&mut self, k: S) -> Result<bool> {
        // TODO if file almost full, then create new file, bump id
        let k = k.into();
        let k_exists = self.im_store.has_key(&k);
        if k_exists {
            // mark entry as deleted
            let _entry = self.persist(k.as_bytes(), b"TOMBSTONE")?;

            // then del from im
            self.im_store.del(&k);
        }

        Ok(k_exists)
    }

    /// lists all the keys in the store
    pub fn list_all(&self) -> Option<Vec<String>> {
        self.im_store.keys()
    }

    fn persist(&mut self, k: &[u8], v: &[u8]) -> Result<InMemEntry> {
        let mut file = File::options()
            .create(true)
            .append(true)
            .open(format!("./{}/{}", self.cur_cask, self.cur_id))?;
        let file_id = self.cur_id;
        let ksz = k.len() as u32;
        let val_pos = file.seek(SeekFrom::End(0))? as usize + 4 + 4 + 4 + 4 + ksz as usize;
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
}
