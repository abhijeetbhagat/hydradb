use crate::in_mem_kv::{InMemEntry, InMemKVStore};
use crate::utils::calc_crc;
use anyhow::Result;
use core::str;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::io::{Seek, SeekFrom};
use std::mem::take;
use std::{
    fs::{DirBuilder, File},
    time::{SystemTime, UNIX_EPOCH},
};

struct DBEntry {
    crc: u32,
    tstamp: u32,
    ksz: usize,
    vsz: usize,
    key: Vec<u8>,
    val: Vec<u8>,
}

fn to_db_entry(crc: u32, tstamp: u32, k: &[u8], v: &[u8]) -> Vec<u8> {
    let mut o = vec![];
    o.extend_from_slice(&crc.to_be_bytes());
    o.extend_from_slice(&tstamp.to_be_bytes());
    o.extend_from_slice(&k.len().to_be_bytes());
    o.extend_from_slice(&v.len().to_be_bytes());
    o.extend_from_slice(&k);
    o.extend_from_slice(&v);
    o
}

/// the main bitcask storage engine
pub struct SotraDB {
    cur_cask: String, // dir name of the cur_cask
    cur_id: usize,    // needs to be atomic
    im_store: InMemKVStore,
}

impl SotraDB {
    /// creates an instance of `SotraDB` with the given `namespace`
    pub fn new<T: Into<String> + Debug>(namespace: T) -> Result<Self> {
        let dir_builder = DirBuilder::new();
        let namespace = namespace.into();
        dir_builder.create(format!("./{}", &namespace))?;

        Ok(Self {
            cur_cask: namespace,
            cur_id: 0,
            im_store: InMemKVStore::new(),
        })
    }

    /// gets the value, if present, for the given key `k`
    pub fn get(&self, k: &str) -> Result<Option<String>> {
        if let Some(in_mem_entry) = self.im_store.get(k) {
            let InMemEntry {
                file_id,
                val_sz,
                val_pos,
                tstamp,
            } = in_mem_entry;
            // println!("val_pos is {val_pos} val sz {val_sz}");

            let mut file = File::options()
                .read(true)
                .open(format!("./{}/{}", self.cur_cask, self.cur_id))?;
            file.seek(SeekFrom::Current(val_pos as i64))?;
            // println!("file pos is {}", file.stream_pos);

            let mut v = Vec::with_capacity(val_sz);
            let mut f = file.take(val_sz as u64);

            f.read_to_end(&mut v)?;

            Ok(Some(str::from_utf8(&v).unwrap().to_string()))
        } else {
            return Ok(None);
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

    pub fn persist(&mut self, k: &[u8], v: &[u8]) -> Result<InMemEntry> {
        let mut file = File::options()
            .create(true)
            .append(true)
            .open(format!("./{}/{}", self.cur_cask, self.cur_id))?;
        let file_id = self.cur_id;
        let ksz = k.len();
        let val_pos = file.seek(SeekFrom::End(0))? as usize + 4 + 4 + 8 + 8 + ksz;
        let vsz = v.len();
        let tstamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u32;
        let crc = calc_crc(tstamp, ksz, vsz, k, v);

        let entry = to_db_entry(crc, tstamp, k, v);
        let _ = file.write_all(&entry);

        Ok(InMemEntry::new(file_id, vsz, val_pos, tstamp))
    }
}

mod tests {
    use crate::sotradb::SotraDB;

    #[test]
    fn test_logging_and_reading() {
        let mut db = SotraDB::new("names-to-addresses").unwrap();
        let _ = db.put("pooja", "kalyaninagar").unwrap();
        let _ = db.put("abhi", "baner").unwrap();
        let _ = db.put("pads", "hinjewadi").unwrap();
        let _ = db.put("ashu", "baner").unwrap();
        let _ = db.put("swap", "usa").unwrap();
        let _ = db.put("jane", "mk").unwrap();

        assert_eq!(db.im_store.len(), 6);
        let e = db.im_store.get("pooja").unwrap();
        assert_eq!(e.file_id, 0);
        assert_eq!(e.val_pos, 29);

        let val = db.get("pooja");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("kalyaninagar".into()));

        let val = db.get("jane");
        assert!(val.is_ok());
        let val = val.unwrap();
        assert_eq!(val, Some("mk".into()))
    }
}
