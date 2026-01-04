use std::fs::File;
use std::io::{Read, BufReader, Seek};
use anyhow::Result;

use crate::in_mem_kv::{InMemKVStore, InMemEntry};

pub trait Restore {
    fn restore(&self,  file: &str, im_store: &mut InMemKVStore) -> Result<()>;
}

pub struct DataFileRestore;

impl Restore for DataFileRestore {
    fn restore(&self, file: &str, im_store: &mut InMemKVStore) -> Result<()> {
        let cur_id = 0; 

        let file = File::options()
            .read(true)
            .open(file)?;

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

            let entry = InMemEntry::new(cur_id, vsz, val_pos, tstamp);
            im_store.put(String::from_utf8(key).unwrap(), entry);
            i = 4;
            j = 7;
        }

        Ok(())

    }
}

pub struct HintFileRestore; 

impl Restore for HintFileRestore {
    fn restore(&self, file: &str, im_store: &mut InMemKVStore) -> Result<()>{
        let cur_id = 0;

        let file = File::options()
            .read(true)
            .open(file)?;

        let mut reader = BufReader::new(file);
        let mut buf = [0; 4 + 4 + 4 + 8]; // 4 tstamp + 4 ksz + 4 vsz + 8 val_pos
        let mut i = 0; // start reading from the beginning of the hint file
        let mut j = 3;

        while let Ok(size) = reader.read(&mut buf) && size != 0 {
            let tstamp = u32::from_be_bytes(buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;
            let ksz = u32::from_be_bytes(buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;
            let vsz = u32::from_be_bytes(buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 8;
            let val_pos = u64::from_be_bytes(buf[i..=j].try_into().unwrap());

            let mut key = vec![0; ksz as usize];
            reader.read(&mut key)?;

            let entry = InMemEntry::new(cur_id, vsz, val_pos, tstamp);
            im_store.put(String::from_utf8(key).unwrap(), entry);
            i = 0;
            j = 3;
        }

        Ok(()) 
    }
}
