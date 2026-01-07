use std::fs::File;
use std::io::{Read, BufReader, Seek};
use anyhow::Result;
use crate::data_file_iter::{DataFileEntry, DataFileIterator};

use crate::key_dir::{KeyDir, KeyDirEntry};

pub trait Restore {
    fn restore(&self, base_path: &str, cask: &str, active_file_num: usize, key_dir: &mut KeyDir) -> Result<()>;
}

pub struct DataFileRestore;

impl Restore for DataFileRestore {
    fn restore(&self, base_path: &str, cask: &str, active_file_num: usize, key_dir: &mut KeyDir) -> Result<()> {

        let file_iter = DataFileIterator::new(format!("{base_path}/{cask}/{active_file_num}"))?;

        for entry in file_iter {
            if let Ok(DataFileEntry {tstamp, ksz: _ksz, vsz, key, val: _val, val_pos}) = entry {
                let key_dir_entry = KeyDirEntry::new(active_file_num, vsz, val_pos, tstamp);
                key_dir.put(String::from_utf8(key).unwrap(), key_dir_entry);
            }
        }

        Ok(())
    }
}

pub struct HintFileRestore; 

impl Restore for HintFileRestore {
    fn restore(&self, base_path: &str, cask: &str, active_file_num: usize, key_dir: &mut KeyDir) -> Result<()>{
        // if there's a hint file, there's an active file as well.
        // in addition to hint file, process the active file as well.
        
        // 1. process hint file
        let file = File::options()
            .read(true)
            .open(format!("{base_path}/{cask}/hint"))?;


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
            reader.read_exact(&mut key)?;

            let entry = KeyDirEntry::new(active_file_num - 1, vsz, val_pos, tstamp);
            key_dir.put(String::from_utf8(key).unwrap(), entry);
            i = 0;
            j = 3;
        }

        // 2. process the active file
        let file = File::options()
            .read(true)
            .open(format!("{base_path}/{cask}/{active_file_num}"))?;

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
            reader.read_exact(&mut key)?;
            let val_pos = reader.stream_position().unwrap();
            let mut val = vec![0; vsz as usize];
            reader.read_exact(&mut val)?;

            // if entry is deleted, then we remove it from key_dir
            if str::from_utf8(&val).unwrap() == "TOMBSTONE" {
                key_dir.del(&key);
            } else {
                // we either insert a key that doesn't exist or overwrite it
                let entry = KeyDirEntry::new(active_file_num, vsz, val_pos, tstamp);
                key_dir.put(String::from_utf8(key).unwrap(), entry);
            }

            i = 4;
            j = 7;
        }

        Ok(()) 
    }
}

