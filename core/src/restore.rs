use crate::data_file_iter::{DataFileEntry, DataFileIterator};
use crate::hint_file_iter::{HintFileEntry, HintFileIterator};
use crate::key_dir::{KeyDir, KeyDirEntry};
use anyhow::Result;

pub trait Restore {
    fn restore(
        &self,
        base_path: &str,
        cask: &str,
        active_file_num: usize,
        key_dir: &mut KeyDir,
    ) -> Result<()>;
}

pub struct DataFileRestore;

impl Restore for DataFileRestore {
    fn restore(
        &self,
        base_path: &str,
        cask: &str,
        active_file_num: usize,
        key_dir: &mut KeyDir,
    ) -> Result<()> {
        let file_iter = DataFileIterator::new(format!("{base_path}/{cask}/{active_file_num}"))?;

        for entry in file_iter {
            if let Ok(DataFileEntry {
                tstamp,
                ksz: _ksz,
                vsz,
                key,
                val: _val,
                val_pos,
            }) = entry
            {
                let key_dir_entry = KeyDirEntry::new(active_file_num, vsz, val_pos, tstamp);
                key_dir.put(String::from_utf8(key).unwrap(), key_dir_entry);
            }
        }

        Ok(())
    }
}

pub struct HintFileRestore;

impl Restore for HintFileRestore {
    fn restore(
        &self,
        base_path: &str,
        cask: &str,
        active_file_num: usize,
        key_dir: &mut KeyDir,
    ) -> Result<()> {
        // if there's a hint file, there's an active file as well.
        // in addition to hint file, process the active file as well.

        // 1. process hint file
        let iter = HintFileIterator::new(format!("{base_path}/{cask}/hint"))?;

        for entry in iter {
            if let Ok(HintFileEntry {
                tstamp,
                ksz,
                vsz,
                key,
                val_pos,
            }) = entry
            {
                let entry = KeyDirEntry::new(active_file_num - 1, vsz, val_pos, tstamp);
                key_dir.put(String::from_utf8(key).unwrap(), entry);
            }
        }

        let file_iter = DataFileIterator::new(format!("{base_path}/{cask}/{active_file_num}"))?;

        for entry in file_iter {
            if let Ok(DataFileEntry {
                tstamp,
                ksz: _ksz,
                vsz,
                key,
                val,
                val_pos,
            }) = entry
            {
                // if entry is deleted, then we remove it from key_dir
                if str::from_utf8(&val).unwrap() == "TOMBSTONE" {
                    key_dir.del(&key);
                } else {
                    // we either insert a key that doesn't exist or overwrite it
                    let entry = KeyDirEntry::new(active_file_num, vsz, val_pos, tstamp);
                    key_dir.put(String::from_utf8(key).unwrap(), entry);
                }
            }
        }

        Ok(())
    }
}
