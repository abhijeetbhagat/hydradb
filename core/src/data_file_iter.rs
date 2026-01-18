use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq, Default)]
pub struct DataFileEntry {
    pub crc: u32,
    pub tstamp: u32,
    pub ksz: u32,
    pub vsz: u32,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub val_pos: u64,
}

impl DataFileEntry {
    pub fn new() -> Self {
        Self {
            key: Vec::with_capacity(32),
            val: Vec::with_capacity(64),
            ..Default::default()
        }
    }
}

/// iterates over a data file
pub struct DataFileIterator {
    buf: [u8; 16], // 4 crc + 4 tstamp + 4 ksz + 4 vsz
    reader: BufReader<File>,
}

impl DataFileIterator {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();
        let file = File::options().read(true).open(&path)?;

        Ok(Self {
            buf: [0; 16],
            reader: BufReader::new(file),
        })
    }
}

impl Iterator for DataFileIterator {
    type Item = Result<DataFileEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(size) = self.reader.read(&mut self.buf)
            && size != 0
        {
            let mut i = 0;
            let mut j = 3;

            let crc = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;

            let tstamp = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;

            let ksz = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;

            let vsz = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());

            // read key using ksz, val using vsz
            let mut key = vec![0; ksz as usize];

            if let Err(e) = self.reader.read_exact(&mut key) {
                return Some(Err(e.into()));
            }

            let val_pos = self.reader.stream_position().unwrap();

            let mut val = vec![0; vsz as usize];
            if let Err(e) = self.reader.read_exact(&mut val) {
                return Some(Err(e.into()));
            }

            let entry = DataFileEntry {
                crc,
                tstamp,
                ksz,
                vsz,
                key,
                val,
                val_pos,
            };

            Some(Ok(entry))
        } else {
            None
        }
    }
}

pub struct OptimizedDataFileIterator {
    buf: [u8; 16], // 4 crc + 4 tstamp + 4 ksz + 4 vsz
    reader: BufReader<File>,
}

impl OptimizedDataFileIterator {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();
        let file = File::options().read(true).open(&path)?;

        Ok(Self {
            buf: [0; 16],
            reader: BufReader::new(file),
        })
    }

    pub fn next_into(&mut self, entry: &mut DataFileEntry) -> Option<Result<()>> {
        if let Ok(size) = self.reader.read(&mut self.buf)
            && size != 0
        {
            let mut i = 0;
            let mut j = 3;

            let crc = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;

            let tstamp = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;

            let ksz = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;

            let vsz = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());

            entry.crc = crc;
            entry.tstamp = tstamp;
            entry.ksz = ksz;
            entry.vsz = vsz;

            // read key using ksz, val using vsz
            // let mut key = vec![0; ksz as usize];
            entry.key.resize(ksz as usize, 0);

            if let Err(e) = self.reader.read_exact(&mut entry.key) {
                return Some(Err(e.into()));
            }

            let val_pos = self.reader.stream_position().unwrap();
            entry.val_pos = val_pos;

            // let mut val = vec![0; vsz as usize];
            entry.val.resize(vsz as usize, 0);
            if let Err(e) = self.reader.read_exact(&mut entry.val) {
                return Some(Err(e.into()));
            }

            // let entry = DataFileEntry {
            //     crc,
            //     tstamp,
            //     ksz,
            //     vsz,
            //     key,
            //     val,
            //     val_pos,
            // };

            Some(Ok(()))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::{self, File};
    use std::io::Write;

    use crate::data_file_iter::{DataFileEntry, DataFileIterator, OptimizedDataFileIterator};

    #[test]
    fn test_data_iter() {
        let mut file = File::options()
            .write(true)
            .create(true)
            .open("data_file_iter_test")
            .unwrap();

        let mut data = vec![];
        data.extend_from_slice(&0u32.to_be_bytes());
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&4u32.to_be_bytes());
        data.extend_from_slice(&4u32.to_be_bytes());
        data.extend_from_slice(b"abhi");
        data.extend_from_slice(b"rust");

        let _ = file.write_all(&data);

        let mut iter = DataFileIterator::new("data_file_iter_test").unwrap();
        let entry = iter.next().unwrap().unwrap();
        assert_eq!(
            entry,
            DataFileEntry {
                crc: 0,
                tstamp: 1,
                ksz: 4,
                vsz: 4,
                key: b"abhi".to_vec(),
                val: b"rust".to_vec(),
                val_pos: 20
            }
        );

        let _ = fs::remove_file("data_file_iter_test");
    }

    #[test]
    fn test_opt_data_iter() {
        let mut file = File::options()
            .write(true)
            .create(true)
            .open("opt_data_file_iter_test")
            .unwrap();

        let mut data = vec![];
        data.extend_from_slice(&0u32.to_be_bytes());
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&4u32.to_be_bytes());
        data.extend_from_slice(&4u32.to_be_bytes());
        data.extend_from_slice(b"abhi");
        data.extend_from_slice(b"rust");

        let _ = file.write_all(&data);

        let mut iter = OptimizedDataFileIterator::new("opt_data_file_iter_test").unwrap();
        let mut entry = DataFileEntry::new();
        iter.next_into(&mut entry).unwrap().unwrap();
        assert_eq!(
            entry,
            DataFileEntry {
                crc: 0,
                tstamp: 1,
                ksz: 4,
                vsz: 4,
                key: b"abhi".to_vec(),
                val: b"rust".to_vec(),
                val_pos: 20
            }
        );

        let _ = fs::remove_file("opt_data_file_iter_test");
    }
}
