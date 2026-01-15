use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

/// iterates over a hint file
pub struct HintFileIterator {
    buf: [u8; 20], // 4 crc + 4 tstamp + 4 vsz + 8 val_pos
    reader: BufReader<File>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct HintFileEntry {
    pub tstamp: u32,
    pub ksz: u32,
    pub vsz: u32,
    pub key: Vec<u8>,
    pub val_pos: u64,
}

impl HintFileIterator {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();

        let file = File::options().read(true).open(&path)?;

        Ok(Self {
            buf: [0; 4 + 4 + 4 + 8],
            reader: BufReader::new(file),
        })
    }
}

impl Iterator for HintFileIterator {
    type Item = Result<HintFileEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(size) = self.reader.read(&mut self.buf)
            && size != 0
        {
            let mut i = 0;
            let mut j = 3;

            let tstamp = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;
            let ksz = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 4;
            let vsz = u32::from_be_bytes(self.buf[i..=j].try_into().unwrap());
            i = j + 1;
            j += 8;
            let val_pos = u64::from_be_bytes(self.buf[i..=j].try_into().unwrap());

            let mut key = vec![0; ksz as usize];
            if let Err(e) = self.reader.read_exact(&mut key) {
                return Some(Err(e.into()));
            }

            let entry = HintFileEntry {
                tstamp,
                ksz,
                vsz,
                key,
                val_pos,
            };

            Some(Ok(entry))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use crate::hint_file_iter::{HintFileEntry, HintFileIterator};
    use std::fs::{self, File};
    use std::io::Write;

    #[test]
    fn test_hint_iter() {
        let mut file = File::options()
            .write(true)
            .create(true)
            .open("hint_file_iter_test")
            .unwrap();

        let mut data = vec![];
        data.extend_from_slice(&1u32.to_be_bytes()); // ts
        data.extend_from_slice(&4u32.to_be_bytes()); // ksz
        data.extend_from_slice(&4u32.to_be_bytes()); // vsz
        data.extend_from_slice(&20u64.to_be_bytes()); // v_pos
        data.extend_from_slice(b"abhi"); // key

        let _ = file.write_all(&data);

        let mut iter = HintFileIterator::new("hint_file_iter_test").unwrap();
        let entry = iter.next().unwrap().unwrap();
        assert_eq!(
            entry,
            HintFileEntry {
                tstamp: 1,
                ksz: 4,
                vsz: 4,
                key: b"abhi".to_vec(),
                val_pos: 20
            }
        );

        let _ = fs::remove_file("hint_file_iter_test");
    }
}
