use crc32fast::Hasher;

pub fn calc_crc(tstamp: u32, key_sz: u32, val_sz: u32, k: &[u8], v: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&tstamp.to_be_bytes());
    hasher.update(&key_sz.to_be_bytes());
    hasher.update(&val_sz.to_be_bytes());
    hasher.update(k);
    hasher.update(v);
    hasher.finalize()
}
