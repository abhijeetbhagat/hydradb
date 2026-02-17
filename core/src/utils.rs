use crc32fast::Hasher;

#[inline]
pub fn calc_crc(tstamp: u32, key_sz: u32, val_sz: u32, k: &[u8], v: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&tstamp.to_be_bytes());
    hasher.update(&key_sz.to_be_bytes());
    hasher.update(&val_sz.to_be_bytes());
    hasher.update(k);
    hasher.update(v);
    hasher.finalize()
}

/// returns a raw db header entry to persist from the given data
/// layout: is_deleted(1) + crc(4) + tstamp(4) + ksz(4) + vsz(4) = 17 bytes
#[inline]
pub fn to_db_entry(is_deleted: u8, crc: u32, tstamp: u32, k: &[u8], v: &[u8]) -> [u8; 17] {
    let mut o = [0; 1 + 4 + 4 + 4 + 4];
    o[0] = is_deleted;

    let kl = k.len() as u32;
    let vl = v.len() as u32;

    o[1..=4].copy_from_slice(&crc.to_be_bytes());
    o[5..=8].copy_from_slice(&tstamp.to_be_bytes());
    o[9..=12].copy_from_slice(&kl.to_be_bytes());
    o[13..=16].copy_from_slice(&vl.to_be_bytes());
    o
}

#[inline]
pub fn to_hint_entry(tstamp: u32, k: &[u8], v: &[u8], val_pos: u64) -> Vec<u8> {
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

// --- Transactional versions (include txn_id in the record format) ---

#[inline]
pub fn calc_crc_txn(tstamp: u32, txn_id: u32, key_sz: u32, val_sz: u32, k: &[u8], v: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&tstamp.to_be_bytes());
    hasher.update(&txn_id.to_be_bytes());
    hasher.update(&key_sz.to_be_bytes());
    hasher.update(&val_sz.to_be_bytes());
    hasher.update(k);
    hasher.update(v);
    hasher.finalize()
}

/// layout: is_deleted(1) + crc(4) + tstamp(4) + txn_id(4) + ksz(4) + vsz(4) = 21 bytes
#[inline]
pub fn to_db_entry_txn(
    is_deleted: u8,
    crc: u32,
    tstamp: u32,
    txn_id: u32,
    k: &[u8],
    v: &[u8],
) -> [u8; 21] {
    let mut o = [0; 1 + 4 + 4 + 4 + 4 + 4];
    o[0] = is_deleted;

    let kl = k.len() as u32;
    let vl = v.len() as u32;

    o[1..=4].copy_from_slice(&crc.to_be_bytes());
    o[5..=8].copy_from_slice(&tstamp.to_be_bytes());
    o[9..=12].copy_from_slice(&txn_id.to_be_bytes());
    o[13..=16].copy_from_slice(&kl.to_be_bytes());
    o[17..=20].copy_from_slice(&vl.to_be_bytes());
    o
}

#[inline]
pub fn to_hint_entry_txn(tstamp: u32, txn_id: u32, k: &[u8], v: &[u8], val_pos: u64) -> Vec<u8> {
    let mut o = Vec::with_capacity(4 + 4 + 4 + 4 + 8 + k.len());

    let kl = k.len() as u32;
    let vl = v.len() as u32;

    o.extend_from_slice(&tstamp.to_be_bytes());
    o.extend_from_slice(&txn_id.to_be_bytes());
    o.extend_from_slice(&kl.to_be_bytes());
    o.extend_from_slice(&vl.to_be_bytes());
    o.extend_from_slice(&val_pos.to_be_bytes());
    o.extend_from_slice(k);
    o
}
