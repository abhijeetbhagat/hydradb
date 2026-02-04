use std::{num::ParseIntError, time::SystemTimeError};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum HydraDBError {
    #[error("data corruption detected: file id {0}, file record crc {1}, computed crc {2}")]
    FileCorruptionError(usize, u32, u32),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("file id parsing error: {0}")]
    InvalidFileIdError(#[from] ParseIntError),

    #[error("sys time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
}

pub type HydraDBResult<T> = Result<T, HydraDBError>;
