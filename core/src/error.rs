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

    #[error("write write conflict")]
    WriteWriteConflict,
}

pub type HydraDBResult<T> = Result<T, HydraDBError>;

impl PartialEq for HydraDBError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                HydraDBError::FileCorruptionError(l_file_id, l_record_crc, l_computed_crc),
                HydraDBError::FileCorruptionError(r_file_id, r_record_crc, r_computed_crc),
            ) => {
                l_file_id == r_file_id
                    && l_record_crc == r_record_crc
                    && l_computed_crc == r_computed_crc
            }
            (HydraDBError::IoError(l_err), HydraDBError::IoError(r_err)) => {
                l_err.kind() == r_err.kind()
            }
            (HydraDBError::InvalidFileIdError(l_err), HydraDBError::InvalidFileIdError(r_err)) => {
                l_err.to_string() == r_err.to_string()
            }
            (HydraDBError::SystemTimeError(l_err), HydraDBError::SystemTimeError(r_err)) => {
                l_err.duration() == r_err.duration()
            }
            (HydraDBError::WriteWriteConflict, HydraDBError::WriteWriteConflict) => true,
            _ => false,
        }
    }
}
