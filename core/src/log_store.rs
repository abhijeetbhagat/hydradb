use crate::NodeId;
use crate::TypeConfig;
use std::fmt::Debug;
use std::io;
use std::ops::Bound;
use std::ops::RangeBounds;

use openraft::storage::LogFlushed;
use openraft::Entry;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogId;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use sled::IVec;

/// RaftLogStore implementation with a in-memory storage
#[derive(Clone, Debug)]
pub struct LogStore {
    /// The Raft log.
    log: sled::Db,

    /// The Raft state log (to store last_purged_log_id, committed state, vote)
    log_state: sled::Db,
}

impl Default for LogStore {
    fn default() -> Self {
        Self {
            // TODO handle opening the db
            log: sled::open("raft_log").unwrap(),
            log_state: sled::open("raft_log_state").unwrap(),
        }
    }
}

impl LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>>
    where
        Entry<TypeConfig>: Clone,
    {
        let start = match range.start_bound() {
            Bound::Included(&s) => Bound::Included(s.to_be_bytes()),
            Bound::Excluded(&s) => Bound::Excluded(s.to_be_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(&s) => Bound::Included(s.to_be_bytes()),
            Bound::Excluded(&s) => Bound::Excluded(s.to_be_bytes()),
            Bound::Unbounded => Bound::Unbounded,
        };

        self.log
            .range((start, end))
            .values()
            .map(|res| {
                let v = res.unwrap();
                serde_json::from_slice(&v).map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(&e),
                })
            })
            .collect::<Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>>>()
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last = self.log.iter().next_back().map(|res| {
            let (_, val) = res.unwrap();
            let entry = serde_json::from_slice::<Entry<TypeConfig>>(&val)
                .map_err(|e| StorageError::<NodeId>::IO {
                    source: StorageIOError::read_logs(&e),
                })
                .unwrap();

            entry.get_log_id().clone()
        });

        let last_purged_log_id = self
            .log_state
            .get("last_purged_log_id".as_bytes())
            .map_err(|e| StorageIOError::read(&e))?;
        let last_purged_log_id = match last_purged_log_id {
            Some(bytes) => {
                Some(serde_json::from_slice(&bytes).map_err(|e| StorageIOError::read(&e))?)
            }
            None => None,
        };

        let last = match last {
            None => last_purged_log_id.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.log_state
            .insert(
                b"committed",
                serde_json::to_vec(&committed).map_err(|e| StorageIOError::write_logs(&e))?,
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Write,
                    &io::Error::new(io::ErrorKind::Other, e),
                ),
            })?;

        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let committed = self
            .log_state
            .get(b"committed")
            .map_err(|e| StorageIOError::read(&e))?;
        let committed = match committed {
            Some(bytes) => {
                Some(serde_json::from_slice(&bytes).map_err(|e| StorageIOError::read(&e))?)
            }
            None => None,
        };

        Ok(committed)
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log_state
            .insert(
                b"vote",
                serde_json::to_vec(&vote).map_err(|e| StorageIOError::write_logs(&e))?,
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Write,
                    &io::Error::new(io::ErrorKind::Other, e),
                ),
            })?;

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let vote = self
            .log_state
            .get(b"vote")
            .map_err(|e| StorageIOError::read(&e))?;
        let vote = match vote {
            Some(bytes) => {
                Some(serde_json::from_slice(&bytes).map_err(|e| StorageIOError::read(&e))?)
            }
            None => None,
        };

        Ok(vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>>,
    {
        // Simple implementation that calls the flush-before-return `append_to_log`.
        for entry in entries {
            self.log.insert(
                u64::to_be_bytes(entry.get_log_id().index),
                serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?,
            );
        }
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let keys: Vec<IVec> = self
            .log
            .range(u64::to_be_bytes(log_id.index)..u64::to_be_bytes(u64::MAX))
            .keys()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    &io::Error::new(io::ErrorKind::Other, e),
                ),
            })?;

        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log_state.insert(
            b"last_purged_log_id",
            serde_json::to_vec(&log_id).map_err(|e| StorageIOError::write_logs(&e))?,
        );

        {
            let keys = self
                .log
                .range(u64::to_be_bytes(0)..=u64::to_be_bytes(log_id.index))
                .keys()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Store,
                        ErrorVerb::Read,
                        &io::Error::new(io::ErrorKind::Other, e),
                    ),
                })?;

            for key in keys {
                self.log.remove(&key);
            }
        }

        Ok(())
    }
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::ops::RangeBounds;

    use crate::NodeId;
    use openraft::storage::LogFlushed;
    use openraft::storage::RaftLogStorage;
    use openraft::Entry;
    use openraft::LogId;
    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::StorageError;
    use openraft::Vote;

    use crate::log_store::LogStore;
    use crate::TypeConfig;

    impl RaftLogReader<TypeConfig> for LogStore
    where
        Entry<TypeConfig>: Clone,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
            self.try_get_log_entries(range).await
        }
    }

    impl RaftLogStorage<TypeConfig> for LogStore
    where
        Entry<TypeConfig>: Clone,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
            self.get_log_state().await
        }

        async fn save_committed(
            &mut self,
            committed: Option<LogId<NodeId>>,
        ) -> Result<(), StorageError<NodeId>> {
            self.save_committed(committed).await
        }

        async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
            self.read_committed().await
        }

        async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
            self.save_vote(vote).await
        }

        async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
            self.read_vote().await
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: LogFlushed<TypeConfig>,
        ) -> Result<(), StorageError<NodeId>>
        where
            I: IntoIterator<Item = Entry<TypeConfig>>,
        {
            self.append(entries, callback).await
        }

        async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
            self.truncate(log_id).await
        }

        async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
            self.purge(log_id).await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}
