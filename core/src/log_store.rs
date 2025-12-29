use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::u64;

use openraft::storage::LogFlushed;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogId;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use sled::IVec;
use tokio::sync::Mutex;

/// RaftLogStore implementation with a in-memory storage
#[derive(Clone, Debug, Default)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

#[derive(Debug)]
pub struct LogStoreInner<C: RaftTypeConfig> {
    /// The last purged log id.
    last_purged_log_id: Option<LogId<C::NodeId>>,

    /// The Raft log.
    log: sled::Db,

    log_state: sled::Db,

    /// The commit log id.
    committed: Option<LogId<C::NodeId>>,

    /// The current granted vote.
    vote: Option<Vote<C::NodeId>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            // TODO handle opening the db
            log: sled::open("raft_log").unwrap(),
            log_state: sled::open("raft_log_state").unwrap(),
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> LogStoreInner<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>
    where
        C::Entry: Clone,
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
            .collect::<Result<Vec<C::Entry>, StorageError<C::NodeId>>>()
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let last = self.log.iter().next_back().map(|res| {
            let (_, val) = res.unwrap();
            let entry = serde_json::from_slice::<C::Entry>(&val)
                .map_err(|e| StorageError::<C::NodeId>::IO {
                    source: StorageIOError::read_logs(&e),
                })
                .unwrap();

            entry.get_log_id().clone()
        });

        let last_purged = self.last_purged_log_id.clone();

        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.vote.clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry>,
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

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
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

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }

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

    use openraft::storage::LogFlushed;
    use openraft::storage::RaftLogStorage;
    use openraft::LogId;
    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::RaftTypeConfig;
    use openraft::StorageError;
    use openraft::Vote;

    use crate::log_store::LogStore;

    impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C>
    where
        C::Entry: Clone,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.try_get_log_entries(range).await
        }
    }

    impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C>
    where
        C::Entry: Clone,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.get_log_state().await
        }

        async fn save_committed(
            &mut self,
            committed: Option<LogId<C::NodeId>>,
        ) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.save_committed(committed).await
        }

        async fn read_committed(
            &mut self,
        ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.read_committed().await
        }

        async fn save_vote(
            &mut self,
            vote: &Vote<C::NodeId>,
        ) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.save_vote(vote).await
        }

        async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.read_vote().await
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: LogFlushed<C>,
        ) -> Result<(), StorageError<C::NodeId>>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let mut inner = self.inner.lock().await;
            inner.append(entries, callback).await
        }

        async fn truncate(
            &mut self,
            log_id: LogId<C::NodeId>,
        ) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.truncate(log_id).await
        }

        async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.purge(log_id).await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}
