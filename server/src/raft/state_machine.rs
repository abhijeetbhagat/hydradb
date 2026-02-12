use bincode::Options;
use bytes::Bytes;
use core::builder::HydraDBBuilder;
use core::hydradb::HydraDB;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::Membership;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::RwLock;

use super::{NodeId, Request, Response, TypeConfig};

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub data: Arc<HydraDB>,
}

impl StateMachineData {
    fn new(namespace: String) -> anyhow::Result<Self> {
        Ok(Self {
            last_applied_log: None,
            last_membership: StoredMembership::new(None, Membership::default()),
            data: Arc::new(HydraDBBuilder::new().with_cask(namespace).build()?),
        })
    }
}

/// Defines a state machine for the Raft cluster. This state machine represents a copy of the
/// data for this node. Additionally, it is responsible for storing the last snapshot of the data.
#[derive(Debug)]
pub struct StateMachineStore {
    /// The Raft state machine.
    pub state_machine: RwLock<StateMachineData>,

    /// current namespace
    pub namespace: String,

    /// Used in identifier for snapshot.
    ///
    /// Note that concurrently created snapshots and snapshots created on different nodes
    /// are not guaranteed to have sequential `snapshot_idx` values, but this does not matter for
    /// correctness.
    snapshot_idx: AtomicU64,

    /// The last received snapshot.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl StateMachineStore {
    pub fn new(namespace: String) -> anyhow::Result<Self> {
        Ok(Self {
            // one writer at a time to the db
            state_machine: RwLock::new(StateMachineData::new(namespace.clone())?),
            namespace,
            snapshot_idx: 0.into(),
            current_snapshot: RwLock::new(None),
        })
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Serialize the data of the state machine.
        let mut snapshot_data = vec![];

        let encoding_options = bincode::DefaultOptions::new().with_fixint_encoding();

        let state_machine = self.state_machine.read().await;
        let db = &state_machine.data;

        for entry in db.get_key_entries() {
            let (tstamp, key, val) = entry.map_err(|e| StorageIOError::read_state_machine(&e))?;
            encoding_options.serialize_into(&mut snapshot_data, &(tstamp, key, val));
        }
        // let data = serde_json::to_vec(&state_machine.data)
        //     .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot_data.clone(),
        };

        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut res = Vec::new(); //No `with_capacity`; do not know `len` of iterator

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Response::Blank { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    Request::Put { key, value } => {
                        let data = sm.data.clone();
                        let key = key.clone();
                        let val = value.clone();

                        let result = tokio::task::spawn_blocking(move || {
                            data.put(key, val).map_err(|e| StorageError::IO {
                                source: StorageIOError::new(
                                    ErrorSubject::<NodeId>::Store,
                                    ErrorVerb::Write,
                                    &io::Error::other(e),
                                ),
                            })
                            // Ok::<(), StorageError<NodeId>>(())
                        })
                        .await;

                        let _ = result.map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Store,
                                ErrorVerb::Write,
                                &io::Error::other(e),
                            ),
                        })?;

                        res.push(Response::Put {
                            prev_value: Some(value.clone()),
                        });
                    }
                    Request::Del { key } => {
                        let existed = sm.data.del(key.clone()).map_err(|e| StorageError::IO {
                            source: StorageIOError::new(
                                ErrorSubject::Store,
                                ErrorVerb::Write,
                                &io::Error::other(e),
                            ),
                        })?;
                        res.push(Response::Del { existed })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response::Mem { value: None })
                }
            };
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let snapshot_data = snapshot.into_inner();

        let encoding_options = bincode::DefaultOptions::new().with_fixint_encoding();

        let namespace = format!("{}_restore", self.namespace);
        // cleanup existing snapshot restore dir before installing new snapshot
        if std::path::Path::new(&namespace).exists() {
            std::fs::remove_dir_all(&namespace)
                .map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        }
        let db = HydraDBBuilder::new()
            .with_cask(namespace)
            .build()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let mut reader = Cursor::new(&snapshot_data);

        // Update the state machine.
        while let Ok((tstamp, key, val)) =
            encoding_options.deserialize_from::<_, (u32, Bytes, Bytes)>(&mut reader)
        {
            // todo: we need a bulk api on the db to speed this up
            db.put_with_tstamp(key, val, tstamp)
                .map_err(|e| StorageIOError::write(&e))?;
        }

        let mut state_machine = self.state_machine.write().await;
        state_machine.data = Arc::new(db);
        state_machine.last_applied_log = meta.last_log_id;
        state_machine.last_membership = meta.last_membership.clone();

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        // Update current snapshot.
        *current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: snapshot_data,
        });

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
