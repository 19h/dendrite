//! Transactional redb-backed daemon state.

use std::{io, path::Path, sync::Arc};

use dendrite_core::{Sha1Hash, Sha256Hash, TorrentId, TorrentState};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

const CURRENT_SCHEMA: u32 = 4;
const META: TableDefinition<'static, &str, u64> = TableDefinition::new("meta");
const TORRENTS: TableDefinition<'static, &[u8], &[u8]> = TableDefinition::new("torrents");
const PROGRESS: TableDefinition<'static, &[u8], &[u8]> = TableDefinition::new("progress");
const HASH_INDEX: TableDefinition<'static, &[u8], &[u8]> = TableDefinition::new("hash_index");
const QUARANTINE: TableDefinition<'static, &[u8], &[u8]> = TableDefinition::new("quarantine");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TorrentRecord {
    pub record_version: u16,
    pub id: TorrentId,
    pub name: String,
    pub state: TorrentState,
    pub v1_info_hash: Option<Sha1Hash>,
    pub v2_info_hash: Option<Sha256Hash>,
    pub total_length: u64,
    pub raw_metainfo: Vec<u8>,
    pub magnet_uri: Option<String>,
    pub stop_on_complete: bool,
    pub completed_pieces: Vec<u8>,
    pub downloaded: u64,
    pub uploaded: u64,
    pub added_at_unix_ms: u64,
}

impl TorrentRecord {
    pub const RECORD_VERSION: u16 = 2;
}

#[derive(Serialize, Deserialize)]
struct TorrentRecordV1 {
    record_version: u16,
    id: TorrentId,
    name: String,
    state: TorrentState,
    v1_info_hash: Option<Sha1Hash>,
    v2_info_hash: Option<Sha256Hash>,
    total_length: u64,
    raw_metainfo: Vec<u8>,
    magnet_uri: Option<String>,
    completed_pieces: Vec<u8>,
    downloaded: u64,
    uploaded: u64,
    added_at_unix_ms: u64,
}

impl From<TorrentRecordV1> for TorrentRecord {
    fn from(record: TorrentRecordV1) -> Self {
        Self {
            record_version: Self::RECORD_VERSION,
            id: record.id,
            name: record.name,
            state: record.state,
            v1_info_hash: record.v1_info_hash,
            v2_info_hash: record.v2_info_hash,
            total_length: record.total_length,
            raw_metainfo: record.raw_metainfo,
            magnet_uri: record.magnet_uri,
            stop_on_complete: false,
            completed_pieces: record.completed_pieces,
            downloaded: record.downloaded,
            uploaded: record.uploaded,
            added_at_unix_ms: record.added_at_unix_ms,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TorrentProgress {
    state: TorrentState,
    completed_pieces: Vec<u8>,
    downloaded: u64,
    uploaded: u64,
}

impl TorrentProgress {
    fn from_record(record: &TorrentRecord) -> Self {
        Self {
            state: record.state,
            completed_pieces: record.completed_pieces.clone(),
            downloaded: record.downloaded,
            uploaded: record.uploaded,
        }
    }

    fn apply_to(self, record: &mut TorrentRecord) {
        record.state = self.state;
        record.completed_pieces = self.completed_pieces;
        record.downloaded = self.downloaded;
        record.uploaded = self.uploaded;
    }
}

struct DownloadProgress {
    id: TorrentId,
    state: TorrentState,
    completed_pieces: Vec<u8>,
    downloaded: u64,
}

#[derive(Clone)]
pub struct StateStore {
    database: Arc<Database>,
    #[cfg(feature = "fault-injection")]
    commit_fault: CommitFaultHandle,
}

#[derive(Clone, Debug)]
pub struct StateStoreHandle {
    sender: mpsc::Sender<StoreCommand>,
}

#[cfg(feature = "fault-injection")]
#[derive(Clone, Debug)]
pub struct CommitFaultHandle {
    state: Arc<std::sync::Mutex<Option<CommitFault>>>,
}

#[cfg(feature = "fault-injection")]
#[derive(Clone, Copy, Debug)]
struct CommitFault {
    successful_commits: usize,
    kind: io::ErrorKind,
}

enum StoreCommand {
    Put(TorrentRecord, oneshot::Sender<Result<(), StoreError>>),
    Replace(TorrentRecord, oneshot::Sender<Result<bool, StoreError>>),
    Get(
        TorrentId,
        oneshot::Sender<Result<Option<TorrentRecord>, StoreError>>,
    ),
    List(oneshot::Sender<Result<Vec<TorrentRecord>, StoreError>>),
    QuarantinedCount(oneshot::Sender<Result<usize, StoreError>>),
    Remove(TorrentId, oneshot::Sender<Result<bool, StoreError>>),
    IncrementUploaded(TorrentId, u64, oneshot::Sender<Result<bool, StoreError>>),
    UpdateDownloadProgress(DownloadProgress, oneshot::Sender<Result<bool, StoreError>>),
}

impl std::fmt::Debug for StateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("StateStore").finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(String),
    #[error("state encoding error: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("database schema {found} is newer than supported schema {supported}")]
    NewerSchema { found: u64, supported: u32 },
    #[error("unsupported torrent record version {0}")]
    RecordVersion(u16),
    #[error("an info hash is already registered to another torrent")]
    DuplicateHash,
    #[error("torrent byte counter overflow")]
    CounterOverflow,
    #[error("database commit failed: {0}")]
    CommitIo(#[source] io::Error),
}

impl StateStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(database_error)?;
        }
        let database = Database::create(path).map_err(database_error)?;
        let store = Self {
            database: Arc::new(database),
            #[cfg(feature = "fault-injection")]
            commit_fault: CommitFaultHandle {
                state: Arc::new(std::sync::Mutex::new(None)),
            },
        };
        store.initialize_schema()?;
        store.quarantine_corrupt_records()?;
        Ok(store)
    }

    pub fn put_torrent(&self, record: &TorrentRecord) -> Result<(), StoreError> {
        if record.record_version != TorrentRecord::RECORD_VERSION {
            return Err(StoreError::RecordVersion(record.record_version));
        }
        let transaction = self.database.begin_write().map_err(database_error)?;
        {
            let mut torrents = transaction.open_table(TORRENTS).map_err(database_error)?;
            let mut progress = transaction.open_table(PROGRESS).map_err(database_error)?;
            let mut hashes = transaction.open_table(HASH_INDEX).map_err(database_error)?;
            let id = record.id.as_uuid();
            let id_bytes = id.as_bytes();
            let previous = torrents
                .get(id_bytes.as_slice())
                .map_err(database_error)?
                .map(|value| decode_record(value.value()))
                .transpose()?;
            let new_hashes = hash_keys(record);
            for key in &new_hashes {
                if let Some(existing) = hashes.get(key.as_slice()).map_err(database_error)?
                    && existing.value() != id_bytes
                {
                    return Err(StoreError::DuplicateHash);
                }
                hashes
                    .insert(key.as_slice(), id_bytes.as_slice())
                    .map_err(database_error)?;
            }
            if let Some(previous) = previous {
                for key in hash_keys(&previous) {
                    if !new_hashes.contains(&key) {
                        hashes.remove(key.as_slice()).map_err(database_error)?;
                    }
                }
            }
            let encoded = postcard::to_allocvec(record)?;
            torrents
                .insert(id_bytes.as_slice(), encoded.as_slice())
                .map_err(database_error)?;
            let encoded_progress = postcard::to_allocvec(&TorrentProgress::from_record(record))?;
            progress
                .insert(id_bytes.as_slice(), encoded_progress.as_slice())
                .map_err(database_error)?;
        }
        self.commit(transaction)
    }

    pub fn get_torrent(&self, id: TorrentId) -> Result<Option<TorrentRecord>, StoreError> {
        let uuid = id.as_uuid();
        let (raw, progress) = {
            let transaction = self.database.begin_read().map_err(database_error)?;
            let torrents = transaction.open_table(TORRENTS).map_err(database_error)?;
            let progress = transaction.open_table(PROGRESS).map_err(database_error)?;
            let raw = torrents
                .get(uuid.as_bytes().as_slice())
                .map_err(database_error)?
                .map(|value| value.value().to_vec());
            let progress = progress
                .get(uuid.as_bytes().as_slice())
                .map_err(database_error)?
                .map(|value| value.value().to_vec());
            (raw, progress)
        };
        let Some(raw) = raw else {
            return Ok(None);
        };
        if let Ok(mut record) = decode_record(&raw) {
            if let Some(progress) = progress {
                postcard::from_bytes::<TorrentProgress>(&progress)?.apply_to(&mut record);
            }
            Ok(Some(record))
        } else {
            self.move_to_quarantine(&[(uuid.as_bytes().to_vec(), raw)])?;
            Ok(None)
        }
    }

    /// Replace an existing record without recreating a concurrently removed
    /// torrent. The writer thread serializes the existence check and update.
    pub fn replace_torrent(&self, record: &TorrentRecord) -> Result<bool, StoreError> {
        if self.get_torrent(record.id)?.is_none() {
            return Ok(false);
        }
        self.put_torrent(record)?;
        Ok(true)
    }

    pub fn list_torrents(&self) -> Result<Vec<TorrentRecord>, StoreError> {
        let mut records = Vec::new();
        let mut corrupt = Vec::new();
        {
            let transaction = self.database.begin_read().map_err(database_error)?;
            let torrents = transaction.open_table(TORRENTS).map_err(database_error)?;
            let progress = transaction.open_table(PROGRESS).map_err(database_error)?;
            for entry in torrents.iter().map_err(database_error)? {
                let (key, value) = entry.map_err(database_error)?;
                match decode_record(value.value()) {
                    Ok(mut record) => {
                        if let Some(value) = progress.get(key.value()).map_err(database_error)? {
                            postcard::from_bytes::<TorrentProgress>(value.value())?
                                .apply_to(&mut record);
                        }
                        records.push(record);
                    }
                    Err(_) => corrupt.push((key.value().to_vec(), value.value().to_vec())),
                }
            }
        }
        if !corrupt.is_empty() {
            self.move_to_quarantine(&corrupt)?;
        }
        records.sort_unstable_by_key(|record| record.id);
        Ok(records)
    }

    pub fn quarantined_record_count(&self) -> Result<usize, StoreError> {
        let transaction = self.database.begin_read().map_err(database_error)?;
        let quarantine = transaction.open_table(QUARANTINE).map_err(database_error)?;
        usize::try_from(quarantine.len().map_err(database_error)?)
            .map_err(|_| StoreError::Database("quarantine count exceeds usize".to_owned()))
    }

    pub fn remove_torrent(&self, id: TorrentId) -> Result<bool, StoreError> {
        let current = self.get_torrent(id)?;
        let Some(record) = current else {
            return Ok(false);
        };
        let transaction = self.database.begin_write().map_err(database_error)?;
        {
            let mut torrents = transaction.open_table(TORRENTS).map_err(database_error)?;
            let mut progress = transaction.open_table(PROGRESS).map_err(database_error)?;
            let mut hashes = transaction.open_table(HASH_INDEX).map_err(database_error)?;
            let uuid = id.as_uuid();
            torrents
                .remove(uuid.as_bytes().as_slice())
                .map_err(database_error)?;
            progress
                .remove(uuid.as_bytes().as_slice())
                .map_err(database_error)?;
            for key in hash_keys(&record) {
                hashes.remove(key.as_slice()).map_err(database_error)?;
            }
        }
        self.commit(transaction)?;
        Ok(true)
    }

    pub fn increment_uploaded(&self, id: TorrentId, bytes: u64) -> Result<bool, StoreError> {
        let transaction = self.database.begin_write().map_err(database_error)?;
        let updated = {
            let torrents = transaction.open_table(TORRENTS).map_err(database_error)?;
            let mut progress_table = transaction.open_table(PROGRESS).map_err(database_error)?;
            let uuid = id.as_uuid();
            let key = uuid.as_bytes();
            let Some(encoded) = torrents.get(key.as_slice()).map_err(database_error)? else {
                return Ok(false);
            };
            let mut progress =
                if let Some(value) = progress_table.get(key.as_slice()).map_err(database_error)? {
                    postcard::from_bytes::<TorrentProgress>(value.value())?
                } else {
                    TorrentProgress::from_record(&decode_record(encoded.value())?)
                };
            drop(encoded);
            progress.uploaded = progress
                .uploaded
                .checked_add(bytes)
                .ok_or(StoreError::CounterOverflow)?;
            let encoded = postcard::to_allocvec(&progress)?;
            progress_table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(database_error)?;
            true
        };
        self.commit(transaction)?;
        Ok(updated)
    }

    fn update_download_progress(&self, update: DownloadProgress) -> Result<bool, StoreError> {
        let transaction = self.database.begin_write().map_err(database_error)?;
        let updated = {
            let torrents = transaction.open_table(TORRENTS).map_err(database_error)?;
            let mut progress_table = transaction.open_table(PROGRESS).map_err(database_error)?;
            let uuid = update.id.as_uuid();
            let key = uuid.as_bytes();
            let Some(encoded) = torrents.get(key.as_slice()).map_err(database_error)? else {
                return Ok(false);
            };
            let mut progress =
                if let Some(value) = progress_table.get(key.as_slice()).map_err(database_error)? {
                    postcard::from_bytes::<TorrentProgress>(value.value())?
                } else {
                    TorrentProgress::from_record(&decode_record(encoded.value())?)
                };
            drop(encoded);
            progress.state = update.state;
            progress.completed_pieces = update.completed_pieces;
            progress.downloaded = update.downloaded;
            let encoded = postcard::to_allocvec(&progress)?;
            progress_table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(database_error)?;
            true
        };
        self.commit(transaction)?;
        Ok(updated)
    }

    fn initialize_schema(&self) -> Result<(), StoreError> {
        let transaction = self.database.begin_write().map_err(database_error)?;
        {
            let mut meta = transaction.open_table(META).map_err(database_error)?;
            let _quarantine = transaction.open_table(QUARANTINE).map_err(database_error)?;
            let _torrents = transaction.open_table(TORRENTS).map_err(database_error)?;
            let _progress = transaction.open_table(PROGRESS).map_err(database_error)?;
            let _hashes = transaction.open_table(HASH_INDEX).map_err(database_error)?;
            let found = meta
                .get("schema_version")
                .map_err(database_error)?
                .map(|version| version.value());
            match found {
                Some(found) if found > u64::from(CURRENT_SCHEMA) => {
                    return Err(StoreError::NewerSchema {
                        found,
                        supported: CURRENT_SCHEMA,
                    });
                }
                Some(found) if found < u64::from(CURRENT_SCHEMA) => {
                    meta.insert("schema_version", u64::from(CURRENT_SCHEMA))
                        .map_err(database_error)?;
                }
                None => {
                    meta.insert("schema_version", u64::from(CURRENT_SCHEMA))
                        .map_err(database_error)?;
                }
                Some(_) => {}
            }
        }
        self.commit(transaction)
    }

    fn quarantine_corrupt_records(&self) -> Result<(), StoreError> {
        let corrupt = {
            let transaction = self.database.begin_read().map_err(database_error)?;
            let torrents = transaction.open_table(TORRENTS).map_err(database_error)?;
            let mut corrupt = Vec::new();
            for entry in torrents.iter().map_err(database_error)? {
                let (key, value) = entry.map_err(database_error)?;
                if decode_record(value.value()).is_err() {
                    corrupt.push((key.value().to_vec(), value.value().to_vec()));
                }
            }
            corrupt
        };
        self.move_to_quarantine(&corrupt)
    }

    fn move_to_quarantine(&self, corrupt: &[(Vec<u8>, Vec<u8>)]) -> Result<(), StoreError> {
        if corrupt.is_empty() {
            return Ok(());
        }
        let corrupt_ids: std::collections::HashSet<&[u8]> =
            corrupt.iter().map(|(id, _)| id.as_slice()).collect();
        let transaction = self.database.begin_write().map_err(database_error)?;
        {
            let mut torrents = transaction.open_table(TORRENTS).map_err(database_error)?;
            let mut progress = transaction.open_table(PROGRESS).map_err(database_error)?;
            let mut quarantine = transaction.open_table(QUARANTINE).map_err(database_error)?;
            for (id, raw) in corrupt {
                quarantine
                    .insert(id.as_slice(), raw.as_slice())
                    .map_err(database_error)?;
                torrents.remove(id.as_slice()).map_err(database_error)?;
                progress.remove(id.as_slice()).map_err(database_error)?;
            }
            let mut hashes = transaction.open_table(HASH_INDEX).map_err(database_error)?;
            let mut orphan_hashes = Vec::new();
            for entry in hashes.iter().map_err(database_error)? {
                let (key, value) = entry.map_err(database_error)?;
                if corrupt_ids.contains(value.value()) {
                    orphan_hashes.push(key.value().to_vec());
                }
            }
            for key in orphan_hashes {
                hashes.remove(key.as_slice()).map_err(database_error)?;
            }
        }
        self.commit(transaction)
    }

    fn commit(&self, transaction: redb::WriteTransaction) -> Result<(), StoreError> {
        self.maybe_fail_commit()?;
        transaction.commit().map_err(database_error)
    }

    #[cfg(feature = "fault-injection")]
    fn maybe_fail_commit(&self) -> Result<(), StoreError> {
        let mut state =
            self.commit_fault.state.lock().map_err(|_| {
                StoreError::Database("commit fault controller was poisoned".to_owned())
            })?;
        let Some(fault) = state.as_mut() else {
            return Ok(());
        };
        if fault.successful_commits > 0 {
            fault.successful_commits -= 1;
            return Ok(());
        }
        Err(StoreError::CommitIo(io::Error::new(
            fault.kind,
            "injected state database commit failure",
        )))
    }

    #[cfg(not(feature = "fault-injection"))]
    fn maybe_fail_commit(&self) -> Result<(), StoreError> {
        Ok(())
    }
}

#[cfg(feature = "fault-injection")]
impl CommitFaultHandle {
    pub fn arm(&self, successful_commits: usize, kind: io::ErrorKind) -> Result<(), StoreError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| StoreError::Database("commit fault controller was poisoned".to_owned()))?;
        *state = Some(CommitFault {
            successful_commits,
            kind,
        });
        Ok(())
    }

    pub fn disarm(&self) -> Result<(), StoreError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| StoreError::Database("commit fault controller was poisoned".to_owned()))?;
        *state = None;
        Ok(())
    }
}

impl StateStoreHandle {
    pub fn start(path: &Path, queue_capacity: usize) -> Result<Self, StoreError> {
        let store = StateStore::open(path)?;
        Self::start_store(store, queue_capacity)
    }

    #[cfg(feature = "fault-injection")]
    pub fn start_with_commit_fault(
        path: &Path,
        queue_capacity: usize,
    ) -> Result<(Self, CommitFaultHandle), StoreError> {
        let store = StateStore::open(path)?;
        let fault = store.commit_fault.clone();
        Self::start_store(store, queue_capacity).map(|handle| (handle, fault))
    }

    fn start_store(store: StateStore, queue_capacity: usize) -> Result<Self, StoreError> {
        let (sender, mut receiver) = mpsc::channel(queue_capacity);
        std::thread::Builder::new()
            .name("dendrite-state".to_owned())
            .spawn(move || {
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        StoreCommand::Put(record, reply) => {
                            let _result_ignored = reply.send(store.put_torrent(&record));
                        }
                        StoreCommand::Replace(record, reply) => {
                            let _result_ignored = reply.send(store.replace_torrent(&record));
                        }
                        StoreCommand::Get(id, reply) => {
                            let _result_ignored = reply.send(store.get_torrent(id));
                        }
                        StoreCommand::List(reply) => {
                            let _result_ignored = reply.send(store.list_torrents());
                        }
                        StoreCommand::QuarantinedCount(reply) => {
                            let _result_ignored = reply.send(store.quarantined_record_count());
                        }
                        StoreCommand::Remove(id, reply) => {
                            let _result_ignored = reply.send(store.remove_torrent(id));
                        }
                        StoreCommand::IncrementUploaded(id, bytes, reply) => {
                            let _result_ignored = reply.send(store.increment_uploaded(id, bytes));
                        }
                        StoreCommand::UpdateDownloadProgress(update, reply) => {
                            let _result_ignored =
                                reply.send(store.update_download_progress(update));
                        }
                    }
                }
            })
            .map_err(database_error)?;
        Ok(Self { sender })
    }

    pub async fn put_torrent(&self, record: TorrentRecord) -> Result<(), StoreError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StoreCommand::Put(record, reply))
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?;
        response
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?
    }

    pub async fn replace_torrent(&self, record: TorrentRecord) -> Result<bool, StoreError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StoreCommand::Replace(record, reply))
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?;
        response
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?
    }

    pub async fn get_torrent(&self, id: TorrentId) -> Result<Option<TorrentRecord>, StoreError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StoreCommand::Get(id, reply))
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?;
        response
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?
    }

    pub async fn list_torrents(&self) -> Result<Vec<TorrentRecord>, StoreError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StoreCommand::List(reply))
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?;
        response
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?
    }

    pub async fn quarantined_record_count(&self) -> Result<usize, StoreError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StoreCommand::QuarantinedCount(reply))
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?;
        response
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?
    }

    pub async fn remove_torrent(&self, id: TorrentId) -> Result<bool, StoreError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StoreCommand::Remove(id, reply))
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?;
        response
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?
    }

    pub async fn increment_uploaded(&self, id: TorrentId, bytes: u64) -> Result<bool, StoreError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StoreCommand::IncrementUploaded(id, bytes, reply))
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?;
        response
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?
    }

    pub async fn update_download_progress(
        &self,
        id: TorrentId,
        state: TorrentState,
        completed_pieces: Vec<u8>,
        downloaded: u64,
    ) -> Result<bool, StoreError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StoreCommand::UpdateDownloadProgress(
                DownloadProgress {
                    id,
                    state,
                    completed_pieces,
                    downloaded,
                },
                reply,
            ))
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?;
        response
            .await
            .map_err(|_| StoreError::Database("state writer stopped".to_owned()))?
    }
}

fn hash_keys(record: &TorrentRecord) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(2);
    if let Some(hash) = record.v1_info_hash {
        let mut key = Vec::with_capacity(21);
        key.push(1);
        key.extend_from_slice(hash.as_bytes());
        keys.push(key);
    }
    if let Some(hash) = record.v2_info_hash {
        let mut key = Vec::with_capacity(33);
        key.push(2);
        key.extend_from_slice(hash.as_bytes());
        keys.push(key);
    }
    keys
}

fn decode_record(bytes: &[u8]) -> Result<TorrentRecord, StoreError> {
    if let Ok(record) = postcard::from_bytes::<TorrentRecord>(bytes) {
        if record.record_version != TorrentRecord::RECORD_VERSION {
            return Err(StoreError::RecordVersion(record.record_version));
        }
        return Ok(record);
    }
    let record: TorrentRecordV1 = postcard::from_bytes(bytes)?;
    if record.record_version != 1 {
        return Err(StoreError::RecordVersion(record.record_version));
    }
    Ok(record.into())
}

fn database_error(error: impl std::fmt::Display) -> StoreError {
    StoreError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temporary_database() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        std::env::temp_dir().join(format!("dendrite-{nonce}.redb"))
    }

    #[test]
    fn records_round_trip_and_hashes_are_unique() -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let store = StateStore::open(&path)?;
        let record = TorrentRecord {
            record_version: TorrentRecord::RECORD_VERSION,
            id: TorrentId::new(),
            name: "test".to_owned(),
            state: TorrentState::Stopped,
            v1_info_hash: Some(Sha1Hash::from_bytes([7; 20])),
            v2_info_hash: None,
            total_length: 4,
            raw_metainfo: b"test".to_vec(),
            magnet_uri: None,
            stop_on_complete: false,
            completed_pieces: Vec::new(),
            downloaded: 0,
            uploaded: 0,
            added_at_unix_ms: 0,
        };
        store.put_torrent(&record)?;
        assert_eq!(
            store.get_torrent(record.id)?.map(|value| value.name),
            Some("test".to_owned())
        );

        let mut duplicate = record.clone();
        duplicate.id = TorrentId::new();
        assert!(matches!(
            store.put_torrent(&duplicate),
            Err(StoreError::DuplicateHash)
        ));
        assert!(store.remove_torrent(record.id)?);
        assert!(!store.replace_torrent(&record)?);
        assert!(store.get_torrent(record.id)?.is_none());
        drop(store);
        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn version_one_records_migrate_with_normal_completion_behavior()
    -> Result<(), Box<dyn std::error::Error>> {
        let id = TorrentId::new();
        let encoded = postcard::to_allocvec(&TorrentRecordV1 {
            record_version: 1,
            id,
            name: "legacy".to_owned(),
            state: TorrentState::Downloading,
            v1_info_hash: Some(Sha1Hash::from_bytes([3; 20])),
            v2_info_hash: None,
            total_length: 42,
            raw_metainfo: b"legacy-metainfo".to_vec(),
            magnet_uri: None,
            completed_pieces: vec![0x80],
            downloaded: 42,
            uploaded: 7,
            added_at_unix_ms: 9,
        })?;
        let record = decode_record(&encoded)?;
        assert_eq!(record.record_version, TorrentRecord::RECORD_VERSION);
        assert_eq!(record.id, id);
        assert!(!record.stop_on_complete);
        assert_eq!(record.downloaded, 42);
        Ok(())
    }

    #[test]
    fn committed_progress_survives_database_restart() -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let id = TorrentId::new();
        {
            let store = StateStore::open(&path)?;
            store.put_torrent(&TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: "restart-safe".to_owned(),
                state: TorrentState::Downloading,
                v1_info_hash: Some(Sha1Hash::from_bytes([9; 20])),
                v2_info_hash: None,
                total_length: 32_768,
                raw_metainfo: b"durable-metainfo".to_vec(),
                magnet_uri: None,
                stop_on_complete: false,
                completed_pieces: vec![0b1000_0000],
                downloaded: 16_384,
                uploaded: 4_096,
                added_at_unix_ms: 42,
            })?;
        }

        let reopened = StateStore::open(&path)?;
        let record = reopened.get_torrent(id)?.ok_or("record disappeared")?;
        assert_eq!(record.state, TorrentState::Downloading);
        assert_eq!(record.completed_pieces, [0b1000_0000]);
        assert_eq!(record.downloaded, 16_384);
        assert_eq!(record.uploaded, 4_096);
        assert_eq!(record.raw_metainfo, b"durable-metainfo");
        drop(reopened);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn hot_progress_updates_never_rewrite_large_metainfo_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let store = StateStore::open(&path)?;
        let mut record = TorrentRecord {
            record_version: TorrentRecord::RECORD_VERSION,
            id: TorrentId::new(),
            name: "large-metainfo".to_owned(),
            state: TorrentState::Downloading,
            v1_info_hash: Some(Sha1Hash::from_bytes([0x44; 20])),
            v2_info_hash: None,
            total_length: 16 * 1024 * 1024,
            raw_metainfo: vec![0x55; 2 * 1024 * 1024],
            magnet_uri: None,
            stop_on_complete: false,
            completed_pieces: vec![0; 128 * 1024],
            downloaded: 0,
            uploaded: 0,
            added_at_unix_ms: 0,
        };
        store.put_torrent(&record)?;
        let base_before = {
            let transaction = store.database.begin_read()?;
            let torrents = transaction.open_table(TORRENTS)?;
            torrents
                .get(record.id.as_uuid().as_bytes().as_slice())?
                .ok_or("missing base record")?
                .value()
                .to_vec()
        };

        record.completed_pieces[0] = 0x80;
        record.downloaded = 16 * 1024 * 1024;
        assert!(store.update_download_progress(DownloadProgress {
            id: record.id,
            state: record.state,
            completed_pieces: record.completed_pieces.clone(),
            downloaded: record.downloaded,
        })?);
        assert!(store.increment_uploaded(record.id, 16 * 1024)?);

        let (base_after, progress_bytes) = {
            let transaction = store.database.begin_read()?;
            let torrents = transaction.open_table(TORRENTS)?;
            let progress = transaction.open_table(PROGRESS)?;
            let key = record.id.as_uuid();
            (
                torrents
                    .get(key.as_bytes().as_slice())?
                    .ok_or("missing base record")?
                    .value()
                    .to_vec(),
                progress
                    .get(key.as_bytes().as_slice())?
                    .ok_or("missing progress record")?
                    .value()
                    .len(),
            )
        };
        assert_eq!(base_after, base_before);
        assert!(progress_bytes < 256 * 1024);
        let updated = store
            .get_torrent(record.id)?
            .ok_or("updated record disappeared")?;
        assert_eq!(updated.raw_metainfo.len(), 2 * 1024 * 1024);
        assert_eq!(updated.downloaded, 16 * 1024 * 1024);
        assert_eq!(updated.uploaded, 16 * 1024);
        drop(store);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn thousands_of_torrents_remain_bounded_and_queryable() -> Result<(), Box<dyn std::error::Error>>
    {
        const TORRENTS: usize = 2_048;

        let path = temporary_database();
        let store = StateStore::open(&path)?;
        for index in 0..TORRENTS {
            let mut hash = [0_u8; 20];
            hash[..8].copy_from_slice(&u64::try_from(index)?.to_be_bytes());
            store.put_torrent(&TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id: TorrentId::new(),
                name: format!("torrent-{index}"),
                state: TorrentState::Stopped,
                v1_info_hash: Some(Sha1Hash::from_bytes(hash)),
                v2_info_hash: None,
                total_length: 1,
                raw_metainfo: Vec::new(),
                magnet_uri: None,
                stop_on_complete: false,
                completed_pieces: vec![0],
                downloaded: 0,
                uploaded: 0,
                added_at_unix_ms: u64::try_from(index)?,
            })?;
        }
        let records = store.list_torrents()?;
        assert_eq!(records.len(), TORRENTS);
        assert!(records.windows(2).all(|pair| pair[0].id <= pair[1].id));
        drop(store);

        let reopened = StateStore::open(&path)?;
        assert_eq!(reopened.list_torrents()?.len(), TORRENTS);
        drop(reopened);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn bounded_state_queue_backpressures_without_losing_updates()
    -> Result<(), Box<dyn std::error::Error>> {
        const UPDATES: usize = 512;

        let path = temporary_database();
        let store = StateStoreHandle::start(&path, 1)?;
        let id = TorrentId::new();
        store
            .put_torrent(TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: "backpressure".to_owned(),
                state: TorrentState::Seeding,
                v1_info_hash: Some(Sha1Hash::from_bytes([0x42; 20])),
                v2_info_hash: None,
                total_length: 1,
                raw_metainfo: Vec::new(),
                magnet_uri: None,
                stop_on_complete: false,
                completed_pieces: vec![0b1000_0000],
                downloaded: 1,
                uploaded: 0,
                added_at_unix_ms: 0,
            })
            .await?;
        let mut updates = tokio::task::JoinSet::new();
        for _ in 0..UPDATES {
            let store = store.clone();
            updates.spawn(async move { store.increment_uploaded(id, 1).await });
        }
        while let Some(result) = updates.join_next().await {
            assert!(result??);
        }
        let record = store.get_torrent(id).await?.ok_or("record disappeared")?;
        assert_eq!(record.uploaded, u64::try_from(UPDATES)?);
        drop(store);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn corrupt_database_is_rejected_without_overwrite() -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let corrupt = b"not a redb database";
        std::fs::write(&path, corrupt)?;
        assert!(matches!(
            StateStore::open(&path),
            Err(StoreError::Database(_))
        ));
        assert_eq!(std::fs::read(&path)?, corrupt);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn newer_schema_is_rejected_and_interrupted_initialization_repairs_safely()
    -> Result<(), Box<dyn std::error::Error>> {
        let newer_path = temporary_database();
        {
            let store = StateStore::open(&newer_path)?;
            let transaction = store.database.begin_write()?;
            {
                let mut meta = transaction.open_table(META)?;
                meta.insert("schema_version", u64::from(CURRENT_SCHEMA) + 1)?;
            }
            transaction.commit()?;
        }
        assert!(matches!(
            StateStore::open(&newer_path),
            Err(StoreError::NewerSchema { found, supported })
                if found == u64::from(CURRENT_SCHEMA) + 1 && supported == CURRENT_SCHEMA
        ));
        std::fs::remove_file(newer_path)?;

        let interrupted_path = temporary_database();
        {
            let database = Database::create(&interrupted_path)?;
            let transaction = database.begin_write()?;
            {
                let _meta = transaction.open_table(META)?;
            }
            transaction.commit()?;
        }
        let repaired = StateStore::open(&interrupted_path)?;
        let transaction = repaired.database.begin_read()?;
        let meta = transaction.open_table(META)?;
        assert_eq!(
            meta.get("schema_version")?.map(|value| value.value()),
            Some(u64::from(CURRENT_SCHEMA))
        );
        drop(meta);
        drop(transaction);
        drop(repaired);
        std::fs::remove_file(interrupted_path)?;
        Ok(())
    }

    #[test]
    fn older_schema_marker_upgrades_without_losing_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let id = TorrentId::new();
        {
            let store = StateStore::open(&path)?;
            store.put_torrent(&TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: "pre-upgrade".to_owned(),
                state: TorrentState::Stopped,
                v1_info_hash: Some(Sha1Hash::from_bytes([0x33; 20])),
                v2_info_hash: None,
                total_length: 1,
                raw_metainfo: Vec::new(),
                magnet_uri: None,
                stop_on_complete: false,
                completed_pieces: vec![0],
                downloaded: 0,
                uploaded: 0,
                added_at_unix_ms: 0,
            })?;
            let transaction = store.database.begin_write()?;
            {
                let mut meta = transaction.open_table(META)?;
                meta.insert("schema_version", 0)?;
            }
            transaction.commit()?;
        }

        let upgraded = StateStore::open(&path)?;
        assert_eq!(
            upgraded.get_torrent(id)?.map(|record| record.name),
            Some("pre-upgrade".to_owned())
        );
        let transaction = upgraded.database.begin_read()?;
        let meta = transaction.open_table(META)?;
        assert_eq!(
            meta.get("schema_version")?.map(|value| value.value()),
            Some(u64::from(CURRENT_SCHEMA))
        );
        drop(meta);
        drop(transaction);
        drop(upgraded);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn second_state_owner_is_rejected_until_first_releases_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let first = StateStore::open(&path)?;
        assert!(matches!(
            StateStore::open(&path),
            Err(StoreError::Database(_))
        ));
        drop(first);
        let second = StateStore::open(&path)?;
        drop(second);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[cfg(feature = "fault-injection")]
    #[tokio::test]
    async fn commit_enospc_and_eio_roll_back_every_state_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let (store, faults) = StateStoreHandle::start_with_commit_fault(&path, 8)?;
        let baseline = TorrentRecord {
            record_version: TorrentRecord::RECORD_VERSION,
            id: TorrentId::new(),
            name: "durable baseline".to_owned(),
            state: TorrentState::Stopped,
            v1_info_hash: Some(Sha1Hash::from_bytes([0x51; 20])),
            v2_info_hash: None,
            total_length: 16,
            raw_metainfo: Vec::new(),
            magnet_uri: None,
            stop_on_complete: false,
            completed_pieces: vec![0],
            downloaded: 0,
            uploaded: 0,
            added_at_unix_ms: 0,
        };
        store.put_torrent(baseline.clone()).await?;

        for kind in [io::ErrorKind::StorageFull, io::ErrorKind::Other] {
            let mut newcomer = baseline.clone();
            newcomer.id = TorrentId::new();
            newcomer.v1_info_hash = Some(Sha1Hash::from_bytes(
                if kind == io::ErrorKind::StorageFull {
                    [0x52; 20]
                } else {
                    [0x53; 20]
                },
            ));
            faults.arm(0, kind)?;
            assert!(matches!(
                store.put_torrent(newcomer.clone()).await,
                Err(StoreError::CommitIo(source)) if source.kind() == kind
            ));
            assert!(store.get_torrent(newcomer.id).await?.is_none());

            faults.disarm()?;
            store.put_torrent(newcomer.clone()).await?;
            assert!(store.remove_torrent(newcomer.id).await?);

            let mut replacement = baseline.clone();
            replacement.state = TorrentState::Downloading;
            replacement.downloaded = 8;
            replacement.completed_pieces = vec![0x80];
            faults.arm(0, kind)?;
            assert!(matches!(
                store.replace_torrent(replacement).await,
                Err(StoreError::CommitIo(source)) if source.kind() == kind
            ));
            let unchanged = store
                .get_torrent(baseline.id)
                .await?
                .ok_or("baseline disappeared after failed replace")?;
            assert_eq!(unchanged.state, TorrentState::Stopped);
            assert_eq!(unchanged.downloaded, 0);

            faults.arm(1, kind)?;
            assert!(store.increment_uploaded(baseline.id, 1).await?);
            assert!(matches!(
                store.increment_uploaded(baseline.id, 1).await,
                Err(StoreError::CommitIo(source)) if source.kind() == kind
            ));
            assert_eq!(
                store
                    .get_torrent(baseline.id)
                    .await?
                    .ok_or("baseline disappeared after failed counter update")?
                    .uploaded,
                if kind == io::ErrorKind::StorageFull {
                    1
                } else {
                    2
                }
            );

            faults.arm(0, kind)?;
            assert!(matches!(
                store.remove_torrent(baseline.id).await,
                Err(StoreError::CommitIo(source)) if source.kind() == kind
            ));
            assert!(store.get_torrent(baseline.id).await?.is_some());
            faults.disarm()?;
        }

        assert!(store.remove_torrent(baseline.id).await?);
        drop(store);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn one_corrupt_record_is_quarantined_without_losing_healthy_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let healthy = TorrentRecord {
            record_version: TorrentRecord::RECORD_VERSION,
            id: TorrentId::new(),
            name: "healthy".to_owned(),
            state: TorrentState::Stopped,
            v1_info_hash: Some(Sha1Hash::from_bytes([0x61; 20])),
            v2_info_hash: None,
            total_length: 1,
            raw_metainfo: Vec::new(),
            magnet_uri: None,
            stop_on_complete: false,
            completed_pieces: vec![0],
            downloaded: 0,
            uploaded: 0,
            added_at_unix_ms: 0,
        };
        let corrupt_id = TorrentId::new();
        let corrupt_hash = Sha1Hash::from_bytes([0x62; 20]);
        {
            let store = StateStore::open(&path)?;
            store.put_torrent(&healthy)?;
            let transaction = store.database.begin_write()?;
            {
                let mut torrents = transaction.open_table(TORRENTS)?;
                let corrupt_uuid = corrupt_id.as_uuid();
                torrents.insert(corrupt_uuid.as_bytes().as_slice(), &[0xff, 0x00][..])?;
                let mut hashes = transaction.open_table(HASH_INDEX)?;
                let mut hash_key = vec![1];
                hash_key.extend_from_slice(corrupt_hash.as_bytes());
                hashes.insert(hash_key.as_slice(), corrupt_uuid.as_bytes().as_slice())?;
            }
            transaction.commit()?;
            assert_eq!(store.list_torrents()?.len(), 1);
            assert_eq!(store.quarantined_record_count()?, 1);
        }

        let recovered = StateStore::open(&path)?;
        assert_eq!(recovered.quarantined_record_count()?, 1);
        assert!(recovered.get_torrent(corrupt_id)?.is_none());
        assert_eq!(
            recovered
                .list_torrents()?
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            vec![healthy.id]
        );
        let mut replacement = healthy.clone();
        replacement.id = TorrentId::new();
        replacement.v1_info_hash = Some(corrupt_hash);
        recovered.put_torrent(&replacement)?;
        assert!(recovered.get_torrent(replacement.id)?.is_some());
        drop(recovered);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn schema_one_backup_restore_and_upgrade_preserve_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let backup = path.with_extension("backup.redb");
        let record = TorrentRecord {
            record_version: TorrentRecord::RECORD_VERSION,
            id: TorrentId::new(),
            name: "schema-one".to_owned(),
            state: TorrentState::Stopped,
            v1_info_hash: Some(Sha1Hash::from_bytes([0x71; 20])),
            v2_info_hash: None,
            total_length: 1,
            raw_metainfo: Vec::new(),
            magnet_uri: None,
            stop_on_complete: false,
            completed_pieces: vec![0],
            downloaded: 0,
            uploaded: 0,
            added_at_unix_ms: 0,
        };
        {
            let database = Database::create(&path)?;
            let transaction = database.begin_write()?;
            {
                let mut meta = transaction.open_table(META)?;
                meta.insert("schema_version", 1)?;
                let mut torrents = transaction.open_table(TORRENTS)?;
                let id = record.id.as_uuid();
                let encoded = postcard::to_allocvec(&record)?;
                torrents.insert(id.as_bytes().as_slice(), encoded.as_slice())?;
                let _hashes = transaction.open_table(HASH_INDEX)?;
            }
            transaction.commit()?;
        }

        {
            let upgraded = StateStore::open(&path)?;
            assert_eq!(
                upgraded.get_torrent(record.id)?.map(|value| value.name),
                Some("schema-one".to_owned())
            );
            let transaction = upgraded.database.begin_read()?;
            let meta = transaction.open_table(META)?;
            assert_eq!(
                meta.get("schema_version")?.map(|value| value.value()),
                Some(u64::from(CURRENT_SCHEMA))
            );
        }
        std::fs::copy(&path, &backup)?;
        {
            let changed = StateStore::open(&path)?;
            assert!(changed.remove_torrent(record.id)?);
        }
        std::fs::copy(&backup, &path)?;
        let restored = StateStore::open(&path)?;
        assert!(restored.get_torrent(record.id)?.is_some());
        drop(restored);
        std::fs::remove_file(path)?;
        std::fs::remove_file(backup)?;
        Ok(())
    }

    #[test]
    fn counter_overflow_is_rejected_without_mutating_durable_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        let store = StateStore::open(&path)?;
        let record = TorrentRecord {
            record_version: TorrentRecord::RECORD_VERSION,
            id: TorrentId::new(),
            name: "overflow".to_owned(),
            state: TorrentState::Seeding,
            v1_info_hash: Some(Sha1Hash::from_bytes([0x72; 20])),
            v2_info_hash: None,
            total_length: 1,
            raw_metainfo: Vec::new(),
            magnet_uri: None,
            stop_on_complete: false,
            completed_pieces: vec![0x80],
            downloaded: 1,
            uploaded: u64::MAX,
            added_at_unix_ms: 0,
        };
        store.put_torrent(&record)?;
        assert!(matches!(
            store.increment_uploaded(record.id, 1),
            Err(StoreError::CounterOverflow)
        ));
        assert_eq!(
            store
                .get_torrent(record.id)?
                .ok_or("overflow record disappeared")?
                .uploaded,
            u64::MAX
        );
        drop(store);
        let reopened = StateStore::open(&path)?;
        assert_eq!(
            reopened
                .get_torrent(record.id)?
                .ok_or("overflow record disappeared after restart")?
                .uploaded,
            u64::MAX
        );
        drop(reopened);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn virtual_week_of_accounting_reuses_pages_and_reopens_cleanly()
    -> Result<(), Box<dyn std::error::Error>> {
        const MINUTES_PER_WEEK: u64 = 7 * 24 * 60;
        const MAX_DATABASE_BYTES: u64 = 16 * 1024 * 1024;
        let path = temporary_database();
        let id = TorrentId::new();
        {
            let store = StateStore::open(&path)?;
            store.put_torrent(&TorrentRecord {
                record_version: TorrentRecord::RECORD_VERSION,
                id,
                name: "week-long-seed".to_owned(),
                state: TorrentState::Seeding,
                v1_info_hash: Some(Sha1Hash::from_bytes([0x73; 20])),
                v2_info_hash: None,
                total_length: 1,
                raw_metainfo: Vec::new(),
                magnet_uri: None,
                stop_on_complete: false,
                completed_pieces: vec![0x80],
                downloaded: 1,
                uploaded: 0,
                added_at_unix_ms: 0,
            })?;
            for _minute in 0..MINUTES_PER_WEEK {
                assert!(store.increment_uploaded(id, 1)?);
            }
        }
        assert!(std::fs::metadata(&path)?.len() <= MAX_DATABASE_BYTES);
        let reopened = StateStore::open(&path)?;
        assert_eq!(
            reopened
                .get_torrent(id)?
                .ok_or("week-long record disappeared")?
                .uploaded,
            MINUTES_PER_WEEK
        );
        drop(reopened);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[cfg(feature = "fault-injection")]
    #[test]
    fn interrupted_schema_upgrade_rolls_back_and_retries_cleanly()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_database();
        {
            let database = Database::create(&path)?;
            let transaction = database.begin_write()?;
            {
                let mut meta = transaction.open_table(META)?;
                meta.insert("schema_version", 1)?;
                let _torrents = transaction.open_table(TORRENTS)?;
                let _hashes = transaction.open_table(HASH_INDEX)?;
            }
            transaction.commit()?;
        }
        let faults = CommitFaultHandle {
            state: Arc::new(std::sync::Mutex::new(Some(CommitFault {
                successful_commits: 0,
                kind: io::ErrorKind::StorageFull,
            }))),
        };
        let failed = StateStore {
            database: Arc::new(Database::create(&path)?),
            commit_fault: faults,
        };
        assert!(matches!(
            failed.initialize_schema(),
            Err(StoreError::CommitIo(source))
                if source.kind() == io::ErrorKind::StorageFull
        ));
        drop(failed);

        let recovered = StateStore::open(&path)?;
        let transaction = recovered.database.begin_read()?;
        let meta = transaction.open_table(META)?;
        assert_eq!(
            meta.get("schema_version")?.map(|value| value.value()),
            Some(u64::from(CURRENT_SCHEMA))
        );
        drop(meta);
        drop(transaction);
        drop(recovered);
        std::fs::remove_file(path)?;
        Ok(())
    }
}
