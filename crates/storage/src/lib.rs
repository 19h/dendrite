//! Capability-confined positional file I/O.

use std::{
    hash::{DefaultHasher, Hash as _, Hasher as _},
    io,
    path::Path,
};

#[cfg(feature = "fault-injection")]
use std::sync::{
    Arc as FaultArc,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::{ambient_authority, fs::Dir};
use dendrite_core::TorrentPath;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendKind {
    Portable,
    #[cfg(target_os = "linux")]
    IoUring,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("storage worker stopped")]
    WorkerStopped,
    #[error("offset plus length overflowed u64")]
    RangeOverflow,
    #[error("short read: expected {expected} bytes, got {actual}")]
    ShortRead { expected: usize, actual: usize },
    #[error("requested storage backend is unavailable: {0}")]
    BackendUnavailable(String),
    #[error("refusing multiply linked torrent payload {0}")]
    HardLink(String),
}

#[derive(Clone)]
pub struct StorageHandle {
    sender: mpsc::Sender<Request>,
    backend: BackendKind,
    #[cfg(feature = "fault-injection")]
    write_fault: Option<FaultArc<InjectedWriteFault>>,
}

#[cfg(feature = "fault-injection")]
struct InjectedWriteFault {
    remaining: AtomicU64,
    kind: io::ErrorKind,
}

impl std::fmt::Debug for StorageHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageHandle")
            .field("backend", &self.backend)
            .finish_non_exhaustive()
    }
}

enum Request {
    Read {
        path: TorrentPath,
        offset: u64,
        length: usize,
        reply: oneshot::Sender<Result<Bytes, StorageError>>,
    },
    Write {
        path: TorrentPath,
        offset: u64,
        data: Bytes,
        file_length: u64,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    Sync {
        path: TorrentPath,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
}

impl Request {
    fn path(&self) -> &TorrentPath {
        match self {
            Self::Read { path, .. } | Self::Write { path, .. } | Self::Sync { path, .. } => path,
        }
    }
}

impl StorageHandle {
    pub fn start(root: &Path, queue_capacity: usize) -> Result<Self, StorageError> {
        Self::start_inner(root, queue_capacity, BackendRequest::Automatic)
    }

    pub fn start_portable(root: &Path, queue_capacity: usize) -> Result<Self, StorageError> {
        Self::start_inner(root, queue_capacity, BackendRequest::Portable)
    }

    /// Starts the portable backend with a deterministic ENOSPC-style write
    /// budget. This is intentionally available only to fault-test builds.
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn start_portable_with_write_budget(
        root: &Path,
        queue_capacity: usize,
        bytes_before_full: u64,
    ) -> Result<Self, StorageError> {
        Self::start_portable_with_write_fault(
            root,
            queue_capacity,
            bytes_before_full,
            io::ErrorKind::StorageFull,
        )
    }

    /// Starts the portable backend with a deterministic write failure after
    /// the given number of successfully accepted bytes.
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn start_portable_with_write_fault(
        root: &Path,
        queue_capacity: usize,
        bytes_before_failure: u64,
        kind: io::ErrorKind,
    ) -> Result<Self, StorageError> {
        let mut storage = Self::start_inner(root, queue_capacity, BackendRequest::Portable)?;
        storage.write_fault = Some(FaultArc::new(InjectedWriteFault {
            remaining: AtomicU64::new(bytes_before_failure),
            kind,
        }));
        Ok(storage)
    }

    #[cfg(target_os = "linux")]
    pub fn start_io_uring(root: &Path, queue_capacity: usize) -> Result<Self, StorageError> {
        Self::start_inner(root, queue_capacity, BackendRequest::IoUring)
    }

    fn start_inner(
        root: &Path,
        queue_capacity: usize,
        requested: BackendRequest,
    ) -> Result<Self, StorageError> {
        if queue_capacity == 0 {
            return Err(StorageError::BackendUnavailable(
                "queue capacity must be nonzero".to_owned(),
            ));
        }
        let directory = Dir::open_ambient_dir(root, ambient_authority())?;
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let backend = spawn_worker(directory, receiver, requested, queue_capacity)?;
        Ok(Self {
            sender,
            backend,
            #[cfg(feature = "fault-injection")]
            write_fault: None,
        })
    }

    #[must_use]
    pub const fn backend(&self) -> BackendKind {
        self.backend
    }

    pub async fn read(
        &self,
        path: TorrentPath,
        offset: u64,
        length: usize,
    ) -> Result<Bytes, StorageError> {
        offset
            .checked_add(u64::try_from(length).map_err(|_| StorageError::RangeOverflow)?)
            .ok_or(StorageError::RangeOverflow)?;
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::Read {
                path,
                offset,
                length,
                reply,
            })
            .await
            .map_err(|_| StorageError::WorkerStopped)?;
        response.await.map_err(|_| StorageError::WorkerStopped)?
    }

    pub async fn write(
        &self,
        path: TorrentPath,
        offset: u64,
        data: Bytes,
        file_length: u64,
    ) -> Result<(), StorageError> {
        let end = offset
            .checked_add(u64::try_from(data.len()).map_err(|_| StorageError::RangeOverflow)?)
            .ok_or(StorageError::RangeOverflow)?;
        if end > file_length {
            return Err(StorageError::RangeOverflow);
        }
        #[cfg(feature = "fault-injection")]
        if let Some(fault) = &self.write_fault {
            let requested = u64::try_from(data.len()).map_err(|_| StorageError::RangeOverflow)?;
            fault
                .remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(requested)
                })
                .map_err(|_| {
                    let message = if fault.kind == io::ErrorKind::StorageFull {
                        "injected storage-full fault".to_owned()
                    } else {
                        format!("injected {:?} write fault", fault.kind)
                    };
                    StorageError::Io(io::Error::new(fault.kind, message))
                })?;
        }
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::Write {
                path,
                offset,
                data,
                file_length,
                reply,
            })
            .await
            .map_err(|_| StorageError::WorkerStopped)?;
        response.await.map_err(|_| StorageError::WorkerStopped)?
    }

    pub async fn sync(&self, path: TorrentPath) -> Result<(), StorageError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::Sync { path, reply })
            .await
            .map_err(|_| StorageError::WorkerStopped)?;
        response.await.map_err(|_| StorageError::WorkerStopped)?
    }
}

#[derive(Clone, Copy)]
enum BackendRequest {
    Automatic,
    Portable,
    #[cfg(target_os = "linux")]
    IoUring,
}

fn spawn_worker(
    directory: Dir,
    receiver: mpsc::Receiver<Request>,
    requested: BackendRequest,
    queue_capacity: usize,
) -> Result<BackendKind, StorageError> {
    #[cfg(target_os = "linux")]
    if !matches!(requested, BackendRequest::Portable) {
        let required = matches!(requested, BackendRequest::IoUring);
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("dendrite-storage-io-uring".to_owned())
            .spawn(move || {
                run_io_uring_or_fallback(
                    directory,
                    receiver,
                    required,
                    queue_capacity,
                    &ready_sender,
                );
            })?;
        return ready_receiver
            .recv()
            .map_err(|_| {
                StorageError::BackendUnavailable(
                    "io_uring worker exited during initialization".to_owned(),
                )
            })?
            .map_err(StorageError::BackendUnavailable);
    }

    let _ = requested;
    std::thread::Builder::new()
        .name("dendrite-storage-portable".to_owned())
        .spawn(move || run_worker(&directory, receiver, queue_capacity))?;
    Ok(BackendKind::Portable)
}

#[cfg(target_os = "linux")]
fn run_io_uring_or_fallback(
    directory: Dir,
    receiver: mpsc::Receiver<Request>,
    required: bool,
    queue_capacity: usize,
    ready: &std::sync::mpsc::SyncSender<Result<BackendKind, String>>,
) {
    match tokio_uring::Runtime::new(&tokio_uring::builder()) {
        Ok(runtime) => {
            if ready.send(Ok(BackendKind::IoUring)).is_ok() {
                runtime.block_on(run_io_uring_worker(directory, receiver));
            }
        }
        Err(error) if required => {
            let _result_ignored = ready.send(Err(error.to_string()));
        }
        Err(_) => {
            if ready.send(Ok(BackendKind::Portable)).is_ok() {
                run_worker(&directory, receiver, queue_capacity);
            }
        }
    }
}

#[cfg(target_os = "linux")]
async fn run_io_uring_worker(directory: Dir, mut receiver: mpsc::Receiver<Request>) {
    let root = std::sync::Arc::new(directory);
    while let Some(request) = receiver.recv().await {
        let root = root.clone();
        tokio_uring::spawn(async move {
            match request {
                Request::Read {
                    path,
                    offset,
                    length,
                    reply,
                } => {
                    let _result_ignored =
                        reply.send(io_uring_read(&root, &path, offset, length).await);
                }
                Request::Write {
                    path,
                    offset,
                    data,
                    file_length,
                    reply,
                } => {
                    let _result_ignored =
                        reply.send(io_uring_write(&root, &path, offset, data, file_length).await);
                }
                Request::Sync { path, reply } => {
                    let _result_ignored = reply.send(io_uring_sync(&root, &path).await);
                }
            }
        });
    }
}

#[cfg(target_os = "linux")]
async fn io_uring_read(
    root: &Dir,
    path: &TorrentPath,
    offset: u64,
    length: usize,
) -> Result<Bytes, StorageError> {
    let file = tokio_uring::fs::File::from_std(open_file(root, path, false, false)?.into_std());
    let (result, buffer) = file.read_exact_at(vec![0_u8; length], offset).await;
    let close = file.close().await;
    result?;
    close?;
    Ok(Bytes::from(buffer))
}

#[cfg(target_os = "linux")]
async fn io_uring_write(
    root: &Dir,
    path: &TorrentPath,
    offset: u64,
    data: Bytes,
    file_length: u64,
) -> Result<(), StorageError> {
    let file = open_file(root, path, true, true)?.into_std();
    file.set_len(file_length)?;
    let file = tokio_uring::fs::File::from_std(file);
    let (result, _buffer) = file.write_all_at(data, offset).await;
    let close = file.close().await;
    result?;
    close?;
    Ok(())
}

#[cfg(target_os = "linux")]
async fn io_uring_sync(root: &Dir, path: &TorrentPath) -> Result<(), StorageError> {
    let file = tokio_uring::fs::File::from_std(open_file(root, path, false, false)?.into_std());
    let result = file.sync_all().await;
    let close = file.close().await;
    result?;
    close?;
    Ok(())
}

fn run_worker(root: &Dir, mut receiver: mpsc::Receiver<Request>, queue_capacity: usize) {
    let worker_count = std::thread::available_parallelism()
        .map_or(4, |parallelism| parallelism.get().saturating_mul(2))
        .clamp(4, 32)
        .min(queue_capacity);
    let shard_capacity = queue_capacity.div_ceil(worker_count);
    let roots = (0..worker_count)
        .map(|_| root.try_clone())
        .collect::<Result<Vec<_>, _>>();
    let Ok(roots) = roots else {
        run_serial_worker(root, &mut receiver);
        return;
    };
    let mut workers = Vec::with_capacity(worker_count);
    for (index, worker_root) in roots.into_iter().enumerate() {
        let (sender, worker_receiver) = std::sync::mpsc::sync_channel(shard_capacity);
        if std::thread::Builder::new()
            .name(format!("dendrite-storage-portable-{index}"))
            .spawn(move || run_portable_worker(&worker_root, worker_receiver))
            .is_ok()
        {
            workers.push(sender);
        }
    }
    if workers.is_empty() {
        run_serial_worker(root, &mut receiver);
        return;
    }
    while let Some(request) = receiver.blocking_recv() {
        let mut hasher = DefaultHasher::new();
        request.path().hash(&mut hasher);
        let index = usize::try_from(hasher.finish()).unwrap_or(0) % workers.len();
        if let Err(error) = workers[index].send(request) {
            fail_request(error.0);
        }
    }
}

fn run_serial_worker(root: &Dir, receiver: &mut mpsc::Receiver<Request>) {
    while let Some(request) = receiver.blocking_recv() {
        execute_request(root, request);
    }
}

fn run_portable_worker(root: &Dir, receiver: std::sync::mpsc::Receiver<Request>) {
    for request in receiver {
        execute_request(root, request);
    }
}

fn execute_request(root: &Dir, request: Request) {
    match request {
        Request::Read {
            path,
            offset,
            length,
            reply,
        } => {
            let _result_ignored = reply.send(read_at(root, &path, offset, length));
        }
        Request::Write {
            path,
            offset,
            data,
            file_length,
            reply,
        } => {
            let _result_ignored =
                reply.send(write_at(root, &path, offset, data.as_ref(), file_length));
        }
        Request::Sync { path, reply } => {
            let _result_ignored = reply.send(sync_file(root, &path));
        }
    }
}

fn fail_request(request: Request) {
    match request {
        Request::Read { reply, .. } => {
            let _result_ignored = reply.send(Err(StorageError::WorkerStopped));
        }
        Request::Write { reply, .. } | Request::Sync { reply, .. } => {
            let _result_ignored = reply.send(Err(StorageError::WorkerStopped));
        }
    }
}

#[cfg(unix)]
fn read_at(
    root: &Dir,
    path: &TorrentPath,
    offset: u64,
    length: usize,
) -> Result<Bytes, StorageError> {
    use std::os::unix::fs::FileExt;

    let file = open_file(root, path, false, false)?.into_std();
    let mut buffer = vec![0_u8; length];
    let actual = file.read_at(&mut buffer, offset)?;
    if actual != length {
        return Err(StorageError::ShortRead {
            expected: length,
            actual,
        });
    }
    Ok(Bytes::from(buffer))
}

#[cfg(unix)]
fn write_at(
    root: &Dir,
    path: &TorrentPath,
    offset: u64,
    data: &[u8],
    file_length: u64,
) -> Result<(), StorageError> {
    use std::os::unix::fs::FileExt;

    let file = open_file(root, path, true, true)?.into_std();
    file.set_len(file_length)?;
    let mut written = 0_usize;
    while written < data.len() {
        let count = file.write_at(
            &data[written..],
            offset + u64::try_from(written).map_err(|_| StorageError::RangeOverflow)?,
        )?;
        if count == 0 {
            return Err(
                io::Error::new(io::ErrorKind::WriteZero, "positional write returned zero").into(),
            );
        }
        written += count;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_file(root: &Dir, path: &TorrentPath) -> Result<(), StorageError> {
    open_file(root, path, false, false)?.sync_all()?;
    Ok(())
}

fn open_file(
    root: &Dir,
    path: &TorrentPath,
    create_parents: bool,
    write: bool,
) -> Result<cap_std::fs::File, StorageError> {
    let Some((file_name, parents)) = path.components().split_last() else {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty torrent path").into());
    };
    let mut directory = root.try_clone()?;
    for component in parents {
        if create_parents {
            match directory.create_dir(component) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        directory = directory.open_dir_nofollow(component)?;
    }
    let mut options = cap_std::fs::OpenOptions::new();
    options
        .read(true)
        .write(write)
        .create(write)
        .follow(FollowSymlinks::No);
    let file = directory.open_with(file_name, &options)?;
    #[cfg(unix)]
    {
        use cap_std::fs::MetadataExt as _;

        if file.metadata()?.nlink() > 1 {
            return Err(StorageError::HardLink(path.to_string()));
        }
    }
    Ok(file)
}

#[cfg(not(unix))]
fn read_at(
    root: &Dir,
    path: &TorrentPath,
    offset: u64,
    length: usize,
) -> Result<Bytes, StorageError> {
    use std::io::{Read as _, Seek as _, SeekFrom};

    let mut file = open_file(root, path, false, false)?.into_std();
    file.seek(SeekFrom::Start(offset))?;
    let mut buffer = vec![0_u8; length];
    file.read_exact(&mut buffer)?;
    Ok(Bytes::from(buffer))
}

#[cfg(not(unix))]
fn write_at(
    root: &Dir,
    path: &TorrentPath,
    offset: u64,
    data: &[u8],
    file_length: u64,
) -> Result<(), StorageError> {
    use std::io::{Seek as _, SeekFrom, Write as _};

    let mut file = open_file(root, path, true, true)?.into_std();
    file.set_len(file_length)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_file(root: &Dir, path: &TorrentPath) -> Result<(), StorageError> {
    open_file(root, path, false, false)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[tokio::test]
    async fn bounded_storage_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("dendrite-storage-{nonce}"));
        std::fs::create_dir_all(&root)?;
        let storage = StorageHandle::start(&root, 8)?;
        let path = TorrentPath::new(["torrent".to_owned(), "file.bin".to_owned()])?;
        storage
            .write(path.clone(), 2, Bytes::from_static(b"test"), 8)
            .await?;
        assert_eq!(
            storage.read(path.clone(), 2, 4).await?,
            Bytes::from_static(b"test")
        );
        storage.sync(path).await?;
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn portable_and_io_uring_backends_have_positional_io_parity()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("dendrite-storage-parity-{nonce}"));
        std::fs::create_dir_all(&root)?;
        let portable_root = root.join("portable");
        let automatic_root = root.join("automatic");
        std::fs::create_dir_all(&portable_root)?;
        std::fs::create_dir_all(&automatic_root)?;
        let portable = StorageHandle::start_portable(&portable_root, 8)?;
        assert_eq!(portable.backend(), BackendKind::Portable);
        exercise_backend(&portable).await?;
        let automatic = StorageHandle::start(&automatic_root, 8)?;
        exercise_backend(&automatic).await?;

        #[cfg(target_os = "linux")]
        if automatic.backend() == BackendKind::IoUring {
            let required_root = root.join("required-uring");
            std::fs::create_dir_all(&required_root)?;
            let required = StorageHandle::start_io_uring(&required_root, 8)?;
            assert_eq!(required.backend(), BackendKind::IoUring);
            exercise_backend(&required).await?;
        }
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn synced_payload_survives_backend_restart() -> Result<(), Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("dendrite-storage-restart-{nonce}"));
        std::fs::create_dir_all(&root)?;
        let path = TorrentPath::new(["restart.bin".to_owned()])?;
        {
            let storage = StorageHandle::start(&root, 8)?;
            storage
                .write(path.clone(), 3, Bytes::from_static(b"durable"), 10)
                .await?;
            storage.sync(path.clone()).await?;
        }
        let reopened = StorageHandle::start_portable(&root, 8)?;
        assert_eq!(reopened.read(path, 3, 7).await?, b"durable".as_slice());
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn accelerated_restart_soak_preserves_every_synced_block()
    -> Result<(), Box<dyn std::error::Error>> {
        const BLOCK_SIZE: usize = 4 * 1024;
        const ROUNDS: usize = 64;

        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("dendrite-storage-soak-{nonce}"));
        std::fs::create_dir_all(&root)?;
        let path = TorrentPath::new(["restart-soak.bin".to_owned()])?;
        let file_length = u64::try_from(BLOCK_SIZE * ROUNDS)?;

        for round in 0..ROUNDS {
            let storage = StorageHandle::start_portable(&root, 8)?;
            let byte = u8::try_from(round)?;
            storage
                .write(
                    path.clone(),
                    u64::try_from(round * BLOCK_SIZE)?,
                    Bytes::from(vec![byte; BLOCK_SIZE]),
                    file_length,
                )
                .await?;
            storage.sync(path.clone()).await?;
        }

        let storage = StorageHandle::start_portable(&root, 8)?;
        for round in 0..ROUNDS {
            let expected = vec![u8::try_from(round)?; BLOCK_SIZE];
            assert_eq!(
                storage
                    .read(path.clone(), u64::try_from(round * BLOCK_SIZE)?, BLOCK_SIZE,)
                    .await?,
                expected
            );
        }
        drop(storage);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[cfg(feature = "fault-injection")]
    #[tokio::test]
    async fn injected_storage_full_is_precise_and_does_not_queue_the_failed_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("dendrite-storage-full-{nonce}"));
        std::fs::create_dir_all(&root)?;
        let path = TorrentPath::new(["full.bin".to_owned()])?;
        let storage = StorageHandle::start_portable_with_write_budget(&root, 8, 4)?;
        storage
            .write(path.clone(), 0, Bytes::from_static(b"safe"), 5)
            .await?;
        let error = match storage
            .write(path.clone(), 4, Bytes::from_static(b"!"), 5)
            .await
        {
            Ok(()) => return Err("write beyond the injected capacity succeeded".into()),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            StorageError::Io(ref source) if source.kind() == io::ErrorKind::StorageFull
        ));
        assert_eq!(storage.read(path, 0, 4).await?, b"safe".as_slice());
        drop(storage);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[cfg(feature = "fault-injection")]
    #[tokio::test]
    async fn injected_filesystem_failures_preserve_native_error_kinds()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("dendrite-storage-faults-{nonce}"));
        std::fs::create_dir_all(&root)?;
        for (name, kind) in [
            ("read-only.bin", io::ErrorKind::ReadOnlyFilesystem),
            ("permission.bin", io::ErrorKind::PermissionDenied),
            ("inode-exhaustion.bin", io::ErrorKind::StorageFull),
            ("fat32-file-too-large.bin", io::ErrorKind::FileTooLarge),
            ("device-io.bin", io::ErrorKind::Other),
        ] {
            let storage = StorageHandle::start_portable_with_write_fault(&root, 8, 0, kind)?;
            let path = TorrentPath::new([name.to_owned()])?;
            let error = match storage
                .write(path, 0, Bytes::from_static(b"blocked"), 7)
                .await
            {
                Ok(()) => return Err(format!("injected {kind:?} write succeeded").into()),
                Err(error) => error,
            };
            assert!(matches!(error, StorageError::Io(source) if source.kind() == kind));
            assert!(!root.join(name).exists());
        }
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cancelled_io_futures_do_not_poison_backend_or_shutdown()
    -> Result<(), Box<dyn std::error::Error>> {
        const OPERATIONS: usize = 256;
        const BLOCK: usize = 1024;

        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("dendrite-storage-cancel-{nonce}"));
        std::fs::create_dir_all(&root)?;
        let storage = match StorageHandle::start_io_uring(&root, 1) {
            Ok(storage) => storage,
            Err(StorageError::BackendUnavailable(_)) => StorageHandle::start_portable(&root, 1)?,
            Err(error) => return Err(error.into()),
        };
        let path = TorrentPath::new(["cancel-race.bin".to_owned()])?;
        let file_length = u64::try_from((OPERATIONS + 1) * BLOCK)?;
        let mut tasks = Vec::with_capacity(OPERATIONS);
        for operation in 0..OPERATIONS {
            let storage = storage.clone();
            let path = path.clone();
            tasks.push(tokio::spawn(async move {
                storage
                    .write(
                        path,
                        u64::try_from(operation * BLOCK)
                            .map_err(|_| StorageError::RangeOverflow)?,
                        Bytes::from(vec![u8::try_from(operation % 251).unwrap_or(0); BLOCK]),
                        file_length,
                    )
                    .await
            }));
        }
        tokio::task::yield_now().await;
        for (index, task) in tasks.iter().enumerate() {
            if index.is_multiple_of(2) {
                task.abort();
            }
        }
        for task in tasks {
            match task.await {
                Ok(result) => result?,
                Err(error) if error.is_cancelled() => {}
                Err(error) => return Err(error.into()),
            }
        }

        let sentinel_offset = u64::try_from(OPERATIONS * BLOCK)?;
        storage
            .write(
                path.clone(),
                sentinel_offset,
                Bytes::from(vec![0xa5; BLOCK]),
                file_length,
            )
            .await?;
        storage.sync(path.clone()).await?;
        assert_eq!(
            storage.read(path.clone(), sentinel_offset, BLOCK).await?,
            vec![0xa5; BLOCK]
        );
        drop(storage);

        let reopened = StorageHandle::start_portable(&root, 1)?;
        assert_eq!(
            reopened.read(path, sentinel_offset, BLOCK).await?,
            vec![0xa5; BLOCK]
        );
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    async fn exercise_backend(storage: &StorageHandle) -> Result<(), StorageError> {
        let path = TorrentPath::new(["nested".to_owned(), "payload.bin".to_owned()])
            .map_err(|error| StorageError::BackendUnavailable(error.to_string()))?;
        storage
            .write(path.clone(), 4, Bytes::from_static(b"backend"), 16)
            .await?;
        assert_eq!(
            storage.read(path.clone(), 4, 7).await?,
            b"backend".as_slice()
        );
        storage.sync(path).await
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_symlinked_parent_directories() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("dendrite-storage-root-{nonce}"));
        let outside = std::env::temp_dir().join(format!("dendrite-storage-outside-{nonce}"));
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(&outside)?;
        symlink(&outside, root.join("torrent"))?;
        let storage = StorageHandle::start(&root, 8)?;
        let path = TorrentPath::new(["torrent".to_owned(), "escape.bin".to_owned()])?;
        assert!(
            storage
                .write(path, 0, Bytes::from_static(b"blocked"), 7)
                .await
                .is_err()
        );
        assert!(!outside.join("escape.bin").exists());
        std::fs::remove_dir_all(root)?;
        std::fs::remove_dir_all(outside)?;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn revalidates_replaced_directories_and_rejects_external_hardlinks()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        let storage = StorageHandle::start_portable(root.path(), 8)?;
        let path = TorrentPath::new(["torrent".to_owned(), "payload.bin".to_owned()])?;
        storage
            .write(path.clone(), 0, Bytes::from_static(b"inside!"), 7)
            .await?;

        std::fs::rename(root.path().join("torrent"), root.path().join("original"))?;
        symlink(outside.path(), root.path().join("torrent"))?;
        assert!(
            storage
                .write(path.clone(), 0, Bytes::from_static(b"escaped"), 7)
                .await
                .is_err()
        );
        assert!(!outside.path().join("payload.bin").exists());

        std::fs::remove_file(root.path().join("torrent"))?;
        std::fs::rename(root.path().join("original"), root.path().join("torrent"))?;
        std::fs::remove_file(root.path().join("torrent/payload.bin"))?;
        let external = outside.path().join("external.bin");
        std::fs::write(&external, b"outside")?;
        std::fs::hard_link(&external, root.path().join("torrent/payload.bin"))?;
        let error = storage
            .write(path, 0, Bytes::from_static(b"corrupt"), 7)
            .await
            .err()
            .ok_or("multiply linked payload was accepted")?;
        assert!(matches!(error, StorageError::HardLink(_)));
        assert_eq!(std::fs::read(external)?, b"outside");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn actual_emfile_is_reported_and_storage_recovers() -> Result<(), Box<dyn std::error::Error>> {
        let executable = std::env::current_exe()?;
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg("ulimit -n 64 && exec \"$@\"")
            .arg("dendrite-emfile")
            .arg(executable)
            .arg("--ignored")
            .arg("--exact")
            .arg("tests::actual_emfile_child")
            .arg("--nocapture")
            .env("DENDRITE_EMFILE_CHILD", "1")
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "EMFILE child failed with {}:\n{}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "subprocess helper for actual_emfile_is_reported_and_storage_recovers"]
    fn actual_emfile_child() -> Result<(), Box<dyn std::error::Error>> {
        const EMFILE: i32 = 24;
        if std::env::var_os("DENDRITE_EMFILE_CHILD").is_none() {
            return Ok(());
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let root = tempfile::tempdir()?;
            let storage = StorageHandle::start_portable(root.path(), 2)?;
            let path = TorrentPath::new(["descriptor-pressure.bin".to_owned()])?;
            let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
            let client = std::net::TcpStream::connect(listener.local_addr()?)?;
            let mut descriptors = Vec::new();
            loop {
                match std::fs::File::open("/dev/null") {
                    Ok(file) => descriptors.push(file),
                    Err(error) if error.raw_os_error() == Some(EMFILE) => break,
                    Err(error) => return Err(error.into()),
                }
            }
            let error = storage
                .write(path.clone(), 0, Bytes::from_static(b"recovery"), 8)
                .await
                .err()
                .ok_or("storage write unexpectedly succeeded under EMFILE")?;
            assert!(matches!(
                error,
                StorageError::Io(source) if source.raw_os_error() == Some(EMFILE)
            ));
            let accept_error = listener
                .accept()
                .err()
                .ok_or("socket accept unexpectedly succeeded under EMFILE")?;
            assert_eq!(accept_error.raw_os_error(), Some(EMFILE));

            drop(descriptors);
            let (accepted, _) = listener.accept()?;
            drop(accepted);
            drop(client);
            storage
                .write(path.clone(), 0, Bytes::from_static(b"recovery"), 8)
                .await?;
            storage.sync(path.clone()).await?;
            assert_eq!(storage.read(path, 0, 8).await?, b"recovery".as_slice());
            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_queue_survives_cgroup_scale_address_space_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let executable = std::env::current_exe()?;
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg("ulimit -v 262144 && exec \"$@\"")
            .arg("dendrite-memory-pressure")
            .arg(executable)
            .arg("--ignored")
            .arg("--exact")
            .arg("tests::address_space_limit_child")
            .arg("--nocapture")
            .env("DENDRITE_MEMORY_PRESSURE_CHILD", "1")
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "memory-pressure child failed with {}:\n{}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "subprocess helper for bounded_queue_survives_cgroup_scale_address_space_limit"]
    fn address_space_limit_child() -> Result<(), Box<dyn std::error::Error>> {
        const OPERATIONS: usize = 256;
        const BLOCK: usize = 64 * 1024;
        if std::env::var_os("DENDRITE_MEMORY_PRESSURE_CHILD").is_none() {
            return Ok(());
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let root = tempfile::tempdir()?;
            let storage = StorageHandle::start_portable(root.path(), 2)?;
            let path = TorrentPath::new(["bounded-memory.bin".to_owned()])?;
            let length = u64::try_from(OPERATIONS * BLOCK)?;
            let mut writes = tokio::task::JoinSet::new();
            for operation in 0..OPERATIONS {
                let storage = storage.clone();
                let path = path.clone();
                writes.spawn(async move {
                    storage
                        .write(
                            path,
                            u64::try_from(operation * BLOCK)
                                .map_err(|_| StorageError::RangeOverflow)?,
                            Bytes::from(vec![u8::try_from(operation % 251).unwrap_or(0); BLOCK]),
                            length,
                        )
                        .await
                });
            }
            while let Some(result) = writes.join_next().await {
                result??;
            }
            storage.sync(path).await?;
            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }
}
