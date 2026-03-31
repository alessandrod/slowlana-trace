pub mod agave;
pub use shaq;
use {
    shaq::mpmc,
    std::{
        ffi::OsString,
        fmt,
        fs::{self, File, OpenOptions},
        io, mem,
        os::fd::AsRawFd as _,
        path::{Path, PathBuf},
        ptr::NonNull,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    },
};

pub const ABI_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceQueuePaths {
    pub queue_path: PathBuf,
    pub meta_path: PathBuf,
}

impl TraceQueuePaths {
    pub fn new(queue_path: impl Into<PathBuf>, meta_path: impl Into<PathBuf>) -> Self {
        Self {
            queue_path: queue_path.into(),
            meta_path: meta_path.into(),
        }
    }
}

#[repr(C)]
pub struct TraceQueueMetaV1 {
    pub abi_version: u32,
    reserved0: u32,
    pub drop_count: AtomicU64,
}

pub struct TraceProducer<T> {
    inner: mpmc::Producer<T>,
    meta: MetaMap,
    paths: TraceQueuePaths,
}

impl<T> TraceProducer<T> {
    pub fn create(paths: TraceQueuePaths, capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidCapacity);
        }

        ensure_parent_dirs(&paths)?;
        let queue_file = create_queue_file(&paths.queue_path)?;
        let file_size = mpmc::minimum_file_size::<T>(capacity);
        let inner =
            unsafe { mpmc::Producer::create(&queue_file, file_size) }.map_err(Error::Shaq)?;
        let meta = MetaMap::create(&paths.meta_path)?;

        Ok(Self { inner, meta, paths })
    }

    pub fn join(paths: TraceQueuePaths) -> Result<Self, Error> {
        let meta = MetaMap::join(&paths.meta_path)?;
        meta.validate()?;
        let queue_file = open_existing_rw(&paths.queue_path)?;
        let inner = unsafe { mpmc::Producer::join(&queue_file) }.map_err(Error::Shaq)?;

        Ok(Self { inner, meta, paths })
    }

    pub fn try_write(&self, item: T) -> Result<(), T> {
        self.inner.try_write(item)
    }

    /// # Safety
    /// - The caller must initialize the reserved slot before the guard is dropped.
    pub unsafe fn reserve_write(&self) -> Option<mpmc::WriteGuard<'_, T>> {
        unsafe { self.inner.reserve_write() }
    }

    /// # Safety
    /// - The caller must initialize all reserved slots before the batch is dropped.
    pub unsafe fn reserve_write_batch(&self, count: usize) -> Option<mpmc::WriteBatch<'_, T>> {
        unsafe { self.inner.reserve_write_batch(count) }
    }

    pub fn record_drop(&self) {
        self.meta.meta().drop_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn drop_count(&self) -> u64 {
        self.meta.meta().drop_count.load(Ordering::Relaxed)
    }

    pub fn queue_path(&self) -> &Path {
        &self.paths.queue_path
    }

    pub fn meta_path(&self) -> &Path {
        &self.paths.meta_path
    }
}

pub struct TraceConsumer<T> {
    inner: mpmc::Consumer<T>,
    meta: MetaMap,
    paths: TraceQueuePaths,
}

impl<T> TraceConsumer<T> {
    pub fn create(paths: TraceQueuePaths, capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidCapacity);
        }

        ensure_parent_dirs(&paths)?;
        let queue_file = create_queue_file(&paths.queue_path)?;
        let file_size = mpmc::minimum_file_size::<T>(capacity);
        let inner =
            unsafe { mpmc::Consumer::create(&queue_file, file_size) }.map_err(Error::Shaq)?;
        let meta = MetaMap::create(&paths.meta_path)?;

        Ok(Self { inner, meta, paths })
    }

    pub fn join(paths: TraceQueuePaths) -> Result<Self, Error> {
        let meta = MetaMap::join(&paths.meta_path)?;
        meta.validate()?;
        let queue_file = open_existing_rw(&paths.queue_path)?;
        let inner = unsafe { mpmc::Consumer::join(&queue_file) }.map_err(Error::Shaq)?;

        Ok(Self { inner, meta, paths })
    }

    pub fn try_read(&self) -> Option<T> {
        self.inner.try_read()
    }

    pub fn reserve_read(&self) -> Option<mpmc::ReadGuard<'_, T>> {
        self.inner.reserve_read()
    }

    pub fn reserve_read_batch(&self, max: usize) -> Option<mpmc::ReadBatch<'_, T>> {
        self.inner.reserve_read_batch(max)
    }

    pub fn drop_count(&self) -> u64 {
        self.meta.meta().drop_count.load(Ordering::Relaxed)
    }

    pub fn queue_path(&self) -> &Path {
        &self.paths.queue_path
    }

    pub fn meta_path(&self) -> &Path {
        &self.paths.meta_path
    }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Shaq(shaq::error::Error),
    AbiVersionMismatch { expected: u32, actual: u32 },
    InvalidMeta(&'static str),
    InvalidCapacity,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Shaq(err) => write!(f, "{err}"),
            Self::AbiVersionMismatch { expected, actual } => {
                write!(f, "trace ABI mismatch, expected {expected}, got {actual}")
            }
            Self::InvalidMeta(message) => write!(f, "invalid trace meta: {message}"),
            Self::InvalidCapacity => write!(f, "trace queue capacity cannot be zero"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

struct MetaMap {
    ptr: NonNull<TraceQueueMetaV1>,
    len: usize,
}

unsafe impl Send for MetaMap {}
unsafe impl Sync for MetaMap {}

impl MetaMap {
    fn create(path: &Path) -> Result<Self, Error> {
        let temp_path = temp_meta_path(path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        let result = (|| {
            file.set_len(mem::size_of::<TraceQueueMetaV1>() as u64)?;
            let meta = Self::map(&file)?;
            meta.initialize()?;
            meta.flush()?;
            drop(meta);
            if !publish_temp_file(&temp_path, path)? {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("trace meta {} already exists", path.display()),
                )));
            }
            Self::join(path)
        })();

        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }

        result
    }

    fn join(path: &Path) -> Result<Self, Error> {
        let file = open_existing_rw(path)?;
        Self::map(&file)
    }

    fn map(file: &File) -> Result<Self, Error> {
        let len = file.metadata()?.len() as usize;
        let expected_len = mem::size_of::<TraceQueueMetaV1>();
        if len < expected_len {
            return Err(Error::InvalidMeta("meta file is too small"));
        }

        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                expected_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            return Err(Error::Io(io::Error::last_os_error()));
        }

        let ptr = NonNull::new(addr.cast::<TraceQueueMetaV1>())
            .ok_or(Error::InvalidMeta("meta map returned null"))?;
        Ok(Self {
            ptr,
            len: expected_len,
        })
    }

    fn initialize(&self) -> Result<(), Error> {
        unsafe { std::ptr::write_bytes(self.ptr.as_ptr().cast::<u8>(), 0, self.len) };
        unsafe {
            (*self.ptr.as_ptr()).abi_version = ABI_VERSION;
            (*self.ptr.as_ptr()).drop_count.store(0, Ordering::Relaxed);
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), Error> {
        let meta = self.meta();
        if meta.abi_version != ABI_VERSION {
            return Err(Error::AbiVersionMismatch {
                expected: ABI_VERSION,
                actual: meta.abi_version,
            });
        }
        Ok(())
    }

    fn meta(&self) -> &TraceQueueMetaV1 {
        unsafe { self.ptr.as_ref() }
    }

    fn flush(&self) -> Result<(), Error> {
        let ret = unsafe {
            libc::msync(
                self.ptr.as_ptr().cast::<libc::c_void>(),
                self.len,
                libc::MS_SYNC,
            )
        };
        if ret != 0 {
            return Err(Error::Io(io::Error::last_os_error()));
        }
        Ok(())
    }
}

impl Drop for MetaMap {
    fn drop(&mut self) {
        let ret = unsafe { libc::munmap(self.ptr.as_ptr().cast::<libc::c_void>(), self.len) };
        if ret != 0 {
            let _ = io::Error::last_os_error();
        }
    }
}

fn ensure_parent_dirs(paths: &TraceQueuePaths) -> Result<(), Error> {
    if let Some(parent) = paths.queue_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = paths.meta_path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn create_queue_file(path: &Path) -> Result<File, Error> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?)
}

fn open_existing_rw(path: &Path) -> Result<File, Error> {
    Ok(OpenOptions::new().read(true).write(true).open(path)?)
}

fn temp_meta_path(path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut file_name = OsString::from(".");
    file_name.push(
        path.file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("trace")),
    );
    file_name.push(format!(".tmp-{}-{nanos}", std::process::id()));
    path.with_file_name(file_name)
}

fn publish_temp_file(temp_path: &Path, path: &Path) -> Result<bool, Error> {
    match fs::hard_link(temp_path, path) {
        Ok(()) => {
            fs::remove_file(temp_path)?;
            Ok(true)
        }
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(temp_path);
            Ok(false)
        }
        Err(err) => {
            let _ = fs::remove_file(temp_path);
            Err(Error::Io(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            Error, TraceConsumer, TraceProducer, TraceQueueMetaV1, TraceQueuePaths, ABI_VERSION,
        },
        shaq::mpmc,
        std::{
            fs::{self, OpenOptions},
            io::{ErrorKind, Seek, SeekFrom, Write},
            mem,
            path::{Path, PathBuf},
            sync::atomic::Ordering,
            time::{SystemTime, UNIX_EPOCH},
        },
    };

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("slowlana-trace-{label}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn queue_paths(dir: &Path, stem: &str) -> TraceQueuePaths {
        TraceQueuePaths::new(
            dir.join(format!("{stem}.q")),
            dir.join(format!("{stem}.meta")),
        )
    }

    #[test]
    fn meta_layout_is_stable() {
        assert_eq!(mem::size_of::<TraceQueueMetaV1>(), 16);
        assert_eq!(mem::align_of::<TraceQueueMetaV1>(), 8);
    }

    #[test]
    fn producer_and_consumer_round_trip() {
        let dir = temp_dir("producer-consumer");
        let paths = queue_paths(&dir, "numbers");

        let producer = TraceProducer::<u64>::create(paths.clone(), 16).expect("producer");
        assert_eq!(producer.try_write(123), Ok(()));
        producer.record_drop();

        let consumer = TraceConsumer::<u64>::join(paths.clone()).expect("consumer");
        assert_eq!(consumer.try_read(), Some(123));
        assert_eq!(consumer.drop_count(), 1);

        cleanup(&paths);
        fs::remove_dir(&dir).expect("remove dir");
    }

    #[test]
    fn producer_can_join_existing_queue() {
        let dir = temp_dir("producer-join");
        let paths = queue_paths(&dir, "numbers");

        let producer = TraceProducer::<u64>::create(paths.clone(), 16).expect("producer");
        let producer_join = TraceProducer::<u64>::join(paths.clone()).expect("producer join");

        assert_eq!(producer.try_write(7), Ok(()));
        assert_eq!(producer_join.try_write(8), Ok(()));

        let consumer = TraceConsumer::<u64>::join(paths.clone()).expect("consumer");
        assert_eq!(consumer.try_read(), Some(7));
        assert_eq!(consumer.try_read(), Some(8));

        cleanup(&paths);
        fs::remove_dir(&dir).expect("remove dir");
    }

    #[test]
    fn consumer_can_create_and_join_existing_queue() {
        let dir = temp_dir("consumer-join");
        let paths = queue_paths(&dir, "numbers");

        let consumer = TraceConsumer::<u64>::create(paths.clone(), 16).expect("consumer");
        let consumer_join = TraceConsumer::<u64>::join(paths.clone()).expect("consumer join");
        let producer = TraceProducer::<u64>::join(paths.clone()).expect("producer");

        assert_eq!(producer.try_write(99), Ok(()));
        assert_eq!(consumer.try_read(), Some(99));
        assert_eq!(consumer_join.try_read(), None);

        cleanup(&paths);
        fs::remove_dir(&dir).expect("remove dir");
    }

    #[test]
    fn join_requires_meta_file() {
        let dir = temp_dir("missing-meta");
        let paths = queue_paths(&dir, "numbers");
        let queue_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&paths.queue_path)
            .expect("queue file");
        let file_size = mpmc::minimum_file_size::<u64>(16);
        let _ = unsafe { mpmc::Producer::<u64>::create(&queue_file, file_size) }.expect("queue");

        let err = match TraceConsumer::<u64>::join(paths.clone()) {
            Ok(_) => panic!("missing meta must fail"),
            Err(err) => err,
        };
        match err {
            Error::Io(inner) => assert_eq!(inner.kind(), ErrorKind::NotFound),
            other => panic!("unexpected error: {other}"),
        }

        let _ = fs::remove_file(&paths.queue_path);
        fs::remove_dir(&dir).expect("remove dir");
    }

    #[test]
    fn join_rejects_invalid_abi_version() {
        let dir = temp_dir("invalid-abi");
        let paths = queue_paths(&dir, "numbers");
        let producer = TraceProducer::<u64>::create(paths.clone(), 16).expect("producer");

        let mut meta = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&paths.meta_path)
            .expect("meta file");
        meta.seek(SeekFrom::Start(0)).expect("seek");
        meta.write_all(&(ABI_VERSION + 1).to_ne_bytes())
            .expect("write abi");
        drop(meta);
        drop(producer);

        let err = match TraceConsumer::<u64>::join(paths.clone()) {
            Ok(_) => panic!("abi mismatch must fail"),
            Err(err) => err,
        };
        match err {
            Error::AbiVersionMismatch { expected, actual } => {
                assert_eq!(expected, ABI_VERSION);
                assert_eq!(actual, ABI_VERSION + 1);
            }
            other => panic!("unexpected error: {other}"),
        }

        cleanup(&paths);
        fs::remove_dir(&dir).expect("remove dir");
    }

    #[test]
    fn wrong_type_still_fails_through_shaq() {
        let dir = temp_dir("wrong-type");
        let paths = queue_paths(&dir, "numbers");

        let producer = TraceProducer::<u64>::create(paths.clone(), 16).expect("producer");
        let err = match TraceConsumer::<u32>::join(paths.clone()) {
            Ok(_) => panic!("wrong type must fail"),
            Err(err) => err,
        };
        match err {
            Error::Shaq(_) => {}
            other => panic!("unexpected error: {other}"),
        }
        drop(producer);

        cleanup(&paths);
        fs::remove_dir(&dir).expect("remove dir");
    }

    #[test]
    fn drop_counter_persists_across_joins() {
        let dir = temp_dir("drop-counter");
        let paths = queue_paths(&dir, "numbers");

        let producer = TraceProducer::<u64>::create(paths.clone(), 16).expect("producer");
        producer.record_drop();
        producer.record_drop();
        drop(producer);

        let consumer = TraceConsumer::<u64>::join(paths.clone()).expect("consumer");
        assert_eq!(
            consumer.drop_count(),
            2,
            "drop count should be readable from a later join"
        );
        assert_eq!(
            consumer.meta.meta().drop_count.load(Ordering::Relaxed),
            2,
            "mapped metadata should hold the same value"
        );

        cleanup(&paths);
        fs::remove_dir(&dir).expect("remove dir");
    }

    fn cleanup(paths: &TraceQueuePaths) {
        let _ = fs::remove_file(&paths.meta_path);
        let _ = fs::remove_file(&paths.queue_path);
    }
}
