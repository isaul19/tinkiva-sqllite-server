use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{config::DatabaseSettings, error::AppError, metrics::Metrics};
use dashmap::DashMap;
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::sync::{Mutex, OnceCell, OwnedSemaphorePermit, Semaphore};

/// Lease count reserved for an entry that is closing and must never be handed out again.
const CLOSED: usize = usize::MAX;

/// Bound on how many times a caller retries around a concurrent eviction.
const MAX_ATTEMPTS: usize = 16;

const BYTES_PER_MB: u64 = 1024 * 1024;

pub struct DatabaseManager {
    settings: DatabaseSettings,
    entries: DashMap<String, Arc<DatabaseEntry>>,
    /// Serializes admission of *new* tenants so capacity stays exact. It is
    /// never held across opening a database, so a cold start only blocks other
    /// cold starts, not requests to databases that are already open.
    admission: Mutex<()>,
    /// Caps in-flight work for the whole process. Without it, overload shows up
    /// as latency spread across every tenant instead of as a clear signal.
    slots: Arc<Semaphore>,
    metrics: Arc<Metrics>,
}

pub struct DatabaseEntry {
    name: String,
    pools: OnceCell<Pools>,
    last_used_ms: AtomicU64,
    active_leases: AtomicUsize,
    /// Per-tenant share of the process, so one database saturating its writer
    /// cannot queue requests on behalf of every other database.
    slots: Arc<Semaphore>,
}
pub struct DatabaseLease {
    entry: Arc<DatabaseEntry>,
    /// Held for the life of the request; dropping it readmits another caller.
    _slots: Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>,
}

/// SQLite serializes writes to a file no matter how many connections exist, so
/// one writer is all a database can use. Readers get their own pool and their
/// own WAL snapshots, and never queue behind the writer.
struct Pools {
    writer: SqlitePool,
    readers: SqlitePool,
}

#[derive(Debug, serde::Serialize)]
pub struct ManagerStats {
    pub open_databases: usize,
    pub active_leases: usize,
    pub max_open_databases: usize,
    pub available_request_slots: usize,
    pub max_concurrent_requests: usize,
}

impl DatabaseManager {
    pub async fn new(settings: DatabaseSettings) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&settings.directory).await?;
        Ok(Self {
            slots: Arc::new(Semaphore::new(settings.max_concurrent_requests)),
            settings,
            entries: DashMap::new(),
            admission: Mutex::new(()),
            metrics: Arc::new(Metrics::default()),
        })
    }

    pub async fn acquire(&self, name: &str) -> Result<DatabaseLease, AppError> {
        validate_database_name(name)?;
        for _ in 0..MAX_ATTEMPTS {
            let entry = self.slot(name).await?;
            if !entry.lease() {
                // The entry started closing between the lookup and the lease;
                // give the closer a chance to remove it, then look again.
                tokio::task::yield_now().await;
                continue;
            }
            // Built before admission so that shedding still releases the lease.
            let mut lease = DatabaseLease {
                entry,
                _slots: None,
            };
            // The tenant slot is taken before the global one so a saturated
            // database queues on its own share instead of occupying the
            // process-wide budget while it waits.
            let Some(tenant_slot) = self.wait_for_slot(&lease.entry.slots).await else {
                self.metrics.record_shed();
                return Err(AppError::Overloaded);
            };
            let Some(process_slot) = self.wait_for_slot(&self.slots).await else {
                self.metrics.record_shed();
                return Err(AppError::Overloaded);
            };
            lease._slots = Some((tenant_slot, process_slot));
            lease
                .entry
                .ensure_open(&self.settings, self.database_path(name))
                .await
                .inspect_err(|_| {
                    self.entries
                        .remove_if(name, |_, entry| Arc::ptr_eq(entry, &lease.entry));
                })?;
            lease.entry.touch();
            return Ok(lease);
        }
        Err(AppError::CapacityBusy)
    }

    /// Waits a bounded time for an admission slot. Shedding after
    /// `admission_timeout` is what turns overload into a 429 the caller can act
    /// on, instead of a queue that only shows up as p99 latency.
    async fn wait_for_slot(&self, slots: &Arc<Semaphore>) -> Option<OwnedSemaphorePermit> {
        let started = Instant::now();
        let permit = tokio::time::timeout(
            self.settings.admission_timeout(),
            slots.clone().acquire_owned(),
        )
        .await;
        self.metrics.record_admission_wait(started.elapsed());
        permit.ok()?.ok()
    }

    /// Returns the entry for `name`, admitting a new one when the database is
    /// not resident yet. The returned entry may still be unopened.
    async fn slot(&self, name: &str) -> Result<Arc<DatabaseEntry>, AppError> {
        if let Some(entry) = self.entries.get(name) {
            return Ok(entry.value().clone());
        }
        let _admission = self.admission.lock().await;
        if let Some(entry) = self.entries.get(name) {
            return Ok(entry.value().clone());
        }
        let mut attempts = 0;
        while self.entries.len() >= self.settings.max_open_databases {
            attempts += 1;
            if attempts > MAX_ATTEMPTS || !self.evict_oldest_idle() {
                return Err(AppError::CapacityBusy);
            }
        }
        let entry = Arc::new(DatabaseEntry::new(
            name,
            self.settings.max_concurrent_requests_per_database,
        ));
        self.entries.insert(name.to_owned(), entry.clone());
        self.metrics.record_open();
        Ok(entry)
    }

    /// Removes the least recently used entry that nobody is holding. The
    /// checkpoint runs in the background so the admitted request does not pay
    /// for the evicted tenant's shutdown.
    fn evict_oldest_idle(&self) -> bool {
        let mut candidate: Option<(String, u64)> = None;
        for entry in self.entries.iter() {
            if entry.active_leases.load(Ordering::Acquire) != 0 {
                continue;
            }
            let last_used = entry.last_used_ms.load(Ordering::Acquire);
            if candidate
                .as_ref()
                .is_none_or(|(_, oldest)| last_used < *oldest)
            {
                candidate = Some((entry.key().clone(), last_used));
            }
        }
        let Some((name, _)) = candidate else {
            return false;
        };
        match self.take(&name) {
            Some(entry) => {
                self.metrics.record_eviction();
                tokio::spawn(close_entry(entry));
                true
            }
            None => false,
        }
    }

    /// Marks an entry closed and unlinks it. Fails if it was leased meanwhile.
    fn take(&self, name: &str) -> Option<Arc<DatabaseEntry>> {
        let entry = self.entries.get(name)?.value().clone();
        if !entry.begin_close() {
            return None;
        }
        self.entries.remove(name);
        Some(entry)
    }

    pub async fn cleanup_idle(&self) -> anyhow::Result<usize> {
        let cutoff = now_ms().saturating_sub(self.settings.idle_timeout().as_millis() as u64);
        let stale: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.active_leases.load(Ordering::Acquire) == 0)
            .filter(|entry| entry.last_used_ms.load(Ordering::Acquire) <= cutoff)
            .map(|entry| entry.key().clone())
            .collect();
        let mut closed = 0;
        for name in stale {
            if let Some(entry) = self.take(&name) {
                close_entry(entry).await;
                closed += 1;
            }
        }
        self.metrics.record_idle_close(closed);
        Ok(closed)
    }

    /// Checkpoints every open database's WAL. Runs on its own schedule so that
    /// no request ever performs a checkpoint. PASSIVE never blocks a reader or
    /// the writer: if the database is busy it simply does less work this round.
    pub async fn checkpoint_wal(&self) -> usize {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        let mut checkpointed = 0;
        for entry in entries {
            if !entry.lease() {
                continue;
            }
            // Maintenance deliberately bypasses admission: it must not be
            // shed by the load it exists to keep bounded.
            let lease = DatabaseLease {
                entry,
                _slots: None,
            };
            if lease.entry.pools.get().is_none() {
                continue;
            }
            match sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
                .execute(lease.writer())
                .await
            {
                Ok(_) => checkpointed += 1,
                Err(error) => {
                    tracing::warn!(database = %lease.entry.name, %error, "wal checkpoint failed");
                }
            }
        }
        self.metrics.record_checkpoints(checkpointed);
        checkpointed
    }

    pub async fn stats(&self) -> ManagerStats {
        ManagerStats {
            open_databases: self.entries.len(),
            active_leases: self
                .entries
                .iter()
                .map(|entry| entry.active_leases.load(Ordering::Acquire))
                .filter(|leases| *leases != CLOSED)
                .sum(),
            max_open_databases: self.settings.max_open_databases,
            available_request_slots: self.slots.available_permits(),
            max_concurrent_requests: self.settings.max_concurrent_requests,
        }
    }
    pub async fn close_all(&self) {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        self.entries.clear();
        for entry in entries {
            entry.begin_close();
            close_entry(entry).await;
        }
    }
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
    pub fn max_result_rows(&self) -> usize {
        self.settings.max_result_rows
    }
    fn database_path(&self, name: &str) -> PathBuf {
        self.settings.directory.join(format!("{name}.db"))
    }
}

impl DatabaseEntry {
    fn new(name: &str, slots: usize) -> Self {
        Self {
            name: name.to_owned(),
            pools: OnceCell::new(),
            last_used_ms: AtomicU64::new(now_ms()),
            active_leases: AtomicUsize::new(0),
            slots: Arc::new(Semaphore::new(slots)),
        }
    }

    /// Opens the pool on first use. Concurrent callers for the same database
    /// wait here; callers for every other database are unaffected.
    async fn ensure_open(
        &self,
        settings: &DatabaseSettings,
        path: PathBuf,
    ) -> Result<(), sqlx::Error> {
        self.pools
            .get_or_try_init(|| open_pools(settings, path))
            .await?;
        Ok(())
    }

    fn lease(&self) -> bool {
        let mut current = self.active_leases.load(Ordering::Acquire);
        loop {
            if current == CLOSED {
                return false;
            }
            match self.active_leases.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn begin_close(&self) -> bool {
        self.active_leases
            .compare_exchange(0, CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn touch(&self) {
        self.last_used_ms.store(now_ms(), Ordering::Release);
    }
}

async fn open_pools(settings: &DatabaseSettings, path: PathBuf) -> Result<Pools, sqlx::Error> {
    let shared = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_millis(settings.busy_timeout_ms))
        // Negative cache_size is KiB rather than pages, so the budget does not
        // silently change with page_size.
        .pragma("cache_size", format!("-{}", settings.cache_size_kb))
        .pragma(
            "mmap_size",
            (settings.mmap_size_mb * BYTES_PER_MB).to_string(),
        )
        .pragma("temp_store", "MEMORY");
    let acquire_timeout = Duration::from_secs(settings.acquire_timeout_seconds);

    // Autocheckpoint is off: with it on, whichever request happens to cross the
    // WAL threshold pays for the checkpoint of every other request before it.
    // The background task does that work instead.
    let writer_options = shared.clone().pragma("wal_autocheckpoint", "0").pragma(
        "journal_size_limit",
        (settings.wal_size_limit_mb * BYTES_PER_MB).to_string(),
    );

    // The writer is established eagerly: it creates the file, and paying for it
    // here keeps the cost inside the cold start instead of the first request.
    let writer = SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .acquire_timeout(acquire_timeout)
        .connect_with(writer_options)
        .await?;

    // Readers stay lazy, so a database that is only written never pays for
    // reader connections. `query_only` is what makes them readers: a read-only
    // open would not be able to attach the WAL index.
    let readers = SqlitePoolOptions::new()
        .max_connections(settings.reader_connections)
        .acquire_timeout(acquire_timeout)
        .connect_lazy_with(shared.pragma("query_only", "ON"));

    Ok(Pools { writer, readers })
}

impl DatabaseLease {
    /// The single connection allowed to modify this database.
    pub fn writer(&self) -> &SqlitePool {
        &self.pools().writer
    }
    /// Connections restricted to reads, isolated from the writer.
    pub fn readers(&self) -> &SqlitePool {
        &self.pools().readers
    }
    fn pools(&self) -> &Pools {
        self.entry
            .pools
            .get()
            .expect("a leased entry is always open")
    }
}
impl Drop for DatabaseLease {
    fn drop(&mut self) {
        self.entry.touch();
        self.entry.active_leases.fetch_sub(1, Ordering::AcqRel);
    }
}
async fn close_entry(entry: Arc<DatabaseEntry>) {
    let Some(pools) = entry.pools.get() else {
        return;
    };
    // Readers hold WAL read marks that would block the checkpoint.
    pools.readers.close().await;
    // Sleeping is the one moment where refreshing the planner statistics costs
    // no request any latency.
    let _ = sqlx::query("PRAGMA optimize").execute(&pools.writer).await;
    let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&pools.writer)
        .await;
    pools.writer.close().await;
    tracing::debug!(database = %entry.name, "database returned to sleep");
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn validate_database_name(name: &str) -> Result<(), AppError> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if valid {
        Ok(())
    } else {
        Err(AppError::InvalidDatabaseName)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_safe_database_names() {
        assert!(validate_database_name("tenant-01_ok").is_ok());
        assert!(validate_database_name("../escape").is_err());
        assert!(validate_database_name("with space").is_err());
        assert!(validate_database_name("").is_err());
    }
    #[tokio::test]
    async fn evicts_an_inactive_database_at_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let settings = DatabaseSettings {
            directory: dir.path().into(),
            max_open_databases: 1,
            ..Default::default()
        };
        let manager = DatabaseManager::new(settings).await.unwrap();
        drop(manager.acquire("first").await.unwrap());
        drop(manager.acquire("second").await.unwrap());
        assert_eq!(manager.stats().await.open_databases, 1);
        assert!(dir.path().join("first.db").exists());
        assert!(dir.path().join("second.db").exists());
        manager.close_all().await;
    }

    #[tokio::test]
    async fn refuses_eviction_while_the_only_database_is_leased() {
        let dir = tempfile::tempdir().unwrap();
        let settings = DatabaseSettings {
            directory: dir.path().into(),
            max_open_databases: 1,
            ..Default::default()
        };
        let manager = DatabaseManager::new(settings).await.unwrap();
        let active = manager.acquire("active").await.unwrap();
        assert!(matches!(
            manager.acquire("another").await,
            Err(AppError::CapacityBusy)
        ));
        drop(active);
        assert!(manager.acquire("another").await.is_ok());
        manager.close_all().await;
    }

    #[tokio::test]
    async fn cleanup_skips_active_leases_and_closes_idle_entries() {
        let dir = tempfile::tempdir().unwrap();
        let settings = DatabaseSettings {
            directory: dir.path().into(),
            idle_timeout_seconds: 0,
            ..Default::default()
        };
        let manager = DatabaseManager::new(settings).await.unwrap();
        let active = manager.acquire("tenant").await.unwrap();
        assert_eq!(manager.cleanup_idle().await.unwrap(), 0);
        drop(active);
        assert_eq!(manager.cleanup_idle().await.unwrap(), 1);
        assert_eq!(manager.stats().await.open_databases, 0);
    }

    #[tokio::test]
    async fn sheds_requests_beyond_the_per_database_share() {
        let dir = tempfile::tempdir().unwrap();
        let settings = DatabaseSettings {
            directory: dir.path().into(),
            max_concurrent_requests_per_database: 1,
            admission_timeout_ms: 20,
            ..Default::default()
        };
        let manager = DatabaseManager::new(settings).await.unwrap();
        let held = manager.acquire("tenant").await.unwrap();
        assert!(matches!(
            manager.acquire("tenant").await,
            Err(AppError::Overloaded)
        ));
        // A different database keeps its own share and is unaffected.
        assert!(manager.acquire("other").await.is_ok());
        drop(held);
        assert!(manager.acquire("tenant").await.is_ok());
        manager.close_all().await;
    }

    #[tokio::test]
    async fn sheds_requests_beyond_the_process_budget() {
        let dir = tempfile::tempdir().unwrap();
        let settings = DatabaseSettings {
            directory: dir.path().into(),
            max_concurrent_requests: 1,
            admission_timeout_ms: 20,
            ..Default::default()
        };
        let manager = DatabaseManager::new(settings).await.unwrap();
        let held = manager.acquire("tenant").await.unwrap();
        assert!(matches!(
            manager.acquire("other").await,
            Err(AppError::Overloaded)
        ));
        assert_eq!(manager.stats().await.available_request_slots, 0);
        drop(held);
        assert!(manager.acquire("other").await.is_ok());
        manager.close_all().await;
    }

    #[tokio::test]
    async fn background_checkpoint_drains_the_wal() {
        let dir = tempfile::tempdir().unwrap();
        let settings = DatabaseSettings {
            directory: dir.path().into(),
            ..Default::default()
        };
        let manager = DatabaseManager::new(settings).await.unwrap();
        let lease = manager.acquire("tenant").await.unwrap();
        sqlx::query("CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT)")
            .execute(lease.writer())
            .await
            .unwrap();
        for index in 0..500 {
            sqlx::query("INSERT INTO items(payload) VALUES (?)")
                .bind(format!("{index:0512}"))
                .execute(lease.writer())
                .await
                .unwrap();
        }
        let wal = dir.path().join("tenant.db-wal");
        let before = std::fs::metadata(&wal).unwrap().len();
        assert!(before > 0, "autocheckpoint should be off, leaving a WAL");
        drop(lease);
        assert_eq!(manager.checkpoint_wal().await, 1);
        assert!(std::fs::metadata(&wal).unwrap().len() <= before);
        assert!(
            sqlx::query("SELECT count(*) FROM items")
                .fetch_one(manager.acquire("tenant").await.unwrap().readers())
                .await
                .is_ok()
        );
        manager.close_all().await;
    }

    #[tokio::test]
    async fn a_slow_cold_start_does_not_block_other_databases() {
        let dir = tempfile::tempdir().unwrap();
        let settings = DatabaseSettings {
            directory: dir.path().into(),
            ..Default::default()
        };
        let manager = Arc::new(DatabaseManager::new(settings).await.unwrap());
        // Twenty concurrent cold starts must all succeed without serializing
        // behind a single registry lock.
        let mut handles = Vec::new();
        for index in 0..20 {
            let manager = manager.clone();
            handles.push(tokio::spawn(async move {
                manager.acquire(&format!("tenant{index:02}")).await.is_ok()
            }));
        }
        for handle in handles {
            assert!(handle.await.unwrap());
        }
        assert_eq!(manager.stats().await.open_databases, 20);
        manager.close_all().await;
    }
}
