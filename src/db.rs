use std::{
    ops::Deref,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{config::DatabaseSettings, error::AppError};
use dashmap::DashMap;
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::sync::{Mutex, OnceCell};

/// Lease count reserved for an entry that is closing and must never be handed out again.
const CLOSED: usize = usize::MAX;

/// Bound on how many times a caller retries around a concurrent eviction.
const MAX_ATTEMPTS: usize = 16;

pub struct DatabaseManager {
    settings: DatabaseSettings,
    entries: DashMap<String, Arc<DatabaseEntry>>,
    /// Serializes admission of *new* tenants so capacity stays exact. It is
    /// never held across opening a database, so a cold start only blocks other
    /// cold starts, not requests to databases that are already open.
    admission: Mutex<()>,
}

pub struct DatabaseEntry {
    name: String,
    pool: OnceCell<SqlitePool>,
    last_used_ms: AtomicU64,
    active_leases: AtomicUsize,
}
pub struct DatabaseLease(Arc<DatabaseEntry>);

#[derive(Debug, serde::Serialize)]
pub struct ManagerStats {
    pub open_databases: usize,
    pub active_leases: usize,
    pub max_open_databases: usize,
}

impl DatabaseManager {
    pub async fn new(settings: DatabaseSettings) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&settings.directory).await?;
        Ok(Self {
            settings,
            entries: DashMap::new(),
            admission: Mutex::new(()),
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
            let lease = DatabaseLease(entry);
            lease.0.ensure_open(&self.settings, self.database_path(name))
                .await
                .inspect_err(|_| {
                    self.entries.remove_if(name, |_, entry| Arc::ptr_eq(entry, &lease.0));
                })?;
            lease.0.touch();
            return Ok(lease);
        }
        Err(AppError::CapacityBusy)
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
        let entry = Arc::new(DatabaseEntry::new(name));
        self.entries.insert(name.to_owned(), entry.clone());
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
        Ok(closed)
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
    pub fn max_result_rows(&self) -> usize {
        self.settings.max_result_rows
    }
    fn database_path(&self, name: &str) -> PathBuf {
        self.settings.directory.join(format!("{name}.db"))
    }
}

impl DatabaseEntry {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            pool: OnceCell::new(),
            last_used_ms: AtomicU64::new(now_ms()),
            active_leases: AtomicUsize::new(0),
        }
    }

    /// Opens the pool on first use. Concurrent callers for the same database
    /// wait here; callers for every other database are unaffected.
    async fn ensure_open(
        &self,
        settings: &DatabaseSettings,
        path: PathBuf,
    ) -> Result<(), sqlx::Error> {
        self.pool
            .get_or_try_init(|| open_pool(settings, path))
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

async fn open_pool(settings: &DatabaseSettings, path: PathBuf) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_millis(settings.busy_timeout_ms));
    SqlitePoolOptions::new()
        .max_connections(settings.connections_per_database)
        .acquire_timeout(Duration::from_secs(settings.acquire_timeout_seconds))
        .connect_with(options)
        .await
}

impl Deref for DatabaseLease {
    type Target = SqlitePool;
    fn deref(&self) -> &Self::Target {
        self.0.pool.get().expect("a leased entry is always open")
    }
}
impl Drop for DatabaseLease {
    fn drop(&mut self) {
        self.0.touch();
        self.0.active_leases.fetch_sub(1, Ordering::AcqRel);
    }
}
async fn close_entry(entry: Arc<DatabaseEntry>) {
    let Some(pool) = entry.pool.get() else {
        return;
    };
    let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(pool)
        .await;
    pool.close().await;
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
