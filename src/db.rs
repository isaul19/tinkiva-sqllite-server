use std::{
    collections::HashMap,
    ops::Deref,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{config::DatabaseSettings, error::AppError};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::sync::Mutex;

pub struct DatabaseManager {
    settings: DatabaseSettings,
    entries: Mutex<HashMap<String, Arc<DatabaseEntry>>>,
}
pub struct DatabaseEntry {
    name: String,
    pool: SqlitePool,
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
            entries: Mutex::new(HashMap::new()),
        })
    }

    pub async fn acquire(&self, name: &str) -> Result<DatabaseLease, AppError> {
        validate_database_name(name)?;
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get(name) {
            entry.active_leases.fetch_add(1, Ordering::AcqRel);
            entry.touch();
            return Ok(DatabaseLease(entry.clone()));
        }
        let evicted = if entries.len() >= self.settings.max_open_databases {
            let candidate = entries
                .iter()
                .filter(|(_, entry)| entry.active_leases.load(Ordering::Acquire) == 0)
                .min_by_key(|(_, entry)| entry.last_used_ms.load(Ordering::Acquire))
                .map(|(name, _)| name.clone())
                .ok_or(AppError::CapacityBusy)?;
            entries.remove(&candidate)
        } else {
            None
        };
        let entry = Arc::new(self.open(name).await?);
        entry.active_leases.store(1, Ordering::Release);
        entries.insert(name.to_owned(), entry.clone());
        drop(entries);
        if let Some(evicted) = evicted {
            tokio::spawn(async move {
                close_entry(evicted).await;
            });
        }
        Ok(DatabaseLease(entry))
    }

    async fn open(&self, name: &str) -> Result<DatabaseEntry, sqlx::Error> {
        let options = SqliteConnectOptions::new()
            .filename(self.database_path(name))
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_millis(self.settings.busy_timeout_ms));
        let pool = SqlitePoolOptions::new()
            .max_connections(self.settings.connections_per_database)
            .acquire_timeout(Duration::from_secs(self.settings.acquire_timeout_seconds))
            .connect_with(options)
            .await?;
        Ok(DatabaseEntry {
            name: name.to_owned(),
            pool,
            last_used_ms: AtomicU64::new(now_ms()),
            active_leases: AtomicUsize::new(0),
        })
    }

    pub async fn cleanup_idle(&self) -> anyhow::Result<usize> {
        let cutoff = now_ms().saturating_sub(self.settings.idle_timeout().as_millis() as u64);
        let mut entries = self.entries.lock().await;
        let names: Vec<_> = entries
            .iter()
            .filter(|(_, entry)| entry.active_leases.load(Ordering::Acquire) == 0)
            .filter(|(_, entry)| entry.last_used_ms.load(Ordering::Acquire) <= cutoff)
            .map(|(name, _)| name.clone())
            .collect();
        let removed: Vec<_> = names
            .iter()
            .filter_map(|name| entries.remove(name))
            .collect();
        drop(entries);
        let count = removed.len();
        for entry in removed {
            close_entry(entry).await;
        }
        Ok(count)
    }

    pub async fn stats(&self) -> ManagerStats {
        let entries = self.entries.lock().await;
        ManagerStats {
            open_databases: entries.len(),
            active_leases: entries
                .values()
                .map(|e| e.active_leases.load(Ordering::Acquire))
                .sum(),
            max_open_databases: self.settings.max_open_databases,
        }
    }
    pub async fn close_all(&self) {
        let mut entries = self.entries.lock().await;
        let removed: Vec<_> = entries.drain().map(|(_, entry)| entry).collect();
        drop(entries);
        for entry in removed {
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
    fn touch(&self) {
        self.last_used_ms.store(now_ms(), Ordering::Release);
    }
}
impl Deref for DatabaseLease {
    type Target = SqlitePool;
    fn deref(&self) -> &Self::Target {
        &self.0.pool
    }
}
impl Drop for DatabaseLease {
    fn drop(&mut self) {
        self.0.touch();
        self.0.active_leases.fetch_sub(1, Ordering::AcqRel);
    }
}
async fn close_entry(entry: Arc<DatabaseEntry>) {
    let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&entry.pool)
        .await;
    entry.pool.close().await;
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
}
