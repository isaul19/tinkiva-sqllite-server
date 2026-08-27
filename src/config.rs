use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(name = "tinkiva-database", version, about)]
pub struct Cli {
    /// Optional TOML configuration file.
    #[arg(short, long, env = "TINKIVA_CONFIG")]
    pub config: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Settings {
    pub server: ServerSettings,
    pub database: DatabaseSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerSettings {
    pub bind: String,
    /// If set, all /v1 endpoints require `Authorization: Bearer <token>`.
    pub auth_token: Option<String>,
    pub body_limit_bytes: usize,
    pub request_timeout_seconds: u64,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:7000".into(),
            auth_token: None,
            body_limit_bytes: 1_048_576,
            request_timeout_seconds: 30,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct DatabaseSettings {
    pub directory: PathBuf,
    pub max_open_databases: usize,
    pub idle_timeout_seconds: u64,
    pub cleanup_interval_seconds: u64,
    /// How often WAL checkpoints run in the background. Requests never
    /// checkpoint, so this is the only thing that bounds WAL growth.
    pub checkpoint_interval_seconds: u64,
    /// Read connections per database. One writer connection is always
    /// implied and is never shared with readers.
    pub reader_connections: u32,
    pub busy_timeout_ms: u64,
    pub acquire_timeout_seconds: u64,
    pub max_result_rows: usize,
    /// In-flight requests allowed across every database.
    pub max_concurrent_requests: usize,
    /// In-flight requests allowed for a single database, so one noisy
    /// tenant cannot occupy the whole server.
    pub max_concurrent_requests_per_database: usize,
    /// How long a request waits for an admission slot before it is shed.
    pub admission_timeout_ms: u64,
    /// Legacy shared cache override. When present it takes precedence over
    /// both role-specific budgets so existing configuration keeps its meaning.
    pub cache_size_kb: Option<u32>,
    /// SQLite page cache for the single writer connection.
    pub writer_cache_size_kb: u32,
    /// SQLite page cache for each reader connection.
    pub reader_cache_size_kb: u32,
    /// Bytes mapped per connection. Mapped pages are file-backed and
    /// evictable, so they cost far less than private page cache.
    pub mmap_size_mb: u64,
    /// Size the WAL is trimmed back to after a checkpoint.
    pub wal_size_limit_mb: u64,
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./data/databases"),
            max_open_databases: 50,
            idle_timeout_seconds: 300,
            cleanup_interval_seconds: 30,
            checkpoint_interval_seconds: 10,
            reader_connections: 1,
            busy_timeout_ms: 5_000,
            acquire_timeout_seconds: 10,
            max_result_rows: 10_000,
            max_concurrent_requests: 512,
            max_concurrent_requests_per_database: 8,
            admission_timeout_ms: 250,
            cache_size_kb: None,
            writer_cache_size_kb: 1_024,
            reader_cache_size_kb: 512,
            mmap_size_mb: 64,
            wal_size_limit_mb: 16,
        }
    }
}

impl DatabaseSettings {
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout_seconds)
    }
    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup_interval_seconds)
    }
    pub fn checkpoint_interval(&self) -> Duration {
        Duration::from_secs(self.checkpoint_interval_seconds)
    }
    pub fn admission_timeout(&self) -> Duration {
        Duration::from_millis(self.admission_timeout_ms)
    }
    pub fn writer_cache_size_kb(&self) -> u32 {
        self.cache_size_kb.unwrap_or(self.writer_cache_size_kb)
    }
    pub fn reader_cache_size_kb(&self) -> u32 {
        self.cache_size_kb.unwrap_or(self.reader_cache_size_kb)
    }
}

impl Settings {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let mut settings = if let Some(path) = path {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read config file {}", path.display()))?;
            toml::from_str(&contents)
                .with_context(|| format!("invalid TOML in {}", path.display()))?
        } else {
            Self::default()
        };
        settings.apply_environment()?;
        settings.validate()?;
        Ok(settings)
    }

    fn apply_environment(&mut self) -> anyhow::Result<()> {
        set_string("TINKIVA_BIND", &mut self.server.bind);
        set_optional_string("TINKIVA_AUTH_TOKEN", &mut self.server.auth_token);
        set_path("TINKIVA_DATABASE_DIR", &mut self.database.directory);
        set_number(
            "TINKIVA_MAX_OPEN_DATABASES",
            &mut self.database.max_open_databases,
        )?;
        set_number(
            "TINKIVA_IDLE_TIMEOUT_SECONDS",
            &mut self.database.idle_timeout_seconds,
        )?;
        set_number(
            "TINKIVA_CLEANUP_INTERVAL_SECONDS",
            &mut self.database.cleanup_interval_seconds,
        )?;
        set_number(
            "TINKIVA_CHECKPOINT_INTERVAL_SECONDS",
            &mut self.database.checkpoint_interval_seconds,
        )?;
        set_number(
            "TINKIVA_READER_CONNECTIONS",
            &mut self.database.reader_connections,
        )?;
        set_number(
            "TINKIVA_BUSY_TIMEOUT_MS",
            &mut self.database.busy_timeout_ms,
        )?;
        set_number(
            "TINKIVA_ACQUIRE_TIMEOUT_SECONDS",
            &mut self.database.acquire_timeout_seconds,
        )?;
        set_number(
            "TINKIVA_MAX_RESULT_ROWS",
            &mut self.database.max_result_rows,
        )?;
        if let Ok(value) = std::env::var("TINKIVA_CACHE_SIZE_KB") {
            self.database.cache_size_kb = Some(
                value
                    .parse()
                    .map_err(|error| anyhow::anyhow!("invalid TINKIVA_CACHE_SIZE_KB: {error}"))?,
            );
        }
        set_number(
            "TINKIVA_WRITER_CACHE_SIZE_KB",
            &mut self.database.writer_cache_size_kb,
        )?;
        set_number(
            "TINKIVA_READER_CACHE_SIZE_KB",
            &mut self.database.reader_cache_size_kb,
        )?;
        set_number(
            "TINKIVA_MAX_CONCURRENT_REQUESTS",
            &mut self.database.max_concurrent_requests,
        )?;
        set_number(
            "TINKIVA_MAX_CONCURRENT_REQUESTS_PER_DATABASE",
            &mut self.database.max_concurrent_requests_per_database,
        )?;
        set_number(
            "TINKIVA_ADMISSION_TIMEOUT_MS",
            &mut self.database.admission_timeout_ms,
        )?;
        set_number("TINKIVA_MMAP_SIZE_MB", &mut self.database.mmap_size_mb)?;
        set_number(
            "TINKIVA_WAL_SIZE_LIMIT_MB",
            &mut self.database.wal_size_limit_mb,
        )?;
        Ok(())
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.database.max_open_databases == 0 {
            bail!("max_open_databases must be greater than zero");
        }
        if self.database.reader_connections == 0 {
            bail!("reader_connections must be greater than zero");
        }
        if self.database.cleanup_interval_seconds == 0 {
            bail!("cleanup_interval_seconds must be greater than zero");
        }
        if self.database.checkpoint_interval_seconds == 0 {
            bail!("checkpoint_interval_seconds must be greater than zero");
        }
        if self.database.cache_size_kb == Some(0) {
            bail!("cache_size_kb must be greater than zero when set");
        }
        if self.database.writer_cache_size_kb == 0 {
            bail!("writer_cache_size_kb must be greater than zero");
        }
        if self.database.reader_cache_size_kb == 0 {
            bail!("reader_cache_size_kb must be greater than zero");
        }
        if self.database.max_concurrent_requests == 0 {
            bail!("max_concurrent_requests must be greater than zero");
        }
        if self.database.max_concurrent_requests_per_database == 0 {
            bail!("max_concurrent_requests_per_database must be greater than zero");
        }
        if self.database.max_result_rows == 0 {
            bail!("max_result_rows must be greater than zero");
        }
        if self.server.body_limit_bytes == 0 {
            bail!("body_limit_bytes must be greater than zero");
        }
        Ok(())
    }
}

fn set_string(key: &str, target: &mut String) {
    if let Ok(value) = std::env::var(key) {
        *target = value;
    }
}
fn set_optional_string(key: &str, target: &mut Option<String>) {
    if let Ok(value) = std::env::var(key) {
        *target = if value.is_empty() { None } else { Some(value) };
    }
}
fn set_path(key: &str, target: &mut PathBuf) {
    if let Ok(value) = std::env::var(key) {
        *target = PathBuf::from(value);
    }
}
fn set_number<T>(key: &str, target: &mut T) -> anyhow::Result<()>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    if let Ok(value) = std::env::var(key) {
        *target = value
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid {key}: {error}"))?;
    }
    Ok(())
}
