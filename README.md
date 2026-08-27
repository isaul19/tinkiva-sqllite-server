# TinkivaDatabase

TinkivaDatabase is a small Rust service that exposes many isolated SQLite files through one HTTP
process. Databases are opened when requested, kept warm for a configurable period, and closed with a
WAL checkpoint when they become idle or the LRU capacity is needed.

It is intended for workloads with many small tenants where only a fraction are active at the same
time. It is not a distributed SQL database: run a single writer service over local persistent storage
and scale by sharding tenants between instances.

## Quick start

```bash
cargo run --release -- --config config.example.toml
```

The default listener is `127.0.0.1:7000`. A database named `acme` is created lazily on its first
request as `data/databases/acme.db`.

```bash
# Create a table
curl -X POST http://127.0.0.1:7000/v1/db/acme/execute \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer change-me" \
  -d '{"sql":"CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)"}'

# Insert safely with positional parameters
curl -X POST http://127.0.0.1:7000/v1/db/acme/execute \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer change-me" \
  -d '{"sql":"INSERT INTO products(name, price) VALUES (?, ?)","params":["Keyboard",49.9]}'

# Query
curl -X POST http://127.0.0.1:7000/v1/db/acme/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer change-me" \
  -d '{"sql":"SELECT * FROM products WHERE price < ?","params":[100]}'
```

## HTTP API

All request bodies are JSON. Parameters are positional and correspond to SQLite `?` placeholders.
Arrays and objects are stored as JSON text. BLOB results are represented as
`{"$blob":"<base64>"}`.

`/query` runs on read-only connections, so it rejects statements that write; those belong on
`/execute` or `/batch`. Any request may add `"format": "arrays"` to receive rows as positional
arrays instead of repeating the column names on every row, which on a wide result set is most of
the payload.

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Unauthenticated liveness check |
| `POST` | `/v1/db/{database}/query` | Run a query and return rows |
| `POST` | `/v1/db/{database}/execute` | Run one DDL/DML statement |
| `POST` | `/v1/db/{database}/batch` | Run multiple DDL/DML statements atomically |
| `GET` | `/v1/admin/stats` | Report open databases, active leases, and capacity |
| `GET` | `/v1/admin/metrics` | Prometheus metrics: latency per route, admission wait, shed requests |

Batch body:

```json
{
  "statements": [
    { "sql": "INSERT INTO products(name, price) VALUES (?, ?)", "params": ["Mouse", 20] },
    { "sql": "UPDATE products SET price = ? WHERE name = ?", "params": [18, "Mouse"] }
  ]
}
```

The batch is rolled back automatically if any statement fails. Every statement reports the rows it
returned alongside `rows_affected` and `last_insert_rowid`, so a read-modify-read sequence fits in
one request and one transaction. Results stop at `max_result_rows` and return `"truncated": true`
when more rows exist.

Database names accept 1–64 ASCII letters, digits, `-`, and `_`, and must start with a letter or
digit. This prevents path traversal and makes each tenant map to exactly one file.

## Configuration

Copy `config.example.toml` and pass it with `--config`. These environment variables override the
most frequently changed values:

- `TINKIVA_CONFIG`
- `TINKIVA_BIND`
- `TINKIVA_AUTH_TOKEN`
- `TINKIVA_DATABASE_DIR`
- `TINKIVA_MAX_OPEN_DATABASES`
- `TINKIVA_IDLE_TIMEOUT_SECONDS`
- `TINKIVA_READER_CONNECTIONS`
- `TINKIVA_MAX_RESULT_ROWS`
- `TINKIVA_WRITER_CACHE_SIZE_KB`
- `TINKIVA_READER_CACHE_SIZE_KB`
- `TINKIVA_CACHE_SIZE_KB` (legacy override for both roles)
- `TINKIVA_MMAP_SIZE_MB`
- `TINKIVA_MAX_CONCURRENT_REQUESTS`
- `RUST_LOG` (for example, `tinkiva_database=debug,tower_http=info`)

Use TLS at a reverse proxy and set a strong bearer token before exposing the service to a network.
The single configured token is suitable for a private service-to-service deployment; per-tenant
credentials and policy enforcement are not part of this MVP.

## Operational model

- SQLite runs in WAL mode with `synchronous=NORMAL`, foreign keys enabled, and a busy timeout.
- Each database gets one writer connection and a separate reader pool, so a write never blocks a
  read. The reader pool is lazy: a database that is only written never opens reader connections.
- Writer and reader page caches have separate budgets because their working sets differ. Resident
  memory is roughly `writer_cache_size_kb + readers × reader_cache_size_kb` per hot database. The
  `mmap_size_mb` window is file-backed and evictable, so it does not add private memory.
- A lease counter protects in-flight requests from idle cleanup or LRU eviction.
- When capacity is full, the least-recently-used inactive database is checkpointed and closed.
- If every open database is active, a new tenant receives HTTP `503 capacity_busy`.
- Requests take a per-database slot and a process-wide slot. Past `admission_timeout_ms` the request
  is shed with HTTP `429 overloaded` and a `Retry-After`, rather than queued into latency.
- WAL checkpoints run on a background timer, so no request pays for one under normal load;
  `wal_size_limit_mb` is the ceiling at which a request will checkpoint as a last resort.
- Graceful shutdown checkpoints and closes all managed pools.
- Database files remain on local persistent storage while sleeping; no remote cold tier exists.

Back up SQLite safely using a SQLite-aware snapshot/backup method. Copying a live `.db` file without
its WAL or without coordinating a checkpoint can produce an inconsistent backup.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

See [guide.md](guide.md) for the architecture and scaling boundaries.
Measured multi-tenant throughput, memory and latency are recorded in [BENCHMARKS.md](BENCHMARKS.md),
including an A/B against the previous build under one client.

## Deployment

For a container deployment, set a token and start the included Compose stack:

```bash
export TINKIVA_AUTH_TOKEN="replace-with-a-long-random-value"
docker compose up -d --build
```

The named volume contains the durable database files, while the container runs as an unprivileged
user and publishes the service on host loopback. Put a TLS reverse proxy in front if remote clients
need access.

For a small Linux VM without containers, build the release binary, create a `tinkiva` system user,
place configuration under `/etc/tinkivadb`, data under `/var/lib/tinkivadb/databases`, and install the
unit from `deploy/tinkiva-database.service`. The unit restricts filesystem access to the data directory
and gives graceful shutdown 30 seconds to checkpoint pools.
