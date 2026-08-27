# Benchmarks

These measurements characterize resource usage; they are not a universal capacity guarantee. They
were collected on Windows from the release build on 2026-08-26. Run `python tools/benchmark.py` on the
target machine before choosing production limits.

## Mixed workload

Each database contained 10,000 rows with a 256-byte text payload (about 2.72 MiB of logical test data).
Five workers targeted every database for eight seconds:

- one writer repeatedly executed a parameterized single-row `UPDATE`;
- four readers selected 100 indexed rows, including their payload;
- setup used `CREATE TABLE`, `CREATE INDEX`, and a transactional bulk `INSERT`;
- WAL mode and the normal server configuration remained enabled.

Except for the explicit five-connection comparison, pools were capped at two connections per database.
`Peak RAM` is the process peak working set. `Retained RAM` was sampled after the workload completed.

| Databases | Users | Pool/DB | Peak RAM | Retained RAM | Throughput | p50 | p95 | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 5 | 2 | 19.94 MiB | 18.97 MiB | 1,001.6 ops/s | 2.58 ms | 20.68 ms | 0 |
| 1 | 5 | 5 | 23.65 MiB | 22.17 MiB | 749.1 ops/s | 3.63 ms | 24.87 ms | 0 |
| 5 | 25 | 2 | 29.84 MiB | 29.75 MiB | 2,658.6 ops/s | 9.19 ms | 14.18 ms | 0 |
| 20 | 100 | 2 | 72.54 MiB | 51.45 MiB | 2,511.0 ops/s | 38.86 ms | 61.03 ms | 0 |
| 50 | 250 | 2 | 150.57 MiB | 103.05 MiB | 2,498.0 ops/s | 97.29 ms | 156.28 ms | 0 |

An empty process in this headless benchmark used about 10.4 MiB. At 20–50 active databases, retained
incremental memory converged near 1.9–2.1 MiB per database and peak incremental memory near 2.8–3.1
MiB per database. The one-database result includes allocator/runtime costs that become amortized at
larger tenant counts.

Opening five connections for five users did not improve this mixed workload. It added roughly 3.2 MiB
and reduced throughput because SQLite still serializes writes. The default pool of two is a better
starting point for one-writer/many-reader workloads; benchmark the actual SQL before increasing it.

## Larger database check

The one-database, two-connection scenario was repeated for 15 seconds with 100,000 rows (about 27.1
MiB on disk). Peak RAM remained 19.99 MiB, retained RAM 18.82 MiB, throughput 923.9 ops/s, p50 2.83 ms,
p95 22.39 ms, and errors zero. Database file size therefore does not translate directly into RAM:
SQLite loads pages on demand and retains a bounded working set. Queries that scan more pages, large
result sets, or additional concurrent requests can produce different peaks.

## Interpretation

For the tested pattern—five users per active database, one writing and four reading—a practical
planning estimate is:

```text
process base:                 10–20 MiB
retained per active DB:        2–3 MiB
short peak per active DB:      3–5 MiB
```

Use at least a 2× safety margin for production because payload size, query plans, result limits,
platform allocator behavior, and request bursts matter. Sleeping database files have no pool and do
not contribute this per-active-database cost.
