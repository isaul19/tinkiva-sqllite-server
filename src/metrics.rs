//! Prometheus counters for the things the benchmarks could not distinguish:
//! whether a queue is in the client, in admission, or in SQLite.

use std::{
    fmt::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::db::ManagerStats;

/// Upper bounds in seconds. Cumulative, as Prometheus `le` buckets require.
const BUCKETS: [f64; 12] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Routes are labelled by their matched path, never by database name: a label
/// per tenant would grow the metric set with the customer list.
const ROUTES: [&str; 3] = [
    "/v1/db/{database}/query",
    "/v1/db/{database}/execute",
    "/v1/db/{database}/batch",
];

#[derive(Default)]
pub struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    count: AtomicU64,
    total_micros: AtomicU64,
}

impl Histogram {
    fn record(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        for (bucket, bound) in self.buckets.iter().zip(BUCKETS) {
            if seconds <= bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, labels: &str) {
        let separator = if labels.is_empty() { "" } else { "," };
        for (bucket, bound) in self.buckets.iter().zip(BUCKETS) {
            let _ = writeln!(
                out,
                "{name}_bucket{{{labels}{separator}le=\"{bound}\"}} {}",
                bucket.load(Ordering::Relaxed)
            );
        }
        let count = self.count.load(Ordering::Relaxed);
        let braces = if labels.is_empty() {
            String::new()
        } else {
            format!("{{{labels}}}")
        };
        let _ = writeln!(
            out,
            "{name}_bucket{{{labels}{separator}le=\"+Inf\"}} {count}"
        );
        let _ = writeln!(
            out,
            "{name}_sum{braces} {}",
            self.total_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
        );
        let _ = writeln!(out, "{name}_count{braces} {count}");
    }
}

#[derive(Default)]
struct RouteMetrics {
    duration: Histogram,
    succeeded: AtomicU64,
    failed: AtomicU64,
}

#[derive(Default)]
pub struct Metrics {
    routes: [RouteMetrics; ROUTES.len()],
    /// How long callers waited for an admission slot. A rising p99 here means
    /// the queue is in admission, not in SQLite.
    admission_wait: Histogram,
    requests_shed: AtomicU64,
    databases_opened: AtomicU64,
    databases_evicted: AtomicU64,
    databases_closed_idle: AtomicU64,
    wal_checkpoints: AtomicU64,
}

impl Metrics {
    pub fn record_request(&self, route: &str, elapsed: Duration, succeeded: bool) {
        let Some(index) = ROUTES.iter().position(|known| *known == route) else {
            return;
        };
        let route = &self.routes[index];
        route.duration.record(elapsed);
        let outcome = if succeeded {
            &route.succeeded
        } else {
            &route.failed
        };
        outcome.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_admission_wait(&self, elapsed: Duration) {
        self.admission_wait.record(elapsed);
    }
    pub fn record_shed(&self) {
        self.requests_shed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_open(&self) {
        self.databases_opened.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_eviction(&self) {
        self.databases_evicted.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_idle_close(&self, databases: usize) {
        self.databases_closed_idle
            .fetch_add(databases as u64, Ordering::Relaxed);
    }
    pub fn record_checkpoints(&self, databases: usize) {
        self.wal_checkpoints
            .fetch_add(databases as u64, Ordering::Relaxed);
    }

    pub fn render(&self, stats: &ManagerStats) -> String {
        let mut out = String::with_capacity(4096);

        out.push_str("# TYPE tinkiva_requests_total counter\n");
        for (route, name) in self.routes.iter().zip(ROUTES) {
            for (outcome, counter) in [("ok", &route.succeeded), ("error", &route.failed)] {
                let _ = writeln!(
                    out,
                    "tinkiva_requests_total{{route=\"{name}\",outcome=\"{outcome}\"}} {}",
                    counter.load(Ordering::Relaxed)
                );
            }
        }

        out.push_str("# TYPE tinkiva_request_duration_seconds histogram\n");
        for (route, name) in self.routes.iter().zip(ROUTES) {
            route.duration.render(
                &mut out,
                "tinkiva_request_duration_seconds",
                &format!("route=\"{name}\""),
            );
        }

        out.push_str("# TYPE tinkiva_admission_wait_seconds histogram\n");
        self.admission_wait
            .render(&mut out, "tinkiva_admission_wait_seconds", "");

        for (name, kind, value) in [
            (
                "tinkiva_requests_shed_total",
                "counter",
                self.requests_shed.load(Ordering::Relaxed),
            ),
            (
                "tinkiva_databases_opened_total",
                "counter",
                self.databases_opened.load(Ordering::Relaxed),
            ),
            (
                "tinkiva_databases_evicted_total",
                "counter",
                self.databases_evicted.load(Ordering::Relaxed),
            ),
            (
                "tinkiva_databases_closed_idle_total",
                "counter",
                self.databases_closed_idle.load(Ordering::Relaxed),
            ),
            (
                "tinkiva_wal_checkpoints_total",
                "counter",
                self.wal_checkpoints.load(Ordering::Relaxed),
            ),
            (
                "tinkiva_open_databases",
                "gauge",
                stats.open_databases as u64,
            ),
            ("tinkiva_active_leases", "gauge", stats.active_leases as u64),
            (
                "tinkiva_max_open_databases",
                "gauge",
                stats.max_open_databases as u64,
            ),
            (
                "tinkiva_available_request_slots",
                "gauge",
                stats.available_request_slots as u64,
            ),
            (
                "tinkiva_max_concurrent_requests",
                "gauge",
                stats.max_concurrent_requests as u64,
            ),
        ] {
            let _ = writeln!(out, "# TYPE {name} {kind}\n{name} {value}");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative() {
        let histogram = Histogram::default();
        histogram.record(Duration::from_millis(30));
        let mut out = String::new();
        histogram.render(&mut out, "latency", "");
        // 30ms falls above the 25ms bound and at or below every later one.
        assert!(out.contains("latency_bucket{le=\"0.025\"} 0"));
        assert!(out.contains("latency_bucket{le=\"0.05\"} 1"));
        assert!(out.contains("latency_bucket{le=\"+Inf\"} 1"));
        assert!(out.contains("latency_count 1"));
    }

    #[test]
    fn unknown_routes_are_not_recorded() {
        let metrics = Metrics::default();
        metrics.record_request("/v1/db/acme/query", Duration::from_millis(1), true);
        metrics.record_request(ROUTES[0], Duration::from_millis(1), true);
        let stats = ManagerStats {
            open_databases: 0,
            active_leases: 0,
            max_open_databases: 1,
            available_request_slots: 1,
            max_concurrent_requests: 1,
        };
        let rendered = metrics.render(&stats);
        assert!(rendered.contains(&format!(
            "tinkiva_requests_total{{route=\"{}\",outcome=\"ok\"}} 1",
            ROUTES[0]
        )));
    }
}
