"""Reproducible load generator for TinkivaDatabase.

The client keeps one persistent HTTP connection per worker so the measurement
reflects server cost instead of TCP handshakes, and it reports the CPU time of
both processes: when the client is the saturated one, the throughput number
describes the generator, not the service.
"""

import argparse
import concurrent.futures
import ctypes
import http.client
import json
import os
import pathlib
import statistics
import subprocess
import threading
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
# Overridable so the same harness can drive an older build: comparing two
# binaries under one client is the only way to attribute a change.
BINARY = pathlib.Path(
    os.environ.get(
        "TINKIVA_BENCH_BINARY",
        ROOT / "target" / "release" / (
            "tinkiva-database.exe" if os.name == "nt" else "tinkiva-database"
        ),
    )
)
TOKEN = "benchmark-only"
ROWS_PER_DATABASE = 10_000
MB = 1024 * 1024


class ProcessMemoryCounters(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


class FileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_ulong), ("high", ctypes.c_ulong)]

    def seconds(self):
        return ((self.high << 32) | self.low) / 1e7


def memory(proc):
    if os.name != "nt":
        values = {}
        with open(f"/proc/{proc.pid}/status", encoding="utf-8") as status:
            for line in status:
                key, _, value = line.partition(":")
                if key in {"VmRSS", "VmHWM", "RssAnon"}:
                    values[key] = int(value.strip().split()[0]) / 1024
        return {
            "working_mb": values.get("VmRSS", 0),
            "peak_working_mb": values.get("VmHWM", 0),
            "private_mb": values.get("RssAnon", 0),
            "peak_private_mb": 0,
        }

    counters = ProcessMemoryCounters()
    counters.cb = ctypes.sizeof(counters)
    ok = ctypes.windll.psapi.GetProcessMemoryInfo(
        int(proc._handle), ctypes.byref(counters), counters.cb
    )
    if not ok:
        raise ctypes.WinError()
    return {
        "working_mb": counters.WorkingSetSize / MB,
        "peak_working_mb": counters.PeakWorkingSetSize / MB,
        "private_mb": counters.PagefileUsage / MB,
        "peak_private_mb": counters.PeakPagefileUsage / MB,
    }


def cpu_seconds(proc):
    """Total user+system CPU seconds consumed by the server process so far."""
    if os.name != "nt":
        with open(f"/proc/{proc.pid}/stat", encoding="utf-8") as stat:
            fields = stat.read().rsplit(") ", 1)[1].split()
        ticks = os.sysconf("SC_CLK_TCK")
        return (int(fields[11]) + int(fields[12])) / ticks

    creation, exit_time, kernel, user = FileTime(), FileTime(), FileTime(), FileTime()
    ok = ctypes.windll.kernel32.GetProcessTimes(
        int(proc._handle),
        ctypes.byref(creation),
        ctypes.byref(exit_time),
        ctypes.byref(kernel),
        ctypes.byref(user),
    )
    if not ok:
        raise ctypes.WinError()
    return kernel.seconds() + user.seconds()


def wal_bytes(data_dir):
    return sum(path.stat().st_size for path in pathlib.Path(data_dir).glob("*.db-wal"))


class Client:
    """One keep-alive connection, reused for every request of a worker."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connection = None

    def connect(self):
        self.connection = http.client.HTTPConnection(self.host, self.port, timeout=30)

    def post(self, path, body):
        payload = json.dumps(body)
        for attempt in range(2):
            if self.connection is None:
                self.connect()
            try:
                self.connection.request(
                    "POST",
                    path,
                    payload,
                    {
                        "Authorization": f"Bearer {TOKEN}",
                        "Content-Type": "application/json",
                        "Connection": "keep-alive",
                    },
                )
                response = self.connection.getresponse()
                data = response.read()
                if response.status >= 400:
                    raise HttpStatusError(response.status)
                return data
            except (http.client.HTTPException, OSError):
                self.close()
                if attempt == 1:
                    raise

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None


class HttpStatusError(RuntimeError):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status = status


def request(base_url, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        base_url + path,
        data=data,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        method="GET" if body is None else "POST",
    )
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.loads(response.read())


def wait_until_ready(base_url):
    for _ in range(200):
        try:
            with urllib.request.urlopen(base_url + "/health", timeout=1) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("server did not become ready")


def initialize_database(base_url, tenant):
    statements = [
        {
            "sql": "CREATE TABLE items (id INTEGER PRIMARY KEY, category INTEGER NOT NULL, payload TEXT NOT NULL, counter INTEGER NOT NULL DEFAULT 0)"
        },
        {"sql": "CREATE INDEX items_category ON items(category)"},
        {
            "sql": f"WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < {ROWS_PER_DATABASE}) "
            "INSERT INTO items(id, category, payload) SELECT x, x % 100, printf('%0256d', x) FROM seq"
        },
    ]
    request(base_url, f"/v1/db/{tenant}/batch", {"statements": statements})


READ_INDEXED = {
    "sql": "SELECT id, payload, counter FROM items WHERE category = ? ORDER BY id LIMIT 100",
    "rotating_param": True,
}
READ_SCAN = {
    "sql": "SELECT count(*) AS total, sum(counter) AS clicks FROM items WHERE payload LIKE ?",
    "rotating_param": False,
}
READ_CHURN = {
    "sql": "SELECT id, payload, counter FROM items WHERE category = ? ORDER BY id LIMIT 100",
    "rotating_param": True,
    # SQL comments produce distinct prepared-statement cache keys without
    # changing the query plan or result set.
    "variants": 100,
}
WRITE_POINT = {
    "sql": "UPDATE items SET counter = counter + 1 WHERE id = ?",
    "rotating_param": True,
}


def worker(host, port, tenant, operation, start, stop_at):
    latencies = []
    operations = 0
    errors = 0
    cursor = 1
    client = Client(host, port)
    client.connect()
    start.wait()
    try:
        while time.monotonic() < stop_at[0]:
            if operation is WRITE_POINT:
                path = f"/v1/db/{tenant}/execute"
                param = cursor
                cursor = cursor % ROWS_PER_DATABASE + 1
            else:
                path = f"/v1/db/{tenant}/query"
                if operation["rotating_param"]:
                    param = cursor
                    cursor = cursor % 100 + 1
                else:
                    param = "%0000%"
            before = time.perf_counter()
            try:
                sql = operation["sql"]
                if operation.get("variants"):
                    sql += f" -- variant {cursor % operation['variants']}"
                client.post(path, {"sql": sql, "params": [param]})
                operations += 1
                latencies.append((time.perf_counter() - before) * 1000)
            except Exception:
                errors += 1
    finally:
        client.close()
    return operation is WRITE_POINT, operations, errors, latencies


def fixed_rate_request(local, host, port, tenant, operation, parameter):
    client = getattr(local, "client", None)
    if client is None:
        client = Client(host, port)
        client.connect()
        local.client = client
    path = f"/v1/db/{tenant}/{'execute' if operation is WRITE_POINT else 'query'}"
    before = time.perf_counter()
    try:
        client.post(path, {"sql": operation["sql"], "params": [parameter]})
        return operation is WRITE_POINT, 200, (time.perf_counter() - before) * 1000
    except HttpStatusError as error:
        return operation is WRITE_POINT, error.status, (time.perf_counter() - before) * 1000
    except Exception:
        return operation is WRITE_POINT, 0, (time.perf_counter() - before) * 1000


def fixed_rate_load(host, port, databases, readers, writers, read_operation, rate, duration, max_outstanding):
    """Issue work by the clock, independently of response latency.

    A closed loop lowers its own offered rate as latency grows. This scheduler
    keeps offering the configured rate and caps only the client-side backlog,
    which lets the server's admission control produce observable 429s.
    """
    local = threading.local()
    pending = set()
    latencies = []
    reads = writes = shed = http_errors = transport_errors = client_dropped = 0
    offered = 0
    mix_size = readers + writers
    started = time.monotonic()
    deadline = started + duration

    def collect(done):
        nonlocal reads, writes, shed, http_errors, transport_errors
        for future in done:
            is_writer, status, latency = future.result()
            latencies.append(latency)
            if status == 200:
                if is_writer:
                    writes += 1
                else:
                    reads += 1
            elif status == 429:
                shed += 1
            elif status == 0:
                transport_errors += 1
            else:
                http_errors += 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_outstanding) as executor:
        while True:
            now = time.monotonic()
            done = {future for future in pending if future.done()}
            pending.difference_update(done)
            collect(done)
            expected = min(int((now - started) * rate), int(duration * rate))
            due = expected - offered
            for _ in range(max(due, 0)):
                ordinal = offered
                offered += 1
                if len(pending) >= max_outstanding:
                    client_dropped += 1
                    continue
                tenant_index = ordinal % databases
                role = (ordinal // databases) % mix_size
                operation = WRITE_POINT if role < writers else read_operation
                parameter = (
                    ordinal % ROWS_PER_DATABASE + 1
                    if operation is WRITE_POINT
                    else (ordinal % 100 + 1 if operation["rotating_param"] else "%0000%")
                )
                pending.add(
                    executor.submit(
                        fixed_rate_request,
                        local,
                        host,
                        port,
                        f"tenant{tenant_index:03d}",
                        operation,
                        parameter,
                    )
                )
            if now >= deadline:
                break
            time.sleep(0.001)

        for future in concurrent.futures.as_completed(pending):
            collect([future])

    return {
        "reads": reads,
        "writes": writes,
        "errors": http_errors + transport_errors,
        "shed_429": shed,
        "client_dropped": client_dropped,
        "offered": offered,
        "latencies": latencies,
    }


def percentile(values, fraction):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * fraction))]


def run_scenario(index, scenario, duration):
    databases = scenario["databases"]
    reader_pool = scenario["reader_pool"]
    readers = scenario["readers"]
    writers = scenario["writers"]
    read_operation = scenario.get("read", READ_INDEXED)
    host = "127.0.0.1"
    port = 17100 + index
    base_url = f"http://{host}:{port}"
    data_dir = ROOT / "data" / ("run-" + scenario["name"])
    env = os.environ.copy()
    env.update(
        {
            "TINKIVA_BIND": f"{host}:{port}",
            "TINKIVA_AUTH_TOKEN": TOKEN,
            "TINKIVA_DATABASE_DIR": str(data_dir),
            "TINKIVA_MAX_OPEN_DATABASES": str(databases),
            "TINKIVA_READER_CONNECTIONS": str(reader_pool),
            "TINKIVA_MAX_RESULT_ROWS": "1000",
            "TINKIVA_MAX_CONCURRENT_REQUESTS_PER_DATABASE": str(
                scenario.get("per_database_limit", 8)
            ),
            "TINKIVA_MAX_CONCURRENT_REQUESTS": str(scenario.get("process_limit", 512)),
            "TINKIVA_ADMISSION_TIMEOUT_MS": str(scenario.get("admission_timeout_ms", 250)),
            "RUST_LOG": "error",
        }
    )
    proc = subprocess.Popen(
        [str(BINARY)],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    try:
        wait_until_ready(base_url)
        baseline = memory(proc)
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, databases)) as executor:
            list(executor.map(lambda i: initialize_database(base_url, f"tenant{i:03d}"), range(databases)))
        after_setup = memory(proc)

        users_per_db = readers + writers
        server_cpu_before = cpu_seconds(proc)
        client_cpu_before = time.process_time()
        wall_before = time.monotonic()
        if "rate" in scenario:
            load = fixed_rate_load(
                host,
                port,
                databases,
                readers,
                writers,
                read_operation,
                scenario["rate"],
                duration,
                scenario.get("max_outstanding", 512),
            )
            reads = load["reads"]
            writes = load["writes"]
            errors = load["errors"]
            latencies = load["latencies"]
            offered = load["offered"]
            shed = load["shed_429"]
            client_dropped = load["client_dropped"]
        else:
            start = threading.Event()
            stop_at = [0.0]
            futures = []
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=databases * users_per_db
            ) as executor:
                for i in range(databases):
                    tenant = f"tenant{i:03d}"
                    for _ in range(writers):
                        futures.append(
                            executor.submit(worker, host, port, tenant, WRITE_POINT, start, stop_at)
                        )
                    for _ in range(readers):
                        futures.append(
                            executor.submit(worker, host, port, tenant, read_operation, start, stop_at)
                        )
                stop_at[0] = wall_before + duration
                start.set()
                results = [future.result() for future in futures]
            reads = sum(ops for is_writer, ops, _, _ in results if not is_writer)
            writes = sum(ops for is_writer, ops, _, _ in results if is_writer)
            errors = sum(errors for _, _, errors, _ in results)
            latencies = [latency for _, _, _, values in results for latency in values]
            offered = reads + writes + errors
            shed = 0
            client_dropped = 0
        wall = time.monotonic() - wall_before
        server_cpu = cpu_seconds(proc) - server_cpu_before
        client_cpu = time.process_time() - client_cpu_before

        final_memory = memory(proc)
        stats = request(base_url, "/v1/admin/stats")
        return {
            "scenario": scenario["name"],
            "databases": databases,
            "reader_connections": reader_pool,
            "users": databases * users_per_db,
            "readers_per_db": readers,
            "writers_per_db": writers,
            "rows_per_db": ROWS_PER_DATABASE,
            "duration_s": round(wall, 2),
            "baseline_mb": round(baseline["working_mb"], 2),
            "after_setup_mb": round(after_setup["working_mb"], 2),
            "final_mb": round(final_memory["working_mb"], 2),
            "peak_mb": round(final_memory["peak_working_mb"], 2),
            "private_mb": round(final_memory["private_mb"], 2),
            "wal_mb": round(wal_bytes(data_dir) / MB, 2),
            "reads": reads,
            "writes": writes,
            "rps": round((reads + writes) / duration, 1),
            "offered": offered,
            "sent": offered - client_dropped,
            "shed_429": shed,
            "client_dropped": client_dropped,
            "latency_p50_ms": round(statistics.median(latencies), 2) if latencies else 0.0,
            "latency_p95_ms": round(percentile(latencies, 0.95), 2),
            "latency_p99_ms": round(percentile(latencies, 0.99), 2),
            "server_cpu_cores": round(server_cpu / wall, 2),
            "client_cpu_cores": round(client_cpu / wall, 2),
            "cpu_ms_per_op": round(server_cpu * 1000 / max(reads + writes, 1), 3),
            "errors": errors,
            "open_databases": stats["open_databases"],
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


SCENARIOS = [
    {"name": "1db-read1", "databases": 1, "reader_pool": 1, "readers": 4, "writers": 1},
    {"name": "1db-read2", "databases": 1, "reader_pool": 2, "readers": 4, "writers": 1},
    {"name": "1db-read4", "databases": 1, "reader_pool": 4, "readers": 4, "writers": 1},
    {"name": "5db-read2", "databases": 5, "reader_pool": 2, "readers": 4, "writers": 1},
    {"name": "20db-read2", "databases": 20, "reader_pool": 2, "readers": 4, "writers": 1},
    {"name": "50db-read1", "databases": 50, "reader_pool": 1, "readers": 4, "writers": 1},
    {"name": "50db-read2", "databases": 50, "reader_pool": 2, "readers": 4, "writers": 1},
    {"name": "20db-writeheavy", "databases": 20, "reader_pool": 2, "readers": 1, "writers": 4},
    {
        "name": "20db-scan",
        "databases": 20,
        "reader_pool": 2,
        "readers": 4,
        "writers": 1,
        "read": READ_SCAN,
    },
    {
        "name": "50db-statement-churn",
        "databases": 50,
        "reader_pool": 1,
        "readers": 4,
        "writers": 1,
        "read": READ_CHURN,
    },
    {
        "name": "50db-statement-churn-readonly",
        "databases": 50,
        "reader_pool": 1,
        "readers": 4,
        "writers": 0,
        "read": READ_CHURN,
    },
    {
        "name": "20db-openloop-2000",
        "databases": 20,
        "reader_pool": 1,
        "readers": 4,
        "writers": 1,
        "rate": 2_000,
        "max_outstanding": 512,
        "per_database_limit": 8,
        "process_limit": 256,
        "admission_timeout_ms": 20,
    },
    {
        "name": "20db-openloop-4000",
        "databases": 20,
        "reader_pool": 1,
        "readers": 4,
        "writers": 1,
        "rate": 4_000,
        "max_outstanding": 512,
        "per_database_limit": 8,
        "process_limit": 256,
        "admission_timeout_ms": 20,
    },
    {
        "name": "20db-openloop-4000-wait250",
        "databases": 20,
        "reader_pool": 1,
        "readers": 4,
        "writers": 1,
        "rate": 4_000,
        "max_outstanding": 512,
        "per_database_limit": 8,
        "process_limit": 256,
        "admission_timeout_ms": 250,
    },
    {
        "name": "20db-openloop-4000-wait50",
        "databases": 20,
        "reader_pool": 1,
        "readers": 4,
        "writers": 1,
        "rate": 4_000,
        "max_outstanding": 512,
        "per_database_limit": 8,
        "process_limit": 256,
        "admission_timeout_ms": 50,
    },
    {
        "name": "20db-openloop-8000",
        "databases": 20,
        "reader_pool": 1,
        "readers": 4,
        "writers": 1,
        "rate": 8_000,
        "max_outstanding": 512,
        "per_database_limit": 8,
        "process_limit": 256,
        "admission_timeout_ms": 20,
    },
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration",
        type=int,
        default=int(os.environ.get("BENCH_DURATION", "60")),
        help="seconds of load per scenario; short runs never reach a WAL checkpoint",
    )
    parser.add_argument(
        "--only",
        action="append",
        help="run only the named scenario; repeatable",
    )
    arguments = parser.parse_args()
    if not BINARY.exists():
        raise SystemExit(f"missing {BINARY}: run `cargo build --release` first")
    for index, scenario in enumerate(SCENARIOS):
        if arguments.only and scenario["name"] not in arguments.only:
            continue
        print(json.dumps(run_scenario(index, scenario, arguments.duration)), flush=True)


if __name__ == "__main__":
    main()
