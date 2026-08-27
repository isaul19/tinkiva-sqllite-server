import concurrent.futures
import ctypes
import json
import os
import pathlib
import statistics
import subprocess
import threading
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
BINARY = ROOT / "target" / "release" / (
    "tinkiva-database.exe" if os.name == "nt" else "tinkiva-database"
)
TOKEN = "benchmark-only"
ROWS_PER_DATABASE = 10_000


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
    mb = 1024 * 1024
    return {
        "working_mb": counters.WorkingSetSize / mb,
        "peak_working_mb": counters.PeakWorkingSetSize / mb,
        "private_mb": counters.PagefileUsage / mb,
        "peak_private_mb": counters.PeakPagefileUsage / mb,
    }


def request(base_url, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        base_url + path,
        data=data,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        method="GET" if body is None else "POST",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read())


def wait_until_ready(base_url):
    for _ in range(100):
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


def worker(base_url, tenant, writer, start, stop_at):
    local_latencies = []
    operations = 0
    errors = 0
    row_id = 1
    category = 0
    start.wait()
    while time.monotonic() < stop_at[0]:
        before = time.perf_counter()
        try:
            if writer:
                request(
                    base_url,
                    f"/v1/db/{tenant}/execute",
                    {"sql": "UPDATE items SET counter = counter + 1 WHERE id = ?", "params": [row_id]},
                )
                row_id = row_id % ROWS_PER_DATABASE + 1
            else:
                request(
                    base_url,
                    f"/v1/db/{tenant}/query",
                    {
                        "sql": "SELECT id, payload, counter FROM items WHERE category = ? ORDER BY id LIMIT 100",
                        "params": [category],
                    },
                )
                category = (category + 1) % 100
            operations += 1
            local_latencies.append((time.perf_counter() - before) * 1000)
        except Exception:
            errors += 1
    return writer, operations, errors, local_latencies


def percentile(values, fraction):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * fraction))]


def run_scenario(index, databases, connections, duration=8):
    port = 17100 + index
    base_url = f"http://127.0.0.1:{port}"
    data_dir = ROOT / "data" / f"run-{index}-{databases}db-{connections}conn"
    env = os.environ.copy()
    env.update(
        {
            "TINKIVA_BIND": f"127.0.0.1:{port}",
            "TINKIVA_AUTH_TOKEN": TOKEN,
            "TINKIVA_DATABASE_DIR": str(data_dir),
            "TINKIVA_MAX_OPEN_DATABASES": str(databases),
            "TINKIVA_CONNECTIONS_PER_DATABASE": str(connections),
            "TINKIVA_MAX_RESULT_ROWS": "1000",
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

        start = threading.Event()
        stop_at = [0.0]
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=databases * 5) as executor:
            for i in range(databases):
                tenant = f"tenant{i:03d}"
                futures.append(executor.submit(worker, base_url, tenant, True, start, stop_at))
                for _ in range(4):
                    futures.append(executor.submit(worker, base_url, tenant, False, start, stop_at))
            stop_at[0] = time.monotonic() + duration
            start.set()
            results = [future.result() for future in futures]

        final_memory = memory(proc)
        stats = request(base_url, "/v1/admin/stats")
        reads = sum(ops for writer, ops, _, _ in results if not writer)
        writes = sum(ops for writer, ops, _, _ in results if writer)
        errors = sum(errors for _, _, errors, _ in results)
        latencies = [latency for _, _, _, values in results for latency in values]
        return {
            "databases": databases,
            "connections_per_db": connections,
            "users": databases * 5,
            "rows_per_db": ROWS_PER_DATABASE,
            "baseline_mb": round(baseline["working_mb"], 2),
            "after_setup_mb": round(after_setup["working_mb"], 2),
            "final_mb": round(final_memory["working_mb"], 2),
            "peak_mb": round(final_memory["peak_working_mb"], 2),
            "private_mb": round(final_memory["private_mb"], 2),
            "reads": reads,
            "writes": writes,
            "rps": round((reads + writes) / duration, 1),
            "latency_p50_ms": round(statistics.median(latencies), 2),
            "latency_p95_ms": round(percentile(latencies, 0.95), 2),
            "errors": errors,
            "open_databases": stats["open_databases"],
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    scenarios = [(1, 2), (1, 5), (5, 2), (20, 2), (50, 2)]
    for index, (databases, connections) in enumerate(scenarios):
        result = run_scenario(index, databases, connections)
        print(json.dumps(result), flush=True)
