"""Run reproducible real HTTP/PostgreSQL delivery workloads."""

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx

from bench.harness import ROOT, Processes, fresh_database
from relay.db import connect
from relay.signing import verify


def run_scenario(
    dsn,
    *,
    events=2000,
    subscriptions=5,
    workers=1,
    threads=8,
    kill=False,
    failure_rate=0.25,
    timeout_rate=0.05,
    deadline_s=600,
    http_timeout_s=2,
    latency_ms=(5, 30),
    timeout_s=3,
    lease_s=5,
):
    expected = events * subscriptions
    with Processes(dsn) as processes, httpx.Client(timeout=10, trust_env=False) as client:
        receiver = processes.server("receiver.mock_receiver:app", "/receipts")
        api = processes.server("relay.api:app", "/healthz")
        secrets = {}
        for _ in range(subscriptions):
            sid = str(uuid4())
            response = client.post(
                api + "/subscriptions",
                json={"url": receiver + "/webhook/" + sid, "event_types": ["*"]},
            )
            response.raise_for_status()
            subscription = response.json()
            # Route identity is receiver-specific; store the actual subscription ID in its URL.
            with connect(dsn) as conn:
                conn.execute(
                    "UPDATE subscriptions SET url=%s WHERE id=%s",
                    (receiver + "/webhook/" + subscription["id"], subscription["id"]),
                )
            secrets[subscription["id"]] = subscription["secret"]
        response = client.post(
            receiver + "/configure",
            json={
                "secrets": secrets,
                "failure_rate": failure_rate,
                "timeout_rate": timeout_rate,
                "latency_min_ms": latency_ms[0],
                "latency_max_ms": latency_ms[1],
                "timeout_s": timeout_s,
                "rng_seed": 417,
            },
        )
        response.raise_for_status()
        for i in range(events):
            response = client.post(api + "/events", json={"type": "benchmark", "payload": {"n": i}})
            response.raise_for_status()
        started = time.monotonic()
        children = [
            processes.start(
                "-m",
                "relay.worker",
                "--concurrency",
                str(threads),
                "--batch",
                str(threads),
                "--lease-s",
                str(lease_s),
                "--poll-ms",
                "20",
                env={"RELAY_HTTP_TIMEOUT_S": str(http_timeout_s)},
            )
            for _ in range(workers)
        ]
        killed = False
        kill_progress = None
        while True:
            with connect(dsn) as conn:
                counts = {
                    r["status"]: r["n"]
                    for r in conn.execute(
                        "SELECT status, count(*) AS n FROM deliveries GROUP BY status"
                    )
                }
            completed = counts.get("succeeded", 0) + counts.get("dead", 0)
            if kill and not killed and expected * 0.3 <= completed < expected:
                victim = next((p for p in children if p.poll() is None), None)
                if victim is None:
                    raise RuntimeError("No live worker at crash injection")
                victim.kill()  # TerminateProcess on Windows, SIGKILL on POSIX.
                victim.wait(timeout=10)
                killed, kill_progress = True, completed / expected
            if completed == expected:
                break
            for child in children:
                if child.poll() is not None and (not killed or child is not victim):
                    raise RuntimeError("Worker exited: " + processes.error(child))
            if time.monotonic() - started > deadline_s:
                logs = [processes.error(child) for child in children]
                raise TimeoutError(f"Workload did not drain: {counts}; worker logs: {logs}")
            time.sleep(0.05)
        wall = time.monotonic() - started
        receipts = client.get(receiver + "/receipts").json()
        acknowledged = [r for r in receipts if r["acknowledged"]]
        pairs = {(r["event_id"], r["subscription_id"]) for r in acknowledged}
        with connect(dsn) as conn:
            dead_pairs = {
                (str(r["event_id"]), str(r["subscription_id"]))
                for r in conn.execute(
                    "SELECT event_id, subscription_id FROM deliveries WHERE status='dead'"
                )
            }
            all_pairs = {
                (str(r["event_id"]), str(r["subscription_id"]))
                for r in conn.execute("SELECT event_id, subscription_id FROM deliveries")
            }
            attempts = conn.execute(
                "SELECT COALESCE(sum(attempt_count), 0) AS n FROM deliveries"
            ).fetchone()["n"]
            latency = conn.execute(
                "SELECT percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP "
                "(ORDER BY EXTRACT(EPOCH FROM d.succeeded_at-e.created_at)*1000) "
                "AS p FROM deliveries d JOIN events e ON e.id=d.event_id "
                "WHERE d.status='succeeded'"
            ).fetchone()["p"]
            server_version = conn.execute("SHOW server_version").fetchone()["server_version"]
        lost = len(all_pairs - (pairs | dead_pairs)) + max(0, expected - len(all_pairs))
        sample = next((r for r in receipts if r["signature_valid"]), None)
        result = dict(
            workers=workers,
            threads=threads,
            deliveries_expected=expected,
            delivered_unique=len(pairs),
            dead=len(dead_pairs),
            lost=lost,
            duplicates=len(acknowledged) - len(pairs),
            attempts_total=attempts,
            wall_s=wall,
            throughput_dps=counts.get("succeeded", 0) / wall,
            p50_ms=latency[0] if latency else None,
            p95_ms=latency[1] if latency else None,
            p99_ms=latency[2] if latency else None,
            kill=kill,
            kill_progress=kill_progress,
            postgres_version=server_version,
            kill_method="TerminateProcess" if os.name == "nt" else "SIGKILL",
        )
        if sample:
            result["signed_sample"] = {
                "body": sample["raw_body"],
                "signature": sample["signature"],
                "verified": verify(
                    secrets[sample["subscription_id"]],
                    sample["signature"],
                    sample["raw_body"].encode(),
                    now=datetime.fromisoformat(sample["received_at"]).timestamp(),
                ),
            }
        if lost or (kill and not killed):
            raise AssertionError(f"Workload acceptance failed: {result}")
        return result


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def test_counts(path):
    root = ET.parse(path).getroot()
    cases = list(root.iter("testcase"))
    failed = sum(c.find("failure") is not None or c.find("error") is not None for c in cases)
    skipped = sum(c.find("skipped") is not None for c in cases)
    return {"passed": len(cases) - failed - skipped, "failed": failed, "skipped": skipped}


def machine():
    info = {
        "os": platform.platform(),
        "cpu_model": platform.processor(),
        "cores": os.cpu_count(),
        "ram_gb": None,
    }
    if os.name == "nt":
        try:
            value = subprocess.check_output(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory",
                ],
                text=True,
            )
            info["ram_gb"] = round(int(value.strip()) / 1024**3, 2)
        except (subprocess.SubprocessError, ValueError):
            pass
    else:
        try:
            info["ram_gb"] = round(
                os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024**3, 2
            )
        except (ValueError, OSError, AttributeError):
            pass
    return info


def source_digest():
    digest = hashlib.sha256()
    for folder in ("relay", "receiver", "bench", "scripts", "tests", "migrations"):
        for path in sorted((ROOT / folder).rglob("*")):
            if path.suffix in (".py", ".sql"):
                digest.update(str(path.relative_to(ROOT)).encode())
                digest.update(path.read_bytes())
    digest.update((ROOT / "pyproject.toml").read_bytes())
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=2000)
    parser.add_argument("--subscriptions", type=int, default=5)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--deadline-s", type=float, default=600)
    args = parser.parse_args()
    if min(args.events, args.subscriptions, args.repeats, *args.workers) <= 0:
        parser.error("Workload sizes must be positive")
    started = datetime.now(UTC).isoformat()
    sha = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain"))
    source_hash = source_digest()
    with tempfile.TemporaryDirectory() as temp:
        junit = Path(temp) / "tests.xml"
        subprocess.run(
            [sys.executable, "-m", "pytest", "--junitxml", str(junit)], cwd=ROOT, check=True
        )
        tests = test_counts(junit)
    runs = []
    for workers in args.workers:
        for repeat in range(args.repeats):
            with fresh_database() as dsn:
                result = run_scenario(
                    dsn,
                    events=args.events,
                    subscriptions=args.subscriptions,
                    workers=workers,
                    deadline_s=args.deadline_s,
                )
            result["repeat"] = repeat + 1
            runs.append(result)
            print(json.dumps(result), flush=True)
    with fresh_database() as dsn:
        runs.append(
            run_scenario(
                dsn,
                events=args.events,
                subscriptions=args.subscriptions,
                workers=4,
                kill=True,
                deadline_s=args.deadline_s,
            )
        )
    if git("rev-parse", "HEAD") != sha or source_digest() != source_hash:
        raise RuntimeError("Commit changed during measurement; rerun benchmark")
    medians = [
        {
            key: statistics.median(
                r[key] for r in runs if r["workers"] == workers and not r["kill"]
            )
            for key in ("throughput_dps", "wall_s", "p50_ms", "p95_ms", "p99_ms")
        }
        | {"workers": workers}
        for workers in args.workers
    ]
    record = dict(
        git_sha=sha,
        dirty=dirty,
        started_at=started,
        finished_at=datetime.now(UTC).isoformat(),
        command=" ".join([sys.executable, "-m", "bench.run", *sys.argv[1:]]),
        params={
            **vars(args),
            "threads": 8,
            "failure_rate": 0.25,
            "timeout_rate": 0.05,
            "http_timeout_s": 2,
            "receiver_timeout_s": 3,
            "latency_ms": [5, 30],
            "retry_base_s": 0.05,
            "retry_cap_s": 2,
            "max_attempts": 12,
            "lease_s": 5,
            "kill_workers": 4,
            "kill_threshold": 0.3,
        },
        machine=machine(),
        python=sys.version,
        tests=tests,
        tests_git_sha=sha,
        source_sha256=source_hash,
        runs=runs,
        medians=medians,
    )
    directory = ROOT / "results"
    directory.mkdir(exist_ok=True)
    path = directory / ("benchmark-" + datetime.now(UTC).strftime("%Y%m%d-%H%M%S") + ".json")
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    from bench.report import generate

    generate(path)
    print(f"Results: {path}")


if __name__ == "__main__":
    main()
