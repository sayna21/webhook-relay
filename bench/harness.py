"""Isolated PostgreSQL databases and supervised local service processes."""

import os
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from relay.config import Settings
from relay.db import apply_migrations

ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def fresh_database(base_dsn=None):
    base_dsn = base_dsn or Settings.from_env().database_url
    name = "relay_test_" + uuid4().hex
    admin_dsn = make_conninfo(base_dsn, dbname="postgres")
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = make_conninfo(base_dsn, dbname=name)
    try:
        apply_migrations(dsn)
        yield dsn
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Processes:
    def __init__(self, dsn):
        self.env = {
            **os.environ,
            "RELAY_DATABASE_URL": dsn,
            "RELAY_ADMIN_TOKEN": "",
            "RELAY_RETRY_BASE_S": "0.05",
            "RELAY_RETRY_CAP_S": "2",
            "RELAY_MAX_ATTEMPTS": "12",
            "PYTHONUNBUFFERED": "1",
        }
        self.children = []

    def start(self, *args, env=None):
        log = tempfile.TemporaryFile()
        process = subprocess.Popen(
            [sys.executable, *args],
            cwd=ROOT,
            env={**self.env, **(env or {})},
            stdout=subprocess.DEVNULL,
            stderr=log,
        )
        process.stderr = log
        self.children.append(process)
        return process

    def error(self, process):
        process.stderr.seek(0)
        return process.stderr.read().decode(errors="replace")[-8000:]

    def server(self, module, health):
        port = free_port()
        process = self.start(
            "-m",
            "uvicorn",
            module,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        )
        url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(self.error(process))
            try:
                if httpx.get(url + health, timeout=1, trust_env=False).status_code == 200:
                    return url
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        raise TimeoutError(f"Service did not start: {module}")

    def close(self):
        for process in reversed(self.children):
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
            process.stderr.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
