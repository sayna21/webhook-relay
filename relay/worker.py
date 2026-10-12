"""Bounded synchronous worker and graceful process entry point."""

import argparse
import signal
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, replace
from pathlib import Path
from uuid import uuid4

import httpx

from relay import store
from relay.config import Settings
from relay.db import make_pool
from relay.deliver import send_delivery


def run_loop(
    settings=None, *, stop_event=None, worker_id=None, max_idle_polls=None, stop_file=None
):
    """Claim only free execution slots; stop claiming on shutdown and drain results.

    HTTP runs outside transactions. Each claim and completion borrows its own
    connection, so no connection or transaction crosses executor threads.
    Failed result writes propagate: the unrecorded lease can then be reclaimed.
    """
    settings = settings if settings is not None else Settings.from_env()
    stop = stop_event if stop_event is not None else threading.Event()

    def stopping():
        return stop.is_set() or (stop_file is not None and Path(stop_file).exists())

    if stopping():
        return
    if max_idle_polls is not None and (type(max_idle_polls) is not int or max_idle_polls < 1):
        raise ValueError("max_idle_polls must be a positive integer")
    identity = worker_id or str(uuid4())
    handlers = {}
    if threading.current_thread() is threading.main_thread():
        for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
            sig = getattr(signal, name, None)
            if sig is not None:
                handlers[sig] = signal.signal(sig, lambda *_: stop.set())
    try:
        with (
            make_pool(settings.database_url, max_size=settings.concurrency + 1) as pool,
            httpx.Client(
                timeout=settings.http_timeout_s,
                follow_redirects=False,
                trust_env=False,
                limits=httpx.Limits(
                    max_connections=settings.concurrency,
                    max_keepalive_connections=settings.concurrency,
                ),
            ) as client,
            ThreadPoolExecutor(max_workers=settings.concurrency) as executor,
        ):

            def attempt(claim):
                result = send_delivery(claim, settings, client=client)
                with pool.connection() as conn:
                    store.record_result(
                        conn,
                        claim,
                        worker_id=identity,
                        max_attempts=settings.max_attempts,
                        **asdict(result),
                    )

            pending = set()
            idle_polls = 0
            while not stopping():
                completed = {future for future in pending if future.done()}
                pending -= completed
                for future in completed:
                    future.result()
                available = settings.concurrency - len(pending)
                if available and not stopping():
                    with pool.connection() as conn:
                        if stopping():
                            break
                        claims = store.claim_due(
                            conn,
                            identity,
                            min(settings.batch, available),
                            settings.lease_s,
                            settings.max_attempts,
                        )
                    # A signal during the claim must still drain that committed batch.
                    pending.update(executor.submit(attempt, claim) for claim in claims)
                    idle_polls = idle_polls + 1 if not claims and not pending else 0
                    if max_idle_polls is not None and idle_polls >= max_idle_polls:
                        break
                if pending:
                    wait(
                        pending,
                        timeout=max(0.001, settings.poll_ms / 1000),
                        return_when=FIRST_COMPLETED,
                    )
                else:
                    stop.wait(max(0.001, settings.poll_ms / 1000))
            for future in pending:
                future.result()
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stop-file", help="Exit gracefully when this file exists")
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--batch", type=int)
    parser.add_argument("--lease-s", type=float)
    parser.add_argument("--poll-ms", type=int)
    args = vars(parser.parse_args(argv))
    stop_file = args.pop("stop_file")
    settings = replace(Settings.from_env(), **{k: v for k, v in args.items() if v is not None})
    run_loop(settings, stop_file=stop_file)


if __name__ == "__main__":
    main()
