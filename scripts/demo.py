"""A short, isolated end-to-end delivery demonstration."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench.harness import fresh_database
from bench.run import run_scenario


def main():
    with fresh_database() as dsn:
        result = run_scenario(
            dsn,
            events=20,
            subscriptions=2,
            workers=1,
            failure_rate=0.5,
            timeout_rate=0,
            deadline_s=90,
        )
    print(
        f"Delivered {result['delivered_unique']} unique event/subscription pairs; "
        f"dead {result['dead']}; duplicates {result['duplicates']}; lost {result['lost']}."
    )
    print("Signed request verified:", json.dumps(result.get("signed_sample")))


if __name__ == "__main__":
    main()
