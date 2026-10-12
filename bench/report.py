"""Generate the readable benchmark table exclusively from measured JSON."""

import argparse
import json
from pathlib import Path


def generate(path):
    path = Path(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    lines = [
        f"Source: `{path.name}`",
        "",
        f"Commit: `{data['git_sha']}`; dirty: {data['dirty']}",
        f"Command: `{data['command']}`",
        f"Machine: {data['machine']}",
        f"Tests at this commit: {data['tests']}",
        "",
        "| Workers | Median deliveries/s | Median p50 ms | Median p95 ms | Median p99 ms |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in data["medians"]:
        lines.append(
            f"| {row['workers']} | {row['throughput_dps']:.2f} | "
            f"{row['p50_ms']:.2f} | {row['p95_ms']:.2f} | {row['p99_ms']:.2f} |"
        )
    lines.extend(
        [
            "",
            "| Run | Expected | Unique acknowledged | Dead | Lost | Duplicates |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in data["runs"]:
        label = "hard kill" if row["kill"] else f"{row['workers']} workers / {row['repeat']}"
        lines.append(
            f"| {label} | {row['deliveries_expected']} | {row['delivered_unique']} | "
            f"{row['dead']} | {row['lost']} | {row['duplicates']} |"
        )
    lines.extend(
        [
            "",
            "Windows hard-kill uses TerminateProcess; POSIX uses SIGKILL.",
            "Both abruptly stop a worker without running its cleanup handlers.",
            "Latency uses PostgreSQL event creation and success timestamps; wall time begins "
            "after seeding and includes worker startup.",
            "",
        ]
    )
    output = path.parent / "latest.md"
    output.write_text("\n".join(lines), encoding="utf-8")
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("json_path", type=Path)
    generate(parser.parse_args().json_path)
