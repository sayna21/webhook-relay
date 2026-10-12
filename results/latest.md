Source: `benchmark-20261011-161927.json`

Commit: `1b2d7f417e7b3d75904fbd8f74625e49cbce36ff`; dirty: False
Command: `D:\friends_projects\webhook-relay\.venv\Scripts\python.exe -m bench.run`
Machine: {'os': 'Windows-10-10.0.26200-SP0', 'cpu_model': 'Intel64 Family 6 Model 186 Stepping 2, GenuineIntel', 'cores': 16, 'ram_gb': 15.65}
Tests at this commit: {'passed': 233, 'failed': 0, 'skipped': 0}

| Workers | Median deliveries/s | Median p50 ms | Median p95 ms | Median p99 ms |
| --- | ---: | ---: | ---: | ---: |
| 1 | 42.58 | 135094.65 | 252357.67 | 278318.23 |
| 2 | 87.11 | 77200.35 | 148486.98 | 168053.93 |
| 4 | 170.26 | 60198.38 | 102023.01 | 113975.83 |

| Run | Expected | Unique acknowledged | Dead | Lost | Duplicates |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 workers / 1 | 10000 | 10000 | 0 | 0 | 722 |
| 1 workers / 2 | 10000 | 10000 | 0 | 0 | 722 |
| 1 workers / 3 | 10000 | 10000 | 0 | 0 | 722 |
| 2 workers / 1 | 10000 | 10000 | 0 | 0 | 722 |
| 2 workers / 2 | 10000 | 10000 | 0 | 0 | 722 |
| 2 workers / 3 | 10000 | 10000 | 0 | 0 | 722 |
| 4 workers / 1 | 10000 | 10000 | 0 | 0 | 722 |
| 4 workers / 2 | 10000 | 10000 | 0 | 0 | 722 |
| 4 workers / 3 | 10000 | 10000 | 0 | 0 | 722 |
| hard kill | 10000 | 10000 | 0 | 0 | 722 |

Windows hard-kill uses TerminateProcess; POSIX uses SIGKILL.
Both abruptly stop a worker without running its cleanup handlers.
Latency uses PostgreSQL event creation and success timestamps; wall time begins after seeding and includes worker startup.
