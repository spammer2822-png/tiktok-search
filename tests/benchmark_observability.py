"""Matched buffered logging and rolling-statistics component measurements."""
import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import platform
import sys
import tempfile
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--source', type=Path, required=True)
p.add_argument('--result', type=Path, required=True)
a = p.parse_args()
sys.path.insert(0, str(a.source.resolve()))
import tiktok_worker_scanner as scanner
from scan_statistics import ScanStatistics

a.result.parent.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(prefix='observability-', dir=a.result.parent) as directory:
    root = Path(directory)
    clock = [0.0]
    stats = ScanStatistics(root, 5000, clock=lambda: clock[0])
    for i in range(15000):
        clock[0] = i*.02
        stats.request_started()
        stats.request_finished(success=True, latency=.02)
    summary = {'remaining_profiles':180000, 'total_profiles':180000}
    started, cpu = time.perf_counter(), time.process_time()
    for _ in range(1000):
        document = stats.snapshot(summary)
    stats_wall, stats_cpu = time.perf_counter()-started, time.process_time()-cpu
    assert document['requests_successful'] == 15000
    started, cpu = time.perf_counter(), time.process_time()
    with (root/'console.txt').open('w', encoding='utf-8') as terminal:
        with contextlib.redirect_stdout(terminal), scanner.RunLog(root):
            for i in range(10000):
                scanner.console(f'[fixture{i}] [FOLLOWERS] Page 2: received 35, +35 unique, total 70, has_more=True')
    log_wall, log_cpu = time.perf_counter()-started, time.process_time()-cpu
    text = (root/'run.log').read_text(encoding='utf-8')
    assert text.count('has_more=True') == 10000
    result = dict(scope='Offline components, real buffered log writes and fsync; no HTTP',
                  python=platform.python_version(), platform=platform.platform(),
                  source_sha256=hashlib.sha256(Path(scanner.__file__).read_bytes()).hexdigest(),
                  statistics_snapshots=1000, statistics_events=15000, logical_accounts=180000,
                  statistics_seconds=stats_wall, statistics_cpu_seconds=stats_cpu,
                  log_messages=10000, log_seconds=log_wall, log_cpu_seconds=log_cpu,
                  log_bytes=(root/'run.log').stat().st_size)
    a.result.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(result))
