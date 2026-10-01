"""Profile CPU time in the main thread AND each scanner-created thread.

This separate instrumented run is not used for throughput comparisons. Each
thread has its own cProfile instance and thread_time clock, so overlapping
executor wall waits are not counted as Python CPU time. Exclusive times form
a partition; cumulative times must still not be added together.
"""
import argparse
import cProfile
import csv
import json
from pathlib import Path
import pstats
import runpy
import sys
import threading
import time
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    options, arguments = parser.parse_known_args()
    if sys.version_info[:2] != (3, 11):
        parser.error('Use Python 3.11 for independent cProfile instances per thread. Python 3.12 monitoring cannot run these instances together.')
    output = options.output
    output.parent.mkdir(parents=True, exist_ok=True)
    profiles = []
    lock = threading.Lock()
    original_run = threading.Thread.run

    def profile_thread(thread):
        profiler = cProfile.Profile(timer=time.thread_time)
        profiler.enable()
        try:
            return original_run(thread)
        finally:
            profiler.disable()
            with lock:
                profiles.append((thread.name, profiler))

    profiler = cProfile.Profile(timer=time.thread_time)
    started_cpu, started_wall = time.process_time(), time.perf_counter()
    with patch.object(threading.Thread, 'run', profile_thread):
        profiler.enable()
        try:
            sys.argv = [str(Path(__file__).with_name('benchmark_hybrid.py')), *arguments]
            runpy.run_path(sys.argv[0], run_name='__main__')
        finally:
            profiler.disable()
    profiles.append(('main', profiler))
    process_cpu, wall = time.process_time()-started_cpu, time.perf_counter()-started_wall
    stats = pstats.Stats(profiles[0][1])
    for _, item in profiles[1:]:
        stats.add(item)
    assert stats.total_tt > 0
    rows = []
    for (filename, line, function), (primitive, calls, exclusive, cumulative, _) in stats.stats.items():
        assert exclusive >= 0, (filename, line, function, exclusive)
        rows.append(dict(file=filename, line=line, function=function, primitive_calls=primitive,
                         calls=calls, exclusive_cpu_seconds=exclusive,
                         percent_of_profiled_cpu=100*exclusive/stats.total_tt,
                         cumulative_cpu_seconds=cumulative))
    rows.sort(key=lambda row: row['exclusive_cpu_seconds'], reverse=True)
    with output.with_suffix('.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
    with output.with_suffix('.txt').open('w', encoding='utf-8') as stream:
        stats.stream = stream
        stats.strip_dirs().sort_stats('tottime').print_stats(100)
    summary = dict(scope='Instrumented per-thread CPU clock, separate from unprofiled throughput',
                   wall_seconds=wall, process_cpu_seconds=process_cpu,
                   profiled_exclusive_cpu_seconds=stats.total_tt,
                   threads=[name for name, _ in profiles], functions=len(rows),
                   exclusive_percentage_sum=sum(row['percent_of_profiled_cpu'] for row in rows),
                   warning='Profiler overhead is included. Cumulative times overlap; use exclusive times for shares.')
    output.with_suffix('.json').write_text(json.dumps(summary, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(summary))


if __name__ == '__main__':
    main()
