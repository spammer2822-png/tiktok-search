"""Offline full-pipeline benchmark. MockTransport timings are NOT live API RPS.

Use --backend worker/direct/hybrid, --workers, --accounts, --followers 10000.
All output, temporary fixtures and optional profiler data stay beside --result.
"""
import argparse
import asyncio
import contextlib
import json
import os
import platform
import shutil
import statistics
import sys
import tempfile
import time
import hashlib
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
if '--source' in sys.argv:
    sys.path.insert(0, str(Path(sys.argv[sys.argv.index('--source')+1]).resolve()))
import httpx
import tiktok_worker_scanner as s
import hybrid_backend as h
import direct_protocol as d
import report_generator


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend', choices=['worker', 'direct', 'hybrid'], required=True)
    parser.add_argument('--workers', type=int, required=True)
    parser.add_argument('--accounts', type=int, default=100)
    parser.add_argument('--followers', type=int, default=-1)
    parser.add_argument('--latency', type=float, default=.02)
    parser.add_argument('--result', type=Path, required=True)
    parser.add_argument('--profile', action='store_true')
    parser.add_argument('--source', type=Path)
    parser.add_argument('--original', action='store_true')
    args = parser.parse_args()
    args.result.parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='hybrid_bench_', dir=args.result.parent))
    cfg = s.default_scan_config()
    cfg.update(backend_mode=args.backend, workers=args.workers, target_username='benchmark_target', keep_raw=True)
    s.KEEP_RAW_MEMBER_DATA = True
    state = s.DurableScanState(root, {'input_imported': True, 'created_at_utc': s.utc_iso()}, root, cfg)
    state.add_jobs((s.ProfileJob(f'bench{i}', f'https://www.tiktok.com/@bench{i}', ('fixture',), str(7100000000000000000+i))
                    for i in range(args.accounts)))
    s.atomic_write_json(root/'scan_config.json', cfg)
    recorder = s.SuccessRecorder(root/'sucess_find.json', target_user='benchmark_target', input_path=root/'input.json', started_at_utc=s.utc_iso())
    lock_times = {}
    class MeasuredLock:
        def __init__(self, lock, name): self.lock, self.name = lock, name
        def __enter__(self):
            before = time.perf_counter()
            self.lock.acquire()
            lock_times[self.name] = lock_times.get(self.name, 0.)+time.perf_counter()-before
            return self
        def __exit__(self, *args): self.lock.release()
    for obj, name in ((state, 'state_lock_wait_seconds'), (recorder, 'target_lock_wait_seconds')):
        obj.lock = MeasuredLock(obj.lock, name)
    clients, latencies, lags = [], [], []
    peak = active = requests = 0
    operations = {'profile': 0, 'followers': 0, 'following': 0}
    peak_workers = 0
    metrics = []
    def total(i): return args.followers if args.followers >= 0 else (0, 1, 4, 10)[i % 4]
    def user(i): return dict(id=str(7200000000000000000+i), uniqueId=f'person{i}', nickname='Fixture',
                             signature='', secUid='member'+str(i), avatarThumb='', privateAccount=False, verified=False)
    async def handle(req):
        nonlocal active, peak, requests
        started = time.perf_counter(); active += 1; peak = max(peak, active); requests += 1
        direct = req.url.host == 'www.tiktok.com'
        try:
            await asyncio.sleep(args.latency)
            profile = req.url.path in ('/', '/api/user/detail/')
            operation = 'profile' if profile else ('followers' if
                (req.url.params.get('scene') == '67' if direct else req.url.path.endswith('/followers')) else 'following')
            operations[operation] += 1
            if profile:
                name = req.url.params['uniqueId' if direct else 'username']; i = int(name[5:])
                obj = user(i); obj.update(id=str(7100000000000000000+i), uniqueId=name, secUid='sec'+str(i))
                payload = {'statusCode': 0, 'userInfo': {'user': obj, 'stats': {'followerCount': total(i), 'followingCount': total(i)}}}
                if not direct: payload = d.normalize_profile(payload)
            else:
                i = int(req.url.params['secUid'][3:]) if direct else int(req.url.params['Uid'])-7100000000000000000
                raw_cursor = req.url.params['minCursor']
                prefix = 'direct:' if direct else 'worker:'
                if raw_cursor == '0': offset = 0
                else:
                    assert raw_cursor.startswith(prefix), 'cross-backend cursor leaked'
                    offset = int(raw_cursor.split(':')[1])
                count = int(req.url.params['count']) if direct else 20
                end = min(total(i), offset+count)
                payload = {'statusCode': 0, 'userList': [{'user': user(n), 'stats': {}} for n in range(offset, end)],
                           'hasMore': end < total(i), 'minCursor': prefix+str(end) if end < total(i) else ''}
                if not direct: payload = d.normalize_page(payload)
            return httpx.Response(200, json=payload)
        finally:
            active -= 1; latencies.append(time.perf_counter()-started)
    original_client = s.WorkerApiClient
    original_dispatcher = h.create_client
    def factory(gate, pool, **kwargs):
        def transport(route, options):
            client = httpx.AsyncClient(transport=httpx.MockTransport(handle), **{k:v for k,v in options.items() if k != 'proxy'})
            clients.append(client); return client
        client = (original_client if args.original else original_dispatcher)(gate, pool, client_factory=transport, **kwargs)
        if hasattr(client, "metrics"): metrics.append(client.metrics)
        return client
    async def run():
        done = asyncio.Event()
        async def ticker():
            nonlocal peak_workers
            prior = time.perf_counter()
            while not done.is_set():
                await asyncio.sleep(.02)
                now = time.perf_counter(); lags.append(max(0, now-prior-.02)); prior = now
                peak_workers = max(peak_workers, sum(task.get_name().startswith('profile-worker-')
                    and not task.done() for task in asyncio.all_tasks()))
        tick = asyncio.create_task(ticker())
        try:
            with patch.object(s if args.original else h, 'WorkerApiClient' if args.original else 'create_client', side_effect=factory):
                return await s.run_scan([], output_directory=root, worker_count=args.workers, pacing=(0, 0),
                                        success_recorder=recorder, state_recorder=state, use_resume=True, pool=s.ProxyPool([]))
        finally:
            done.set(); await tick
    profiler = None
    def process_io():
        try:
            return {key: int(value) for key, value in (line.split(':', 1) for line in Path('/proc/self/io').read_text().splitlines())}
        except (OSError, ValueError): return {}
    io_before = process_io()
    if args.profile:
        import cProfile
        profiler = cProfile.Profile(); profiler.enable()
    started, cpu = time.perf_counter(), time.process_time()
    with (root/'console.log').open('w') as log, contextlib.redirect_stdout(log), s.RunLog(root):
        result = asyncio.run(run())
        seconds = time.perf_counter()-started
        assert result[2] is None, result[2]
        assert state.summary()['completed_overall'] == args.accounts, state.summary()
        assert all(client.is_closed for client in clients)
        then = time.perf_counter(); report = report_generator.generate_report(root, clean=s.REDACTOR.clean, emit=s.console)
        report_seconds = time.perf_counter()-then
        then = time.perf_counter(); state.recover(recorder); resume_seconds = time.perf_counter()-then
    saved_stats = json.loads((root/"scan_stats.json").read_text())
    state.close()
    if profiler:
        profiler.disable(); profiler.dump_stats(str(args.result.with_suffix('.pstats')))
    quant = lambda values, q: sorted(values)[min(len(values)-1, int(len(values)*q))] if values else 0
    doc = dict(scope='OFFLINE HTTPX MockTransport; real signing, parsing, SQLite, exports and report; no real sockets/proxy/TLS/live service claims',
               harness_version=2, production_dispatcher=True, mixed_profile_counts=[0,1,4,10], lists_per_profile=2, keep_raw=True, backend=args.backend, python=platform.python_version(), workers=args.workers, accounts=args.accounts,
               page_sizes={'worker': 20, 'direct': cfg.get('direct_page_size', 35)}, mock_latency_seconds=args.latency,
               completion_seconds=seconds, requests=requests, successful_rps=requests/seconds, accounts_per_second=args.accounts/seconds,
               followers_per_second=sum(total(i) for i in range(args.accounts))/seconds,
               following_per_second=sum(total(i) for i in range(args.accounts))/seconds,
               cpu_seconds=time.process_time()-cpu, peak_active_mock_requests=peak, pool_connections=None,
               mean_latency=statistics.mean(latencies), median_latency=statistics.median(latencies),
               p95_latency=quant(latencies,.95), p99_latency=quant(latencies,.99),
               loop_p95=quant(lags,.95), loop_max=max(lags), report_seconds=report_seconds, resume_seconds=resume_seconds,
               database_bytes=sum(p.stat().st_size for p in root.rglob('*.sqlite3')),
               generated_bytes=sum(p.stat().st_size for p in root.rglob('*') if p.is_file()),
               report_bytes=report.stat().st_size if report else 0,
               backend_metrics=metrics[0].snapshot() if metrics else {}, persistence_timings=saved_stats.get("persistence_timings", {}), errors=0, retries=0)
    doc.update(source_file_sha256=hashlib.sha256(Path(s.__file__).read_bytes()).hexdigest(),
               platform=platform.platform(), cpu_count=os.cpu_count(), measured_at_utc=s.utc_iso(),
               attempted_rps=requests/seconds, profiles_per_second=operations['profile']/seconds,
               successful_records_per_second=(args.accounts+2*sum(total(i) for i in range(args.accounts)))/seconds,
               requests_by_operation=operations, follower_pages_per_second=operations['followers']/seconds,
               following_pages_per_second=operations['following']/seconds,
               users_per_follower_page=sum(total(i) for i in range(args.accounts))/max(1, operations['followers']),
               average_active_mock_requests=sum(latencies)/seconds, peak_active_profile_workers=peak_workers,
               connection_ceiling=saved_stats.get('connection_ceiling'),
               effective_concurrency_at_finish=saved_stats.get('effective_concurrency'),
               lock_timings=lock_times, disk_io_bytes={key:value-io_before.get(key,value) for key,value in process_io().items()},
               error_rate=0., timeout_rate=0., risk_control_rate=0.)
    try:
        import resource
        doc['peak_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    except ImportError:
        doc['peak_rss_bytes'] = None
    args.result.write_text(json.dumps(doc, indent=2), encoding='utf-8')
    try: shutil.rmtree(root)
    except OSError: pass
    print(json.dumps({'result': str(args.result), 'seconds': seconds, 'successful_rps': requests/seconds}))


if __name__ == '__main__': main()
