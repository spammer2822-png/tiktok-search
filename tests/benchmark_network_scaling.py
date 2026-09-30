"""High-worker real loopback TLS benchmark; no remote TikTok performance claim.

Each configured worker sends two sequential profile requests. Request signing,
normalization, HTTPX pooling and backend admission are real. Only the origin is
redirected to a verified local TLS fixture. Run separately from CPU benchmarks.
"""
import argparse
import asyncio
import contextlib
import json
import os
import platform
import statistics
import sys
import time
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
import hybrid_backend as h
import tiktok_worker_scanner as s
from scan_runtime import runtime_ceiling
from test_hybrid_backend import direct_profile, worker_profile
from test_proxy_wire import ProxyWireTests
from connection_metrics import pool_snapshot


class Origin(ProxyWireTests):
    async def origin_handler(self, reader, writer):
        task = asyncio.current_task()
        self.handler_tasks.add(task)
        self.connections += 1
        try:
            while True:
                raw = await reader.readuntil(b'\r\n\r\n')
                self.received += 1
                await asyncio.sleep(.02)
                payload = direct_profile() if b'/api/user/detail/' in raw else worker_profile()
                body = json.dumps(payload).encode()
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '
                             + str(len(body)).encode() + b'\r\nConnection: keep-alive\r\n\r\n' + body)
                await writer.drain()
        except (OSError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            self.handler_tasks.discard(task)


def loopback_bytes():
    try:
        row = next(x for x in Path('/proc/net/dev').read_text().splitlines() if x.strip().startswith('lo:'))
        fields = row.split(':')[1].split()
        return dict(received=int(fields[0]), transmitted=int(fields[8]))
    except (OSError, StopIteration):
        return {}


async def measure(mode, workers):
    origin = Origin()
    origin.connections = origin.received = 0
    start_server = asyncio.start_server
    async def server_with_backlog(*args, **kwargs):
        kwargs.setdefault('backlog', max(128, workers))
        return await start_server(*args, **kwargs)
    with patch.object(asyncio, 'start_server', side_effect=server_with_backlog):
        await origin.asyncSetUp()
    ceiling = runtime_ceiling(workers)
    gate = s.AsyncRequestGate(0, 0, asyncio.Event(), ceiling=ceiling, initial=ceiling, adaptive=False)
    clients = []
    async def redirect(request):
        local = httpx.URL(origin.origin_url)
        request.url = request.url.copy_with(scheme=local.scheme, host=local.host, port=local.port)
    def factory(route, options):
        hooks = {k: list(v) for k, v in options.get('event_hooks', {}).items()}
        hooks.setdefault('request', []).insert(0, redirect)
        client = httpx.AsyncClient(**{**options, 'verify': origin.verify, 'event_hooks': hooks})
        clients.append(client)
        return client
    client = h.HybridClient(gate, s.ProxyPool([]), settings={**h.DEFAULTS, 'backend_mode': mode}, client_factory=factory)
    lags, latencies, errors = [], [], []
    active_jobs = peak_jobs = peak_connections = 0
    active_area = idle_area = 0.
    done = asyncio.Event()
    async def sample():
        nonlocal peak_connections, active_area, idle_area
        previous = time.perf_counter()
        while not done.is_set():
            await asyncio.sleep(.02)
            now = time.perf_counter()
            lags.append(max(0., now-previous-.02))
            pool = pool_snapshot(clients)
            active = pool['active'] or 0
            peak_connections = max(peak_connections, active)
            active_area += active*(now-previous)
            idle_area += (pool['idle_keepalive'] or 0)*(now-previous)
            previous = now
    async def work(i):
        nonlocal active_jobs, peak_jobs
        active_jobs += 1
        peak_jobs = max(peak_jobs, active_jobs)
        try:
            for _ in range(2):
                backend = client.choose()
                start = time.perf_counter()
                try:
                    result = await client.backends[backend].request_json('profile', {'username': f'bench{i}'})
                    assert result['status'] == 'ok'
                except Exception as exc:
                    errors.append(type(exc).__name__)
                latencies.append(time.perf_counter()-start)
        finally:
            active_jobs -= 1
    before_bytes = loopback_bytes()
    cpu, start = time.process_time(), time.perf_counter()
    try:
        async with client:
            ticker = asyncio.create_task(sample())
            try:
                await asyncio.gather(*(work(i) for i in range(workers)))
            finally:
                done.set()
                await ticker
            metrics = client.metrics.snapshot()
        elapsed, cpu = time.perf_counter()-start, time.process_time()-cpu
        after_bytes = loopback_bytes()
        values = sorted(latencies)
        output = dict(scope='real verified loopback TLS; synthetic upstream, no profile persistence',
            mode=mode, configured_workers=workers, actual_connection_ceiling=ceiling,
            peak_active_jobs=peak_jobs, peak_admitted_requests=gate.peak_active,
            sampled_peak_active_connections=peak_connections,
            sampled_average_active_connections=active_area/elapsed,
            sampled_average_idle_connections=idle_area/elapsed, elapsed_seconds=elapsed,
            attempted_requests=len(latencies), successful_requests=len(latencies)-len(errors),
            attempted_rps=len(latencies)/elapsed, successful_rps=(len(latencies)-len(errors))/elapsed,
            new_tls_connections=origin.connections, server_requests=origin.received,
            average_latency=statistics.mean(values), median_latency=statistics.median(values),
            p95_latency=values[min(len(values)-1, int(len(values)*.95))],
            p99_latency=values[min(len(values)-1, int(len(values)*.99))],
            event_loop_p95=sorted(lags)[min(len(lags)-1, int(len(lags)*.95))], event_loop_max=max(lags),
            cpu_seconds=cpu, cpu_percent_of_one_core=100*cpu/elapsed, errors=errors,
            loopback_interface_bytes={k: after_bytes[k]-v for k,v in before_bytes.items()},
            network_scope='lo kernel interface counters include both client and server TLS traffic',
            backend_metrics=metrics, python=platform.python_version(), platform=platform.platform(),
            measured_at_utc=s.utc_iso())
        try:
            import resource
            output['peak_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
        except ImportError:
            output['peak_rss_bytes'] = None
        assert all(c.is_closed for c in clients)
        assert gate.active == 0 and origin.received == workers*2, output
        assert not errors, output
        return output
    finally:
        await origin.asyncTearDown()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['worker','direct','hybrid'], required=True)
    p.add_argument('--workers', type=int, required=True)
    p.add_argument('--result', type=Path, required=True)
    args = p.parse_args()
    args.result.parent.mkdir(parents=True, exist_ok=True)
    result = asyncio.run(measure(args.mode, args.workers))
    args.result.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({k: result[k] for k in ('mode','configured_workers','elapsed_seconds','successful_rps','errors')}))
