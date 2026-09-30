"""Incremental backend measurements. No scan-sized arrays in routing or stats."""
from __future__ import annotations

import time
from collections import Counter, deque
from contextlib import contextmanager


class BackendMetrics:
    def __init__(self, clock=time.monotonic):
        self.clock, self.started = clock, clock()
        self.totals = {name: Counter() for name in ('worker', 'direct')}
        self.buckets = {name: deque() for name in self.totals}
        self.latencies = {name: deque(maxlen=4096) for name in self.totals}
        self.recent = {name: Counter() for name in self.totals}
        self.events, self.stages = Counter(), Counter()
        self.proxy = {}
        self.active = Counter()
        self.last_backend = None
        self.last_snapshot = (0, {})

    def add(self, backend, **values):
        now = int(self.clock()); buckets = self.buckets[backend]
        if not buckets or buckets[-1][0] != now:
            buckets.append((now, Counter()))
        buckets[-1][1].update(values)
        self.totals[backend].update(values)
        self.recent[backend].update(values)
        self.prune(backend)

    def prune(self, backend):
        queue = self.buckets[backend]
        while queue and queue[0][0] + 1 <= self.clock()-60:
            self.recent[backend].subtract(queue.popleft()[1])
        latency = self.latencies[backend]
        while latency and latency[0][0] < self.clock()-60:
            latency.popleft()

    def begin(self, backend):
        self.active[backend] += 1
        self.totals[backend]['peak_active_requests'] = max(self.totals[backend]['peak_active_requests'], self.active[backend])
        self.add(backend, requests_total=1)

    def end(self, backend, operation, elapsed, *, success, records=0, kind=None, code=None, proxy=None, size=0):
        self.active[backend] -= 1
        values = {'requests_successful' if success else 'requests_failed': 1,
                  'latency_seconds': elapsed, 'response_body_bytes': size}
        if success:
            values['records'] = records
            values[operation+'_received'] = records
        if kind:
            values[kind+'_count'] = 1
            if 'timeout' in kind: values['timeout_count'] = 1
            if 'network' in kind: values['network_error_count'] = 1
            if 'risk' in kind: values['risk_control_count'] = 1
            if 'invalid' in kind: values['invalid_response_count'] = 1
        if code and code >= 400:
            values['http_error_count'] = 1
            if code == 429: values['429_count'] = 1
        self.add(backend, **values)
        self.latencies[backend].append((self.clock(), elapsed))
        if proxy:
            counters = self.proxy.setdefault(backend+':'+proxy, Counter())
            counters.update(values)

    def score(self, backend):
        self.prune(backend)
        r = self.recent[backend]
        attempts = r['requests_total']
        if attempts < 2:
            return 1.0
        success = r['requests_successful']
        latency = r['latency_seconds']/max(1, r['requests_successful']+r['requests_failed'])
        yield_per_request = r['records']/max(1, attempts)
        # Expected valid records per occupied connection second, penalized by
        # observed failure and risk rates. Fast failures never inflate the score.
        return max(.001, yield_per_request/max(.001, latency)*(success/max(1, attempts)) /
                   (1+4*r['risk_control_count']/max(1, attempts)))

    @contextmanager
    def stage(self, name):
        start = self.clock()
        try:
            yield
        finally:
            self.stages[name+'_seconds'] += self.clock()-start
            self.stages[name+'_calls'] += 1

    def snapshot(self):
        now = self.clock()
        output = dict(self.events)
        duration = max(.001, min(60., now-self.started))
        for name in self.totals:
            self.prune(name)
            totals, recent = self.totals[name], self.recent[name]
            for key, value in totals.items(): output[name+'_'+key] = value
            for key in ('requests_total', 'requests_successful', 'requests_failed', 'followers_received',
                        'following_received', 'risk_control_count'):
                output.setdefault(name+'_'+key, 0)
            output[name+'_requests_per_second'] = recent['requests_total']/duration
            output[name+'_successful_rps'] = recent['requests_successful']/duration
            output[name+'_records_per_second'] = recent['records']/duration
            output[name+'_active_requests'] = self.active[name]
            output[name+'_recent_success_percent'] = 100*recent['requests_successful']/max(1, recent['requests_total'])
            values = sorted(v for _, v in self.latencies[name])
            output[name+'_average_latency'] = sum(values)/len(values) if values else 0
            for suffix, quant in (('median_latency', .5), ('p95_latency', .95), ('p99_latency', .99)):
                output[name+'_'+suffix] = values[min(len(values)-1, int(len(values)*quant))] if values else 0
            for kind in ('timeout', 'network_error', 'http_error', 'risk_control', 'invalid_response'):
                output[name+'_'+kind+'_rate'] = recent[kind+'_count']/max(1, recent['requests_total'])
        output['hybrid_total_successful_rps'] = sum(output[n+'_successful_rps'] for n in self.totals)
        output['hybrid_records_per_second'] = sum(output[n+'_records_per_second'] for n in self.totals)
        for key in ('backend_switches', 'backend_failovers', 'cursor_chain_restarts'):
            output.setdefault(key, 0)
        if getattr(self, 'pool_provider', None):
            output['backend_connection_pools'] = self.pool_provider()
        output['backend_stages'] = dict(self.stages)
        output['backend_proxy_metrics'] = {name: dict(values) for name, values in self.proxy.items()}
        output['backend_metric_scope'] = '60s rolling rates; latency sample bounded to latest 4096 attempts; response_body_bytes excludes TLS/wire overhead'
        self.last_snapshot = now, output
        return output

    def lines(self):
        data = self.snapshot()
        return '\n'.join(f"{n.upper():6} | {data[n+'_successful_rps']:.1f} success/s | "
                         f"{data[n+'_records_per_second']:.1f} records/s | "
                         f"{1000*data[n+'_average_latency']:.0f}ms | "
                         f"{100-data[n+'_recent_success_percent']:.1f}% unsuccessful" for n in self.totals)
