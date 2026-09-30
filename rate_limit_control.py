"""One execution's backend admission state, shared by bootstrap and scanning.

HTTP response hooks change state synchronously before another request can start.
Only network child tasks are interrupted; profile owners retain their durable
pages and translate the interruption into a backend migration or central wait.
"""
from __future__ import annotations

import asyncio
import time

import tiktok_worker_scanner as s


class BackendPaused(s.WorkerApiError):
    def __init__(self, *, code=None):
        super().__init__('Direct traffic paused for rate-limit recovery.',
                         kind='backend_paused', code=code, retryable=True)


class RateLimitController:
    def __init__(self, metrics, settings):
        self.metrics, self.settings = metrics, settings
        self.worker_disabled = False
        self.direct_state = 'HEALTHY'
        self.generation = 0
        self.recovery_used = False
        self.cooldown_until = 0.
        self.ramp_successes = 0
        self.recovery_limit = 1
        self.ready = asyncio.Event()
        self.ready.set()
        self.timer = None
        self.checkpoints = set()
        self.gates = {}
        self.parent = None
        self.metrics.rate_limit_provider = self.snapshot

    def attach(self, parent, backends):
        self.parent = parent
        self.gates = {name: backend.gate for name, backend in backends.items()}
        for name, gate in self.gates.items():
            gate.controller = self
            if name == 'worker':
                gate.disabled = self.worker_disabled
            elif self.direct_state != 'HEALTHY':
                gate.local.set_limit(self.recovery_limit)

    def snapshot(self):
        events = self.metrics.events
        return {
            **{key: events[key] for key in (
                'findtik_429_count', 'findtik_jobs_moved_to_direct',
                'findtik_requests_cancelled_after_429', 'direct_cooldown_count',
                'direct_recovery_attempts', 'direct_recovery_successes', 'direct_recovery_429s')},
            'findtik_disabled_after_429': self.worker_disabled,
            'direct_rate_limit_state': self.direct_state,
            'direct_cooldown_remaining_seconds': max(0., self.cooldown_until-time.monotonic())
                if self.direct_state == 'COOLDOWN' else 0.,
            'scanner_stopped_due_to_all_backends_rate_limited':
                bool(events['scanner_stopped_due_to_all_backends_rate_limited']),
        }

    def check(self, name):
        if name == 'worker' and self.worker_disabled:
            raise s.WorkerApiError('Worker disabled after HTTP 429 for this execution.',
                                   kind='backend_unavailable')
        if name == 'direct' and self.direct_state == 'COOLDOWN':
            raise BackendPaused()

    async def wait_ready(self, name):
        self.parent.check()
        if name == 'direct' and self.direct_state == 'COOLDOWN':
            self.parent.parked_retries = getattr(self.parent, 'parked_retries', 0)+1
            try:
                # All parked continuations share this event and ONE timer.
                await self.ready.wait()
            finally:
                self.parent.parked_retries -= 1
        self.parent.check()
        self.check(name)

    def interrupt_network(self, name):
        gate = self.gates[name]
        current = asyncio.current_task()
        for task in tuple(gate.network_tasks):
            if task is current or task.done():
                continue
            task.backend_interrupted = name
            task.cancel()
            if name == 'worker':
                self.metrics.events['findtik_requests_cancelled_after_429'] += 1
        gate.wake_capacity()

    def save_state(self):
        stats = self.parent.stats
        callback = getattr(stats, 'rate_limit_checkpoint', None)
        if callback:
            task = asyncio.create_task(callback(), name='rate-limit-checkpoint')
            self.checkpoints.add(task)
            def done(future):
                self.checkpoints.discard(future)
                if not future.cancelled() and future.exception() is not None:
                    self.parent.blocked_reason = 'Rate-limit checkpoint failed; stopping to protect progress.'
                    self.parent.stop_event.set()
            task.add_done_callback(done)

    def received_429(self, name, retry_after, generation):
        if name == 'worker':
            self.metrics.events['findtik_429_count'] += 1
            if not self.worker_disabled:
                self.worker_disabled = True
                self.gates[name].disabled = True
                self.interrupt_network(name)
                self.save_state()
                s.console('[429] Worker disabled for this execution. Remaining compatible work moves to Direct.', error=True)
            return
        # Responses admitted before the same 429 wave are not recovery probes.
        if generation != self.generation:
            return
        if self.recovery_used:
            self.direct_state = 'STOPPED'
            self.metrics.events['direct_recovery_429s'] += 1
            self.metrics.events['scanner_stopped_due_to_all_backends_rate_limited'] = int(
                self.worker_disabled or 'worker' not in self.gates)
            self.ready.set()
            self.parent.stop_rate_limited('HTTP 429 from Direct after its one recovery opportunity')
            self.save_state()
            return
        self.direct_state = 'COOLDOWN'
        self.generation += 1
        self.ready.clear()
        self.recovery_limit = 1
        self.gates['direct'].local.set_limit(1)
        self.metrics.events['direct_cooldown_count'] += 1
        delay = retry_after if retry_after is not None else self.settings.get('direct_429_cooldown', 60.)
        self.cooldown_until = time.monotonic()+max(0., delay)
        self.interrupt_network('direct')
        self.save_state()
        s.console(f'[429] Direct paused for {delay:.2f}s. One recovery probe will follow.', error=True)
        self.timer = asyncio.create_task(self.cooldown(), name='direct-rate-limit-timer')

    async def cooldown(self):
        try:
            try:
                await asyncio.wait_for(self.parent.stop_event.wait(),
                                       timeout=max(0., self.cooldown_until-time.monotonic()))
            except asyncio.TimeoutError:
                self.direct_state = 'PROBING'
                self.recovery_used = True
                self.metrics.events['direct_recovery_attempts'] += 1
            finally:
                self.ready.set()
        finally:
            self.timer = None

    def succeeded(self, name, generation):
        if name != 'direct' or generation != self.generation:
            return
        if self.direct_state == 'PROBING':
            self.metrics.events['direct_recovery_successes'] += 1
            self.direct_state = 'RECOVERING'
            s.console('[429] Direct recovery succeeded; increasing concurrency with valid responses.')
        if self.direct_state == 'RECOVERING':
            self.ramp_successes += 1
            if self.ramp_successes >= self.recovery_limit:
                self.ramp_successes = 0
                gate = self.gates['direct']
                self.recovery_limit = min(gate.ceiling, self.recovery_limit*2)
                gate.local.set_limit(self.recovery_limit)
                if self.recovery_limit == gate.ceiling:
                    self.direct_state = 'HEALTHY'

    async def detach(self):
        if self.timer:
            self.timer.cancel()
            await asyncio.gather(self.timer, return_exceptions=True)
        if self.checkpoints:
            await asyncio.gather(*self.checkpoints, return_exceptions=True)
        self.gates = {}

