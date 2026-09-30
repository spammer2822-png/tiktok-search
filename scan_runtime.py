"""Bounded scheduling primitives; no network or scanner-specific dependencies."""
from __future__ import annotations

import asyncio
import heapq
import itertools
import os
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from collections import Counter


async def await_durable(future):
    """Cancellation requests never let the caller close an active writer."""
    interrupted = False
    while True:
        try:
            value = await asyncio.shield(future)
            break
        except asyncio.CancelledError:
            if future.cancelled():
                raise
            interrupted = True
    if interrupted:
        raise asyncio.CancelledError
    return value


def runtime_ceiling(configured: int, settings: dict | None = None) -> int:
    """A local resource budget, NOT a claim about an API/proxy plan's quota."""
    settings = settings or {}
    budget = int(settings.get('max_connections', 0)) or configured
    budget = max(1, min(configured, budget))
    try:
        import resource
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft != resource.RLIM_INFINITY:
            # Each profile can own SQLite/WAL files as well as a socket.
            budget = min(budget, max(1, (soft - 128) // 10))
    except (ImportError, ValueError, OSError):
        pass  # Windows: respect the user budget; do not invent a RAM/plan limit.
    return max(1, min(configured, budget))


class DiskLane:
    """One bounded producer/consumer lane owns shared persistence operations.

    SQLite transactions and fsync run off the event loop. Awaiting each command
    provides backpressure; no unbounded executor backlog can be accumulated.
    """
    def __init__(self, capacity: int = 256, io_workers: int = 0, profile_workers: int = 10000, emit=None):
        self.queue = asyncio.Queue(maxsize=capacity)
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='scan-writer')
        self.io_auto = io_workers == 0
        self.io_ceiling = max(1, min(profile_workers, io_workers or (os.cpu_count() or 1)))
        self.io_limit = 1 if self.io_auto else self.io_ceiling
        self.io_active = self.io_waiting = self.io_peak = 0
        self.io_condition = asyncio.Condition()
        self.io_clock, self.io_cpu = time.monotonic(), time.process_time()
        self.emit = emit
        self.io_executor = ThreadPoolExecutor(max_workers=self.io_ceiling, thread_name_prefix='scan-profile-io')
        self.task = None
        self.peak_queued = 0
        self.timings = Counter()

    async def __aenter__(self):
        self.task = asyncio.create_task(self._run(), name='scan-writer')
        return self

    async def call(self, function, *args, **kwargs):
        future = asyncio.get_running_loop().create_future()
        await self.queue.put((partial(function, *args, **kwargs), future, time.perf_counter(), getattr(function, "__qualname__", type(function).__name__)))
        self.peak_queued = max(self.peak_queued, self.queue.qsize())
        return await await_durable(future)

    async def independent(self, function, *args, **kwargs):
        """Per-profile stores/files may overlap; shared scan state stays ordered.

        Each profile awaits its own calls sequentially, including close. Slots
        bound submitted executor work, and cancellation waits for its commit.
        """
        queued = time.perf_counter()
        async with self.io_condition:
            self.io_waiting += 1
            try:
                await self.io_condition.wait_for(lambda: self.io_active < self.io_limit)
                self.io_active += 1
                self.io_peak = max(self.io_peak, self.io_active)
            finally:
                self.io_waiting -= 1
        duration = [0.]
        try:
            self.timings['profile_queue_wait_seconds'] += time.perf_counter()-queued
            def measured():
                began = time.perf_counter()
                try: return function(*args, **kwargs)
                finally: duration[0] = time.perf_counter()-began
            future = asyncio.get_running_loop().run_in_executor(self.io_executor, measured)
            return await await_durable(future)
        finally:
            name = getattr(function, '__qualname__', type(function).__name__)
            self.timings[name+'_seconds'] += duration[0]
            self.timings[name+'_calls'] += 1
            async with self.io_condition:
                self.io_active -= 1
                now, cpu = time.monotonic(), time.process_time()
                if self.io_auto and now-self.io_clock >= 1:
                    # A CPU-saturated Python pipeline gets slower from competing
                    # writers. Actual idle CPU plus queued I/O justifies growth.
                    utilization = (cpu-self.io_cpu)/(now-self.io_clock)
                    desired = (max(1,self.io_limit//2) if utilization >= .9 else
                               min(self.io_ceiling,self.io_limit*2) if utilization < .75 and self.io_waiting else self.io_limit)
                    if desired != self.io_limit:
                        self.io_limit = desired
                        if self.emit:
                            self.emit(f'[IO] Profile writers adjusted to {desired} (automatic ceiling {self.io_ceiling}).')
                    self.io_clock, self.io_cpu = now, cpu
                self.io_condition.notify(max(0, self.io_limit-self.io_active))

    async def _run(self):
        loop = asyncio.get_running_loop()
        while True:
            item = await self.queue.get()
            try:
                if item is None:
                    return
                function, future, queued, name = item
                self.timings['shared_queue_wait_seconds'] += time.perf_counter()-queued
                duration = [0.]
                def measured():
                    began = time.perf_counter()
                    try: return function()
                    finally: duration[0] = time.perf_counter()-began
                try:
                    result = await loop.run_in_executor(self.executor, measured)
                except BaseException as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)
                finally:
                    self.timings[name+'_seconds'] += duration[0]
                    self.timings[name+'_calls'] += 1
            finally:
                self.queue.task_done()

    async def __aexit__(self, *args):
        await self.queue.put(None)
        await asyncio.shield(self.task)
        self.executor.shutdown(wait=True)
        self.io_executor.shutdown(wait=True)


class RetryQueue:
    """One timer for delayed request continuations, holding no HTTP permits.

    The profile continuation awaits a Future. Other bounded profile workers can
    use the freed connections while it waits. Stop wakes every queued retry.
    """
    def __init__(self, gate):
        self.gate = gate
        self.heap = []
        self.sequence = itertools.count()
        self.changed = asyncio.Event()
        self.task = None
        self.closed = False
        self.peak_pending = 0

    async def wait(self, delay):
        self.gate.check()
        if self.closed:
            raise RuntimeError('Retry queue closed')
        future = asyncio.get_running_loop().create_future()
        heapq.heappush(self.heap, (time.monotonic() + delay, next(self.sequence), future))
        self.peak_pending = max(self.peak_pending, len(self.heap))
        self.changed.set()
        if self.task is None:
            self.task = asyncio.create_task(self._run(), name='scan-retry-timer')
        parent = getattr(self.gate, 'parent', self.gate)
        parent.parked_retries = getattr(parent, 'parked_retries', 0) + 1
        try:
            await future
            self.gate.check()
        finally:
            parent.parked_retries -= 1

    async def _run(self):
        stopped = asyncio.create_task(self.gate.stop_event.wait())
        try:
            while not self.closed and not self.gate.stop_event.is_set():
                while self.heap and (self.heap[0][2].done() or self.heap[0][0] <= time.monotonic()):
                    _, _, future = heapq.heappop(self.heap)
                    if not future.done():
                        future.set_result(None)
                self.changed.clear()
                changed = asyncio.create_task(self.changed.wait())
                delay = max(0, self.heap[0][0] - time.monotonic()) if self.heap else None
                try:
                    await asyncio.wait((changed, stopped), timeout=delay, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    changed.cancel()
                    await asyncio.gather(changed, return_exceptions=True)
        finally:
            stopped.cancel()
            await asyncio.gather(stopped, return_exceptions=True)
            for _, _, future in self.heap:
                if not future.done():
                    future.set_result(None)  # Each continuation checks the gate.
            self.heap.clear()

    async def close(self):
        self.closed = True
        self.changed.set()
        if self.task:
            await self.task


class ShutdownGuard:
    """First SIGINT/SIGTERM cancels once; repeated signals cannot tear a save."""
    def __init__(self, emit):
        self.emit, self.cancel = emit, None
        self.requested = False
        self.previous = {}

    def __enter__(self):
        import signal
        for kind in dict.fromkeys((signal.SIGINT, signal.SIGTERM)):
            self.previous[kind] = signal.getsignal(kind)
            signal.signal(kind, self._signal)
        return self

    def _signal(self, *_):
        if self.requested:
            # Avoid logging here: a signal may interrupt the console lock itself.
            return
        self.requested = True
        if self.cancel is not None:
            self.cancel()
        # Once scan work finishes, let final snapshots/report complete normally.

    def __exit__(self, *_):
        import signal
        for kind, handler in self.previous.items():
            signal.signal(kind, handler)
