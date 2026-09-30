"""Worker/Direct dispatch underneath the existing durable scanner pipeline."""
from __future__ import annotations

import asyncio
import random
import time
from contextlib import asynccontextmanager
from dataclasses import asdict

import tiktok_worker_scanner as s
from backend_metrics import BackendMetrics
from connection_metrics import RequestTrace, pool_snapshot
from rate_limit_control import BackendPaused, RateLimitController
from direct_protocol import ORIGIN, Session, ProtocolError, load_session, normalize_profile, normalize_page


DEFAULTS = dict(backend_mode='worker', worker_page_size=0, direct_page_size=35,
                worker_backend_max_connections=0, direct_backend_max_connections=0,
                worker_backend_keepalive_connections=0, direct_backend_keepalive_connections=0,
                connections_per_proxy=0, direct_initial_concurrency=0, avatar_workers=0,
                direct_session_file='', race_failed_requests=False,
                direct_retry_attempts=4, worker_retry_attempts=4,
                direct_429_cooldown=60.0)


def validate(config):
    if config.get('backend_mode', 'worker') not in ('worker', 'direct', 'hybrid'):
        raise s.ExporterError('backend_mode must be worker, direct or hybrid.')
    for key in DEFAULTS:
        if type(DEFAULTS[key]) is int:
            value = config.get(key, DEFAULTS[key])
            if type(value) is not int or value < 0:
                raise s.ExporterError(f'{key} must be a nonnegative integer.')
    if config.get('worker_page_size', 0) != 0:
        raise s.ExporterError('worker_page_size must be 0: this Worker has no confirmed count parameter.')
    if not 1 <= config.get('direct_page_size', 35) <= 1000:
        raise s.ExporterError('direct_page_size must be 1..1000; 35 is the test candidate, not a verified list maximum.')
    for key in ('worker_retry_attempts', 'direct_retry_attempts'):
        if not 1 <= config.get(key, 4) <= 20:
            raise s.ExporterError(f'{key} must be 1..20.')
    if type(config.get('race_failed_requests', False)) is not bool:
        raise s.ExporterError('race_failed_requests must be a boolean.')
    if not isinstance(config.get('direct_session_file', ''), str):
        raise s.ExporterError('direct_session_file must be a path, never inline cookies.')
    cooldown = config.get('direct_429_cooldown', 60.)
    if type(cooldown) not in (int, float) or not s.math.isfinite(cooldown) or cooldown < 0:
        raise s.ExporterError('direct_429_cooldown must be finite and nonnegative.')


class BackendGate:
    """Backend admission and an aggregate request budget share one global pacer."""
    def __init__(self, parent, name, settings, metrics):
        self.parent, self.name, self.metrics = parent, name, metrics
        ceiling = settings.get(name+'_backend_max_connections', 0) or parent.ceiling
        self.ceiling = min(parent.ceiling, ceiling)
        initial = settings.get('direct_initial_concurrency', 0) if name == 'direct' else 0
        self.local = s.AsyncRequestGate(0, 0, parent.stop_event, ceiling=self.ceiling,
                                       initial=min(self.ceiling, initial or self.ceiling), adaptive=False)
        self.stop_event, self.stats = parent.stop_event, parent.stats
        self.network_tasks = set()
        self.blocked_reason = None
        self.rate_limit_event = parent.rate_limit_event
        self.rate_limit_exhausted = False
        self.last_adjustment = time.monotonic()
        self.previous_score = 0.
        self.disabled = False

    @property
    def limit(self): return self.local.limit

    def check(self):
        self.parent.check()
        if hasattr(self, 'controller'):
            self.controller.check(self.name)
        if self.disabled:
            raise s.WorkerApiError(f'{self.name} backend is unavailable.', kind='backend_unavailable')
        if self.blocked_reason:
            raise s.WorkerApiError(self.blocked_reason, kind='access_denied')

    @asynccontextmanager
    async def slot(self):
        then = time.monotonic()
        self.check()
        async with self.local.slot():
            async with self.parent.slot():
                self.check()
                self.metrics.stages[self.name+'_admission_wait_seconds'] += time.monotonic()-then
                yield

    async def pace(self):
        self.check()
        await self.local.pace()
        await self.parent.pace()
        self.check()

    def wake_capacity(self): self.local.wake_capacity(); self.parent.wake_capacity()
    async def wait(self, seconds): await self.parent.wait(seconds)
    def cooldown(self, seconds): self.local.cooldown(seconds)
    def penalize(self, seconds): self.local.penalize(seconds)
    def stop_rate_limited(self, reason='HTTP 429 received from API'):
        self.parent.stop_rate_limited(reason)

    def observe(self, latency, pool):
        if hasattr(self, 'controller') and self.controller.direct_state != 'HEALTHY':
            return
        if self.name != 'direct' or time.monotonic()-self.last_adjustment < 1:
            return
        recent = self.metrics.recent[self.name]
        score = self.metrics.score(self.name)
        failed = recent['requests_failed']/max(1, recent['requests_total'])
        if failed > .15 or recent['risk_control_count'] > 0:
            self.local.set_limit(max(1, self.limit//2))
        elif score >= self.previous_score*.95:
            self.local.set_limit(min(self.ceiling, max(self.limit+1, self.limit*2)))
        self.previous_score, self.last_adjustment = score, time.monotonic()

    def observe_failure(self):
        self.observe(0, None)


class ApiBackend(s.WorkerApiClient):
    """Persistent clients and retry budgets are isolated by backend AND proxy."""
    def __init__(self, parent, pool, name, metrics, *, settings, raw_directory=None, client_factory=None):
        self.name, self.metrics = name, metrics
        self.sessions, self.session_source = {}, {}
        gate = BackendGate(parent, name, settings, metrics)
        super().__init__(gate, pool, settings=settings, raw_directory=raw_directory, client_factory=client_factory)
        self.proxy_slots = {}
        self.transport_factory = client_factory
        self.connection_slots = {}

    async def __aenter__(self):
        if self.name == 'direct':
            try:
                self.session_source = await s.disk_call(load_session, self.settings.get('direct_session_file'))
            except ProtocolError as exc:
                raise s.ExporterError(str(exc)) from None
            secrets = list(self.session_source.get('cookies', {}).values())
            for redactor in (self.redactor, s.REDACTOR):
                redactor.secrets = sorted(set(redactor.secrets).union(filter(None, secrets)), key=len, reverse=True)
        return self

    def get_client(self, route):
        key = route.label if route else 'direct'
        if key in self.clients:
            return self.clients[key]
        session = self.sessions.setdefault(key, Session(self.session_source)) if self.name == 'direct' else None
        def factory(r, options):
            max_conn = self.gate.ceiling
            per_proxy = self.settings.get('connections_per_proxy', 0)
            if r and per_proxy:
                max_conn = min(max_conn, per_proxy)
            keep = self.settings.get(self.name+'_backend_keepalive_connections', 0) or self.gate.ceiling
            keep = max(1, s.math.ceil(keep/max(1, len(self.pool.states))))
            options['limits'] = self.httpx.Limits(max_connections=max_conn,
                max_keepalive_connections=min(max_conn, keep), keepalive_expiry=90.)
            self.connection_slots[key] = asyncio.Semaphore(max_conn)
            if session:
                options['headers'] = {'User-Agent': session.user_agent, 'Accept': 'application/json',
                    'Accept-Language': 'en-US,en;q=0.9', 'Referer': ORIGIN+'/', 'Origin': ORIGIN}
                jar = self.httpx.Cookies()
                for name, value in session.cookies.items():
                    jar.set(name, value, domain='.tiktok.com', path='/')
                options['cookies'] = jar
            return self.transport_factory(r, options) if self.transport_factory else self.httpx.AsyncClient(**options)
        self.client_factory = factory
        return super().get_client(route)

    async def response_headers(self, response):
        if response.status_code == 429:
            control = self.gate.controller
            control.received_429(self.name,
                s.retry_after_seconds(response.headers.get('Retry-After')),
                response.request.extensions.get('rate_limit_generation', control.generation))
            if self.gate.parent.rate_limit_exhausted:
                raise s.WorkerApiError(self.gate.parent.rate_limit_reason, kind='rate_limited', code=429)
            self.gate.parent.check()
            if self.name == 'direct':
                raise BackendPaused(code=429)
            raise s.WorkerApiError('Worker HTTP 429; disabled for this execution.', kind='backend_unavailable', code=429)

    async def request_json(self, operation, params, *, context=None):
        attempts = self.settings.get(self.name+'_retry_attempts', self.settings.get('retry_attempts', 4))
        route, select_route, extra_404 = None, True, False
        attempt = 0
        while attempt <= attempts:
            await self.gate.controller.wait_ready(self.name)
            self.request_retries.set(attempt)
            response = task = failure = None
            success = cancelled = started = False
            records = size = 0; status = None; retry_after = None
            if select_route:
                with self.metrics.stage('proxy_selection'):
                    route = self.pool.choose()
                select_route = False
            try:
                client = await self.borrow_client(route)
            except BackendPaused:
                continue
            if route: route.in_use += 1
            begin = time.monotonic()
            generation = self.gate.controller.generation
            key = route.label if route else 'direct'
            acquired = proxy_acquired = False
            proxy_slot = None
            try:
                waiting = time.monotonic()
                await self.connection_slots[key].acquire()
                acquired = True
                self.metrics.add(self.name, connection_pool_wait_seconds=time.monotonic()-waiting)
                if route and self.settings.get('connections_per_proxy', 0):
                    slots = getattr(self.pool, '_hybrid_connection_slots', None)
                    if slots is None: slots = self.pool._hybrid_connection_slots = {}
                    proxy_slot = slots.setdefault(key, asyncio.Semaphore(self.settings['connections_per_proxy']))
                    waiting = time.monotonic()
                    await proxy_slot.acquire()
                    proxy_acquired = True
                    self.metrics.add(self.name, proxy_capacity_wait_seconds=time.monotonic()-waiting)
                async def send():
                    nonlocal begin, started, generation
                    if self.name == 'direct':
                        key = route.label if route else 'direct'
                        # Native signing is CPU work; keeping it off-loop bounds
                        # loop stalls under thousands of independently queued calls.
                        cookies = {cookie.name: cookie.value for cookie in client.cookies.jar
                                   if cookie.domain in ('.tiktok.com', 'www.tiktok.com', 'tiktok.com')}
                        with self.metrics.stage('signing'):
                            origin, query = await asyncio.to_thread(self.sessions[key].signed_url,
                                operation, params, self.settings.get('direct_page_size', 35), cookies)
                        url = self.httpx.URL(origin).copy_with(query=query)
                        method, options = 'GET', {}
                    else:
                        url = self.worker_origin + ('/' if operation == 'profile' else '/api/'+operation)
                        method = 'GET' if operation == 'profile' else 'POST'
                        options = {'params': params, 'content': None if method == 'GET' else b''}
                    await self.gate.pace()
                    self.gate.check()
                    generation = self.gate.controller.generation
                    begin, started = time.monotonic(), True
                    self.metrics.begin(self.name)
                    if self.gate.stats: self.gate.stats.request_started(retry=attempt > 0)
                    async with asyncio.timeout(self.settings.get('timeouts', {}).get('total', 55.)):
                        return await client.request(method, url, extensions={
                            'trace': RequestTrace(self.metrics, self.name),
                            'rate_limit_generation': generation}, **options)
                async with self.gate.slot():
                    task = asyncio.create_task(send(), name=self.name+'-api-request')
                    self.gate.network_tasks.add(task); self.gate.parent.network_tasks.add(task)
                    response = await task
                status, size = response.status_code, len(response.content)
                self.pool.good_response(route)
                retry_after = s.retry_after_seconds(response.headers.get('Retry-After'))
                if not 200 <= status < 300:
                    kind = 'access_denied' if status in (401, 403) else 'temporary_server_failure' if status >= 500 else 'http_error'
                    raise s.WorkerApiError(f'{self.name} HTTP {status}.', kind=kind, code=status, retryable=status >= 500)
                with self.metrics.stage('json_parsing'):
                    try: payload = response.json()
                    except ValueError:
                        raise ProtocolError('empty_response' if not response.content else 'invalid_response',
                                            'API returned empty or non-JSON content.', retryable=not response.content) from None
                with self.metrics.stage('normalization'):
                    if self.name == 'direct':
                        original_payload = payload
                        payload = normalize_profile(payload) if operation == 'profile' else normalize_page(payload,
                            expected_count=params.get('_expected_count'), cursor=params['minCursor'], keep_raw=s.KEEP_RAW_MEMBER_DATA)
                        if operation == 'profile' and s.KEEP_RAW_MEMBER_DATA:
                            payload['data']['raw_direct'] = original_payload
                    try:
                        checked = s.check_worker_error(payload, operation)
                    except s.WorkerApiError as exc:
                        if exc.kind == 'rate_limited':
                            raise s.WorkerApiError(str(exc), kind='upstream_rate_limited') from None
                        raise
                    if operation == 'profile':
                        try: s.parse_profile_response(checked, params['username'])
                        except s.PublicProfileRequired: pass  # A confirmed private profile is a valid observation.
                        records = 1
                    else:
                        page = s.parse_list_page(checked, operation)
                        records = len(page.records)
                success = True
                self.gate.controller.succeeded(self.name, generation)
                return checked
            except asyncio.CancelledError:
                cancelled = True
                if task and getattr(task, 'backend_interrupted', None) and not asyncio.current_task().cancelling():
                    failure = BackendPaused() if self.name == 'direct' else s.WorkerApiError(
                        'Worker request cancelled after HTTP 429.', kind='backend_unavailable')
                else:
                    raise
            except ProtocolError as exc:
                failure = s.WorkerApiError(str(exc), kind=exc.kind, retryable=exc.retryable)
            except self.httpx.ProxyError as exc:
                failure = self.proxy_exception(exc)
            except self.httpx.ConnectTimeout:
                failure = s.WorkerApiError('Connection timed out.', kind='proxy_timeout' if route else 'network_timeout', retryable=True)
            except self.httpx.ConnectError:
                failure = s.WorkerApiError('Connection or TLS negotiation failed.', kind='proxy_connection_failure' if route else 'network_error', retryable=True)
            except (self.httpx.TimeoutException, TimeoutError):
                failure = s.WorkerApiError('Response timed out.', kind='response_timeout', retryable=True)
            except self.httpx.RequestError:
                failure = s.WorkerApiError('Transport failed.', kind='network_error', retryable=True)
            except s.WorkerApiError as exc:
                failure = exc
            finally:
                if task:
                    self.gate.network_tasks.discard(task); self.gate.parent.network_tasks.discard(task)
                if started:
                    elapsed = time.monotonic()-begin
                    self.metrics.end(self.name, operation, elapsed, success=success, records=records,
                        kind=failure.kind if failure else None, code=failure.code if failure else status,
                        proxy=route.label if route else None, size=size)
                    if self.gate.stats:
                        self.gate.stats.request_finished(success=success, latency=elapsed,
                            kind=failure.kind if failure else None, code=failure.code if failure else status,
                            retry=attempt > 0, cancelled=cancelled)
                    self.gate.observe(elapsed, self.pool)
                if route: route.in_use -= 1
                if response: await response.aclose()
                if acquired: self.connection_slots[key].release()
                if proxy_acquired: proxy_slot.release()
                self.release_client(route)
            failure.backend_failure = self.name+'_'+failure.kind
            failure.retry_count = attempt
            if failure.kind == 'backend_paused':
                continue
            if failure.kind == 'access_denied':
                # A restriction must never trigger a different backend or proxy.
                self.gate.blocked_reason = str(failure)
                raise failure
            if route and failure.kind in ('proxy_authentication_failure', 'proxy_not_allocated',
                    'proxy_connection_failure', 'proxy_timeout', 'proxy_temporarily_unavailable'):
                self.pool.failed(route, failure.kind)
                select_route = True
            if failure.code == 404 and not extra_404:
                extra_404 = True
            elif not failure.retryable or attempt+1 >= attempts:
                raise failure
            delay = max(retry_after or 0., min(60., 2.**(attempt+1))+random.random())
            if failure.kind == 'proxy_concurrency_limited':
                self.gate.parent.penalize(delay)
            try:
                await self.retry_wait(delay)
            except BackendPaused:
                pass
            attempt += 1
        raise failure

    async def lookup_profile(self, username):
        profile = await super().lookup_profile(username)
        profile.metadata['backend'] = self.name
        return profile

    async def save_raw(self, username, name, payload):
        await super().save_raw(username, self.name+'_'+name, payload)

    async def fetch_page(self, profile, list_name, cursor, page_number):
        params = {'Uid': profile.uid, 'minCursor': cursor}
        if self.name == 'direct':
            if not profile.sec_uid:
                resolved = await self.lookup_profile(profile.username)
                if resolved.uid != profile.uid:
                    raise s.InvalidWorkerResponse('Direct resolved a different numeric UID; pagination stopped.')
                profile.sec_uid = resolved.sec_uid
            params.update(secUid=profile.sec_uid, _expected_count=profile.follower_count if list_name == 'followers' else profile.following_count)
        payload = await self.request_json(list_name, params)
        await self.save_raw(profile.username, list_name+f'_{page_number:06d}', payload)
        batch = s.parse_list_page(payload, list_name)
        batch.retry_count = self.request_retries.get()
        return batch


class HybridClient:
    def __init__(self, gate, pool, *, settings=None, raw_directory=None, avatar_directory=None, client_factory=None):
        self.gate, self.pool, self.settings = gate, pool, settings or {}
        self.mode = self.settings.get('backend_mode', 'hybrid')
        self.metrics = getattr(gate.stats, 'backends', None) or BackendMetrics()
        if gate.stats: gate.stats.backends = self.metrics
        self.backends = {name: ApiBackend(gate, pool, name, self.metrics, settings=self.settings,
                           raw_directory=raw_directory, client_factory=client_factory)
                         for name in ('worker', 'direct') if self.mode in ('hybrid', name)}
        # Images have a separate cookie-free client. Direct session credentials
        # never reach the Worker API or image CDN.
        self.assets = s.WorkerApiClient(gate, pool, settings=settings, avatar_directory=avatar_directory,
                                       client_factory=client_factory)
        self.avatars = self.assets.avatars
        self.inflight_profiles = {}
        self.profile_waiters = {}
        self.route_sequence = 0
        self.bootstrap_mode = False
        self.redactor = self.assets.redactor
        self.metrics.pool_provider = lambda: {name: pool_snapshot(backend.clients.values()) for name, backend in self.backends.items()}
        self.rate_limits = getattr(gate.stats, 'rate_limits', None) or RateLimitController(self.metrics, self.settings)
        if gate.stats:
            gate.stats.rate_limits = self.rate_limits
        self.rate_limits.attach(gate, self.backends)

    @property
    def avatar_backfill_task(self): return self.assets.avatar_backfill_task
    def start_avatar_backfill(self, root): self.assets.start_avatar_backfill(root)

    async def __aenter__(self):
        try:
            for backend in self.backends.values(): await backend.__aenter__()
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, *args):
        for task in self.inflight_profiles.values(): task.cancel()
        await asyncio.gather(*self.inflight_profiles.values(), return_exceptions=True)
        await self.assets.__aexit__(*args)
        for backend in self.backends.values(): await backend.__aexit__(*args)
        await self.rate_limits.detach()

    def choose(self, exclude=None):
        with self.metrics.stage('backend_routing'):
            available = [name for name, backend in self.backends.items()
                         if name != exclude and not backend.gate.disabled and not backend.gate.blocked_reason]
            if not available:
                raise s.WorkerApiError('No available backend remains.', kind='backend_unavailable')
            if self.rate_limits.direct_state == 'COOLDOWN' and 'worker' in available:
                available = [name for name in available if name != 'direct']
            if len(available) == 1: return available[0]
            self.route_sequence += 1
            # One in twenty assignments explores the less-used path, preventing
            # a slow initial sample from starving a recovered backend indefinitely.
            if self.route_sequence % 20 == 0:
                return min(available, key=lambda n: self.metrics.totals[n]['requests_total'])
            weights = [self.metrics.score(n)/(1+self.metrics.active[n]/self.backends[n].gate.ceiling) for n in available]
            return random.choices(available, weights=weights, k=1)[0]

    @staticmethod
    def can_failover(error):
        return isinstance(error, s.WorkerApiError) and error.kind in {
            'network_error', 'network_timeout', 'response_timeout', 'temporary_server_failure',
            'invalid_response', 'empty_response', 'backend_unavailable', 'proxy_connection_failure',
            'proxy_timeout', 'proxy_temporarily_unavailable', 'proxy_authentication_failure', 'proxy_not_allocated'}

    async def lookup_profile(self, username):
        key = username.casefold()
        async def lookup():
            first = self.choose()
            try:
                return await self.backends[first].lookup_profile(username)
            except s.WorkerApiError as exc:
                if not self.can_failover(exc) or self.mode != 'hybrid': raise
                try:
                    other = self.choose(exclude=first)
                except s.WorkerApiError:
                    raise exc
                self.metrics.events['backend_failovers'] += 1
                if first == 'worker' and self.rate_limits.worker_disabled:
                    self.metrics.events['findtik_jobs_moved_to_direct'] += 1
                if not self.settings.get('race_failed_requests', False) or self.backends[first].gate.disabled:
                    return await self.backends[other].lookup_profile(username)
                tasks = {asyncio.create_task(self.backends[n].lookup_profile(username)) for n in (first, other)}
                try:
                    while tasks:
                        finished, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                        last = exc
                        winner = None
                        for task in finished:
                            try: result = task.result()
                            except s.WorkerApiError as error: last = error
                            else: winner = result
                        if winner is not None: return winner
                    raise last
                finally:
                    for task in tasks: task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        task = self.inflight_profiles.get(key)
        if task is None:
            task = asyncio.create_task(lookup(), name='coalesced-profile')
            self.inflight_profiles[key] = task
        self.profile_waiters[key] = self.profile_waiters.get(key, 0) + 1
        try:
            return await asyncio.shield(task)
        except s.PublicProfileRequired as exc:
            if self.avatars is not None:
                metadata = getattr(exc, 'profile_metadata', {})
                await self.avatars.ensure(metadata.get('uid', ''), username, metadata.get('avatar_url'))
            raise
        finally:
            self.profile_waiters[key] -= 1
            if not self.profile_waiters[key]:
                if not task.done():
                    task.cancel(); await asyncio.gather(task, return_exceptions=True)
                self.profile_waiters.pop(key, None)
                self.inflight_profiles.pop(key, None)

    async def prepare_chain(self, store, result, cursor):
        owner = result.backend or ('worker' if result.batches else '')
        if not owner: owner = self.choose()
        if owner not in self.backends or self.backends[owner].gate.disabled:
            owner = self.choose(exclude=owner)
            await s.disk_call(store.restart_chain, result, owner)
            self.metrics.events['cursor_chain_restarts'] += 1
            cursor = '0'
        result.backend = owner
        await s.disk_call(store.save_checkpoint, result.list_name,
                         {'result': asdict(result), 'next_cursor': cursor, 'duplicate_only_pages': 0})
        return cursor

    async def fetch_chain_page(self, store, result, profile, list_name, cursor):
        owner = result.backend
        try:
            return await self.backends[owner].fetch_page(profile, list_name, cursor, result.batches+1), cursor, False
        except s.WorkerApiError as exc:
            result.backend_failure = getattr(exc, 'backend_failure', owner+'_'+exc.kind)
            disabled_worker = owner == 'worker' and self.rate_limits.worker_disabled
            if self.mode != 'hybrid' or not self.can_failover(exc) or (result.cursor_chain_restarts and not disabled_worker):
                raise
            try:
                other = self.choose(exclude=owner)
            except s.WorkerApiError:
                raise exc
            # No cross-backend cursor compatibility has been verified. Commit the
            # ownership change and zero cursor BEFORE making the replacement call.
            await s.disk_call(store.restart_chain, result, other)
            self.metrics.events.update(backend_failovers=1, backend_switches=1, cursor_chain_restarts=1)
            if disabled_worker:
                self.metrics.events['findtik_jobs_moved_to_direct'] += 1
            batch = await self.backends[other].fetch_page(profile, list_name, '0', result.batches+1)
            return batch, '0', True


def create_client(gate, pool, **kwargs):
    settings = kwargs.get('settings') or {}
    if settings.get('backend_mode', 'worker') == 'worker':
        client = s.WorkerApiClient(gate, pool, **kwargs)
        async def prepare_chain(store, result, cursor):
            # Worker-only resumes can follow an earlier Direct execution. Its
            # opaque cursor must never be sent to the Worker endpoint.
            if result.backend and result.backend != 'worker':
                await s.disk_call(store.restart_chain, result, 'worker')
                client.metrics.events['cursor_chain_restarts'] += 1
                cursor = '0'
            result.backend = 'worker'
            await s.disk_call(store.save_checkpoint, result.list_name,
                             {'result': asdict(result), 'next_cursor': cursor, 'duplicate_only_pages': 0})
            return cursor
        async def fetch_chain_page(store, result, profile, list_name, cursor):
            return await client.fetch_page(profile, list_name, cursor, result.batches+1), cursor, False
        client.prepare_chain, client.fetch_chain_page = prepare_chain, fetch_chain_page
        return client
    return HybridClient(gate, pool, **kwargs)


def edit_backend_config(config):
    result = dict(config)
    s.console('Backend mode: [1] Worker  [2] Direct  [3] Hybrid')
    result['backend_mode'] = {'1': 'worker', '2': 'direct', '3': 'hybrid'}[s.ask_choice('Backend: ', {'1', '2', '3'})]
    for name in ('worker_backend_max_connections', 'direct_backend_max_connections',
                 'worker_backend_keepalive_connections', 'direct_backend_keepalive_connections',
                 'connections_per_proxy', 'direct_initial_concurrency'):
        result[name] = s.ask_number(f'{name} (0 = scales with workers)', result.get(name, 0), integer=True)
    result['direct_page_size'] = s.ask_number('Direct list page size', result.get('direct_page_size', 35), integer=True, minimum=1, maximum=1000)
    result['direct_session_file'] = input('Direct session JSON path (blank = guest, cookies stay private): ').strip().strip('"')
    result['race_failed_requests'] = s.ask_bool('Race failed profile requests? [Y/N, default N]: ', False)
    validate(result)
    return result
