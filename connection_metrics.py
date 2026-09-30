"""HTTPX/httpcore trace counters, with no URL/header/credential capture."""
import time
class RequestTrace:
    def __init__(self, metrics, backend):
        self.metrics, self.backend = metrics, backend
        self.started = {}; self.new_connection = self.sent = False
    async def __call__(self, event, info):
        now = time.monotonic(); key, _, phase = event.rpartition('.')
        category = 'connect_tcp' if 'connect_tcp' in key else 'tls_handshake' if 'start_tls' in key else None
        if category:
            if phase == 'started':
                self.started[key] = now
                self.metrics.add(self.backend, **{category+'_attempts': 1})
            elif phase in ('complete', 'failed'):
                self.metrics.add(self.backend, **{category+'_seconds': now-self.started.pop(key, now), category+('_successful' if phase == 'complete' else '_failed'): 1})
                if category == 'connect_tcp' and phase == 'complete': self.new_connection = True
        if event.endswith('send_request_headers.started') and not self.sent:
            self.sent = True
            self.metrics.add(self.backend, **{'new_connection_requests' if self.new_connection else 'reused_connection_requests': 1})
def pool_snapshot(clients):
    active = idle = 0; observed = False; seen = set()
    for client in clients:
        for transport in [getattr(client, '_transport', None), *getattr(client, '_mounts', {}).values()]:
            pool = getattr(transport, '_pool', None)
            if pool is None or id(pool) in seen: continue
            seen.add(id(pool)); connections = getattr(pool, 'connections', None)
            if connections is None: continue
            observed = True
            for conn in connections:
                if conn.is_closed(): continue
                if conn.is_idle(): idle += 1
                else: active += 1
    return {'active': active if observed else None, 'idle_keepalive': idle if observed else None}
