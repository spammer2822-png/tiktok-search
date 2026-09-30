"""Optional Webshare connectivity check; no TikTok or Worker requests.

Uses Webshare's documented HTTPS diagnostic endpoint and the same HTTPX proxy
configuration as the scanner. One bounded request per route, without retries.
https://apidocs.webshare.io/proxy-connection
"""
from __future__ import annotations
import asyncio
import ipaddress

TEST_URL = 'https://ipv4.webshare.io/'

async def validate_pool(api, pool, config, root, *, client_factory=None):
    spacing = config['request_delay']
    from scan_runtime import runtime_ceiling
    ceiling = runtime_ceiling(config['workers'], config)
    gate = api.AsyncRequestGate(spacing['minimum_seconds'],spacing['maximum_seconds'],
        asyncio.Event(),ceiling=ceiling,initial=ceiling,adaptive=False)
    results = [None]*len(pool.states)
    options = {"client_factory":client_factory} if client_factory is not None else {}
    async with api.WorkerApiClient(gate,pool,settings=config,**options) as client:
        async def check(index,route):
            reason = None
            try:
                async with gate.slot():
                    connection = await client.borrow_client(route)
                    await gate.pace()
                    async with asyncio.timeout(12):
                        # Proxy auth belongs to HTTPX.Proxy, never origin headers.
                        # Streaming bounds diagnostic body size as well as time.
                        async with connection.stream('GET',TEST_URL,timeout=10) as response:
                            if response.status_code == 407:
                                reason = 'proxy_authentication_failure'
                            elif response.status_code != 200:
                                reason = 'diagnostic_http_'+str(response.status_code)
                                if response.status_code == 429:
                                    gate.cooldown(max(2,api.retry_after_seconds(response.headers.get('Retry-After')) or 0))
                            else:
                                body = b''
                                async for chunk in response.aiter_bytes():
                                    body += chunk
                                    if len(body)>256: break
                                try: ipaddress.ip_address(body.decode('ascii').strip())
                                except (ValueError,UnicodeError): reason='invalid_diagnostic_response'
            except client.httpx.ProxyError as exc: reason=client.proxy_exception(exc).kind
            except (client.httpx.TimeoutException,TimeoutError): reason='proxy_timeout'
            except client.httpx.ConnectError: reason='proxy_connection_failure'
            except client.httpx.RequestError: reason='proxy_transport_failure'
            except api.MissingDependency: reason='missing_socks_dependency'
            except Exception: reason='proxy_check_error'
            finally:
                if 'connection' in locals():
                    client.release_client(route)
            valid = reason is None
            route.disabled = not valid
            route.last_failure = reason
            if valid:
                route.success_count += 1
                route.cooldown_until = 0
            result = {'host':route.config.host,'port':route.config.port,'status':'valid' if valid else 'invalid','failure_category':reason}
            results[index] = result
            api.console(f"[{index+1}] CHECKING PROXY {route.config.host}:{route.config.port} - {'VALID' if valid else 'INVALID'}"+(f' ({reason})' if reason else ''))
        # Bound tasks as well as connections, even for very large uploaded lists.
        pending = iter(enumerate(pool.states))
        async def worker():
            for index,route in pending: await check(index,route)
        await asyncio.gather(*(worker() for _ in range(min(ceiling,len(pool.states)))))
    valid = sum(r['status']=='valid' for r in results)
    document = {'validated_at_utc':api.utc_iso(),'total':len(results),'valid':valid,
                'invalid':len(results)-valid,'active_pool_size':valid,'proxies':results}
    await api.disk_call(api.atomic_write_json, root/'proxy_validation.json', document)
    api.console(f"[PROXY CHECK] Total: {len(results)} | Valid: {valid} | Invalid: {len(results)-valid} | Active pool: {valid}")
    return valid
