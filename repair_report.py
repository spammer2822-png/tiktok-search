"""Cache missing saved avatars and rebuild an existing report without rescanning.

Windows: py -3.11 repair_report.py
Or:      py -3.11 repair_report.py "C:\\Users\\vailo\\Downloads\\TiktokSearch_Logs\\amlie7951"
"""
from __future__ import annotations
import argparse
import asyncio
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import tiktok_worker_scanner as s
from report_generator import generate_report
from scan_runtime import DiskLane, ShutdownGuard, runtime_ceiling


async def repair_avatars(root: Path, config: dict, pool: s.ProxyPool) -> dict:
    stop = asyncio.Event()
    delay = config['request_delay']
    ceiling = min(config.get('avatar_workers', 0) or config['workers'], runtime_ceiling(config['workers'], config))
    gate = s.AsyncRequestGate(delay['minimum_seconds'], delay['maximum_seconds'], stop,
                             ceiling=ceiling, initial=ceiling, adaptive=False)
    async with DiskLane() as lane:
        token = s._DISK_LANE.set(lane)
        try:
            async with s.WorkerApiClient(gate, pool, settings=config, avatar_directory=root) as client:
                return await client.cache_saved_avatars(root)
        finally:
            stop.set()
            s._DISK_LANE.reset(token)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Repair saved report avatars; no profile or list rescans.')
    parser.add_argument('folder', nargs='?', help='Existing search folder containing scan_config.json')
    args = parser.parse_args(argv)
    try:
        value = args.folder or input('Existing search folder: ').strip().strip('"')
        root = Path(value).expanduser().resolve()
        if not (root/'state.sqlite3').is_file():
            raise s.ExporterError('Choose the existing search folder containing state.sqlite3 and scan_config.json.')
        config = s.read_scan_config(root)
        with s.ScanLock(root):
            proxies = s.load_proxy_configs(Path(config['proxy_file'])) if config['use_proxies'] else []
            s.REDACTOR = s.CredentialRedactor(proxies)
            if config['proxy_only'] and not proxies:
                raise s.ExporterError('Proxy-only mode is enabled but no configured proxies could be loaded.')
            pool = s.ProxyPool(proxies, proxy_only=config['proxy_only'])
            with s.RunLog(root), ShutdownGuard(s.console) as guard:
                s.console('[AVATAR] Repairing saved profile pictures. Scan progress and account results are preserved.')
                interrupted = False
                try:
                    with asyncio.Runner() as runner:
                        async def run():
                            task = asyncio.current_task()
                            guard.cancel = lambda: runner.get_loop().call_soon_threadsafe(task.cancel)
                            try:
                                return await repair_avatars(root, config, pool)
                            finally:
                                guard.cancel = None
                        counts = runner.run(run())
                    s.console(f"[AVATAR] {counts['available']:,} saved image references available locally; "
                              f"{counts['unavailable']:,} unavailable from their saved URLs.")
                except (KeyboardInterrupt, asyncio.CancelledError):
                    interrupted = True
                    s.console('[AVATAR] Repair stopped; downloaded pictures are saved. Run repair again to continue.')
                path = generate_report(root, clean=s.REDACTOR.clean, emit=s.console)
                return 130 if interrupted else 0 if path else 2
    except (s.ExporterError, OSError, ValueError, EOFError) as exc:
        s.console(f'[ERROR] Report repair failed: {type(exc).__name__}. Check the search folder and proxy configuration.', error=True)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
