"""Create the six-section offline avatar QA report without external requests."""
import asyncio
import hashlib
import sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
import tiktok_worker_scanner as s
import report_generator as report
import repair_report
from test_report_avatar_fix import fixture
from test_avatar_update import picture

async def main(root):
    root.mkdir(parents=True, exist_ok=True)
    cfg = fixture(root)
    original = s.WorkerApiClient
    calls = []
    def factory(gate, pool, **kwargs):
        def transport(route, options):
            async def handle(request):
                assert request.url.host == 'p16-sign.tiktokcdn.com'
                calls.append(request.url)
                return httpx.Response(200, content=picture(), headers={'Content-Type':'image/png'})
            return httpx.AsyncClient(transport=httpx.MockTransport(handle), event_hooks=options.get('event_hooks', {}))
        return original(gate, pool, client_factory=transport, **kwargs)
    names = ('state.sqlite3','scan_state.json','scan_config.json')
    before = {n: hashlib.sha256((root/n).read_bytes()).hexdigest() for n in names}
    with patch.object(s,'WorkerApiClient', side_effect=factory):
        await repair_report.repair_avatars(root, cfg, s.ProxyPool([]))
        await repair_report.repair_avatars(root, cfg, s.ProxyPool([]))
    assert len(calls) == 5
    assert before == {n: hashlib.sha256((root/n).read_bytes()).hexdigest() for n in names}
    assert report.generate_report(root, emit=lambda _: None)
    print('PASS: five images, one placeholder, idempotent repair, unchanged scan state')
asyncio.run(main(Path(sys.argv[1]).resolve()))
