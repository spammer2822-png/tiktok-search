"""Speed-path correctness: lossless data, ownership and repeated interruption."""
import asyncio
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import report_generator as r
import tiktok_worker_scanner as s
from scan_runtime import DiskLane, runtime_ceiling
from test_async_scanner import member
from test_configured_scanner import job

class DataTests(unittest.TestCase):
    def test_report_delta_roundtrip_and_closed_database(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);cfg=s.default_scan_config()
            state=s.DurableScanState(root,{'input_imported':True},root,cfg)
            state.add_jobs([job('one'),job('two')])
            state.discover('seed','followers',[{'username':'two','id':'2','display_name':'</script>🌈','private_account':False}])
            state.snapshot()
            expected=r.build_data(root,s.REDACTOR.clean)['rows']
            path=r.generate_report(root,clean=s.REDACTOR.clean,emit=lambda _:None)
            text=path.read_text()
            metadata=json.loads(re.search(r'<script[^>]+id="report-data"[^>]*>(.*?)</script>',text,re.S)[1])
            raw=re.search(r'<script[^>]+id="report-rows"[^>]*>(.*?)</script>',text,re.S)[1]
            actual=[]
            for line in raw.strip().splitlines():
                delta=json.loads(line);row=dict(metadata['row_defaults'])
                for k in range(0,len(delta),2):row[metadata['row_fields'][delta[k]]]=delta[k+1]
                actual.append(row)
            self.assertEqual(actual,expected)
            with r.open_database(root/'state.sqlite3') as db:self.assertEqual(db.execute('SELECT 1').fetchone(),(1,))
            with self.assertRaises(sqlite3.ProgrammingError):db.execute('SELECT 1')
            state.close()

    def test_encoded_export_matches_decoded_unicode_and_duplicate_identity(self):
        with tempfile.TemporaryDirectory() as d,s.UserStore(Path(d)/'pages.sqlite3') as store:
            value=member('wanted','42');value['signature']='🌈 <script> \\ "\n';value['extra']={'password':'hidden'}
            with patch.object(s,'KEEP_RAW_MEMBER_DATA',True):
                store.save_batch('followers',[value,member('wanted','42')],target_username='wanted')
            self.assertEqual([json.loads(x) for x in store.iter_encoded_members('followers')],list(store.iter_members('followers')))
            self.assertEqual(store.count('followers'),1)

    def test_pending_resume_skips_nonexistent_exports_but_recovers_real_ones(self):
        from test_persistent_scanner import write_valid
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);state=s.DurableScanState(root,{'input_imported':True},root)
            state.add_jobs([job('one'),job('two')]);write_valid(root,'one')
            success=s.SuccessRecorder(root/'sucess_find.json',target_user=s.TARGET_USER,input_path=root/'input.json',started_at_utc=s.utc_iso())
            original=s.inspect_reusable_export;seen=[]
            def check(path,**kwargs):seen.append(path.name);return original(path,**kwargs)
            with patch.object(s,'inspect_reusable_export',side_effect=check):state.recover(success)
            self.assertEqual(len(seen),1);self.assertEqual(state.summary()['completed_overall'],1)
            self.assertEqual(state.claim().username,'two');state.close()

    def test_redaction_fast_paths(self):
        redactor=s.CredentialRedactor([s.ProxyConfig('p.webshare.io',80,'login-secret','pass-secret')])
        data={'normal':'plain @person', 'unusual':'https://login-secret:pass-secret@host/a',
              'nested':[{'Proxy-Authorization':'token','error':'pass-secret'}]}
        out=redactor.clean(data);self.assertEqual(out['normal'],data['normal'])
        self.assertNotIn('pass-secret',json.dumps(out));self.assertNotIn('token',json.dumps(out))

    def test_workers_not_capped_at_64(self):
        self.assertGreater(runtime_ceiling(1000),64)
        self.assertEqual(runtime_ceiling(1000,{'max_connections':23}),23)

class OwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_dns_tls_and_disconnect_retry_then_exhaustion(self):
        import httpx
        import socket
        import ssl
        from test_async_scanner import PROFILE
        for cause in (socket.gaierror(-2, 'Synthetic DNS failure'),
                      ssl.SSLError('Synthetic TLS negotiation failure'),
                      ConnectionResetError('Synthetic network disconnect')):
            for recover in (True, False):
                with self.subTest(cause=type(cause).__name__, recover=recover):
                    calls=[]
                    async def handler(request):
                        calls.append(request)
                        if recover and len(calls)==2:
                            return httpx.Response(200,json=PROFILE)
                        raise httpx.ConnectError('Synthetic connect failure',request=request) from cause
                    def factory(route,options):
                        return httpx.AsyncClient(transport=httpx.MockTransport(handler),event_hooks=options.get('event_hooks',{}))
                    gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=2,initial=2,adaptive=False)
                    client=s.WorkerApiClient(gate,s.ProxyPool([]),client_factory=factory,settings={'retry_attempts':2})
                    with patch.object(s,'console'),patch.object(client,'retry_wait',return_value=None):
                        async with client:
                            if recover:
                                self.assertEqual((await client.request_json('profile',{'username':'roblox'}))['status'],'ok')
                            else:
                                with self.assertRaises(s.WorkerApiError) as caught:
                                    await client.request_json('profile',{'username':'roblox'})
                                self.assertEqual(caught.exception.kind,'network_error')
                    self.assertEqual(len(calls),2)
                    self.assertFalse(gate.stop_event.is_set())

    async def test_independent_io_overlaps_and_repeated_cancel_waits_for_commit(self):
        both=threading.Barrier(2);release=threading.Event();saved=[]
        def write(i):both.wait(timeout=2);release.wait(2);saved.append(i)
        async with DiskLane(io_workers=2) as lane:
            first=asyncio.create_task(lane.independent(write,1));second=asyncio.create_task(lane.independent(write,2))
            await asyncio.sleep(.05)
            first.cancel();await asyncio.sleep(.01);first.cancel();await asyncio.sleep(.01)
            self.assertFalse(first.done());release.set()
            result=await asyncio.gather(first,second,return_exceptions=True)
            self.assertIsInstance(result[0],asyncio.CancelledError);self.assertEqual(sorted(saved),[1,2])

@unittest.skipIf(os.name == 'nt', 'This signal-injection harness requires POSIX; test Ctrl+C in a Windows console.')
class RepeatedSignalTests(unittest.TestCase):
    def test_repeated_sigint_main_saves_one_report(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            script="""import sys,time
from pathlib import Path
sys.path.insert(0,'tests')
import report_generator as r
from test_persistent_scanner import child_sigterm
old=r.generate_report
def slow(*a,**k):
 print('SAVING_REPORT',flush=True);time.sleep(.5);return old(*a,**k)
r.generate_report=slow
raise SystemExit(child_sigterm(Path(sys.argv[1])))
"""
            child=subprocess.Popen([sys.executable,'-u','-c',script,str(root)],cwd=Path(s.__file__).parent,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                self.assertEqual(child.stdout.readline().strip(),'READY')
                child.send_signal(signal.SIGINT)
                self.assertEqual(child.stdout.readline().strip(),'SAVING_REPORT')
                for _ in range(4):child.send_signal(signal.SIGINT);time.sleep(.03)
                out,err=child.communicate(timeout=10)
                self.assertEqual(child.returncode,130,err)
                checkpoint=next(root.glob('*/scan_state.json'));doc=json.loads(checkpoint.read_text())
                self.assertEqual(doc['summary']['pending_profiles'],1)
                self.assertEqual(len(list(checkpoint.parent.glob('report_*.html'))),1)
            finally:
                if child.poll() is None:child.kill();child.communicate()
                child.stdout.close();child.stderr.close()

if __name__=='__main__':unittest.main()
