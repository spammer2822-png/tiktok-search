"""Recovery, bounded runtime, rolling metrics and immediate global stop contracts."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import httpx
import tiktok_worker_scanner as s
from scan_runtime import DiskLane, RetryQueue, runtime_ceiling
from scan_statistics import ScanStatistics
from test_async_scanner import PROFILE, page, member, AsyncFakeClient, profile
from test_configured_scanner import ConfigurationTests, job, response_profile
from test_persistent_scanner import write_valid


class MetricsTests(unittest.TestCase):
    def test_report_markers_in_metadata_do_not_break_generation(self):
        import report_generator
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);s.atomic_write_json(root/'scan_config.json',{'target_username':'__REPORT_ROWS__'})
            path=report_generator.generate_report(root,emit=lambda _:None)
            self.assertIsNotNone(path)
            self.assertIn('"target":"__REPORT_ROWS__"',path.read_text())

    def test_rolling_window_eta_unique_profiles_and_request_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            now=[0.0]
            stats=ScanStatistics(Path(directory),10000,clock=lambda:now[0],wall=lambda:1000000+now[0])
            summary={'remaining_profiles':100,'total_profiles':106}
            for i in range(5):
                stats.request_started()
                stats.request_finished(success=True,latency=.2)
                result=s.error_profile_result(job('a'+str(i)),status='complete',started_at_utc='',error='')
                result.complete=True
                stats.account_finished(result);stats.account_finished(result)
            self.assertIsNone(stats.snapshot(summary)['estimated_seconds_remaining'])
            now[0]=60
            doc=stats.snapshot(summary)
            self.assertEqual(doc['accounts_last_5_minutes'],5)
            self.assertEqual(doc['estimated_seconds_remaining'],1200)
            stats.request_started(retry=True)
            self.assertEqual(stats.snapshot(summary)['active_requests'],1)
            stats.request_finished(success=False,latency=.5,kind='response_timeout',code=503)
            stats.request_started(retry=True)
            stats.request_finished(success=True,latency=.3,retry=True)
            failure=s.error_profile_result(job('failed'),status='network_error',started_at_utc='',error='fixture')
            stats.account_finished(failure)
            doc=stats.snapshot(summary)
            self.assertEqual(doc['requests_total'],7)
            self.assertEqual(doc['requests_successful'],6)
            self.assertEqual(doc['retry_attempts'],2)
            self.assertEqual(doc['recovered_after_retry'],1)
            self.assertEqual(doc['network_timeouts'],1)
            self.assertEqual(doc['http_5xx_count'],1)
            self.assertEqual(doc['failed_accounts'],1)
            self.assertEqual(doc['active_requests'],0)
            self.assertEqual(doc['peak_in_flight'],1)
            now[0]=301
            doc=stats.snapshot(summary)
            self.assertEqual(doc['accounts_last_5_minutes'],1)
            self.assertEqual(doc['successful_accounts_last_5_minutes'],0)
            self.assertEqual(doc['requests_last_5_minutes'],2)
            self.assertEqual(doc['requests_total'],7)
            self.assertIsNone(doc['estimated_completion_time'])
            stats.stop('stopped_rate_limited','HTTP 429 received from API')
            stats.stop('stopped_error','secondary')
            self.assertEqual(stats.snapshot(summary)['scan_status'],'stopped_rate_limited')
            self.assertLessEqual(len(stats.buckets),301)

    def test_history_append_resume_and_torn_tail_repair(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);stats=ScanStatistics(root,4)
            first=stats.snapshot({});stats.write(first,s.atomic_write_json,history=True)
            history=root/'scan_stats_history.jsonl';old=history.read_bytes()
            with history.open('ab') as output:output.write(b'{"torn":')
            resumed=ScanStatistics(root,4);resumed.write(resumed.snapshot({}),s.atomic_write_json,history=True)
            self.assertTrue(history.read_bytes().startswith(old))
            entries=[json.loads(line) for line in history.read_text().splitlines()]
            self.assertEqual(len(entries),2);self.assertNotEqual(entries[0]['session_id'],entries[1]['session_id'])
            self.assertEqual(json.loads((root/'scan_stats.json').read_text())['session_id'],resumed.session_id)

    def test_connection_budget_and_legacy_config(self):
        for count in (4,64,400,5000,10000):
            self.assertLessEqual(runtime_ceiling(count),count)
            self.assertGreaterEqual(runtime_ceiling(count), min(count, 64))
        self.assertLessEqual(runtime_ceiling(10000,{'max_connections':128}),128)
        config=s.default_scan_config();config.pop('max_connections');config['workers']=10000
        s.validate_scan_config(config)
        config['max_connections']=10000
        s.validate_scan_config(config)
        config['max_connections']=10001
        s.validate_scan_config(config)
        config['max_connections']=-1
        with self.assertRaises(s.ExporterError):s.validate_scan_config(config)


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_periodic_stats_write_and_five_minute_history(self):
        with tempfile.TemporaryDirectory() as directory:
            now=[0.0];stats=ScanStatistics(Path(directory),8,clock=lambda:now[0],wall=lambda:1000000+now[0])
            class State:
                def summary(self):return {'remaining_profiles':100}
                def snapshot(self):pass
            async def disk(function,*args,**kwargs):return function(*args,**kwargs)
            writes=[]
            def atomic(path,doc):writes.append((now[0],doc));s.atomic_write_json(path,doc)
            async def tick(coroutine,timeout):
                coroutine.close();now[0]+=15
                if now[0]>600:stats.done.set();return
                stats.request_started();stats.request_finished(success=True,latency=.1)
                raise asyncio.TimeoutError
            output=[]
            with patch('scan_statistics.asyncio.wait_for',side_effect=tick):
                await stats.report_loop(State(),disk,atomic,output.append)
            entries=[json.loads(line) for line in (Path(directory)/'scan_stats_history.jsonl').read_text().splitlines()]
            self.assertEqual(len(writes),40);self.assertEqual(len(entries),2)
            self.assertEqual([x['elapsed_seconds'] for x in entries],[300,600])
            self.assertEqual(entries[-1]['requests_total'],40)
            self.assertEqual(entries[-1]['requests_last_5_minutes'],21)  # 1-second boundary bucket is retained.
            self.assertEqual(sum('5 MINUTE SCAN STATS' in x for x in output),2)

    async def test_writer_backpressure_cancellation_and_event_loop(self):
        started=threading.Event();release=threading.Event();saved=[]
        def commit():started.set();release.wait(2);saved.append('durable')
        async with DiskLane(capacity=2) as writer:
            task=asyncio.create_task(writer.call(commit))
            while not started.is_set():await asyncio.sleep(.002)
            task.cancel();await asyncio.sleep(.02)
            self.assertFalse(task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await task
            self.assertEqual(saved,['durable']);self.assertLessEqual(writer.peak_queued,2)

    async def test_retry_queue_wakes_without_resending_after_stop(self):
        gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=8,initial=8,adaptive=False)
        retry=RetryQueue(gate)
        tasks=[asyncio.create_task(retry.wait(30)) for _ in range(100)]
        await asyncio.sleep(.01)
        with patch.object(s,'console'):gate.stop_rate_limited()
        outcomes=await asyncio.wait_for(asyncio.gather(*tasks,return_exceptions=True),1)
        self.assertTrue(all(isinstance(x,s.WorkerApiError) and x.kind=='rate_limited' for x in outcomes))
        await retry.close();self.assertEqual(gate.active,0);self.assertEqual(retry.heap,[])

    async def test_429_headers_stop_before_reading_slow_body(self):
        class SlowBody(httpx.AsyncByteStream):
            async def __aiter__(self):
                raise AssertionError('429 body must not be awaited')
                yield b''
        calls=[]
        async def handler(request):calls.append(request);return httpx.Response(429,stream=SlowBody())
        gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=4,initial=4,adaptive=False)
        def factory(route,opts):return httpx.AsyncClient(transport=httpx.MockTransport(handler),event_hooks=opts['event_hooks'])
        with patch.object(s,'console'):
            async with s.WorkerApiClient(gate,s.ProxyPool([]),client_factory=factory) as client:
                results=await asyncio.gather(*(client.request_json('profile',{'username':'a'}) for _ in range(100)),return_exceptions=True)
        self.assertEqual(len(calls),1);self.assertTrue(gate.stop_event.is_set())
        self.assertTrue(all(isinstance(r,s.WorkerApiError) for r in results))

    async def test_10000_workers_429_preserves_pending_stats_and_completed_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);cfg=s.default_scan_config();cfg.update(workers=10000)
            state=s.DurableScanState(root,{'input_imported':True},root,cfg)
            state.add_jobs(job('a'+str(i)) for i in range(1000))
            completed=state.claim();state.record(write_valid(root,completed.username))
            success=s.SuccessRecorder(root/s.SUCCESS_FILE_NAME,target_user=s.TARGET_USER,input_path=root/'starting_dataset.json',started_at_utc=s.utc_iso())
            original=s.WorkerApiClient;starts=[];after=[];task_count=[];barrier=asyncio.Event()
            def factory(gate,pool,**kwargs):
                async def handle(request):
                    self.assertTrue((root/'scan_stats.json').exists())
                    if gate.rate_limit_event.is_set():after.append(request)
                    starts.append(str(request.url));number=len(starts)
                    task_count.append(len(asyncio.all_tasks()))
                    if number==8:barrier.set()
                    await barrier.wait()
                    if number==1:return httpx.Response(429)
                    await asyncio.sleep(.05)
                    name=request.url.params.get('username','a')
                    return httpx.Response(200,json=response_profile(name,number+1))
                def clients(route,opts):return httpx.AsyncClient(transport=httpx.MockTransport(handle),event_hooks=opts['event_hooks'])
                return original(gate,pool,client_factory=clients,**kwargs)
            with patch.object(s,'WorkerApiClient',side_effect=factory),patch.object(s,'console'):
                result=await s.run_scan([],output_directory=root,worker_count=10000,pacing=(0,0),success_recorder=success,state_recorder=state,use_resume=True,initial_concurrency=8)
            self.assertTrue(result[2]);self.assertEqual(after,[]);self.assertEqual(len(starts),8)
            self.assertLess(max(task_count),600)
            summary=state.summary();self.assertEqual(summary['remaining_profiles'],999);self.assertEqual(summary['failed_profiles'],0)
            self.assertEqual(summary['completed_overall'],1)
            stats=json.loads((root/'scan_stats.json').read_text())
            self.assertEqual(stats['scan_status'],'stopped_rate_limited');self.assertEqual(stats['http_429_count'],1)
            self.assertEqual(stats['requests_total'],8);self.assertEqual(stats['pending_accounts'],999)
            self.assertIsNone(stats['estimated_completion_time']);self.assertEqual(stats['configured_workers'],10000)
            self.assertEqual(stats['connection_ceiling'],runtime_ceiling(10000,cfg))
            self.assertEqual(len((root/'scan_stats_history.jsonl').read_text().splitlines()),1)
            state.close()

    async def test_client_pool_count_bounded_with_many_proxy_routes(self):
        gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=4,initial=4,adaptive=False)
        pool=s.ProxyPool([s.ProxyConfig('proxy'+str(i)+'.example',8000,'user','password') for i in range(100)])
        trust=[]
        def factory(route,opts):
            trust.append(opts['verify'])
            return httpx.AsyncClient(transport=httpx.MockTransport(lambda request:httpx.Response(200,json=PROFILE)))
        with patch.object(s,'console'):
            async with s.WorkerApiClient(gate,pool,client_factory=factory) as client:
                for _ in range(150):
                    await client.request_json('profile',{'username':'a'})
                    self.assertLessEqual(len(client.clients),4)
                self.assertEqual(sum(client._client_users.values()),0)
                self.assertEqual(len({id(context) for context in trust}),1)
                self.assertTrue(trust[0].check_hostname)

    async def test_retry_partial_preserves_cursor_complete_list_and_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);cfg=s.default_scan_config();cfg['target_username']='wanted'
            state=s.DurableScanState(root,{'input_imported':True},root,cfg);state.add_jobs([job('seed')])
            success=s.SuccessRecorder(root/s.SUCCESS_FILE_NAME,target_user='wanted',input_path=root/'input.json',started_at_utc=s.utc_iso())
            first=AsyncFakeClient(profile('seed','2','0'),{'followers':[page([member('wanted','55')],True,'opaque+/='),s.WorkerApiError('network',kind='network_error')],'following':[page()]})
            state.claim()
            with patch.object(s,'console'):
                result=await s.process_profile(first,job('seed'),output_directory=root,success_recorder=success,stop_event=asyncio.Event(),use_resume=True,store_directory=root/'pages')
                state.record(result);self.assertEqual(state.retry_failed(),1)
                second=AsyncFakeClient(first.p,{'followers':[page([member('another','56')])]})
                second.lookup_profile=lambda _: (_ for _ in ()).throw(AssertionError('saved profile must be reused'))
                resumed=await s.process_profile(second,state.claim(),output_directory=root,success_recorder=success,stop_event=asyncio.Event(),use_resume=True,store_directory=root/'pages')
                state.record(resumed)
            self.assertEqual(second.calls,[('followers','opaque+/=',2)])
            self.assertTrue(resumed.complete);self.assertEqual(success.found_lists_for('seed'),['followers'])
            self.assertEqual(state.connection.execute('SELECT COUNT(*) FROM retry_history').fetchone()[0],1)
            self.assertEqual(state.summary()['completed_overall'],1);state.close()


class ResumeSelectionTests(ConfigurationTests):
    def test_bootstrap_429_exits_as_rate_limit_and_preserves_cursor(self):
        async def handler(request):
            if request.url.params.get('username'):
                return httpx.Response(200,json=response_profile('seed',9,'2','0'))
            if request.url.params.get('minCursor')=='0':
                return httpx.Response(200,json=page([member('a','1')],True,'opaque-next'))
            return httpx.Response(429)
        code,out,_=self.run_main(['0','2','seed','wanted','0','0','1','4','0','0','n','1'],handler)
        self.assertEqual(code,2);self.assertEqual(len(self.calls),3)
        root=self.base/'wanted'
        stats=json.loads((root/'scan_stats.json').read_text())
        self.assertEqual(stats['scan_status'],'stopped_rate_limited');self.assertEqual(stats['http_429_count'],1)
        with s.UserStore(root/'bootstrap'/'pages'/'seed.sqlite3') as store:
            self.assertEqual(store.checkpoint('followers')['next_cursor'],'opaque-next')

    def prepare_failed(self):
        root,cfg,meta=self.create_saved(('a','b','c','d','e'))
        state=s.DurableScanState(root,meta,root,cfg)
        for status in ('network_timeout','network_error','partial','http_error'):
            entry=state.claim();state.record(s.error_profile_result(entry,status=status,started_at_utc=s.utc_iso(),error='fixture'))
        entry=state.claim();state.record(write_valid(root,entry.username))
        state.finalize(scan_complete=True,fatal_error=None);state.close()
        return root

    def test_resume_yes_only_retries_selected_statuses(self):
        root=self.prepare_failed()
        async def handler(request):
            name=request.url.params.get('username')
            if name:
                self.assertIn(name,('a','b','c'))
                return httpx.Response(200,json=response_profile(name,{'a':1,'b':2,'c':3}[name]))
            return httpx.Response(200,json=page())
        code,out,prompts=self.run_main(['1','1','1','y'],handler)
        self.assertEqual(code,0);self.assertEqual(len(self.calls),9)
        self.assertTrue(any('Retry failed profiles?' in p for p in prompts))
        summary=json.loads((root/'scan_state.json').read_text())['summary']
        self.assertEqual(summary['completed_overall'],4);self.assertEqual(summary['statuses']['http_error'],1)
        self.assertEqual(len(list(self.base.iterdir())),1)

    def test_resume_no_keeps_failed_results_untouched(self):
        root=self.prepare_failed()
        async def forbidden(request):raise AssertionError('No pending jobs: no network permitted')
        code,_,_=self.run_main(['1','1','1','n'],forbidden)
        self.assertEqual(code,0);self.assertEqual(self.calls,[])
        statuses=json.loads((root/'scan_state.json').read_text())['summary']['statuses']
        for key in ('network_timeout','network_error','partial','http_error','completed'):self.assertEqual(statuses[key],1)


class RetryCrashTests(unittest.TestCase):
    def test_forcekill_after_requeue_and_claim_is_recoverable(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);state=s.DurableScanState(root,{'input_imported':True},root)
            state.add_jobs([job('a'),job('b')]);entry=state.claim()
            state.record(s.error_profile_result(entry,status='network_timeout',started_at_utc='',error='first failure'))
            state.close()
            script="""import sys,time
from pathlib import Path
import tiktok_worker_scanner as s
p=Path(sys.argv[1]);state=s.DurableScanState(p,{'input_imported':True},p)
state.retry_failed();state.claim();print('committed',flush=True);time.sleep(60)
"""
            process=subprocess.Popen([sys.executable,'-u','-c',script,str(root)],cwd=Path(s.__file__).parent,stdout=subprocess.PIPE,text=True)
            try:
                self.assertEqual(process.stdout.readline().strip(),'committed')
                process.kill();process.wait(timeout=5)
            finally:
                if process.poll() is None:process.kill();process.wait(timeout=5)
                process.stdout.close()
            state=s.DurableScanState(root,{'input_imported':True},root)
            success=s.SuccessRecorder(root/s.SUCCESS_FILE_NAME,target_user=s.TARGET_USER,input_path=root/'input.json',started_at_utc=s.utc_iso())
            state.recover(success)
            self.assertEqual(state.summary()['pending_profiles'],2)
            saved=state.connection.execute('SELECT previous_status,previous_result FROM retry_history').fetchone()
            self.assertEqual(saved[0],'network_timeout');self.assertIn('first failure',saved[1])
            state.close()


if __name__=='__main__':unittest.main()
