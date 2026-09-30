"""Independent local components, verified TLS + authenticated CONNECT. No remote API."""
import argparse,asyncio,contextlib,json,os,platform,statistics,sys,tempfile,time
try:
    import resource
except ImportError:
    resource = None
from pathlib import Path
from unittest.mock import patch
p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--result',type=Path,required=True)
a=p.parse_args();sys.path[:0]=[str(a.source.resolve()),str(a.source.resolve()/'tests')]
import tiktok_worker_scanner as s
import httpx
from test_proxy_wire import ProxyWireTests
from test_async_scanner import PROFILE
out={'version':a.source.parent.name,'python':platform.python_version(),'scope':'local only; no upstream service measured'}
root=Path(tempfile.mkdtemp(dir=a.result.parent,prefix='components_'))
raw={'users':[{'uniqueId':f'account{i}','user_id':str(7400000000000000000+i),'nickname':'Synthetic 🌈',
    'avatarThumb':'','signature':'Synthetic bio','privateAccount':False,'verified':'No❌'} for i in range(20)],'hasMore':True,'minCursor':'opaque+/='}
encoded=json.dumps(raw,ensure_ascii=False)
def measure(fn,n):
    start=time.perf_counter()
    for _ in range(n):fn()
    return (time.perf_counter()-start)/n
out['parse_page_mean_seconds']=measure(lambda:json.loads(encoded),20000)
out['serialize_page_mean_seconds']=measure(lambda:json.dumps(raw,ensure_ascii=False,indent=2),2000)
out['clean_page_mean_seconds']=measure(lambda:s.REDACTOR.clean(raw),5000)
out['normalize_page_mean_seconds']=measure(lambda:[s.normalize_member(x) for x in raw['users']],10000)
out['atomic_json_mean_seconds']=measure(lambda:s.atomic_write_json(root/'atomic.json',raw),100)
out['parse_proxy_mean_seconds']=measure(lambda:s.parse_proxy_line('192.0.2.10:8000:fixture-user:fixture-pass'),20000)
with s.UserStore(root/'members.sqlite3') as store:
    seq=[0]
    def page():
        seq[0]+=1
        items=[{**x,'user_id':str(int(x['user_id'])+seq[0]*100)} for x in raw['users']]
        store.save_batch('followers',items)
    out['member_commit_page_mean_seconds']=measure(page,500)
    out['duplicate_page_mean_seconds']=measure(lambda:store.save_batch('following',raw['users']),500)
    out['member_rows']=store.count('followers')

class Wire(ProxyWireTests):
 async def origin_handler(self,reader,writer):
  task=asyncio.current_task();self.handler_tasks.add(task);self.connections+=1
  try:
   while True:
    data=await reader.readuntil(b'\r\n\r\n');self.headers.append(data.decode());await asyncio.sleep(.005)
    body=json.dumps(PROFILE).encode();writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '+str(len(body)).encode()+b'\r\nConnection: keep-alive\r\n\r\n'+body);await writer.drain()
  except (OSError,asyncio.IncompleteReadError):pass
  finally:
   writer.close()
   with contextlib.suppress(Exception):await writer.wait_closed()
   self.handler_tasks.discard(task)

async def network(routes):
 fixture=Wire();fixture.connections=0;await fixture.asyncSetUp();latencies=[];created=[]
 try:
  configs=[await fixture.proxy() for _ in range(routes)]
  gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=32,initial=32,adaptive=False)
  def factory(route,opts):
   created.append(route.label if route else 'direct')
   return httpx.AsyncClient(**{**opts,'verify':fixture.verify})
  client=s.WorkerApiClient(gate,s.ProxyPool(configs),client_factory=factory)
  calls=iter(range(640));cpu=time.process_time();started=time.perf_counter()
  async with client:
   async def work():
    for _ in calls:
     t=time.perf_counter();response=await client.request_json('profile',{'username':'synthetic'})
     assert response['status']=='ok';latencies.append(time.perf_counter()-t)
   await asyncio.gather(*(work() for _ in range(32)))
  elapsed=time.perf_counter()-started;cpu=time.process_time()-cpu
  assert len(fixture.headers)==640
  assert all('proxy-authorization' not in h.casefold() and 'fixture-pass' not in h for h in fixture.headers)
  ordered=sorted(latencies)
  return dict(seconds=elapsed,requests_per_second=640/elapsed,requests=640,successes=640,failures=0,retries=0,timeouts=0,
      latency_mean_ms=statistics.mean(latencies)*1000,latency_p95_ms=ordered[int(len(ordered)*.95)]*1000,
      client_count=len(created),tcp_tls_connections=fixture.connections,connect_tunnels=len(fixture.connect_headers),
      requests_per_connection=640/fixture.connections,peak_in_flight=gate.peak_active,cpu_seconds=cpu)
 finally:await fixture.asyncTearDown()
for routes in (0,1,4):out['wire_'+str(routes)+'_proxies']=asyncio.run(network(routes))
out['peak_rss_mib']=(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1048576 if sys.platform=='darwin' else 1024) if resource else None)
a.result.write_text(json.dumps(out,indent=2));print(json.dumps(out,indent=2))
import shutil
shutil.rmtree(root,ignore_errors=True)
