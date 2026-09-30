"""Serial profile export and request-statistics costs, with identical saved data."""
import argparse, importlib.util, json, platform, shutil, statistics, sys, tempfile, time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--result',type=Path,required=True);a=p.parse_args()
sys.path[:0]=[str(a.source.resolve()),str(a.source.resolve()/'tests')]
import tiktok_worker_scanner as s
from test_async_scanner import PROFILE
root=Path(tempfile.mkdtemp(prefix='exports_',dir=a.result.parent))
out={'version':a.source.parent.name,'python':platform.python_version(),'members':10000,'export_repetitions':5}
try:
 with s.UserStore(root/'pages.sqlite3') as store:
  for page in range(100):
   store.save_batch('followers',[{'uniqueId':f'person{page*100+i}','user_id':str(7500000000000000000+page*100+i),'nickname':'Synthetic 🌈','avatarThumb':'','signature':'Synthetic bio','privateAccount':False,'verified':'No❌'} for i in range(100)])
  profile=s.parse_profile_response(PROFILE,PROFILE['data']['username']);samples=[]
  for _ in range(5):
   start=time.perf_counter()
   s.write_final_json(root/'profile.json',profile=profile,selected_lists=['followers'],results={'followers':s.ListExportResult(list_name='followers',profile_count_at_start=10000,complete=True)},store=store,started_at=s.utc_iso())
   samples.append(time.perf_counter()-start)
  payload=json.loads((root/'profile.json').read_text())
  assert len(payload['followers'])==10000 and payload['followers'][-1]['list_position']==10000
  out.update(export_mean_seconds=statistics.mean(samples),export_min_seconds=min(samples),export_max_seconds=max(samples),export_bytes=(root/'profile.json').stat().st_size)
 if (a.source/'scan_statistics.py').exists():
  from scan_statistics import ScanStatistics
  stats=ScanStatistics(root,2500);n=100000;start=time.perf_counter()
  for _ in range(n):
   stats.request_started();stats.request_finished(success=True,latency=.02)
  out['statistics_request_pair_seconds']=(time.perf_counter()-start)/n
  summary={'total_profiles':180000,'remaining_profiles':100000,'current_phase':2};start=time.perf_counter()
  for _ in range(10000):stats.snapshot(summary)
  out['statistics_snapshot_seconds']=(time.perf_counter()-start)/10000
 else:
  out['statistics_request_pair_seconds']=out['statistics_snapshot_seconds']=None
 a.result.write_text(json.dumps(out,indent=2));print(json.dumps(out))
finally:shutil.rmtree(root,ignore_errors=True)
