"""Large finalization on the saved synthetic report fixture, never a user scan."""
import argparse,json,sys,time
try:
    import resource
except ImportError:
    resource = None
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--fixture',type=Path,required=True);p.add_argument('--result',type=Path,required=True);a=p.parse_args()
sys.path.insert(0,str(a.source.resolve()));import tiktok_worker_scanner as s
root=a.fixture;cfg=json.loads((root/'scan_config.json').read_text());meta=json.loads((root/'scan_state.json').read_text())
state=s.DurableScanState(root,meta,root,cfg)
t=time.perf_counter();cpu=time.process_time();doc=state.finalize(scan_complete=False,fatal_error=None)
elapsed=time.perf_counter()-t;cpu=time.process_time()-cpu
assert doc['summary']['total_profiles']==180000
# Check all generated arrays, count and complete provenance; outside timing.
discoveries=json.loads((root/'discovered_profiles.json').read_text());queue=json.loads((root/'phase2_queue.json').read_text())
assert len(discoveries)==len(queue)==180000
assert all(len(x['discovered_from'])==3 for x in discoveries)
out={'version':a.source.parent.name,'seconds':elapsed,'cpu_seconds':cpu,'accounts':180000,'peak_rss_mib':(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1048576 if sys.platform=='darwin' else 1024) if resource else None),'discovery_json_bytes':(root/'discovered_profiles.json').stat().st_size,'queue_json_bytes':(root/'phase2_queue.json').stat().st_size}
state.close();a.result.write_text(json.dumps(out,indent=2));print(json.dumps(out))
