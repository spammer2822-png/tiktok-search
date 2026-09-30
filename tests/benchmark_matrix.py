"""Sequential matched offline benchmark matrix; invoke from project directory."""
import argparse, subprocess, sys
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source',type=Path);p.add_argument('--label',required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--original',action='store_true');a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True)
source=(a.source or Path(__file__).resolve().parents[1]).resolve();harness=Path(__file__).with_name('benchmark_hybrid.py').resolve()
for mode in (('worker',) if a.original else ('worker','direct','hybrid')):
 cases=[(str(w),w,100,-1,.02) for w in (100,500,1000,2500,5000)]+[('10kfollowers',100,1,10000,.001),('1000accounts',2500,1000,-1,.001),('10000accounts',2500,10000,-1,.001)]
 for suffix,w,n,f,lat in cases:
  result=a.output/f'{a.label}_{mode}_{suffix}.json'
  if result.exists():continue
  cmd=[sys.executable,str(harness),'--backend',mode,'--workers',str(w),'--accounts',str(n),'--followers',str(f),'--latency',str(lat),'--result',str(result),'--source',str(source)]
  if a.original:cmd+=['--original']
  subprocess.run(cmd,check=True)
