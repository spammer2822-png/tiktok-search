"""Run durable CI verification from an immutable source snapshot.

Generated evidence is committed after each completed case. Heavy workloads run
sequentially within each job; comparison variants always share the same runner.
No live services, user credentials or external accounts are used.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import traceback
from zipfile import ZipFile

REPO = Path.cwd()
RUN_ID = os.environ.get('GITHUB_RUN_ID', 'local')
JOB = sys.argv[1]
EVIDENCE = REPO / 'verification_runs' / RUN_ID / JOB
EVIDENCE.mkdir(parents=True, exist_ok=True)
WORK = Path(tempfile.mkdtemp(prefix='scanner-verification-', dir=os.environ.get('RUNNER_TEMP')))
FINAL = WORK / 'final'
shutil.copytree(REPO, FINAL, ignore=shutil.ignore_patterns('.git', 'verification_runs', '__pycache__'))
SOURCES = {'final': FINAL}
for label in ('original', 'first_hybrid'):
    dest = WORK / label
    with ZipFile(REPO/'development_sources'/f'{label}.zip') as z:
        z.extractall(dest)
    candidates = sorted(dest.rglob('tiktok_worker_scanner.py'), key=lambda p: len(p.parts))
    assert candidates, label
    SOURCES['first' if label == 'first_hybrid' else label] = candidates[0].parent
PYTHON = sys.executable
HARNESS = FINAL/'tests'
RESULTS = WORK/'results'
RESULTS.mkdir()
provenance = {
    'run_id': RUN_ID, 'job': JOB, 'commit': os.environ.get('GITHUB_SHA'),
    'python': platform.python_version(), 'platform': platform.platform(),
    'cpu_count': os.cpu_count(), 'runner_image': os.environ.get('ImageVersion'),
    'sources': {k: hashlib.sha256((v/'tiktok_worker_scanner.py').read_bytes()).hexdigest()
                for k,v in SOURCES.items()},
    'scope': 'Offline fixtures and real loopback TLS only; no live TikTok claims',
}
(EVIDENCE/'provenance.json').write_text(json.dumps(provenance, indent=2), encoding='utf-8')


def publish(message):
    for p in RESULTS.iterdir():
        if p.is_file() and p.suffix in ('.json', '.log', '.txt', '.csv', '.pstats', '.png'):
            shutil.copy2(p, EVIDENCE/p.name)
    if not os.environ.get('GITHUB_ACTIONS'):
        return
    def git(*args, check=True):
        return subprocess.run(['git', *args], cwd=REPO, check=check)
    git('config', 'user.name', 'github-actions[bot]')
    git('config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com')
    git('add', '--', str(EVIDENCE.relative_to(REPO)))
    if git('diff', '--cached', '--quiet', check=False).returncode == 0:
        return
    git('commit', '-m', f'Verification {RUN_ID}/{JOB}: {message}')
    for attempt in range(4):
        git('pull', '--rebase', 'origin', 'main')
        if git('push', 'origin', 'HEAD:main', check=False).returncode == 0:
            return
    raise RuntimeError('Evidence commit could not be pushed; retained as CI artifact')


def run(args, name):
    print('RUN', name, flush=True)
    with (RESULTS/f'{name}.log').open('w', encoding='utf-8') as log:
        result = subprocess.run(args, cwd=FINAL, stdout=log, stderr=subprocess.STDOUT)
    publish(f'{name} exit={result.returncode}')
    if result.returncode:
        print((RESULTS/f'{name}.log').read_text(encoding='utf-8')[-12000:], flush=True)
        raise RuntimeError(f'{name}: exit {result.returncode}')


def compare():
    for label, source in SOURCES.items():
        # All variants in one job, one case at a time. No simultaneous benchmark processes.
        for mode in (('worker',) if label == 'original' else ('worker', 'direct', 'hybrid')):
            cases = [(str(w), w, 100, -1, .02) for w in (100,500,1000,2500,5000)]
            cases += [('10kfollowers',100,1,10000,.001),
                      ('1000accounts',2500,1000,-1,.001), ('10000accounts',2500,10000,-1,.001)]
            for suffix, workers, accounts, followers, latency in cases:
                name = f'{label}_{mode}_{suffix}'
                command = [PYTHON,str(HARNESS/'benchmark_hybrid.py'),'--source',str(source),
                    '--backend',mode,'--workers',str(workers),'--accounts',str(accounts),
                    '--followers',str(followers),'--latency',str(latency),
                    '--result',str(RESULTS/f'{name}.json')]
                if label == 'original':
                    command += ['--original']
                run(command, name)
        run([PYTHON,str(HARNESS/'benchmark_components.py'),'--source',str(source),
             '--result',str(RESULTS/f'{label}_components.json')], f'{label}_components')
        # CPU function profile is separate from the unprofiled throughput comparison.
        run([PYTHON,str(HARNESS/'benchmark_hybrid.py'),'--source',str(source),
             '--backend','worker' if label == 'original' else 'hybrid',
             '--workers','2500','--accounts','1000','--latency','.001','--profile',
             '--result',str(RESULTS/f'{label}_profile.json')]
            + (['--original'] if label == 'original' else []), f'{label}_profile')
        import pstats
        with (RESULTS/f'{label}_profile.txt').open('w', encoding='utf-8') as stream:
            pstats.Stats(str(RESULTS/f'{label}_profile.pstats'), stream=stream).strip_dirs().sort_stats('cumulative').print_stats(80)
        publish(f'{label} components and CPU profile')
    rows = []
    for p in sorted(RESULTS.glob('*_worker_*.json')):
        if p.name.startswith('original_'):
            a = json.loads(p.read_text())
            suffix = p.name[len('original_'):]
            for label in ('first', 'final'):
                b = json.loads((RESULTS/f'{label}_{suffix}').read_text())
                for key in ('completion_seconds','successful_rps','accounts_per_second',
                            'followers_per_second','following_per_second','cpu_seconds',
                            'peak_rss_bytes','report_seconds','resume_seconds'):
                    av, bv = a.get(key), b.get(key)
                    rows.append(dict(case=suffix, comparison=f'original_vs_{label}', metric=key,
                                     original=av, changed=bv, ratio_original_over_changed=av/bv if bv else None))
    (RESULTS/'comparison.json').write_text(json.dumps(rows, indent=2), encoding='utf-8')
    publish('complete matched comparison')


def network():
    for mode in ('worker','direct','hybrid'):
        for workers in (100,500,1000,2500,5000):
            name = f'{mode}_{workers}_tls'
            run([PYTHON,str(HARNESS/'benchmark_network_scaling.py'),'--mode',mode,
                 '--workers',str(workers),'--result',str(RESULTS/f'{name}.json')], name)


def browser():
    for label, source in SOURCES.items():
        name = f'{label}_report_180k'
        run([PYTHON,str(HARNESS/'benchmark_suite.py'),'report','--source',str(source),
             '--accounts','180000','--result',str(RESULTS/f'{name}.json'),'--keep'], name)
        doc = json.loads((RESULTS/f'{name}.json').read_text())
        for engine in ('worker','fallback'):
            run(['node',str(HARNESS/'benchmark_sparse_report.cjs'),doc['report_path'],
                 os.environ['CHROMIUM'],str(RESULTS/f'{name}_{engine}.json'),engine],
                f'{name}_{engine}')
    run([PYTHON,str(HARNESS/'create_avatar_fixture.py'),str(WORK/'avatars')], 'avatar_fixture')
    report = next((WORK/'avatars').glob('report_*.html'))
    for engine in ('worker','fallback'):
        run(['node',str(HARNESS/'check_avatar_sections.cjs'),str(report),
             os.environ['CHROMIUM'],engine], f'avatar_{engine}')
        shutil.copy2(str(report)+'.avatars.png', RESULTS/f'avatar_{engine}.png')
        publish(f'avatar {engine}')


def cpu():
    # Independent per-thread profilers require Python 3.11; this job is pinned
    # separately from the unprofiled Python 3.12 throughput comparison.
    for label, source in SOURCES.items():
        modes = ('worker',) if label == 'original' else ('worker', 'direct', 'hybrid')
        for mode in modes:
            name = f'{label}_{mode}'
            run([PYTHON, str(HARNESS/'profile_cpu.py'), '--output', str(RESULTS/(name+'_cpu')),
                 '--source', str(source), '--backend', mode, '--workers', '2500',
                 '--accounts', '1000', '--latency', '.001', '--result', str(RESULTS/(name+'_scan.json'))]
                + (['--original'] if label == 'original' else []), name)
        for sample in range(1, 4):
            name = f'{label}_observability_{sample}'
            run([PYTHON, str(HARNESS/'benchmark_observability.py'), '--source', str(source),
                 '--result', str(RESULTS/(name+'.json'))], name)


try:
    publish('start from preserved source snapshots')
    {'comparison': compare, 'network': network, 'browser': browser, 'cpu': cpu}[JOB]()
    (RESULTS/'completion.json').write_text(json.dumps({'status':'COMPLETED', **provenance}, indent=2))
    publish('completed')
except BaseException:
    (RESULTS/'failure.log').write_text(traceback.format_exc(), encoding='utf-8')
    publish('failure preserved')
    raise
