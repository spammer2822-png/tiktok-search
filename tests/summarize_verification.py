"""Build auditable comparisons from preserved JSON; never combine runner timings."""
import argparse
import csv
import json
from pathlib import Path


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def flatten(value, prefix=''):
    result = {}
    for key, item in value.items():
        name = prefix + key
        if isinstance(item, dict):
            result.update(flatten(item, name + '.'))
        elif isinstance(item, (float, int)) and not isinstance(item, bool):
            result[name] = item
    return result


def write_csv(path, rows):
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def comparison_rows(group, measurements, pairs):
    rows = []
    for before, after in pairs:
        left, right = measurements[before], measurements[after]
        a, b = flatten(left), flatten(right)
        for metric in sorted(a.keys() | b.keys()):
            av, bv = a.get(metric), b.get(metric)
            rows.append(dict(group=group, before=before, after=after, metric=metric,
                before_value=av, after_value=bv,
                percent_change=(100*(bv/av-1) if av and bv is not None else None),
                before_over_after=(av/bv if bv and av is not None else None)))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for group in ('scan', 'network', 'browser', 'shutdown'):
        parser.add_argument('--'+group+'-run', required=True)
    parser.add_argument('--output', type=Path, default=Path('verification_analysis'))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parents[1] / 'verification_runs'
    folders = {group: root/getattr(args, group+'_run')/('comparison' if group == 'scan' else group)
               for group in ('scan','network','browser','shutdown')}
    for group, path in folders.items():
        marker = 'summary.json' if group == 'shutdown' else 'completion.json'
        assert (path/marker).is_file(), f'{group} verification not complete: {path}'
    scans, network, browser = {}, {}, {}
    for version in ('original','first','final'):
        for mode in (('worker',) if version == 'original' else ('worker','direct','hybrid')):
            for case in ('100','500','1000','2500','5000','10kfollowers','1000accounts','10000accounts'):
                name = f'{version}_{mode}_{case}'
                scans[name] = read(folders['scan']/(name+'.json'))
                assert scans[name]['errors'] == 0
        for suffix in ('report_180k','report_180k_worker','report_180k_fallback'):
            name = version+'_'+suffix
            browser[name] = read(folders['browser']/(name+'.json'))
            if suffix != 'report_180k':
                assert browser[name]['errors'] == [] and browser[name]['external_requests'] == 0
    for version in ('original','first_hybrid','final'):
        for mode in (('worker',) if version == 'original' else ('worker','direct','hybrid')):
            for workers in (100,500,1000,2500,5000):
                name = f'{version}_{mode}_{workers}'
                network[name] = read(folders['network']/(name+'.json'))
                assert not network[name]['errors']
    pairs = []
    for name in scans:
        if name.startswith('final_'):
            suffix = name[6:]
            pairs.append(('first_'+suffix, name))
            pairs.append(('original_worker_'+suffix.split('_',1)[1], name))
    scan_rows = comparison_rows('full pipeline', scans, pairs)
    net_pairs = []
    for name in network:
        if name.startswith('final_'):
            suffix = name[6:]
            net_pairs += [('first_hybrid_'+suffix,name), ('original_worker_'+suffix.rsplit('_',1)[1],name)]
    network_rows = comparison_rows('real loopback TLS', network, net_pairs)
    browser_pairs = [(version+'_'+suffix,'final_'+suffix)
                     for version in ('original','first')
                     for suffix in ('report_180k','report_180k_worker','report_180k_fallback')]
    browser_rows = comparison_rows('offline browser', browser, browser_pairs)
    write_csv(args.output/'scan_comparisons.csv', scan_rows)
    write_csv(args.output/'network_comparisons.csv', network_rows)
    write_csv(args.output/'browser_comparisons.csv', browser_rows)

    # Per-operation elapsed durations overlap across tasks and executor threads.
    # Reporting each against wall time exposes concurrency without pretending
    # these independent observations form a disjoint 100% CPU time partition.
    stages = []
    for mode in ('worker','direct','hybrid'):
        d = scans[f'final_{mode}_10000accounts']
        wall = d['completion_seconds']
        timing = d.get('persistence_timings', {})
        backend = d.get('backend_metrics', {})
        observed = dict(timing)
        observed.update(backend.get('backend_stages', {}))
        observed.update(d.get('lock_timings', {}))
        observed['mock_network_wait_seconds'] = d['average_active_mock_requests']*wall
        for name in ('worker','direct'):
            observed[name+'_connection_pool_wait_seconds'] = backend.get(name+'_connection_pool_wait_seconds')
        observed['report_seconds'] = d['report_seconds']
        observed['background_log_write_seconds'] = d.get('background_log_write_seconds')
        observed['process_cpu_seconds'] = d['cpu_seconds']
        for name, value in observed.items():
            if name.endswith('_seconds') and value is not None:
                denominator = d.get('pipeline_total_seconds', wall+d['report_seconds']+d['resume_seconds']) if name in ('report_seconds','background_log_write_seconds','process_cpu_seconds') else wall
                stages.append(dict(mode=mode, stage=name, aggregate_seconds=value,
                    denominator_wall_seconds=denominator, percent_of_wall=100*value/denominator,
                    scope='Overlapping observed durations; not an additive CPU partition'))
    write_csv(args.output/'stage_timings.csv', stages)

    components = {v: read(folders['scan']/(v+'_components.json')) for v in ('original','first','final')}
    write_csv(args.output/'component_comparisons.csv', comparison_rows('components', components,
        [('original','final'),('first','final')]))
    shutdown = read(folders['shutdown']/'summary.json')
    assert all(r['returncode'] == 0 for r in shutdown['results'])
    summary = {'runs': {k:getattr(args,k+'_run') for k in folders},
               'scan_cases':len(scans),'network_cases':len(network),
               'browser_measurements':len(browser),'shutdown_cases':len(shutdown['results']),
               'scope':'Offline fixtures / real loopback TLS only; no live production claim',
               'csv_percent_change':'100 * (after / before - 1); negative is better for time/bytes and worse for throughput',
               'stage_percentages':'Overlapping aggregate observed task durations / wall time; may exceed 100%. Do not sum.',
               'sources':{k:read(v/'provenance.json') for k,v in folders.items()}}
    (args.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    lines = ['# Matched hybrid verification measurements','',
        'Generated from preserved measurements. Each comparison uses variants from the same runner/job. '
        'Scan, network, browser and shutdown jobs use separate hosts; no ratios cross those jobs. '
        'These are single samples per matrix cell. Small differences are observations, not statistical speed guarantees.','',
        '## Ten thousand mixed accounts','',
        '| Version / mode | Seconds | Successful RPS | Accounts/s | CPU seconds | Peak RSS MiB |',
        '|---|---:|---:|---:|---:|---:|']
    for version in ('original','first','final'):
        for mode in (('worker',) if version=='original' else ('worker','direct','hybrid')):
            d=scans[f'{version}_{mode}_10000accounts']
            lines.append(f"| {version} / {mode} | {d['completion_seconds']:.3f} | {d['successful_rps']:.2f} | {d['accounts_per_second']:.2f} | {d['cpu_seconds']:.3f} | {d['peak_rss_bytes']/1048576:.1f} |")
    lines += ['', 'Both follower and following counts cycle through 0, 1, 4 and 10 per profile. '
        'Raw captures, SQLite, exports and report/resume code are real. Transport latency is synthetic. '
        'CPU seconds include the scan and post-scan report/resume; completion_seconds is scan time.','',
        '## Ten thousand followers and ten thousand following','',
        '| Version / mode | Seconds | Requests | Users/follower page | Followers/s |',
        '|---|---:|---:|---:|---:|']
    for version in ('original','first','final'):
        for mode in (('worker',) if version=='original' else ('worker','direct','hybrid')):
            d=scans[f'{version}_{mode}_10kfollowers']
            lines.append(f"| {version} / {mode} | {d['completion_seconds']:.3f} | {d['requests']} | {d['users_per_follower_page']:.2f} | {d['followers_per_second']:.2f} |")
    lines += ['', '## Real loopback TLS successful RPS','',
        '| Version / mode | 100 | 500 | 1,000 | 2,500 | 5,000 workers |',
        '|---|---:|---:|---:|---:|---:|']
    for version in ('original','first_hybrid','final'):
        for mode in (('worker',) if version=='original' else ('worker','direct','hybrid')):
            lines.append('| '+version+' / '+mode+' | '+' | '.join(f"{network[f'{version}_{mode}_{w}']['successful_rps']:.2f}" for w in (100,500,1000,2500,5000))+' |')
    lines += ['', 'Every configured logical worker sends two sequential requests. HTTPX, TLS verification, signing, '
        'pooling and production backend selection are real. The local origin shares the process/CPU, so this '
        'measures the combined client/server fixture, not TikTok or proxy-plan capacity. Kernel loopback byte '
        'counters include both ends. Full active/idle connections, latencies, network bytes, CPU/RAM and waits '
        'are retained in network_comparisons.csv and source JSON.','',
        '## Offline 180,000-account browser','',
        '| Version / engine | Load ms | Sort ms | Search ms | Maximum initial UI lag ms |',
        '|---|---:|---:|---:|---:|']
    for version in ('original','first','final'):
        for engine in ('worker','fallback'):
            d=browser[f'{version}_report_180k_{engine}']
            lines.append(f"| {version} / {engine} | {d['load_ms']:.2f} | {d['sort_ms']:.2f} | {d['search_ms']:.2f} | {d['initial_ui_lag_ms']:.2f} |")
    lines += ['', 'All cases validate 180,000 searchable accounts, page sizes 20/500, sorting, filters, '
        'details and mobile layout, with zero external requests and JavaScript errors. Six-section avatar '
        'checks additionally validate saved pictures, placeholders and idempotent repair.','',
        '## Interpreting the complete comparisons','',
        '- CSVs include every available numeric metric, including slowdowns. Empty cells mean the older '
        'source did not expose that metric; they do not mean zero.',
        '- stage_timings.csv reports aggregate task/operation elapsed durations as percentages of wall time. '
        'Concurrent waits overlap, so values can exceed 100%. Signing includes executor wait. Disk stages '
        'include the called operation and any internal JSON/SQLite work. They are not exclusive CPU shares.',
        '- CPU profiles preserve per-function exclusive/cumulative observations in the source run. '
        'Cumulative coroutine/thread times overlap and must not be added together.',
        '- Full-pipeline 100-job worker-scaling cases cannot activate more than 100 profile owners. The '
        'separate TLS matrix activates all 100–5,000 configured jobs and records actual connection use.',
        '- The original scanner already contains its prior report/persistence speed release. The first '
        'hybrid and final variants add signing/routing/metrics; universal speedup is not assumed.',
        '- Live compatibility, largest reliable page size, real service risk rates and live upstream '
        'baseline throughput remain unverified; see LIVE_VERIFICATION.md.', '',
        'Exact runs: '+', '.join(k+'='+getattr(args,k+'_run') for k in folders)+'.','']
    (args.output/'MEASUREMENTS.md').write_text('\n'.join(lines),encoding='utf-8')
    print(json.dumps({k:summary[k] for k in ('runs','scan_cases','network_cases','browser_measurements','shutdown_cases')}))


if __name__ == '__main__':
    main()
