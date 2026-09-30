"""Build an offline report exclusively from saved files. No network dependencies."""
from __future__ import annotations
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from avatar_cache import AvatarReader, embedded_avatar

LISTS = ('followers', 'following')
CONFIG_FIELDS = ('target_username','input_source','starting_username','source_json_path',
    'scan_mode','follower_skip_limit','following_skip_limit','workers','request_delay',
    'current_phase','use_proxies','proxy_only','keep_raw','retry_attempts','timeouts','worker_origin','max_connections')
LIST_FIELDS = ('complete','status','stop_reason','endpoint_exhausted','advertised_count',
    'profile_count_at_start','unique_records_saved','returned_unique_count','count_difference',
    'usable','error','warnings','http_code','retry_count','visibility_reason','see_following')

def read_json(path: Path, default: Any) -> Any:
    if not path.exists(): return default
    return json.loads(path.read_text(encoding='utf-8-sig'))

@contextmanager
def open_database(path: Path):
    db = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)
    try:
        yield db
    finally:
        db.close()

def saved_profile(root: Path, name: str, bootstrap: bool = False, available=None) -> dict:
    component = name.casefold()
    if re.fullmatch(r'(?:con|prn|aux|nul|com[1-9]|lpt[1-9])',component.split('.',1)[0]): component = '_'+component
    path = root / ('bootstrap/pages' if bootstrap else 'pages') / (component+'.sqlite3')
    if (path.name in available if available is not None else path.is_file()):
        with open_database(path) as db:
            row = db.execute("SELECT payload FROM checkpoint WHERE name='profile'").fetchone()
            if row: return json.loads(row[0])
    return {}

def count_value(profile: dict, name: str) -> int | None:
    key = 'follower_count' if name=='followers' else 'following_count'
    value = profile.get(key)
    if isinstance(value, int) and not isinstance(value, bool): return value
    raw = str(profile.get('advertised_counts',{}).get(name,'')).strip().upper().replace(',','')
    match = re.fullmatch(r'(\d+(?:\.\d+)?)\s*([KMB]?)',raw)
    return int(float(match[1])*{'':1,'K':1000,'M':1000000,'B':1000000000}[match[2]]) if match else None

def certainty(found: list, results: dict) -> dict:
    values = [True if name in found else (False if results.get(name,{}).get('complete') is True else None) for name in LISTS]
    return dict(zip(('target_follows_profile','profile_follows_target','mutual'),
                    values+[None if None in values else all(values)]))

def local_avatar(value: Any) -> str:
    return embedded_avatar(value)


def profile_for_report(root: Path, db, name: str, uid: str, result: dict, *, available=None, discovery=None, prefetched=False) -> dict:
    profile = result.get('metadata',{}).get('profile',{})
    # Error/partial results can contain only a small metadata fragment. A saved
    # profile checkpoint may still hold its avatar even when metadata is nonempty.
    if not profile.get('avatar_url'):
        saved = saved_profile(root,name,available=available)
        if not profile:
            profile = saved
        elif saved.get('avatar_url'):
            profile = {**profile, 'avatar_url':saved['avatar_url']}
    if not profile.get('avatar_url'):
        # A skipped/not-found profile may still have a confirmed list-member
        # avatarThumb, saved by normalize_member as avatar_url.
        row = (discovery,) if discovery else None
        if not prefetched:
            row = db.execute('SELECT payload FROM discoveries WHERE username=?', (name,)).fetchone()
        if row is None and uid and not prefetched:
            row = db.execute('SELECT payload FROM discoveries WHERE uid=? ORDER BY rowid LIMIT 1', (uid,)).fetchone()
        if row:
            member = json.loads(row[0])
            profile = {**profile, 'avatar_url':member.get('avatar_url',''),
                       'display_name':profile.get('display_name') or member.get('display_name','')}
    return profile


def iter_avatar_profiles(root: Path):
    """Read saved profile URLs for optional scanner-side backfill; no requests."""
    cfg = read_json(root/'scan_config.json',{})
    if cfg.get('input_source') == 'json':
        # Queue jobs intentionally contain identity only. Preserve access to the
        # input's saved member avatars, including before profile lookup succeeds.
        source = (root / str(cfg.get('starting_json') or 'starting_dataset.json')).resolve()
        if source.is_relative_to(root.resolve()) and source.is_file():
            from tiktok_worker_scanner import parse_username, numeric_uid, first_value, ExporterError
            document = read_json(source,[])
            groups = [document] if isinstance(document,list) else [document.get(k,[]) for k in LISTS] if isinstance(document,dict) else []
            for group in groups:
                if not isinstance(group,list):
                    continue
                for item in group:
                    if not isinstance(item,dict):
                        continue
                    url = item.get('avatar_url') or item.get('avatarThumb')
                    if not isinstance(url,str) or not url:
                        continue
                    try:
                        name = parse_username(str(first_value(item.get('profile_url'),item.get('url'),item.get('username'),item.get('unique_id'),item.get('uniqueId')) or ''))
                    except ExporterError:
                        continue
                    uid = numeric_uid(first_value(item.get('id'),item.get('uid'),item.get('user_id'),item.get('userid')))
                    yield uid, name, url
            del document, groups
    if (root/'state.sqlite3').is_file():
        pages = root/'pages'
        available = {entry.name for entry in os.scandir(pages)} if pages.is_dir() else set()
        after = 0
        while True:
            # Release the reader before yielding to image downloads. Holding a
            # SQLite read transaction for a large scan prevents WAL reclamation.
            with open_database(root/'state.sqlite3') as db:
                batch = db.execute('''SELECT j.rowid,j.username,j.uid,j.result,
                    COALESCE(d.payload,(SELECT payload FROM discoveries WHERE uid=j.uid AND j.uid<>'' ORDER BY rowid LIMIT 1))
                    FROM jobs j LEFT JOIN discoveries d ON d.username=j.username
                    WHERE j.rowid>? ORDER BY j.rowid LIMIT 256''', (after,)).fetchall()
            if not batch:
                break
            for rowid, name, uid, result_json, discovery in batch:
                after = rowid
                result = json.loads(result_json) if result_json else {}
                profile = profile_for_report(root,None,name,uid,result,available=available,discovery=discovery,prefetched=True)
                yield str(uid or profile.get('uid','')), name, profile.get('avatar_url','')
    bootstrap = read_json(root/'scan_state.json',{}).get('bootstrap',{})
    name = bootstrap.get('username')
    if name:
        profile = saved_profile(root,name,True)
        yield profile.get('uid',''), name, profile.get('avatar_url','')

def build_data(root: Path, clean: Callable = lambda value:value) -> dict:
    reader = AvatarReader(root)
    try:
        return _build_data(root,clean,reader)
    finally:
        reader.close()


def _build_data(root: Path, clean: Callable, avatars: AvatarReader) -> dict:
    cfg = read_json(root/'scan_config.json',{})
    state = read_json(root/'scan_state.json',{})
    success = read_json(root/'sucess_find.json',{})
    matches = {str(x.get('username','')).casefold():x for x in success.get('profiles',[])}
    rows, seen = [], set()
    sources: dict[str,list[str]] = {}
    phase_discovered: dict[int,int] = {}
    duplicate_count = 0
    all_discovered = 0
    pages = root/'pages'
    available = {entry.name for entry in os.scandir(pages)} if pages.is_dir() else set()
    if (root/'state.sqlite3').is_file():
        with open_database(root/'state.sqlite3') as db:
            for name, owner, kind in db.execute('SELECT username,owner,list_name FROM discovery_sources'):
                sources.setdefault(name,[]).append('@'+owner+'/'+kind)
            phase_discovered = dict(db.execute('SELECT phase,COUNT(*) FROM discoveries GROUP BY phase'))
            all_discovered = sum(phase_discovered.values())
            value = db.execute("SELECT value FROM scan_control WHERE name='phase2_duplicates'").fetchone()
            duplicate_count = json.loads(value[0]) if value else 0
            jobs = db.execute("""SELECT j.username,j.uid,j.phase,j.status,j.job,j.result,j.attempts,j.updated,
                COALESCE(d.payload,(SELECT payload FROM discoveries WHERE uid=j.uid AND j.uid<>'' ORDER BY rowid LIMIT 1))
                FROM jobs j LEFT JOIN discoveries d ON d.username=j.username ORDER BY j.rowid""")
            for name, uid, phase, status, job_json, result_json, attempts, updated, discovery in jobs:
                if cfg.get('scan_mode')=='normal' and phase>1: continue
                result = json.loads(result_json) if result_json else {}
                job = json.loads(job_json)
                rows.append(clean(make_row(name,uid,phase,status,result,profile_for_report(root,db,name,uid,result,available=available,discovery=discovery,prefetched=True),matches.get(name,{}),
                                     sources.pop(name, job.get('source_lists',[])),attempts,updated,avatars=avatars)))
                seen.add(name)
    # Bootstrap target observations belong in the report even though the source
    # itself need not be a main queue member.
    for name, entry in matches.items():
        if name not in seen:
            bootstrap = state.get('bootstrap',{})
            result = {'list_results':bootstrap.get('list_results',{}), 'completed_at_utc':bootstrap.get('completed_at_utc')}
            rows.append(clean(make_row(name,'','bootstrap','target_found',result,saved_profile(root,name,True),entry,['bootstrap'],0,'',avatars=avatars)))
    summary = {**state.get('summary',{}), 'all_discovered_accounts':all_discovered,
               'input_duplicates_removed':state.get('input',{}).get('duplicates_removed',0),
               'phase2_duplicates_removed':duplicate_count,
               'resumed_profiles':state.get('summary',{}).get('already_processed',0)}
    status_counts = Counter(r['status'] for r in rows if r['phase']!='bootstrap')
    phases = {}
    for phase in (1,2):
        items = [r for r in rows if r['phase']==phase]
        phases[str(phase)] = {'queued':len(items),'processed':sum(r['status'] not in ('pending','in_progress') for r in items),
            'complete':sum(r['complete'] for r in items),'partial':sum(r['status']=='partial' for r in items),
            'private':sum(r['status']=='private' for r in items),'errors':sum(r['hard_error'] for r in items),
            'skipped':sum(r['skipped'] for r in items),'target_matches':sum(r['target_found'] for r in items),
            'discovered':phase_discovered.get(phase,0),
            'eligible_after_deduplication':len(items) if phase==2 else None,
            'duplicates':duplicate_count if phase==2 else state.get('input',{}).get('duplicates_removed',0)+sum(r['status']=='skipped_duplicate' for r in items)}
    start = state.get('created_at_utc') or cfg.get('created_at_utc') or success.get('started_at_utc')
    finish = state.get('updated_at_utc') or success.get('updated_at_utc')
    try: runtime = max(0,int((datetime.fromisoformat(finish.replace('Z','+00:00'))-datetime.fromisoformat(start.replace('Z','+00:00'))).total_seconds()))
    except (TypeError,ValueError,AttributeError): runtime = None
    errors = sum(r['hard_error'] or r['status'] in ('partial','restricted','pending','in_progress','cancelled') for r in rows if r['phase']!='bootstrap')
    result = {'target':cfg.get('target_username',success.get('target_user','unknown')),'folder':str(root),
        'scan_complete':bool(state.get('scan_complete')),'data_complete':bool(state.get('data_complete')),
        'start':start,'finish':finish,'runtime_seconds':runtime,'configuration':{k:cfg.get(k) for k in CONFIG_FIELDS},
        'summary':{k:v for k,v in summary.items() if k not in ('network','proxy_pool')},'status_counts':dict(status_counts),
        'cards':{'processed':summary.get('processed_profiles',0),'matches':len(matches),
                 'mutuals':sum(r['relationship']['mutual'] is True for r in rows),'errors':errors},
        'phases':phases,'rows':rows,'bootstrap':state.get('bootstrap',{}),
        'generated_at_utc':datetime.now(timezone.utc).isoformat()}
    # Clean one row at a time; don't retain a second deep copy of a large report.
    result.pop('rows')
    result = clean(result)
    result['rows'] = rows
    return result

def make_row(name,uid,phase,status,result,profile,match,sources,attempts,updated, *, avatars=None):
    profile = profile or result.get('metadata',{}).get('profile',{})
    resolved_uid = str(uid or profile.get('uid','') or result.get('metadata',{}).get('user_id',''))
    avatar = avatars.reference(resolved_uid,name,profile.get('avatar_url')) if avatars is not None else ''
    if status == 'completed' and result.get('status') == 'resumed_complete': status = 'resumed_complete'
    lists = {kind:{k:v for k,v in result.get('list_results',{}).get(kind,{}).items() if k in LIST_FIELDS} for kind in LISTS}
    found = sorted(set(match.get('found_in',[])) | set(result.get('target_found_in',[])))
    rel = certainty(found,lists)
    relationship = ('Mutual' if rel['mutual'] is True else 'Target follows this account' if rel['target_follows_profile'] is True
                    else 'This account follows target' if rel['profile_follows_target'] is True
                    else 'No relationship confirmed' if all(rel[k] is False for k in ('target_follows_profile','profile_follows_target')) else 'Unknown / incomplete')
    skipped = status.startswith('skipped') or status in ('private','not_found','invalid_username')
    harmless = ('complete','completed','resumed_complete','pending','in_progress','restricted','target_found')
    errors = [info.get('error') for info in lists.values() if info.get('error')]
    reason = result.get('error') or '; '.join(errors)
    metadata = result.get('metadata',{})
    profile_retries = metadata.get('profile_retries',profile.get('metadata',{}).get('lookup_retry_count'))
    retry_count = (sum(info.get('retry_count',0) or 0 for info in lists.values())+int(profile_retries or 0)
                   if profile_retries is not None or any('retry_count' in info for info in lists.values()) else None)
    return {'username':name,'uid':resolved_uid,
        'display_name':str(profile.get('display_name','')),'avatar':avatar or local_avatar(profile.get('avatar_url')),
        'followers':count_value(profile,'followers'),'following':count_value(profile,'following'),
        'advertised_counts':{k:profile.get('advertised_counts',{}).get(k) for k in LISTS},
        'phase':phase,'status':status,'complete':bool(result.get('complete')),'target_found':bool(found),
        'found_in':found,'relationship':rel,'relationship_label':relationship,'mutual':rel['mutual'],
        'sources':sources,'skip_reason':str(metadata.get('reason') or reason or status) if skipped else '',
        'skipped':skipped,'error':str(reason or ''),'hard_error':status not in harmless and status!='partial' and not skipped,
        'list_results':lists,'retry_count':retry_count,
        'attempts':attempts,'http_code':metadata.get('http_code') or next((v.get('http_code') for v in lists.values() if v.get('http_code')),None),
        'started_at_utc':result.get('started_at_utc'),'completed_at_utc':result.get('completed_at_utc') or updated,
        'match_observed_at_utc':match.get('match_observed_at_utc',{}),
        'warnings':[w for info in lists.values() for w in info.get('warnings',[])],
        'size_limits':metadata.get('counts',{})}

def generate_report(root: Path, *, clean: Callable = lambda value:value, emit: Callable = print) -> Path | None:
    """Called only after state/success finalization. Failure preserves prior HTML."""
    path = None
    try:
        data = build_data(root,clean)
        name = re.sub(r'[^A-Za-z0-9._-]','_',root.name).strip(' .') or 'scan'
        path = root/f'report_{name}.html'
        template = Path(__file__).with_name('report_template.html').read_text(encoding='utf-8')
        worker = Path(__file__).with_name('report_worker.js').read_text(encoding='utf-8')
        template = template.replace('__REPORT_WORKER__', worker)
        rows = data.pop('rows')
        fields = list(rows[0]) if rows else []
        data['row_fields'] = fields
        # Lossless default/delta encoding removes repeated empty lists, flags,
        # labels and metadata. Every row reconstructs the identical object.
        defaults = rows[0] if rows else {}
        data['row_defaults'] = defaults
        data['row_encoding'] = 'indexed_delta_v1'
        def encode(value):
            return json.dumps(value,ensure_ascii=True,separators=(',',':')).replace('<','\\u003c').replace('>','\\u003e').replace('&','\\u0026')
        # Compact columnar NDJSON: one copy, no repeated object keys, independently
        # parseable rows for a cooperative fallback. Never construct a giant HTML string.
        before, after = template.split('__REPORT_ROWS__')
        before = before.replace('__REPORT_DATA__', encode(data))
        temporary = path.with_suffix('.html.tmp')
        try:
            with temporary.open('w',encoding='utf-8',newline='\n') as stream:
                stream.write(before)
                for row in rows:
                    delta = []
                    for index, field in enumerate(fields):
                        value = row.get(field)
                        if value != defaults.get(field):
                            delta.extend((index, value))
                    stream.write(encode(delta)+'\n')
                stream.write(after);stream.flush();os.fsync(stream.fileno())
            os.replace(temporary,path)
        finally: temporary.unlink(missing_ok=True)
        success_path = root/'sucess_find.json'
        doc = read_json(success_path,{})
        doc['report'] = {'filename':path.name,'path':str(path),'generated_at_utc':data['generated_at_utc']}
        temporary = success_path.with_suffix('.json.report.tmp')
        try:
            with temporary.open('w',encoding='utf-8') as stream:
                json.dump(doc,stream,ensure_ascii=False,indent=2);stream.flush();os.fsync(stream.fileno())
            os.replace(temporary,success_path)
        finally: temporary.unlink(missing_ok=True)
        emit(f"[REPORT] {path}\nProfiles checked: {data['cards']['processed']:,} | Target matches: {data['cards']['matches']:,} | Confirmed mutuals: {data['cards']['mutuals']:,}")
        return path
    except Exception as exc:
        # Type only: filesystem/JSON errors can include user data or credentials.
        emit(f'[REPORT_ERROR] Failed to generate HTML report: {type(exc).__name__}. Saved scan data retained.')
        return None
