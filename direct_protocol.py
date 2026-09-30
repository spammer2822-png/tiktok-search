"""Lightweight TikTok adapters derived from Evil0ctal 737bf3d (Apache-2.0).

The native signer is vendored unchanged. HTTP 200 alone never establishes success.
No browser automation, token fabrication, captcha solving, or private-list access.
"""
from __future__ import annotations

import json
import re
import secrets
import threading
from pathlib import Path
from urllib.parse import quote

from vendor.tiktok_sign import sign

ORIGIN = 'https://www.tiktok.com'
USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36')


class ProtocolError(ValueError):
    def __init__(self, kind, message, *, retryable=False):
        super().__init__(message)
        self.kind, self.retryable = kind, retryable


def load_session(filename):
    if not filename:
        return {}
    try:
        data = json.loads(Path(filename).read_text(encoding='utf-8-sig'))
    except (OSError, ValueError):
        raise ProtocolError('invalid_session', 'Cannot read the Direct session JSON file.') from None
    if not isinstance(data, dict) or not isinstance(data.get('cookies', {}), dict):
        raise ProtocolError('invalid_session', 'Direct session must contain a cookies object.')
    cookies = data.get('cookies', {})
    if any(not isinstance(k, str) or not re.fullmatch(r'[A-Za-z0-9_\-]+', k)
           or not isinstance(v, str) or any(c in v for c in '\r\n;') for k, v in cookies.items()):
        raise ProtocolError('invalid_session', 'Invalid Direct cookie name or value.')
    ua = data.get('user_agent', USER_AGENT)
    if not isinstance(ua, str) or '\r' in ua or '\n' in ua:
        raise ProtocolError('invalid_session', 'Invalid Direct User-Agent.')
    device = data.get('device_id', '')
    if device and (not isinstance(device, str) or not device.isdigit()):
        raise ProtocolError('invalid_session', 'Direct device_id must be a numeric string.')
    return {'cookies': cookies, 'user_agent': ua, 'device_id': device}


class Session:
    """One stable device/browser identity per proxy connection pool."""
    def __init__(self, supplied):
        self.cookies = dict(supplied.get('cookies', {}))
        self.user_agent = supplied.get('user_agent', USER_AGENT)
        self.device_id = supplied.get('device_id') or str(10**18 + secrets.randbelow(9 * 10**18))
        self.sequence = 1
        self.sequence_lock = threading.Lock()

    def signed_url(self, operation, params, count, cookies=None):
        ua = self.user_agent
        match = re.search(r'Mozilla/([\d.]+) \(([^;)]+)', ua)
        version = f'{match[1]} ({match[2].split()[0]})' if match else '5.0 (Windows)'
        os_name, platform = ('Mac', 'MacIntel') if 'Macintosh' in ua else (
            ('Linux', 'Linux x86_64') if 'Linux' in ua else ('Windows', 'Win32'))
        query = dict(aid='1988', app_language='en', app_name='tiktok_web',
                     browser_language='en-US', browser_name='Mozilla', browser_online='true',
                     browser_platform=platform, browser_version=version, channel='tiktok_web',
                     cookie_enabled='true', device_id=self.device_id, device_platform='web_pc',
                     focus_state='true', from_page='user', history_len='4', is_fullscreen='false',
                     is_page_visible='true', language='en', os=os_name.lower(), priority_region='US',
                     referer='', region='US', root_referer=ORIGIN+'/', screen_height='1080',
                     screen_width='1920', tz_name='America/Los_Angeles', webcast_language='en')
        if operation == 'profile':
            path = '/api/user/detail/'
            query.update(secUid='', uniqueId=params['username'])
        else:
            path = '/api/user/list/'
            if not params.get('secUid'):
                raise ProtocolError('invalid_response', 'Direct list requests require secUid.')
            query.update(secUid=params['secUid'], count=str(count), minCursor=params['minCursor'],
                         maxCursor='0', scene='67' if operation == 'followers' else '21')
        # Escape delimiters in opaque identifiers BEFORE signing. Keep the browser
        # representation of punctuation in ordinary values; never re-encode the seal.
        pairs = [(k, quote(str(v), safe="()/,:")) for k, v in query.items()]
        with self.sequence_lock:
            sequence = self.sequence
            self.sequence += 1
        token = (cookies or self.cookies).get('msToken', '')
        if any(c in token for c in '&?#\r\n'):
            raise ProtocolError('invalid_session', 'Invalid msToken value.')
        signed, _ = sign(pairs, ua, ms_token=token, sequence=sequence)
        return ORIGIN + path, signed.encode('ascii')


def verified(value):
    return 'Yes✅' if value is True else 'No❌' if value is False else None


def guard(payload):
    if not isinstance(payload, dict):
        raise ProtocolError('invalid_response', 'Direct JSON root must be an object.')
    if not payload:
        raise ProtocolError('empty_response', 'Direct returned an empty object.', retryable=True)
    code = payload.get('statusCode', payload.get('status_code', 0))
    if isinstance(code, str) and code.isdigit():
        code = int(code)
    if payload.get('captcha') or payload.get('verify_event') or code in (10000, 10101):
        raise ProtocolError('risk_control', 'Direct returned a risk-control response.', retryable=True)
    if code not in (0, None):
        # Unknown business codes must not be guessed to mean deleted/private/429.
        raise ProtocolError('api_error', 'Direct reported an upstream business error.')


def normalize_profile(payload):
    guard(payload)
    info = payload.get('userInfo')
    if not isinstance(info, dict) or not isinstance(info.get('user'), dict) or not info['user']:
        raise ProtocolError('empty_response', 'Direct returned no profile user.', retryable=True)
    user = info['user']
    stats = info.get('statsV2') or info.get('stats') or {}
    if not isinstance(stats, dict):
        raise ProtocolError('invalid_response', 'Direct profile statistics are invalid.')
    private = user.get('privateAccount')
    data = dict(profile=user.get('avatarLarger') or user.get('avatarMedium') or user.get('avatarThumb'),
                username=user.get('uniqueId'), fullname=user.get('nickname'), userid=user.get('id'),
                verified=verified(user.get('verified')), private='Private Account' if private is True
                else 'Public Account' if private is False else None,
                followers=stats.get('followerCount'), following=stats.get('followingCount'),
                likes=stats.get('heartCount', stats.get('heart')), video=stats.get('videoCount'),
                secUid=user.get('secUid'), region=user.get('region'), created_time=user.get('createTime'),
                last_change_name=user.get('nickNameModifyTime'), language=user.get('language'),
                open_favorite=None, see_following=None, heart_count=stats.get('heartCount', stats.get('heart')),
                signature=user.get('signature'))
    return {'status': 'ok', 'data': data}


def normalize_page(payload, *, expected_count=None, cursor='0', keep_raw=False):
    guard(payload)
    users = payload.get('userList')
    if not isinstance(users, list):
        raise ProtocolError('invalid_response', 'Direct response is missing userList.')
    more = payload.get('hasMore')
    if type(more) not in (bool, int) or more not in (True, False, 0, 1):
        raise ProtocolError('invalid_response', 'Direct response has invalid or missing hasMore.')
    if not users and (bool(more) or (cursor == '0' and expected_count and expected_count > 0)):
        raise ProtocolError('empty_response', 'Direct returned an unexpectedly empty list.', retryable=True)
    members = []
    for row in users:
        if not isinstance(row, dict) or not isinstance(row.get('user'), dict):
            members.append(None)
            continue
        user = row['user']; stats = row.get('stats') or row.get('statsV2') or {}
        if not isinstance(stats, dict):
            stats = {}
            members.append(None)
        member = dict(uniqueId=user.get('uniqueId'), nickname=user.get('nickname'),
                      avatarThumb=user.get('avatarThumb') or user.get('avatarMedium') or user.get('avatarLarger'),
                      user_id=str(user['id']) if type(user.get('id')) in (int, str) else None,
                      signature=user.get('signature'), privateAccount=user.get('privateAccount')
                      if type(user.get('privateAccount')) is bool else None, verified=verified(user.get('verified')),
                      secUid=user.get('secUid'), followers=stats.get('followerCount'), following=stats.get('followingCount'),
                      videoCount=stats.get('videoCount'), heartCount=stats.get('heartCount', stats.get('heart')))
        if keep_raw:
            member['raw_direct'] = row
        members.append(member)
    return {'users': members, 'hasMore': bool(more), 'minCursor': payload.get('minCursor')}
