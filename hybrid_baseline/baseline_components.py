"""Reproducible OFFLINE component baseline; never a live/service RPS claim.

Run with upstream's documented uv environment after make install:
    uv run --frozen python ../baseline_components.py
Does not patch upstream code, policies, tests, or dependencies.
"""
from __future__ import annotations

import json
import math
import platform
import resource
import statistics
import subprocess
import time
from dataclasses import asdict
from pathlib import Path

from dtk.platforms.tiktok import ADAPTER, endpoints, parser
from dtk.platforms.tiktok import params
from dtk.scheduler.policies import policy_for
from dtk.signing.native.tiktok_sign import sign
from dtk.transport.wreq_transport import (
    DEFAULT_CONNECT_TIMEOUT_SECONDS, DEFAULT_MAX_CLIENTS,
    DEFAULT_POOL_IDLE_SECONDS, DEFAULT_TIMEOUT_SECONDS, POOL_MAX_IDLE_PER_HOST,
)

ROOT = Path(__file__).resolve().parent
UPSTREAM = ROOT / "upstream"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"


def measure(operation, count):
    samples = []
    cpu = time.process_time()
    start = time.perf_counter()
    for _ in range(count):
        lap = time.perf_counter()
        operation()
        samples.append(time.perf_counter() - lap)
    wall = time.perf_counter() - start
    cpu = time.process_time() - cpu
    samples.sort()
    return {
        "iterations": count, "wall_seconds": wall, "cpu_seconds": cpu,
        "cpu_percent_one_core": cpu / wall * 100,
        "component_operations_per_second": count / wall,
        "average_seconds": statistics.mean(samples),
        "median_seconds": statistics.median(samples),
        "p95_seconds": samples[math.ceil(count * .95) - 1],
        "p99_seconds": samples[math.ceil(count * .99) - 1],
    }


def entry(number):
    return {"user": {"id": str(7100000000000000000 + number),
                     "secUid": "synthetic-sec-" + str(number),
                     "uniqueId": "synthetic" + str(number),
                     "nickname": "Synthetic account",
                     "signature": "Offline fixture",
                     "avatarThumb": "https://example.invalid/avatar.jpg",
                     "verified": number % 2 == 0,
                     "privateAccount": number % 3 == 0},
            "stats": {"followerCount": number, "followingCount": 20,
                      "videoCount": 3, "heartCount": 0}}


def pagination_fixture(endpoint, total):
    # These cursor values belong to the SYNTHETIC SERVER fixture. The client
    # follows only the exact cursor parsed from its previous response.
    page_size = params.DEFAULT_PAGE_SIZE
    pages = {}
    cursor = "0"
    for offset in range(0, total, page_size):
        end = min(total, offset + page_size)
        following_cursor = str(1700000000 - end)
        pages[cursor] = {"statusCode": 0,
                         "userList": [entry(i) for i in range(offset, end)],
                         "hasMore": end < total, "minCursor": following_cursor}
        cursor = following_cursor
    expected_ids = {str(7100000000000000000 + i) for i in range(total)}
    cursor = None
    seen, cursors = set(), set()
    count = 0
    start = time.perf_counter()
    cpu = time.process_time()
    while True:
        request = ADAPTER.build_request(endpoint, sec_uid="synthetic-owner", cursor=cursor)
        assert request["url"] == "https://www.tiktok.com/api/user/list/"
        assert request["params"]["scene"] == ("67" if endpoint == endpoints.AUTHOR_FOLLOWERS else "21")
        key = request["params"]["minCursor"]
        assert key == (cursor or "0") and key not in cursors
        cursors.add(key)
        page = ADAPTER.parse_author_list(pages[key])
        ids = [item.uid for item in page.items]
        assert all(isinstance(uid, str) for uid in ids)
        assert len(ids) == len(set(ids)) and not seen.intersection(ids)
        seen.update(ids)
        count += 1
        if not page.has_more:
            assert page.cursor is None
            break
        cursor = page.cursor
    elapsed = time.perf_counter() - start
    assert seen == expected_ids
    return {"scope": "synthetic offline builder/parser, no HTTP or scheduler",
            "endpoint": endpoint, "fixture_records": total,
            "fixture_pages": count, "wall_seconds": elapsed,
            "cpu_seconds": time.process_time() - cpu,
            "parsed_fixture_records_per_second": total / elapsed,
            "no_duplicates": True, "no_skipped_fixture_records": True,
            "exact_cursor_progression": True, "terminal_page_verified": True}


def main():
    assert subprocess.check_output(["git", "diff", "--name-only"], cwd=UPSTREAM).strip() == b""
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=UPSTREAM, text=True).strip()
    profile = {"statusCode": 0, "userInfo": entry(3)}
    author = parser.parse_author(profile)
    assert author.uid == profile["userInfo"]["user"]["id"]
    assert author.sec_uid == "synthetic-sec-3"
    assert author.avatar.url == "https://example.invalid/avatar.jpg"
    assert author.verified is False
    assert author.raw["privateAccount"] is True
    list_payload = {"statusCode": 0, "userList": [entry(i) for i in range(30)],
                    "hasMore": True, "minCursor": "1700000000"}
    page = parser.parse_author_list(list_payload)
    normalized_private = "privateAccount" in author.model_dump() or "private_account" in author.model_dump()
    request_params = params.author_followers_params(sec_uid="synthetic-owner")
    query, signed = sign(list(request_params.items()), UA)
    assert all(key in signed for key in ("X-Dynosaur", "X-Gnarly", "X-Bogus", "msToken"))
    assert signed["msToken"] == "" and signed["X-Bogus"] == "1"
    assert query.endswith("X-Gnarly=" + signed["X-Gnarly"])
    risk = {"empty": parser.detect_risk_control({}),
            "captcha": parser.detect_risk_control({"captcha": True}),
            "status_integer": parser.detect_risk_control({"statusCode": 10000}),
            "status_string": parser.detect_risk_control({"statusCode": "10000"})}
    assert all(risk.values())
    output = {
        "commit": commit, "source_changes": [], "python": platform.python_version(),
        "scope": "OFFLINE COMPONENT MICROBENCHMARK ONLY; not service or live throughput",
        "configuration": {
            "identity_count": 0, "network_workers": 0, "connections": 0, "proxies": 0,
            "page_size": params.DEFAULT_PAGE_SIZE,
            "upstream_policies": [asdict(policy_for(e)) for e in
                                  (endpoints.AUTHOR_PROFILE, endpoints.AUTHOR_FOLLOWERS, endpoints.AUTHOR_FOLLOWING)],
            "transport_defaults": {"timeout_seconds": DEFAULT_TIMEOUT_SECONDS,
                                   "connect_timeout_seconds": DEFAULT_CONNECT_TIMEOUT_SECONDS,
                                   "keepalive_seconds": DEFAULT_POOL_IDLE_SECONDS,
                                   "max_clients": DEFAULT_MAX_CLIENTS,
                                   "idle_connections_per_host": POOL_MAX_IDLE_PER_HOST},
            "scheduler_exercised": False, "retries_exercised": False,
        },
        "checks": {"profile_identifiers_avatar_verified": True,
                   "profile_private_flag_in_raw": True,
                   "profile_private_flag_normalized": normalized_private,
                   "list_member_raw_preserved_by_upstream": page.items[0].raw is not None,
                   "risk_signatures": risk,
                   "note": "Private-account normalization and member raw preservation are integration gaps, not baseline test regressions."},
        "components": {"profile_parser": measure(lambda: parser.parse_author(profile), 10000),
                       "page_parser_30_users": measure(lambda: parser.parse_author_list(list_payload), 1000),
                       "native_signing": measure(lambda: sign(list(request_params.items()), UA), 1000)},
        "pagination": [pagination_fixture(e, total) for e in (endpoints.AUTHOR_FOLLOWERS, endpoints.AUTHOR_FOLLOWING) for total in (65, 10000)],
        "actual_http_requests": 0,
        "live_metrics": {key: None for key in ("successful_requests_per_second", "attempted_requests_per_second", "average_latency", "median_latency", "p95_latency", "p99_latency", "profile_lookups_per_second", "follower_pages_per_second", "following_pages_per_second", "followers_per_second", "following_per_second", "network_bytes_per_second", "error_rate", "timeout_rate", "risk_control_rate")},
        "live_metrics_status": "BLOCKED BY ENVIRONMENT: deployment and identity not configured; component benchmark is not a substitute",
        "peak_rss_bytes_linux": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
    }
    (ROOT / "baseline_evil_benchmark.json").write_text(json.dumps(output, indent=2) + "\n")
    print("PASS: unmodified component parsing/signing, 65- and 10,000-record follower/following fixtures; live metrics unverified.")


if __name__ == "__main__":
    main()
