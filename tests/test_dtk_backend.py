"""DTK-only backend regressions. No Docker or live TikTok traffic is used here."""
import asyncio
import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import dtk_backend as d
import tiktok_worker_scanner as s
from scan_statistics import ScanStatistics


def author(name="person1", uid="7100000000000000001", sec_uid="MS4wLjABAAAAfixture"):
    return {
        "platform": "tiktok",
        "uid": uid,
        "sec_uid": sec_uid,
        "unique_id": name,
        "nickname": "Person One",
        "signature": "bio",
        "avatar": {"url": "https://p16-sign.tiktokcdn-us.com/tos-useast5-avt-0068-tx/test.jpeg"},
        "web_url": f"https://www.tiktok.com/@{name}",
        "verified": False,
        "stats": {
            "follower_count": 123,
            "following_count": 45,
            "content_count": 7,
            "total_digg": 900,
        },
        "raw": {"privateAccount": False},
    }


class ConfigTests(unittest.TestCase):
    def test_build_is_strictly_dtk_only(self):
        config = d.migrate_config({
            "backend_mode": "hybrid",
            "direct_session_file": "secret.json",
            "worker_retry_attempts": 4,
        })
        self.assertEqual(config["backend_mode"], "dtk")
        self.assertNotIn("direct_session_file", config)
        self.assertNotIn("worker_retry_attempts", config)
        upgraded = d.migrate_config({"dtk_identity_wait_seconds": 120})
        self.assertEqual(upgraded["dtk_identity_wait_seconds"], 600)
        d.validate(config)
        with self.assertRaises(s.ExporterError):
            d.validate({**config, "backend_mode": "worker"})

    def test_profile_and_member_normalization(self):
        profile = d._profile_from_author(author(), "person1")
        self.assertEqual(profile.uid, "7100000000000000001")
        self.assertEqual(profile.sec_uid, "MS4wLjABAAAAfixture")
        self.assertEqual(profile.follower_count, 123)
        self.assertEqual(profile.following_count, 45)
        self.assertEqual(profile.metadata["backend"], "dtk")
        member = d._member_from_author(author())
        self.assertEqual(member["user_id"], profile.uid)
        self.assertEqual(member["secUid"], profile.sec_uid)
        self.assertIs(member["privateAccount"], False)

    def test_private_profile_is_rejected(self):
        value = author()
        value["raw"]["privateAccount"] = True
        with self.assertRaises(s.PublicProfileRequired):
            d._profile_from_author(value, "person1")


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gate = s.AsyncRequestGate(0, 0, asyncio.Event(), ceiling=8, initial=8, adaptive=False)
        self.client = d.DtkClient(self.gate, s.BackendRuntimeState(), settings={**d.DEFAULTS, "dtk_request_attempts": 1})

    async def asyncTearDown(self):
        if self.client.client is not None and hasattr(self.client.client, "aclose"):
            await self.client.client.aclose()

    async def test_control_request_normalizes_local_connection_failure(self):
        async def handle(request):
            raise httpx.ConnectError("fixture connection refused", request=request)
        self.client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            base_url="http://127.0.0.1:8000",
        )
        with self.assertRaises(d.DtkApiError) as caught:
            await self.client._control_request("GET", "/api/v1/admin/identities/pool")
        self.assertEqual(caught.exception.dtk_code, "INTERNAL")
        self.assertEqual(caught.exception.details["transport"], "connect")

    async def test_auth_none_rate_limit_is_rendered_as_not_reported(self):
        async def handle(request):
            return httpx.Response(200, json={
                "success": True,
                "data": {
                    "user": {"scopes": ["tiktok:read", "identity:manage"]},
                    "rate_limit_per_min": None,
                },
            })
        self.client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            base_url="http://127.0.0.1:8000",
        )
        with patch.object(s, "console") as emit:
            await self.client._validate_key()
        message = emit.call_args.args[0]
        self.assertIn("not reported", message)
        self.assertNotIn("None requests/minute", message)

    async def test_auth_me_reads_scopes_from_user_payload(self):
        async def handle(request):
            self.assertEqual(request.url.path, "/api/v1/auth/me")
            return httpx.Response(200, json={
                "success": True,
                "data": {
                    "user": {"username": "admin", "role": "admin", "scopes": ["tiktok:read", "identity:manage"]},
                    "rate_limit_per_min": 5000,
                    "api_key_id": "fixture",
                },
            })
        self.client.client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://127.0.0.1:8000")
        await self.client._validate_key()
        self.assertIn("tiktok:read", self.client.key_scopes)
        self.assertIn("identity:manage", self.client.key_scopes)

    async def test_auth_me_refuses_key_without_tiktok_scope(self):
        async def handle(request):
            return httpx.Response(200, json={
                "success": True,
                "data": {"user": {"scopes": ["archive:read"]}, "rate_limit_per_min": 120},
            })
        self.client.client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://127.0.0.1:8000")
        with self.assertRaises(s.ExporterError):
            await self.client._validate_key()

    async def test_pool_marks_are_automatically_aligned_for_tiktok_only(self):
        self.client.key_scopes = {"admin", "identity:manage", "tiktok:read"}
        initial = {
            "platforms": [
                {"platform": "tiktok", "usable": 4, "auto": True, "min_size": 1, "target_size": 2},
                {"platform": "douyin", "usable": 2, "auto": True, "min_size": 3, "target_size": 8},
            ]
        }
        updated = {
            "platforms": [
                {"platform": "tiktok", "usable": 4, "auto": True, "min_size": 3, "target_size": 8},
                {"platform": "douyin", "usable": 2, "auto": False, "min_size": 0, "target_size": 8},
            ]
        }
        self.client._admin_get = AsyncMock(side_effect=[initial, updated])
        self.client._admin_put = AsyncMock(return_value={})
        await self.client._ensure_identity_pool()
        calls = [(c.args[0], c.args[1]) for c in self.client._admin_put.await_args_list]
        self.assertIn(("/api/v1/admin/settings/pool.tiktok.min_size", 3), calls)
        self.assertIn(("/api/v1/admin/settings/pool.tiktok.target_size", 8), calls)
        self.assertIn(("/api/v1/admin/settings/pool.douyin.min_size", 0), calls)

    async def test_proxy_inventory_excludes_bound_reserved_unhealthy_and_cooling_exits(self):
        now = time.monotonic()
        self.client.identity_task_proxies["running-task"] = "proxy-reserved"
        self.client.identity_proxy_cooldown_until["proxy-cooling"] = now + 120
        self.client._admin_get = AsyncMock(side_effect=[
            [
                {"id": "proxy-free", "healthy": True, "decryptable": True, "country": "GB"},
                {"id": "proxy-bound", "healthy": True, "decryptable": True, "country": "FR"},
                {"id": "proxy-reserved", "healthy": True, "decryptable": True, "country": "CA"},
                {"id": "proxy-cooling", "healthy": True, "decryptable": True, "country": "PT"},
                {"id": "proxy-bad", "healthy": False, "decryptable": True, "country": "US"},
            ],
            [
                {"proxy_id": "proxy-bound", "state": "active"},
            ],
        ])
        inventory = await self.client._mint_proxy_inventory()
        self.assertEqual([p["id"] for p in inventory["candidates"]], ["proxy-free"])
        self.assertEqual(inventory["bound"], 1)
        self.assertEqual(inventory["reserved"], 1)
        self.assertEqual(inventory["healthy"], 4)
        self.assertEqual(len(inventory["cooling"]), 1)

    async def test_partial_mint_batch_is_returned_if_later_submission_hits_rate_limit(self):
        self.client.key_scopes = {"admin", "identity:manage", "tiktok:read"}
        self.client._mint_proxy_inventory = AsyncMock(return_value={
            "configured": 2,
            "healthy": 2,
            "bound": 0,
            "reserved": 0,
            "cooling": [],
            "candidates": [
                {"id": "proxy-a", "healthy": True, "decryptable": True},
                {"id": "proxy-b", "healthy": True, "decryptable": True},
            ],
        })
        attempts = 0

        async def management(method, path, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return {"task_ids": ["task-a"], "count": 1}, SimpleNamespace(status_code=202)
            raise d.DtkApiError("RATE_LIMITED", "wait", retry_after=9)

        self.client._management_data = AsyncMock(side_effect=management)
        self.client._record_dtk_error = AsyncMock()
        tasks = await self.client._request_identity_mint(2, reason="partial")
        self.assertEqual(tasks, ["task-a"])
        self.assertEqual(self.client.identity_task_proxies["task-a"], "proxy-a")
        self.client._record_dtk_error.assert_awaited_once()

    async def test_request_identity_mint_rotates_across_distinct_healthy_proxies(self):
        self.client.key_scopes = {"admin", "identity:manage", "tiktok:read"}
        self.client._mint_proxy_inventory = AsyncMock(return_value={
            "configured": 3,
            "healthy": 3,
            "bound": 0,
            "cooling": [],
            "candidates": [
                {"id": "proxy-a", "healthy": True, "decryptable": True, "country": "GB"},
                {"id": "proxy-b", "healthy": True, "decryptable": True, "country": "FR"},
                {"id": "proxy-c", "healthy": True, "decryptable": True, "country": "CA"},
            ],
        })
        calls = []

        async def management(method, path, **kwargs):
            calls.append((method, path, kwargs))
            proxy_id = kwargs["json"]["proxy_id"]
            task_id = {"proxy-a": "task-a", "proxy-b": "task-b"}[proxy_id]
            return {"task_ids": [task_id], "count": 1}, SimpleNamespace(status_code=202)

        self.client._management_data = AsyncMock(side_effect=management)
        task_ids = await self.client._request_identity_mint(2, reason="fixture")
        self.assertEqual(task_ids, ["task-a", "task-b"])
        self.assertEqual(self.client.identity_task_states["task-a"], "submitted")
        self.assertEqual(self.client.identity_task_proxies["task-a"], "proxy-a")
        self.assertEqual(self.client.identity_task_proxies["task-b"], "proxy-b")
        self.assertEqual(calls[0][2]["json"], {"platform": "tiktok", "count": 1, "proxy_id": "proxy-a"})
        self.assertEqual(calls[1][2]["json"], {"platform": "tiktok", "count": 1, "proxy_id": "proxy-b"})

    async def test_request_identity_mint_uses_direct_only_when_no_proxies_configured(self):
        self.client.key_scopes = {"admin", "identity:manage", "tiktok:read"}
        self.client._mint_proxy_inventory = AsyncMock(return_value={
            "configured": 0,
            "healthy": 0,
            "bound": 0,
            "cooling": [],
            "candidates": [],
        })
        payloads = []

        async def management(method, path, **kwargs):
            payloads.append(kwargs["json"])
            task_id = f"task-{len(payloads)}"
            return {"task_ids": [task_id], "count": 1}, SimpleNamespace(status_code=202)

        self.client._management_data = AsyncMock(side_effect=management)
        task_ids = await self.client._request_identity_mint(2, reason="direct fixture")
        self.assertEqual(task_ids, ["task-1", "task-2"])
        self.assertEqual(payloads, [
            {"platform": "tiktok", "count": 1},
            {"platform": "tiktok", "count": 1},
        ])

    async def test_failed_proxy_mint_is_quarantined_and_mapping_removed(self):
        self.client.identity_task_proxies["fixture"] = "proxy-bad"
        self.client.identity_proxy_info["proxy-bad"] = {"id": "proxy-bad", "country": "IN"}
        self.client._record_dtk_error = AsyncMock()
        self.client._identity_task_view = AsyncMock(return_value={
            "state": "failed",
            "error": {"code": "INTERNAL", "message": "minting produced no identity"},
        })
        pending = {"fixture"}
        with patch.object(s, "console"):
            succeeded, failed, _retry = await self.client._poll_identity_tasks(pending)
        self.assertEqual(succeeded, [])
        self.assertEqual(len(failed), 1)
        self.assertEqual(pending, set())
        self.assertIn("proxy-bad", self.client.identity_proxy_cooldown_until)
        self.assertNotIn("fixture", self.client.identity_task_proxies)

    async def test_configured_but_unavailable_proxies_do_not_fall_back_to_direct(self):
        self.client.key_scopes = {"admin", "identity:manage", "tiktok:read"}
        self.client._mint_proxy_inventory = AsyncMock(return_value={
            "configured": 12,
            "healthy": 8,
            "bound": 2,
            "cooling": [("proxy-a", time.monotonic() + 30)],
            "candidates": [],
        })
        with self.assertRaises(d.DtkApiError) as caught:
            await self.client._request_identity_mint(1, reason="fixture")
        self.assertEqual(caught.exception.dtk_code, "QUEUE_FULL")
        self.assertEqual(caught.exception.details["reason"], "no_eligible_proxy")

    async def test_management_rate_limit_creates_one_shared_retry_after_window(self):
        calls = 0

        async def handle(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(
                    429,
                    json={
                        "success": False,
                        "error": {"code": "RATE_LIMITED", "message": "wait", "retry_after": 7},
                    },
                )
            return httpx.Response(200, json={"success": True, "data": {"ok": True}})

        self.client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            base_url="http://127.0.0.1:8000",
        )
        self.client.gate.wait = AsyncMock()
        with self.assertRaises(d.DtkApiError):
            await self.client._management_data("GET", "/first")
        self.assertGreater(self.client.identity_control_blocked_until, time.monotonic())
        await self.client._management_data("GET", "/second")
        self.client.gate.wait.assert_awaited()
        self.assertGreaterEqual(self.client.gate.wait.await_args.args[0], 0)

    async def test_auto_mint_disabled_does_not_bypass_hard_minimum(self):
        self.client.settings["dtk_auto_mint"] = False
        self.client.settings["dtk_min_usable_identities"] = 5
        self.client.settings["dtk_target_identities"] = 8
        self.client.key_scopes = {"tiktok:read"}
        self.client._identity_pool_row = AsyncMock(return_value=(
            {"platforms": [], "activity": {"current": None, "recent": [], "backoff": None}},
            {"platform": "tiktok", "usable": 1, "min_size": 5, "target_size": 8, "auto": False},
        ))
        self.client._emit_identity_activity = AsyncMock()
        with self.assertRaisesRegex(s.ExporterError, "automatic minting is disabled"):
            await self.client._ensure_identity_pool()

    async def test_identity_supervisor_queues_shortfall_before_ready(self):
        self.client.key_scopes = {"admin", "identity:manage", "tiktok:read"}
        self.client.settings["dtk_min_usable_identities"] = 5
        self.client.settings["dtk_target_identities"] = 8
        pool1 = {"platforms": [], "activity": {"current": None, "recent": [], "backoff": None}}
        row1 = {"platform": "tiktok", "usable": 1, "min_size": 5, "target_size": 8, "auto": True}
        pool5 = {"platforms": [], "activity": {"current": None, "recent": [], "backoff": None}}
        row5 = {"platform": "tiktok", "usable": 5, "min_size": 5, "target_size": 8, "auto": True}
        self.client._identity_pool_row = AsyncMock(side_effect=[(pool1, row1), (pool5, row5)])
        self.client._poll_identity_tasks = AsyncMock(return_value=([], [], 0.0))
        self.client._request_identity_mint = AsyncMock(return_value=["a", "b", "c", "d"])
        self.client._emit_identity_activity = AsyncMock()
        with patch.object(d.asyncio, "sleep", new=AsyncMock()):
            row = await self.client._wait_for_minimum_identities(
                5, initial_usable=1, reason="test startup"
            )
        self.assertEqual(row["usable"], 5)
        self.client._request_identity_mint.assert_awaited_once_with(
            4, reason="test startup; need 5, currently 1"
        )

    async def test_identity_supervisor_replaces_failed_mint_task(self):
        self.client.key_scopes = {"admin", "identity:manage", "tiktok:read"}
        self.client.settings["dtk_min_usable_identities"] = 2
        self.client.settings["dtk_target_identities"] = 8
        self.client.settings["dtk_identity_wait_seconds"] = 600
        requests = []

        async def mint(count, *, reason):
            task_id = f"task{len(requests) + 1}"
            requests.append((count, reason, task_id))
            self.client.identity_task_states[task_id] = "submitted"
            return [task_id]

        async def poll(pending):
            if "task1" in pending:
                pending.remove("task1")
                return [], [d.DtkApiError(
                    "INTERNAL",
                    "browser temporarily unavailable",
                    details={"reason": "rpc_unavailable"},
                )], 0.0
            if "task2" in pending:
                pending.remove("task2")
                return ["task2"], [], 0.0
            return [], [], 0.0

        async def pool_row():
            usable = 2 if len(requests) >= 2 else 1
            return (
                {"platforms": [], "activity": {"current": None, "recent": [], "backoff": None}},
                {"platform": "tiktok", "usable": usable, "min_size": 2, "target_size": 8, "auto": True},
            )

        self.client._request_identity_mint = AsyncMock(side_effect=mint)
        self.client._poll_identity_tasks = AsyncMock(side_effect=poll)
        self.client._identity_pool_row = AsyncMock(side_effect=pool_row)
        self.client._emit_identity_activity = AsyncMock()
        ticks = iter(range(0, 5000, 10))
        fake_time = SimpleNamespace(monotonic=lambda: next(ticks))
        with patch.object(d, "time", fake_time), patch.object(d.asyncio, "sleep", new=AsyncMock()):
            row = await self.client._wait_for_minimum_identities(
                2, initial_usable=1, reason="replacement test"
            )
        self.assertEqual(row["usable"], 2)
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0][0], 1)
        self.assertEqual(requests[1][0], 1)

    async def test_poll_identity_task_surfaces_failed_task_details(self):
        async def handle(request):
            self.assertTrue(request.url.path.startswith("/api/v1/tasks/"))
            return httpx.Response(200, json={
                "success": True,
                "data": {
                    "task_id": "fixture",
                    "state": "failed",
                    "endpoint": "identity.mint",
                    "error": {
                        "code": "INTERNAL",
                        "message": "minting produced no identity",
                        "details": {"reason": "rpc_unavailable", "platform": "tiktok"},
                    },
                },
            })
        self.client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            base_url="http://127.0.0.1:8000",
        )
        self.client._record_dtk_error = AsyncMock()
        pending = {"fixture"}
        succeeded, failed, _retry = await self.client._poll_identity_tasks(pending)
        self.assertEqual(succeeded, [])
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].details["reason"], "rpc_unavailable")
        self.assertEqual(pending, set())
        self.client._record_dtk_error.assert_awaited_once()

    def test_no_free_proxy_mint_failure_is_actionable_and_fatal(self):
        exc = d.DtkApiError(
            "INTERNAL",
            "minting produced no identity",
            details={"reason": "no_free_proxy", "platform": "tiktok"},
        )
        message = self.client._mint_failure_action(exc)
        self.assertIn("no unused proxy", message)
        self.assertIn("lower the scanner minimum", message)

    async def test_identity_self_heal_waits_for_hard_minimum(self):
        self.client.key_scopes = {"admin", "identity:manage", "tiktok:read"}
        self.client.settings["dtk_min_usable_identities"] = 5
        self.client.settings["dtk_target_identities"] = 8
        self.client._identity_pool_row = AsyncMock(return_value=(
            {"platforms": [], "activity": {"current": None, "recent": [], "backoff": None}},
            {"platform": "tiktok", "usable": 2},
        ))
        self.client._emit_identity_activity = AsyncMock()
        self.client._wait_for_minimum_identities = AsyncMock()
        await self.client._repair_identity_pool("IDENTITY_POOL_EXHAUSTED")
        self.client._wait_for_minimum_identities.assert_awaited_once_with(
            5,
            initial_usable=2,
            reason="runtime self-heal after IDENTITY_POOL_EXHAUSTED",
        )

    async def test_identity_error_in_request_triggers_self_heal_before_retry(self):
        attempts = 0

        async def handle(request):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return httpx.Response(
                    503,
                    json={
                        "success": False,
                        "error": {
                            "code": "IDENTITY_POOL_EXHAUSTED",
                            "message": "no usable identity",
                            "retry_after": 0,
                        },
                    },
                )
            return httpx.Response(
                200,
                json={"success": True, "data": author()},
            )

        self.client.settings["dtk_request_attempts"] = 2
        self.client.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            base_url="http://127.0.0.1:8000",
        )
        self.client._repair_identity_pool = AsyncMock()
        with patch.object(self.client.gate, "wait", new=AsyncMock()):
            data = await self.client._call("/api/v1/tiktok/user", {"url": "x"}, "profile")
        self.assertEqual(data["unique_id"], "person1")
        self.client._repair_identity_pool.assert_awaited_once_with("IDENTITY_POOL_EXHAUSTED")

    async def test_dtk_errors_are_saved_to_structured_log(self):
        with tempfile.TemporaryDirectory() as temp:
            self.client.output_directory = Path(temp)
            exc = d.DtkApiError(
                "IDENTITY_POOL_EXHAUSTED",
                "fixture identity failure",
                status=503,
                retry_after=2,
                request_id="fixture-request",
            )
            with patch.object(s, "console") as emit:
                await self.client._record_dtk_error(exc, "identity preflight")
            path = Path(temp) / "dtk_errors.jsonl"
            self.assertTrue(path.is_file())
            payload = __import__("json").loads(path.read_text(encoding="utf-8").strip())
            self.assertEqual(payload["context"], "identity preflight")
            self.assertEqual(payload["code"], "IDENTITY_POOL_EXHAUSTED")
            self.assertEqual(payload["request_id"], "fixture-request")
            emit.assert_called_once()
            self.assertTrue(emit.call_args.kwargs["error"])

    def test_scan_statistics_start_hidden_until_dtk_preflight_finishes(self):
        with tempfile.TemporaryDirectory() as temp:
            stats = ScanStatistics(Path(temp), 2000)
            summary = {
                "remaining_profiles": 10,
                "current_phase": 1,
                "total_profiles": 10,
                "processed_profiles": 0,
                "discovered_profiles": 0,
                "completed_overall": 0,
                "failed_profiles": 0,
                "statuses": {"pending": 10},
            }
            before = stats.snapshot(summary)
            self.assertFalse(before["scan_requests_enabled"])
            stats.scan_requests_enabled = True
            stats.scan_started_at = "fixture"
            after = stats.snapshot(summary)
            self.assertTrue(after["scan_requests_enabled"])
            self.assertEqual(after["scan_started_at"], "fixture")

    async def test_profile_and_lists_use_only_dtk_routes(self):
        self.client._call = AsyncMock(return_value=author())
        profile = await self.client.lookup_profile("person1")
        args = self.client._call.await_args.args
        self.assertEqual(args[0], "/api/v1/tiktok/user")
        self.assertEqual(profile.metadata["backend"], "dtk")

        self.client._call = AsyncMock(return_value={
            "items": [author("person2", "7100000000000000002", "MS4wLjABBBBBfixture")],
            "cursor": "opaque-next",
            "has_more": True,
        })
        page = await self.client.fetch_page(profile, "followers", "opaque-old", 2)
        args = self.client._call.await_args.args
        self.assertEqual(args[0], "/api/v1/tiktok/user/followers")
        self.assertEqual(args[1]["sec_user_id"], profile.sec_uid)
        self.assertEqual(args[1]["cursor"], "opaque-old")
        self.assertEqual(args[1]["count"], 35)
        self.assertEqual(page.min_cursor, "opaque-next")
        self.assertTrue(page.has_more)
        self.assertEqual(page.records[0]["uniqueId"], "person2")

    async def test_key_loader_prefers_environment_and_never_needs_a_repo_secret(self):
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {"DTK_API_KEY": "dtk_fixture_secret"}, clear=False):
            value = await d.load_api_key({**d.DEFAULTS, "dtk_api_key_file": str(Path(temp) / "missing.txt")})
        self.assertEqual(value, "dtk_fixture_secret")


if __name__ == "__main__":
    unittest.main()
