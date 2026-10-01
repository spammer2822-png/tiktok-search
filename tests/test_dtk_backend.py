"""DTK-only backend regressions. No Docker or live TikTok traffic is used here."""
import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import dtk_backend as d
import tiktok_worker_scanner as s


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
        self.client = d.DtkClient(self.gate, s.ProxyPool([]), settings={**d.DEFAULTS, "dtk_request_attempts": 1})

    async def asyncTearDown(self):
        if self.client.client is not None and hasattr(self.client.client, "aclose"):
            await self.client.client.aclose()

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
