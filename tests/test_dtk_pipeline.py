"""Durable pipeline tests for the DTK-only scanner."""
import asyncio
import tempfile
import unittest
from pathlib import Path

import tiktok_worker_scanner as s


def profile():
    return s.ProfileSnapshot(
        username="seed",
        uid="7100000000000000001",
        display_name="Seed",
        sec_uid="MS4wLjABAAAAseed",
        profile_url="https://www.tiktok.com/@seed",
        private_account=False,
        verified=False,
        avatar_url="",
        follower_count=3,
        following_count=0,
        likes_count=0,
        video_count=0,
        advertised_counts={"followers": 3, "following": 0},
        following_visible=True,
        metadata={"backend": "dtk"},
        raw_user={},
    )


def member(name, uid):
    return {
        "uniqueId": name,
        "nickname": name.title(),
        "avatarThumb": "",
        "user_id": str(uid),
        "secUid": "MS4wLjAB" + name,
        "signature": "",
        "privateAccount": False,
        "verified": "No❌",
    }


class FakeDtkClient:
    bootstrap_mode = False
    settings = {"resolved_target_uid": ""}
    def __init__(self, pages):
        self.pages = iter(pages)
        self.calls = []
    async def fetch_page(self, p, list_name, cursor, page_number):
        self.calls.append((list_name, cursor, page_number))
        value = next(self.pages)
        if isinstance(value, Exception):
            raise value
        return value


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_deduplicates_pages_and_persists_terminal_cursor(self):
        first = s.BatchResponse(
            records=[member("one", 1), member("wanted", 2)],
            has_more=True,
            min_cursor="opaque-next",
        )
        second = s.BatchResponse(
            records=[member("wanted", 2), member("three", 3)],
            has_more=False,
            min_cursor="",
        )
        client = FakeDtkClient([first, second])
        matches = []
        async def matched(name, row):
            matches.append((name, row["username"]))
        with tempfile.TemporaryDirectory() as temp:
            with s.UserStore(Path(temp) / "members.sqlite3") as store:
                result = await s.export_one_list(
                    client, store, profile(), "followers",
                    target_username="wanted", on_target_match=matched,
                    stop_event=asyncio.Event(),
                )
                self.assertTrue(result.complete)
                self.assertEqual(result.unique_records_saved, 3)
                self.assertEqual(result.duplicates_ignored, 1)
                self.assertEqual(store.count("followers"), 3)
                self.assertEqual(matches, [("followers", "wanted")])
                checkpoint = store.checkpoint("followers")
                self.assertTrue(checkpoint["result"]["endpoint_exhausted"])

                # A completed chain is reusable without another DTK request.
                again = FakeDtkClient([])
                resumed = await s.export_one_list(
                    again, store, profile(), "followers",
                    target_username="wanted", stop_event=asyncio.Event(),
                )
                self.assertTrue(resumed.complete)
                self.assertEqual(again.calls, [])

    async def test_failure_keeps_committed_page_and_cursor_for_resume(self):
        first = s.BatchResponse(
            records=[member("one", 1), member("two", 2)],
            has_more=True,
            min_cursor="resume-here",
        )
        failure = s.ScannerApiError("temporary DTK failure", kind="network_error", retryable=True)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "members.sqlite3"
            with s.UserStore(path) as store:
                client = FakeDtkClient([first, failure])
                result = await s.export_one_list(
                    client, store, profile(), "followers",
                    target_username="nobody", stop_event=asyncio.Event(),
                )
                self.assertFalse(result.complete)
                self.assertEqual(result.stop_reason, "network_error")
                self.assertEqual(store.count("followers"), 2)
                checkpoint = store.checkpoint("followers")
                self.assertEqual(checkpoint["next_cursor"], "resume-here")

            with s.UserStore(path) as store:
                final = s.BatchResponse(
                    records=[member("three", 3)],
                    has_more=False,
                    min_cursor="",
                )
                client = FakeDtkClient([final])
                result = await s.export_one_list(
                    client, store, profile(), "followers",
                    target_username="nobody", stop_event=asyncio.Event(),
                )
                self.assertTrue(result.complete)
                self.assertEqual(client.calls[0][1], "resume-here")
                self.assertEqual(store.count("followers"), 3)


if __name__ == "__main__":
    unittest.main()
