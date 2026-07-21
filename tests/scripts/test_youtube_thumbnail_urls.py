import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from common.youtube_thumbnails import (
    deterministic_thumbnail_url,
    is_allowed_youtube_thumbnail_url,
    safe_youtube_thumbnail_url,
    select_thumbnail_reference,
    thumbnail_url_only_metadata,
)


class YouTubeThumbnailUrlTests(unittest.TestCase):
    def test_metadata_url_has_priority_and_dimensions_are_preserved(self):
        reference = select_thumbnail_reference(
            [
                {"url": "https://i.ytimg.com/vi/abc/default.jpg", "width": 120, "height": 90},
                {"url": "https://i.ytimg.com/vi/abc/maxresdefault.jpg", "width": 1280, "height": 720},
            ],
            video_id="abc",
            updated_at=datetime(2026, 7, 21, tzinfo=timezone.utc),
            source="yt-dlp",
        )
        self.assertEqual(reference.url, "https://i.ytimg.com/vi/abc/maxresdefault.jpg")
        self.assertEqual((reference.width, reference.height), (1280, 720))
        self.assertEqual(reference.source, "yt-dlp")

    def test_missing_or_unsafe_metadata_uses_deterministic_fallback(self):
        for thumbnails in (None, [], [{"url": "https://127.0.0.1/private.jpg"}]):
            with self.subTest(thumbnails=thumbnails):
                reference = select_thumbnail_reference(thumbnails, video_id="abc_123")
                self.assertEqual(
                    reference.url,
                    "https://img.youtube.com/vi/abc_123/default.jpg",
                )
                self.assertEqual(reference.source, "img.youtube.com_fallback")

    def test_only_allowlisted_https_thumbnail_urls_are_display_safe(self):
        self.assertTrue(is_allowed_youtube_thumbnail_url("https://i.ytimg.com/vi/x/a.jpg"))
        self.assertTrue(is_allowed_youtube_thumbnail_url("https://i9.ytimg.com/vi/x/a.jpg"))
        for value in (
            "http://i.ytimg.com/vi/x/a.jpg",
            "https://example.com/vi/x/a.jpg",
            "https://localhost/vi/x/a.jpg",
            "https://127.0.0.1/vi/x/a.jpg",
            "https://i.ytimg.com.evil.test/vi/x/a.jpg",
            "https://i10.ytimg.com/vi/x/a.jpg",
            "https://user@i.ytimg.com/vi/x/a.jpg",
        ):
            with self.subTest(value=value):
                self.assertIsNone(safe_youtube_thumbnail_url(value))

    def test_selection_performs_no_http_request_and_creates_no_image_file(self):
        with tempfile.TemporaryDirectory() as directory:
            before = set(Path(directory).iterdir())
            with patch("urllib.request.urlopen") as urlopen:
                reference = select_thumbnail_reference(None, video_id="abc")
            self.assertEqual(before, set(Path(directory).iterdir()))
            urlopen.assert_not_called()
            self.assertEqual(reference.url, deterministic_thumbnail_url("abc"))

    def test_event_fields_contain_no_binary_or_base64_image(self):
        fields = select_thumbnail_reference(None, video_id="abc").to_event_fields()
        encoded = json.dumps(fields)
        self.assertIsInstance(fields["thumbnail_url"], str)
        self.assertNotIn("data:image", encoded)
        self.assertNotIn("base64", encoded.lower())
        self.assertFalse(any(isinstance(value, bytes) for value in fields.values()))

    def test_intermediate_metadata_keeps_only_safe_url_dimensions(self):
        sanitized = thumbnail_url_only_metadata(
            {
                "title": "video",
                "thumbnail": "data:image/png;base64,AAAA",
                "thumbnail_bytes": b"not-persisted",
                "thumbnail_base64": "AAAA",
                "thumbnail_path": "/tmp/thumbnail.jpg",
                "thumbnails": [
                    {
                        "url": "https://i.ytimg.com/vi/abc/default.jpg",
                        "width": 120,
                        "height": 90,
                        "data": b"not-persisted",
                    },
                    {"url": "https://example.test/image.jpg", "data": "unsafe"},
                ],
            }
        )
        self.assertIsNone(sanitized["thumbnail"])
        self.assertNotIn("thumbnail_bytes", sanitized)
        self.assertNotIn("thumbnail_base64", sanitized)
        self.assertNotIn("thumbnail_path", sanitized)
        self.assertEqual(
            sanitized["thumbnails"],
            [
                {
                    "url": "https://i.ytimg.com/vi/abc/default.jpg",
                    "width": 120,
                    "height": 90,
                }
            ],
        )
        self.assertNotIn("base64", json.dumps(sanitized).lower())


if __name__ == "__main__":
    unittest.main()
