"""Pure YouTube thumbnail URL selection and validation.

This module deliberately performs no network or filesystem I/O. Thumbnail bytes are
owned by the user's browser; the pipeline persists only allow-listed HTTPS URLs.
"""

from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

from .collection import isoformat_utc, sanitize_json_value


_VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_THUMBNAIL_HOSTS = frozenset(
    {"i.ytimg.com", "img.youtube.com", *(f"i{index}.ytimg.com" for index in range(1, 10))}
)
_THUMBNAIL_PATH_PREFIXES = ("/vi/", "/vi_webp/", "/an_webp/")


def is_allowed_youtube_thumbnail_url(value: Any) -> bool:
    """Return whether *value* is a public, allow-listed YouTube thumbnail URL."""

    if not isinstance(value, str) or not value.strip():
        return False
    try:
        parsed = urlsplit(value.strip())
        port = parsed.port
    except (TypeError, ValueError):
        return False
    return bool(
        parsed.scheme.lower() == "https"
        and parsed.hostname
        and parsed.hostname.lower() in _THUMBNAIL_HOSTS
        and parsed.username is None
        and parsed.password is None
        and port in {None, 443}
        and parsed.path.startswith(_THUMBNAIL_PATH_PREFIXES)
    )


def safe_youtube_thumbnail_url(value: Any) -> str | None:
    """Return a normalized display-safe URL, or ``None`` without fetching it."""

    return value.strip() if is_allowed_youtube_thumbnail_url(value) else None


def deterministic_thumbnail_url(video_id: Any) -> str | None:
    """Build the no-network ``img.youtube.com`` fallback for a safe video id."""

    normalized = str(video_id or "").strip()
    if not _VIDEO_ID.fullmatch(normalized):
        return None
    return f"https://img.youtube.com/vi/{normalized}/default.jpg"


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


@dataclass(frozen=True)
class ThumbnailReference:
    """Serializable reference to a remote thumbnail; never contains image data."""

    url: str | None
    width: int | None = None
    height: int | None = None
    source: str | None = None
    available: bool | None = None
    updated_at: str | None = None

    def to_event_fields(self) -> dict[str, Any]:
        return sanitize_json_value(
            {
                f"thumbnail_{key}": value
                for key, value in asdict(self).items()
            }
        )


def select_thumbnail_reference(
    thumbnails: Iterable[Mapping[str, Any]] | None,
    *,
    video_id: Any,
    updated_at: datetime | str | None = None,
    source: str = "youtube_metadata",
) -> ThumbnailReference:
    """Choose the largest valid metadata URL, then the deterministic URL fallback."""

    candidates: list[tuple[float, int, str, int | None, int | None]] = []
    for index, item in enumerate(thumbnails or ()):
        if not isinstance(item, Mapping):
            continue
        url = safe_youtube_thumbnail_url(item.get("url"))
        if not url:
            continue
        width = _positive_int(item.get("width"))
        height = _positive_int(item.get("height"))
        area = float(width * height) if width and height else -math.inf
        candidates.append((area, index, url, width, height))

    timestamp = isoformat_utc(updated_at) if isinstance(updated_at, datetime) else updated_at
    if candidates:
        _, _, url, width, height = max(candidates, key=lambda item: (item[0], item[1]))
        return ThumbnailReference(
            url=url,
            width=width,
            height=height,
            source=source,
            available=True,
            updated_at=str(timestamp) if timestamp else None,
        )

    fallback = deterministic_thumbnail_url(video_id)
    return ThumbnailReference(
        url=fallback,
        source="img.youtube.com_fallback" if fallback else None,
        available=True if fallback else None,
        updated_at=str(timestamp) if timestamp else None,
    )


def thumbnail_url_only_metadata(info: Mapping[str, Any]) -> dict[str, Any]:
    """Drop unsafe thumbnail representations from an otherwise raw metadata object."""

    rejected_thumbnail_keys = {
        "thumbnail_base64",
        "thumbnail_bytes",
        "thumbnail_data",
        "thumbnail_file",
        "thumbnail_filename",
        "thumbnail_path",
        "thumbnails_base64",
        "thumbnails_bytes",
        "thumbnails_data",
        "thumbnails_files",
    }
    sanitized = {
        key: value
        for key, value in info.items()
        if str(key).lower() not in rejected_thumbnail_keys
    }
    sanitized["thumbnail"] = safe_youtube_thumbnail_url(info.get("thumbnail"))
    sanitized["thumbnails"] = [
        {
            "url": url,
            "width": item.get("width"),
            "height": item.get("height"),
        }
        for item in info.get("thumbnails") or ()
        if isinstance(item, Mapping)
        and (url := safe_youtube_thumbnail_url(item.get("url")))
    ]
    return sanitized
