import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


YOUTUBE_WATCH_URL = "https://www.youtube.com/watch"
INITIAL_DATA_MARKERS = (
    "var ytInitialData = ",
    'window["ytInitialData"] = ',
    "ytInitialData = ",
)


def _extract_initial_data(html: str) -> dict | None:
    decoder = json.JSONDecoder()
    for marker in INITIAL_DATA_MARKERS:
        marker_index = html.find(marker)
        if marker_index < 0:
            continue
        try:
            value, _ = decoder.raw_decode(html[marker_index + len(marker) :])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return None


def _walk_dicts(value) -> Iterator[dict]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _channel_id_from_list_item(item: dict) -> str | None:
    view_model = item.get("listItemViewModel", item)
    title = view_model.get("title", {})
    for command_run in title.get("commandRuns", []):
        command = (
            command_run.get("onTap", {})
            .get("innertubeCommand", {})
            .get("browseEndpoint", {})
        )
        channel_id = command.get("browseId")
        if isinstance(channel_id, str) and channel_id.startswith("UC"):
            return channel_id

    leading_accessory = view_model.get("leadingAccessory", {})
    for node in _walk_dicts(leading_accessory):
        channel_id = node.get("browseId")
        if isinstance(channel_id, str) and channel_id.startswith("UC"):
            return channel_id
    return None


def extract_youtube_collaborator_channel_ids(
    html: str,
    owner_channel_id: str,
) -> list[str] | None:
    initial_data = _extract_initial_data(html)
    if initial_data is None:
        return None

    owner_renderer = next(
        (
            node["videoOwnerRenderer"]
            for node in _walk_dicts(initial_data)
            if isinstance(node.get("videoOwnerRenderer"), dict)
        ),
        None,
    )
    if owner_renderer is None:
        return None

    show_dialog = (
        owner_renderer.get("navigationEndpoint", {})
        .get("showDialogCommand", {})
    )
    if not show_dialog:
        return []

    list_items = (
        show_dialog.get("panelLoadingStrategy", {})
        .get("inlineContent", {})
        .get("dialogViewModel", {})
        .get("customContent", {})
        .get("listViewModel", {})
        .get("listItems")
    )
    if not isinstance(list_items, list):
        return None

    collaborator_ids = []
    for item in list_items:
        channel_id = _channel_id_from_list_item(item)
        if (
            channel_id
            and channel_id != owner_channel_id
            and channel_id not in collaborator_ids
        ):
            collaborator_ids.append(channel_id)
    return collaborator_ids


def fetch_youtube_collaborator_channel_ids(
    video_id: str,
    owner_channel_id: str,
    timeout_seconds: float = 20,
) -> list[str] | None:
    try:
        response = requests.get(
            YOUTUBE_WATCH_URL,
            params={"v": video_id, "hl": "en"},
            headers={
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/137.0.0.0 Safari/537.36"
                ),
            },
            timeout=timeout_seconds,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(
            f"[YouTube] Collaborator page error for {video_id}: {exc}"
        )
        return None

    collaborator_ids = extract_youtube_collaborator_channel_ids(
        response.text,
        owner_channel_id,
    )
    if collaborator_ids is None:
        print(
            f"[YouTube] Collaborator metadata unavailable for {video_id}"
        )
    return collaborator_ids


def fetch_youtube_collaborators(
    video_owners: dict[str, str],
    timeout_seconds: float = 20,
    max_workers: int = 8,
) -> dict[str, list[str] | None]:
    if not video_owners:
        return {}

    worker_count = max(1, min(max_workers, len(video_owners)))
    results = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                fetch_youtube_collaborator_channel_ids,
                video_id,
                owner_channel_id,
                timeout_seconds,
            ): video_id
            for video_id, owner_channel_id in video_owners.items()
        }
        for future in as_completed(futures):
            video_id = futures[future]
            try:
                results[video_id] = future.result()
            except Exception as exc:
                print(
                    f"[YouTube] Collaborator refresh failed for "
                    f"{video_id}: {exc}"
                )
                results[video_id] = None
    return results
