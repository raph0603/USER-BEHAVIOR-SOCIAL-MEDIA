from __future__ import annotations

import random
import re
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "x_urls.txt"

CDP_URL = "http://127.0.0.1:9222"

SCROLL_ROUNDS_PER_QUERY = 18
SCROLL_PIXELS_MIN = 1600
SCROLL_PIXELS_MAX = 2600
SCROLL_WAIT_MIN_MS = 1800
SCROLL_WAIT_MAX_MS = 3200

SEARCH_QUERIES = [
    '(electric vehicle OR EV OR "electric car") lang:en -filter:replies min_faves:20 since:2025-01-01',
    '(Tesla OR "Tesla Europe" OR Model Y OR Model 3) lang:en -filter:replies min_faves:20 since:2025-01-01',
    '("EV charging" OR "charging network" OR Supercharger) lang:en -filter:replies min_faves:10 since:2025-01-01',
    '("battery degradation" OR "EV battery" OR "battery range") lang:en -filter:replies min_faves:10 since:2025-01-01',
    '(Ioniq 5 OR "Hyundai Ioniq 5" OR Kia EV6) lang:en -filter:replies min_faves:10 since:2025-01-01',
    '(from:Tesla OR from:teslaeurope OR from:InsideEVs) lang:en since:2025-01-01',
]

STATUS_URL_RE = re.compile(r"^https://x\.com/[^/]+/status/\d+$")


def build_search_url(query: str) -> str:
    encoded = quote(query, safe="")
    return f"https://x.com/search?q={encoded}&src=typed_query&f=live"


def normalize_status_url(url: str) -> str:
    url = url.strip()
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    url = url.replace("twitter.com/", "x.com/")
    return url.split("?")[0].rstrip("/")


def collect_status_urls(page) -> set[str]:
    results = set()
    anchors = page.locator('a[href*="/status/"]')
    count = anchors.count()

    for i in range(count):
        try:
            href = anchors.nth(i).get_attribute("href", timeout=1000)
            if not href:
                continue
            if href.startswith("/"):
                href = "https://x.com" + href
            href = normalize_status_url(href)
            if STATUS_URL_RE.match(href):
                results.add(href)
        except Exception:
            continue

    return results


def try_switch_to_latest(page) -> None:
    candidates = [
        "Latest",
        "Derniers",
        "Les plus récents",
    ]

    for label in candidates:
        try:
            tab = page.get_by_role("tab", name=label)
            if tab.count() > 0:
                tab.first.click(timeout=3000)
                page.wait_for_timeout(2500)
                print(f"Onglet sélectionné : {label}")
                return
        except Exception:
            continue

    try:
        page.goto(page.url + "&f=live", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2500)
        print("Fallback : navigation forcée vers f=live")
    except Exception:
        pass


def main() -> None:
    all_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP_URL)

        if browser.contexts:
            context = browser.contexts[0]
        else:
            context = browser.new_context()

        page = context.new_page()

        for idx, query in enumerate(SEARCH_QUERIES, start=1):
            search_url = build_search_url(query)

            print(f"\n--- Requête {idx}/{len(SEARCH_QUERIES)} ---")
            print(query)

            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(5000)
            except PlaywrightTimeoutError:
                print("Timeout chargement recherche, on continue...")
            except Exception as e:
                print(f"Erreur chargement recherche : {e}")
                continue

            try_switch_to_latest(page)

            query_urls = set()
            stagnant_rounds = 0
            last_count = 0

            for round_idx in range(1, SCROLL_ROUNDS_PER_QUERY + 1):
                visible_urls = collect_status_urls(page)
                query_urls.update(visible_urls)

                current_count = len(query_urls)
                print(f"Scroll round {round_idx}: {current_count} URLs cumulées")

                if current_count > last_count:
                    stagnant_rounds = 0
                    last_count = current_count
                else:
                    stagnant_rounds += 1

                if stagnant_rounds >= 4:
                    print("Arrêt scroll recherche : plus de nouvelles URLs.")
                    break

                scroll_pixels = random.randint(SCROLL_PIXELS_MIN, SCROLL_PIXELS_MAX)
                page.mouse.wheel(0, scroll_pixels)
                page.wait_for_timeout(random.randint(SCROLL_WAIT_MIN_MS, SCROLL_WAIT_MAX_MS))

            print(f"URLs retenues pour cette requête : {len(query_urls)}")
            all_urls.update(query_urls)

        sorted_urls = sorted(all_urls)
        OUTPUT_FILE.write_text("\n".join(sorted_urls) + ("\n" if sorted_urls else ""), encoding="utf-8")

        print(f"\n[TERMINÉ] {len(sorted_urls)} URLs sauvegardées dans {OUTPUT_FILE}")


if __name__ == "__main__":
    main()