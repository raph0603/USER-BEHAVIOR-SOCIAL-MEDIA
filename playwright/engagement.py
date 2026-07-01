import re


def parse_count(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)

    normalized = re.sub(r"\s+", " ", str(value)).strip().lower().replace(",", "")
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*([kmb])?", normalized)
    if not match:
        return None

    try:
        number = float(match.group(1))
        multiplier = {
            "k": 1_000,
            "m": 1_000_000,
            "b": 1_000_000_000,
        }.get(match.group(2), 1)
        result = number * multiplier
        import math
        if math.isinf(result) or math.isnan(result):
            return None
        return int(result)
    except (OverflowError, ValueError):
        return None


def extract_x_metric(article, test_id: str) -> int | None:
    selectors = [f'[data-testid="{test_id}"]']
    if test_id == "analytics":
        selectors.append('a[href*="/analytics"]')

    analytics_link_found = False
    for selector in selectors:
        try:
            locator = article.locator(selector)
            if locator.count() == 0:
                continue

            node = locator.first
        except Exception:
            continue

        if test_id == "analytics" and "/analytics" in selector:
            analytics_link_found = True

        for read_value in (
            lambda: node.inner_text(timeout=1000),
            lambda: node.get_attribute("aria-label", timeout=1000),
        ):
            try:
                raw_value = read_value()
                parsed = parse_count(raw_value)
                if parsed is not None:
                    return parsed
            except Exception:
                pass

    # X hides the numeric label for zero-view posts but still renders the
    # analytics link. Its presence distinguishes zero views from missing data.
    if test_id == "analytics" and analytics_link_found:
        return 0
    return None


def extract_x_followers(article) -> int | None:
    try:
        page = article.page
        author_link = article.locator('[data-testid="User-Name"] a').first
        author_link.hover()

        hover_card = page.locator('[data-testid="HoverCard"], [data-testid="hoverCardRoot"]')
        hover_card.wait_for(state="visible", timeout=2000)

        links = hover_card.locator('a[href$="/verified_followers"], a[href$="/followers"]')
        count = links.count()
        for idx in range(count):
            try:
                inner_text = links.nth(idx).inner_text(timeout=1000)
                parsed = parse_count(inner_text)
                if parsed is not None:
                    return parsed
            except Exception:
                pass
        return None
    except Exception:
        return None

