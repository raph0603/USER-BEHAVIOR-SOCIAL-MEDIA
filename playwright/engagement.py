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

    number = float(match.group(1))
    multiplier = {
        "k": 1_000,
        "m": 1_000_000,
        "b": 1_000_000_000,
    }.get(match.group(2), 1)
    return int(number * multiplier)
