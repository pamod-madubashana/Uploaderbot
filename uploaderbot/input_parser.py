from __future__ import annotations

import re
from pathlib import PurePosixPath
from urllib.parse import urlparse


RANGE_INPUT_PATTERN = re.compile(r"^(?P<url>\S+)\s+(?P<start>\d+)\s*-\s*(?P<end>\d+)$")
URL_TOKEN_PATTERN = re.compile(r"(?:(?:https?://)?(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:/[^\s]*)?)")
PLACEHOLDER_PATTERN = re.compile(r"\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}")
ASSIGNMENT_PATTERN = re.compile(r"^(?P<name>[A-Za-z_][A-Za-z0-9_]*)=(?P<start>\d+)(?:-(?P<end>\d+))?$")
NUMBER_PATTERN = re.compile(r"\d+")
TRIMMABLE_URL_CHARS = "<>()[]{}\"'.,;:!?"
DEFAULT_PLACEHOLDER_NAMES = {"n", "num", "number"}


class QueueInputError(ValueError):
    pass


def parse_queue_text(text: str) -> list[str]:
    urls: list[str] = []

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        try:
            urls.extend(expand_queue_line(line))
        except QueueInputError as exc:
            raise QueueInputError(f"Line {line_number}: {exc}") from exc

    return urls


def expand_queue_line(line: str) -> list[str]:
    range_match = RANGE_INPUT_PATTERN.match(line)
    if range_match:
        url = normalize_url(range_match.group("url"))
        start = int(range_match.group("start"))
        end = int(range_match.group("end"))
        return expand_url_pattern(url, start, end)

    parts = line.split()
    if not parts:
        raise QueueInputError("No valid URL found")

    url = normalize_url(parts[0])
    assignments = _parse_assignments(parts[1:])
    if assignments:
        return expand_placeholder_assignments(url, assignments)

    tokens = URL_TOKEN_PATTERN.findall(line)
    if not tokens:
        raise QueueInputError("No valid URL found")

    return [normalize_url(token) for token in tokens]


def normalize_url(value: str) -> str:
    candidate = value.strip().strip(TRIMMABLE_URL_CHARS)
    if not candidate:
        raise QueueInputError("Missing URL")

    parsed = urlparse(candidate)
    if not parsed.scheme:
        candidate = f"https://{candidate}"
        parsed = urlparse(candidate)

    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise QueueInputError(f"Invalid URL: {value}")

    return candidate


def expand_url_pattern(url: str, start: int, end: int) -> list[str]:
    if end < start:
        raise QueueInputError("Range end must be greater than or equal to range start")

    placeholders = PLACEHOLDER_PATTERN.findall(url)
    if placeholders:
        unsupported = set(placeholders) - DEFAULT_PLACEHOLDER_NAMES
        if unsupported:
            names = ", ".join(sorted(unsupported))
            raise QueueInputError(
                f"Placeholder(s) {names} need explicit assignments like {names}=1-100"
            )
        return [PLACEHOLDER_PATTERN.sub(str(number), url) for number in range(start, end + 1)]

    replace_start, replace_end, width, zero_fill = _locate_number_to_replace(url)
    expanded_urls: list[str] = []

    for number in range(start, end + 1):
        replacement = str(number).zfill(width) if zero_fill else str(number)
        expanded_urls.append(f"{url[:replace_start]}{replacement}{url[replace_end:]}")

    return expanded_urls


def expand_placeholder_assignments(
    url: str,
    assignments: dict[str, tuple[int, int]],
) -> list[str]:
    placeholder_names = set(PLACEHOLDER_PATTERN.findall(url))
    if not placeholder_names:
        raise QueueInputError("Assignments can only be used with placeholders like {part}")

    missing = placeholder_names - set(assignments)
    if missing:
        names = ", ".join(sorted(missing))
        raise QueueInputError(f"Missing assignment for placeholder(s): {names}")

    extra = set(assignments) - placeholder_names
    if extra:
        names = ", ".join(sorted(extra))
        raise QueueInputError(f"Unknown assignment placeholder(s): {names}")

    ranged_names = [name for name, (start, end) in assignments.items() if start != end]
    if len(ranged_names) > 1:
        names = ", ".join(sorted(ranged_names))
        raise QueueInputError(f"Only one ranged placeholder is supported at a time: {names}")

    if not ranged_names:
        return [_render_placeholder_url(url, {name: start for name, (start, _) in assignments.items()})]

    ranged_name = ranged_names[0]
    range_start, range_end = assignments[ranged_name]
    if range_end < range_start:
        raise QueueInputError("Range end must be greater than or equal to range start")

    urls: list[str] = []
    for number in range(range_start, range_end + 1):
        values = {name: start for name, (start, _) in assignments.items()}
        values[ranged_name] = number
        urls.append(_render_placeholder_url(url, values))
    return urls


def _render_placeholder_url(url: str, values: dict[str, int]) -> str:
    def replacer(match: re.Match[str]) -> str:
        name = match.group("name")
        return str(values[name])

    return PLACEHOLDER_PATTERN.sub(replacer, url)


def _parse_assignments(parts: list[str]) -> dict[str, tuple[int, int]]:
    assignments: dict[str, tuple[int, int]] = {}
    for part in parts:
        cleaned = part.strip().rstrip(",")
        match = ASSIGNMENT_PATTERN.match(cleaned)
        if match is None:
            continue

        start = int(match.group("start"))
        end = int(match.group("end") or match.group("start"))
        assignments[match.group("name")] = (start, end)
    return assignments


def _locate_number_to_replace(url: str) -> tuple[int, int, int, bool]:
    parsed = urlparse(url)
    host_prefix = f"{parsed.scheme}://{parsed.netloc}"
    search_offset = len(host_prefix)
    path_start = url.find(parsed.path, search_offset)
    path_without_suffix = parsed.path
    suffix = PurePosixPath(parsed.path).suffix
    if suffix:
        path_without_suffix = parsed.path[: -len(suffix)]

    matches = list(NUMBER_PATTERN.finditer(path_without_suffix))

    if matches and path_start >= 0:
        match = matches[-1]
        start = path_start + match.start()
        end = path_start + match.end()
        raw_value = match.group(0)
        return start, end, len(raw_value), raw_value.startswith("0")

    matches = list(NUMBER_PATTERN.finditer(url[search_offset:]))

    if not matches:
        raise QueueInputError("Pattern range needs either a placeholder or a number in the URL")

    match = matches[-1]
    start = search_offset + match.start()
    end = search_offset + match.end()
    raw_value = match.group(0)
    return start, end, len(raw_value), raw_value.startswith("0")
