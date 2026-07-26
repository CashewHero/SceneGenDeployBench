from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any


DEFAULT_LIST_LIMIT = 50
DAY_ORDER = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
DAY_LABELS = {
    "mon": "Mon",
    "tue": "Tue",
    "wed": "Wed",
    "thu": "Thu",
    "fri": "Fri",
    "sat": "Sat",
    "sun": "Sun",
}


def parse_iso_timestamp(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_timestamp(value: str | None) -> str:
    if not value:
        return "-"
    dt = parse_iso_timestamp(value).astimezone()
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z").rstrip()


def format_relative_time(value: str | None) -> str:
    if not value:
        return "-"
    dt = parse_iso_timestamp(value)
    delta = datetime.now(timezone.utc) - dt
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 60:
        return "now" if seconds < 5 else f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    if seconds < 604800:
        return f"{seconds // 86400}d ago"
    return dt.astimezone().strftime("%Y-%m-%d %H:%M")


def format_duration(start_value: str | None, end_value: str | None) -> str:
    if not start_value or not end_value:
        return "-"
    seconds = int((parse_iso_timestamp(end_value) - parse_iso_timestamp(start_value)).total_seconds())
    seconds = max(seconds, 0)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def render_table(columns: list[str], rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    widths: dict[str, int] = {
        column: max(
            len(column),
            *(len(str(row.get(column, ""))) for row in rows),
        )
        for column in columns
    }
    header = "  ".join(column.ljust(widths[column]) for column in columns)
    body = [
        "  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns)
        for row in rows
    ]
    return "\n".join([header, *body])


def render_key_value(rows: list[tuple[str, Any]]) -> str:
    items = [(label, "-" if value is None or value == "" else str(value)) for label, value in rows]
    if not items:
        return ""
    width = max(len(label) for label, _ in items)
    return "\n".join(f"{label.ljust(width)}  {value}" for label, value in items)


def add_list_controls(parser: argparse.ArgumentParser, *, limit_help: str = "maximum number of rows to show") -> None:
    parser.add_argument("--limit", type=int, help=limit_help)
    parser.add_argument("--all", action="store_true", help="show all rows")


def effective_row_limit(args: argparse.Namespace) -> int | None:
    if getattr(args, "all", False):
        return None
    limit = getattr(args, "limit", None)
    if limit is not None:
        return max(limit, 0)
    return DEFAULT_LIST_LIMIT


def limit_rows(rows: list[Any], args: argparse.Namespace) -> tuple[list[Any], int]:
    total = len(rows)
    limit = effective_row_limit(args)
    if limit is None:
        return rows, total
    return rows[:limit], total


def print_truncation_notice(shown: int, total: int, *, label: str = "rows") -> None:
    if shown >= total:
        return
    print()
    print(f"Showing {shown} of {total} {label}. Use --all to show all.")


def output_format(args: argparse.Namespace, default: str) -> str:
    if args.json:
        return "json"
    if args.format:
        return args.format
    return default


def print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2))


def format_schedule_days(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "Daily"
    normalized: list[str] = []
    for item in value:
        key = str(item).strip().lower()[:3]
        if key in DAY_LABELS and key not in normalized:
            normalized.append(key)
    if not normalized:
        return ",".join(str(item).strip() for item in value if str(item).strip()) or "Daily"
    if normalized == DAY_ORDER:
        return "Daily"

    positions = sorted(DAY_ORDER.index(item) for item in normalized)
    ranges: list[str] = []
    start = positions[0]
    end = positions[0]
    for position in positions[1:]:
        if position == end + 1:
            end = position
            continue
        ranges.append(format_day_range(start, end))
        start = end = position
    ranges.append(format_day_range(start, end))
    return ",".join(ranges)


def format_day_range(start: int, end: int) -> str:
    if start == end:
        return DAY_LABELS[DAY_ORDER[start]]
    return f"{DAY_LABELS[DAY_ORDER[start]]}-{DAY_LABELS[DAY_ORDER[end]]}"


def schedule_window_rows(scheduling: dict[str, Any]) -> list[dict[str, str]]:
    windows = scheduling.get("allowed_windows")
    if not isinstance(windows, list):
        return []
    rows: list[dict[str, str]] = []
    for window in windows:
        if not isinstance(window, dict):
            continue
        rows.append(
            {
                "DAYS": format_schedule_days(window.get("days")),
                "START": str(window.get("start", "")).strip() or "-",
                "END": str(window.get("end", "")).strip() or "-",
                "START POLICY": str(window.get("start_policy", "")).strip() or "-",
                "END POLICY": str(window.get("end_policy", "")).strip() or "-",
            }
        )
    return rows
