from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dt_time
from typing import Any
from zoneinfo import ZoneInfo

START_POLICY_OPEN = "open"

END_POLICY_END_NOW = "end_now"
END_POLICY_FINISH_JOB = "finish_job"
END_POLICY_FINISH_BATCH = "finish_batch"

VALID_START_POLICIES = {START_POLICY_OPEN}
VALID_END_POLICIES = {
    END_POLICY_END_NOW,
    END_POLICY_FINISH_JOB,
    END_POLICY_FINISH_BATCH,
}
START_POLICY_PRIORITY = {
    START_POLICY_OPEN: 0,
}
END_POLICY_PRIORITY = {
    END_POLICY_END_NOW: 0,
    END_POLICY_FINISH_JOB: 1,
    END_POLICY_FINISH_BATCH: 2,
}
VALID_DAY_ALIASES = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}


@dataclass(frozen=True)
class WindowState:
    active: bool
    start_policy: str | None = None
    end_policy: str | None = None


def parse_hhmm(value: str) -> dt_time:
    normalized = value.strip()
    if normalized == "24:00":
        return dt_time(0, 0)
    hour_text, minute_text = normalized.split(":", 1)
    return dt_time(int(hour_text), int(minute_text))


def normalize_allowed_windows(value: Any, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field_name}.allowed_windows must be a list")

    normalized_windows: list[dict[str, Any]] = []
    for index, raw_window in enumerate(value, start=1):
        if not isinstance(raw_window, dict):
            raise ValueError(f"{field_name}.allowed_windows[{index}] must be a mapping")

        window = dict(raw_window)

        days_payload = window.get("days")
        if days_payload is not None:
            if not isinstance(days_payload, list):
                raise ValueError(f"{field_name}.allowed_windows[{index}].days must be a list")
            days = [str(day).strip().lower()[:3] for day in days_payload if str(day).strip()]
            invalid_days = [day for day in days if day not in VALID_DAY_ALIASES]
            if invalid_days:
                raise ValueError(
                    f"{field_name}.allowed_windows[{index}].days contains invalid values: "
                    f"{', '.join(invalid_days)}"
                )
            window["days"] = days

        start_value = str(window.get("start", "00:00")).strip() or "00:00"
        end_value = str(window.get("end", "24:00")).strip() or "24:00"
        parse_hhmm(start_value)
        parse_hhmm(end_value)
        window["start"] = start_value
        window["end"] = end_value

        start_policy = str(window.get("start_policy", START_POLICY_OPEN)).strip().lower() or START_POLICY_OPEN
        if start_policy not in VALID_START_POLICIES:
            raise ValueError(
                f"{field_name}.allowed_windows[{index}].start_policy must be one of: "
                f"{', '.join(sorted(VALID_START_POLICIES))}"
            )
        window["start_policy"] = start_policy

        end_policy = (
            str(window.get("end_policy", END_POLICY_FINISH_BATCH)).strip().lower()
            or END_POLICY_FINISH_BATCH
        )
        if end_policy not in VALID_END_POLICIES:
            raise ValueError(
                f"{field_name}.allowed_windows[{index}].end_policy must be one of: "
                f"{', '.join(sorted(VALID_END_POLICIES))}"
            )
        window["end_policy"] = end_policy

        normalized_windows.append(window)

    return normalized_windows


def _day_alias(day_index: int) -> str:
    return ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][day_index]


def _window_matches(now_local: datetime, window: dict[str, object]) -> bool:
    days_payload = window.get("days")
    days = {str(day).strip().lower()[:3] for day in days_payload} if isinstance(days_payload, list) else None
    start = parse_hhmm(str(window.get("start", "00:00")))
    end = parse_hhmm(str(window.get("end", "24:00")))
    current_time = now_local.time().replace(tzinfo=None)
    current_day = _day_alias(now_local.weekday())

    if start == end:
        return days is None or current_day in days

    if start < end:
        if days is not None and current_day not in days:
            return False
        return start <= current_time < end

    if current_time >= start:
        if days is not None and current_day not in days:
            return False
        return True

    previous_day = _day_alias((now_local.weekday() - 1) % 7)
    if days is not None and previous_day not in days:
        return False
    return current_time < end


def _effective_end_policy(windows: list[dict[str, Any]]) -> str:
    return max(
        (
            str(window.get("end_policy", END_POLICY_FINISH_BATCH)).strip().lower()
            or END_POLICY_FINISH_BATCH
            for window in windows
        ),
        key=lambda value: END_POLICY_PRIORITY.get(value, END_POLICY_PRIORITY[END_POLICY_FINISH_BATCH]),
    )


def _effective_start_policy(windows: list[dict[str, Any]]) -> str:
    return max(
        (
            str(window.get("start_policy", START_POLICY_OPEN)).strip().lower()
            or START_POLICY_OPEN
            for window in windows
        ),
        key=lambda value: START_POLICY_PRIORITY.get(value, START_POLICY_PRIORITY[START_POLICY_OPEN]),
    )


def evaluate_window_state(scheduling: dict[str, Any], *, now: datetime | None = None) -> WindowState:
    windows = scheduling.get("allowed_windows")
    timezone_name = str(scheduling.get("timezone") or "UTC")
    if not windows:
        return WindowState(active=True, start_policy=START_POLICY_OPEN)

    zone = ZoneInfo(timezone_name)
    now_local = now.astimezone(zone) if now is not None else datetime.now(zone)
    matching_windows = [
        window
        for window in windows
        if isinstance(window, dict) and _window_matches(now_local, window)
    ]
    if not matching_windows:
        return WindowState(active=False)
    return WindowState(
        active=True,
        start_policy=_effective_start_policy(matching_windows),
        end_policy=_effective_end_policy(matching_windows),
    )
