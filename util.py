"""Small helpers not tied to API / storage models."""

import re

def _parse_colon_duration_to_seconds(s: str) -> float | None:
    """Parse ``HH:MM:SS.ss``, ``MM:SS.ss``, or ``:SS.ss`` into total seconds."""
    s = s.strip()
    if not s:
        return None

    if s.startswith(":") and s.count(":") == 1:
        try:
            return float(s[1:])
        except ValueError:
            return None

    parts = s.split(":")
    try:
        if len(parts) == 3:
            h, mm, sec_s = int(parts[0]), int(parts[1]), float(parts[2])
            return float(h) * 3600.0 + float(mm) * 60.0 + sec_s
        if len(parts) == 2:
            mm, sec_s = int(parts[0]), float(parts[1])
            return float(mm) * 60.0 + sec_s
    except ValueError:
        pass
    return None


def _time_cap_value_to_seconds(value: str | int | float | None) -> float | None:
    """Seconds from a numeric cap, or the same colon duration formats as scores."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return _parse_colon_duration_to_seconds(value)
    return None

def parse_timed_workout_score(
    score: str | int | float | None,
    *,
    time_cap: str | int | float | None = None,
    time_cap_seconds: str | int | float | None = None,
) -> float | None:
    """
    Parse a timed workout display string into total seconds (float).

    Colon formats (after stripping whitespace):
      - ``HH:MM:SS`` or ``HH:MM:SS.ss``
      - ``MM:SS`` or ``MM:SS.ss``
      - ``:SS`` or ``:SS.ss`` (leading colon → seconds only)

    If the string does not match those patterns, it is treated as a cap score
    like ``CAP+20``: returns cap duration in seconds plus ``remaining_reps``
    (each rep adds one second), e.g. ``time_cap="10:00"`` and ``CAP+20`` →
    620.0.

    ``time_cap`` may be seconds (``int`` / ``float``) or the same colon string
    formats as the score. If neither ``time_cap`` nor ``time_cap_seconds`` is
    set for a cap-style score, returns ``None``. When both are set,
    ``time_cap`` wins. ``time_cap_seconds`` is a legacy alias for ``time_cap``.

    Non-string score inputs are returned as ``float``.
    """
    cap_base = _time_cap_value_to_seconds(
        time_cap if time_cap is not None else time_cap_seconds
    )

    if score is None:
        return None
    if isinstance(score, bool):
        return None
    if isinstance(score, (int, float)):
        return float(score)
    if not isinstance(score, str):
        return None

    s = score.strip()
    if not s:
        return None

    colon = _parse_colon_duration_to_seconds(s)
    if colon is not None:
        return colon

    cap_m = re.match(r"(?i)cap\s*\+\s*(\d+)", s)
    if cap_m:
        if cap_base is None:
            return None
        return cap_base + float(cap_m.group(1))

    return None

def convert_seconds_to_time_score(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = float(seconds % 60)

    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:05.2f}"
    elif minutes > 0:
        return f"{minutes:02d}:{secs:05.2f}"
    else:
        return f"{secs:05.2f}"

def convert_value_to_display(score_type: str, value: float) -> str:
    from pandas import isna
    if isna(value):
        return None
    elif value == 0:
        return '--'
    elif score_type == 'time':
        return convert_seconds_to_time_score(value)
    else:
        return str(int(value))