"""METAR parser: RMK section, T-group, and common remark groups.

Consolidates METAR parsing used by AWC, MADIS, and aviationweather collectors.
METAR format: https://www.weather.gov/media/wrh/mesowest/metar_decode_key.pdf

T-group (in RMK): T followed by 8 digits — 4 for temp, 4 for dewpoint.
  First digit of each 4: 0=positive, 1=negative; next 3 = tenths °C.
  Example: T10171133 → temp=-1.7°C, dewpoint=-13.3°C
"""

from __future__ import annotations

import re
from dataclasses import dataclass


# RMK section: everything after " RMK " (case-insensitive)
_RMK_RE = re.compile(r"\s+RMK\s+(.+)$", re.IGNORECASE)

# T-group: T + 8 digits (temp 4 + dewpoint 4). Tenth-degree Celsius.
_TGROUP_RE = re.compile(r"\bT(\d{4})(\d{4})\b")

# T-group temp-only fallback (some METARs have abbreviated form): T + 4 digits
_TGROUP_TEMP_ONLY_RE = re.compile(r"\bT([01])(\d{3})\b")


@dataclass
class MetarParseResult:
    """Parsed METAR fields."""

    raw_ob: str | None
    rmk: str | None
    temp_c: float | None
    dewpoint_c: float | None
    temp_high_accuracy: bool  # True if from T-group (0.1°C precision)


class MetarParser:
    """Parse raw METAR string for RMK section and T-group temperature."""

    @staticmethod
    def extract_rmk(raw_ob: str | None) -> str | None:
        """Extract RMK (remarks) portion from raw METAR.

        Returns everything after " RMK " or None if not present.
        """
        if not raw_ob:
            return None
        m = _RMK_RE.search(raw_ob)
        return m.group(1).strip() if m else None

    @staticmethod
    def parse_tgroup(raw_ob: str | None) -> tuple[float | None, float | None]:
        """Parse T-group from raw METAR for tenth-degree precision.

        Returns (temp_c, dewpoint_c). T-group format: T + 8 digits.
        First digit of each 4: 0=positive, 1=negative.
        """
        if not raw_ob:
            return None, None
        m = _TGROUP_RE.search(raw_ob)
        if not m:
            return None, None
        raw_t, raw_d = m.group(1), m.group(2)
        t_sign = -1 if raw_t[0] == "1" else 1
        d_sign = -1 if raw_d[0] == "1" else 1
        temp_c = t_sign * int(raw_t[1:]) / 10.0
        dew_c = d_sign * int(raw_d[1:]) / 10.0
        return temp_c, dew_c

    @staticmethod
    def parse_temp_only(raw_ob: str | None) -> float | None:
        """Parse T-group temp from abbreviated form (T + 4 digits) or full 8-digit.

        Used when only temp is needed. Checks full T-group first, then temp-only.
        """
        if not raw_ob:
            return None
        temp_c, _ = MetarParser.parse_tgroup(raw_ob)
        if temp_c is not None:
            return temp_c
        m = _TGROUP_TEMP_ONLY_RE.search(raw_ob)
        if m:
            sign = 1 if m.group(1) == "0" else -1
            return sign * int(m.group(2)) / 10.0
        return None

    @classmethod
    def parse(cls, raw_ob: str | None) -> MetarParseResult:
        """Parse raw METAR into RMK, temp, dewpoint. Prefers T-group when available."""
        if not raw_ob:
            return MetarParseResult(
                raw_ob=None, rmk=None, temp_c=None, dewpoint_c=None,
                temp_high_accuracy=False,
            )
        rmk = cls.extract_rmk(raw_ob)
        temp_c, dewpoint_c = cls.parse_tgroup(raw_ob)
        if temp_c is not None:
            return MetarParseResult(
                raw_ob=raw_ob, rmk=rmk, temp_c=temp_c, dewpoint_c=dewpoint_c,
                temp_high_accuracy=True,
            )
        return MetarParseResult(
            raw_ob=raw_ob, rmk=rmk, temp_c=None, dewpoint_c=None,
            temp_high_accuracy=False,
        )
