from __future__ import annotations

from datetime import datetime
from typing import Any, Optional


def parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            normalized = value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except Exception:
            return None
    return None


def to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        try:
            if isinstance(value, str):
                cleaned = value.replace("\\t", "").replace("\t", "").strip()
                return float(cleaned)
        except Exception:
            return None
    return None
