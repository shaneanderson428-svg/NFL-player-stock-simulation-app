"""Small runtime-safe coercion helpers used across scripts.

These helpers tolerate None, lists, dicts and non-numeric strings and
return a sensible default instead of raising in float()/int() calls.
"""
from __future__ import annotations

from typing import Any


def safe_float(v: Any, default: float = 0.0) -> float:
    """Return a float for common inputs. If v is a list/tuple take the last
    element, if dict try to extract a numeric-looking value, otherwise try
    to coerce to float and return default on failure."""
    try:
        if v is None:
            return default
        # unwrap single-element containers
        if isinstance(v, (list, tuple)) and len(v) > 0:
            v = v[-1]
        # dict: try common keys
        if isinstance(v, dict):
            for k in ("price", "value", "val", "p"):
                if k in v:
                    return safe_float(v.get(k), default=default)
            # fallback to string representation
            v = str(v)
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if s == "":
            return default
        # try direct float conversion
        try:
            return float(s)
        except Exception:
            # strip non-numeric chars
            import re

            cleaned = re.sub(r"[^0-9.+\-eE]", "", s)
            if cleaned == "":
                return default
            try:
                return float(cleaned)
            except Exception:
                return default
    except Exception:
        return default


def safe_int(v: Any, default: int = 0) -> int:
    try:
        f = safe_float(v, default=float(default))
        return int(f)
    except Exception:
        return default
