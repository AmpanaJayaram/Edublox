"""
app/config.py
=============
Environment variable helpers and application-level settings.
All DB credentials are read from environment — never hardcoded.
"""

import os
from dotenv import load_dotenv

load_dotenv()


# ── Env helpers ───────────────────────────────────────────────

def _require_env(key: str) -> str:
    """Return a non-empty env var or raise a clear error at startup."""
    value = os.environ.get(key, "").strip()
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


def _int_env(key: str) -> int:
    """Return an env var parsed as int, with a clear error if it isn't one."""
    value = _require_env(key)
    try:
        return int(value)
    except ValueError:
        raise EnvironmentError(
            f"Environment variable {key} must be an integer, got: {value!r}"
        )


def _safe_int_env(name: str, default: int) -> int:
    """Return an env var as int, or a default if the var is absent/blank."""
    value = os.environ.get(name)
    if value is None or str(value).strip() == "":
        return default
    return int(value)


# ── Database ──────────────────────────────────────────────────

DB_CONFIG: dict = {
    "host":     _require_env("DB_HOST"),
    "port":     _int_env("DB_PORT"),
    "dbname":   _require_env("DB_NAME"),
    "user":     _require_env("DB_USER"),
    "password": _require_env("DB_PASSWORD"),
}

# ── Pagination ────────────────────────────────────────────────

PAGE_SIZE: int = 24
