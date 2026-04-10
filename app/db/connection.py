"""
app/db/connection.py
====================
Single place that opens a psycopg2 connection.
All rows are returned as dicts via RealDictCursor.
"""

import psycopg2
import psycopg2.extras
from app.config import DB_CONFIG


def get_conn():
    """Return a new psycopg2 connection using the app DB config."""
    return psycopg2.connect(
        **DB_CONFIG,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
