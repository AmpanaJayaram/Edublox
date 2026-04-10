"""
scripts/data_import/db_config.py
=================================
Shared database config for all data import scripts.
Reads credentials from .env — never hardcoded.
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG: dict = {
    "dbname":   os.environ.get("DB_NAME",  "unisearch"),
    "user":     os.environ.get("DB_USER",  "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),  # no fallback — must be set in .env
    "host":     os.environ.get("DB_HOST",  "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
}


def get_conn():
    """Return a psycopg2 connection with RealDictCursor."""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
