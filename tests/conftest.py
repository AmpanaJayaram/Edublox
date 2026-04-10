"""
tests/conftest.py
=================
Shared pytest fixtures for the UniSearch test suite.

Fixtures
--------
app             — Flask app wired for testing (no real DB, no real Groq)
client          — Flask test client from the app fixture
mock_db         — patches get_conn() so every test gets a MagicMock cursor
mock_groq       — patches groq_client so AI tests never hit the real API
sample_uni      — a realistic university dict (mirrors get_university_detail output)
sample_uni_list — a list of two lightweight university dicts (mirrors get_universities_flat)
"""

import pytest
from unittest.mock import MagicMock, patch


# ── Env vars (must be set before create_app() is imported) ────

@pytest.fixture(scope="session", autouse=True)
def _set_env(monkeypatch_session):
    """Ensure required env vars exist before any app code runs."""
    import os
    os.environ.setdefault("DB_HOST",     "localhost")
    os.environ.setdefault("DB_PORT",     "5432")
    os.environ.setdefault("DB_NAME",     "unisearch_test")
    os.environ.setdefault("DB_USER",     "postgres")
    os.environ.setdefault("DB_PASSWORD", "test")
    os.environ.setdefault("GROQ_API_KEY","test-groq-key")


# pytest does not provide monkeypatch at session scope by default — add it:
@pytest.fixture(scope="session")
def monkeypatch_session():
    from _pytest.monkeypatch import MonkeyPatch
    mp = MonkeyPatch()
    yield mp
    mp.undo()


# ── App / client ──────────────────────────────────────────────

@pytest.fixture()
def app():
    """
    Create a test-mode Flask app.

    Both startup migrations and the states cache call are patched so the
    fixture works without a real database.
    """
    with (
        patch("migrations.startup.run_startup_migrations"),
        patch("app.services.university_service.get_all_states", return_value=["Texas", "California"]),
    ):
        from app import create_app
        flask_app = create_app()

    flask_app.config.update(
        TESTING=True,
        # Disable propagation so error handlers return JSON instead of raising
        PROPAGATE_EXCEPTIONS=False,
    )
    yield flask_app


@pytest.fixture()
def client(app):
    """Flask test client."""
    return app.test_client()


# ── DB mock ───────────────────────────────────────────────────

@pytest.fixture()
def mock_db():
    """
    Patch app.db.connection.get_conn so every service call gets a
    MagicMock connection/cursor rather than touching PostgreSQL.

    Yields the mock cursor so individual tests can set .fetchone /
    .fetchall / .description as needed.
    """
    mock_cursor = MagicMock()
    mock_conn   = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__  = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)

    with patch("app.db.connection.get_conn", return_value=mock_conn):
        yield mock_cursor


# ── Groq mock ─────────────────────────────────────────────────

@pytest.fixture()
def mock_groq():
    """
    Patch app.extensions.groq_client so AI endpoints never call the
    real Groq API.  Yields a MagicMock whose
    .chat.completions.create() return value tests can customise.
    """
    mock_client   = MagicMock()
    mock_response = MagicMock()
    mock_response.choices[0].message.content = "<div class='ai-section'>Test AI output</div>"
    mock_client.chat.completions.create.return_value = mock_response

    with patch("app.extensions.groq_client", mock_client):
        yield mock_client


# ── Sample data ───────────────────────────────────────────────

@pytest.fixture()
def sample_uni():
    """A realistic university detail dict (mirrors get_university_detail output)."""
    return {
        "id":                  1,
        "name":                "University of Texas at Austin",
        "state":               "Texas",
        "city":                "Austin",
        "control":             "Public",
        "carnegie_category":   "Doctoral Universities: Very High Research Activity",
        "enrollment":          50_000,
        "acceptance_rate":     31.0,
        "graduation_rate":     84.0,
        "avg_net_price":       15_000,
        "median_earnings":     55_000,
        "us_news_rank":        38,
        "forbes_rank":         None,
        "website":             "https://www.utexas.edu",
        "facts":               {"founded": 1883, "nickname": "Longhorns"},
        "awards":              [],
        "programs":            [],
    }


@pytest.fixture()
def sample_uni_list():
    """Two lightweight university dicts (mirrors get_universities_flat output)."""
    return [
        {
            "id": 1, "name": "University of Texas at Austin",
            "state": "Texas", "city": "Austin", "control": "Public",
            "enrollment": 50_000, "acceptance_rate": 31.0,
            "graduation_rate": 84.0, "us_news_rank": 38,
        },
        {
            "id": 2, "name": "Texas A&M University",
            "state": "Texas", "city": "College Station", "control": "Public",
            "enrollment": 74_000, "acceptance_rate": 63.0,
            "graduation_rate": 82.0, "us_news_rank": 69,
        },
    ]
