"""
app/extensions.py
=================
Third-party client initialisation.
Clients are created once at import time and shared across the app.
"""

import os

# ── Groq ──────────────────────────────────────────────────────

groq_client = None

try:
    from groq import Groq

    _api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if _api_key:
        groq_client = Groq(api_key=_api_key)
except Exception:
    pass  # App starts fine without Groq; AI routes return 503 when client is None.
