"""
run.py
======
Application entry point.

Development:   uv run run.py
Production:    uv run gunicorn "app:create_app()"
"""

import os
from app import create_app

app = create_app()

if __name__ == "__main__":
    port  = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"

    print("\n" + "=" * 50)
    print("  UniSearch v2 — PostgreSQL backend")
    print(f"  Open: http://localhost:{port}")
    print("=" * 50 + "\n")

    app.run(debug=debug, host="0.0.0.0", port=port, threaded=True)
