"""
app/__init__.py
===============
Application factory.

Usage:
    from app import create_app
    app = create_app()
"""

from flask import Flask


def create_app() -> Flask:
    # All sub-imports are local to avoid circular dependencies at module load time.
    # (migrations.startup → app.db.connection → app, which would re-enter __init__)
    from migrations.startup import run_startup_migrations
    from app.services.university_service import get_all_states
    from app.routes.pages import pages_bp
    from app.routes.universities import universities_bp
    from app.routes.ai import ai_bp

    app = Flask(__name__, template_folder="../templates", static_folder="../static")

    # Run idempotent schema migrations before accepting any traffic
    run_startup_migrations()

    # Cache the states list in app config so page routes can access it
    # without an extra DB call per request
    try:
        app.config["ALL_STATES"] = get_all_states()
    except Exception as e:
        app.logger.warning(f"Could not load states at startup: {e}")
        app.config["ALL_STATES"] = []

    app.register_blueprint(pages_bp)
    app.register_blueprint(universities_bp)
    app.register_blueprint(ai_bp)

    return app
