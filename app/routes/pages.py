"""
app/routes/pages.py
====================
Server-rendered page routes. Each returns a rendered HTML template.
"""

from flask import Blueprint, render_template, current_app, abort
from app.db.connection import get_conn
from app.utils.constants import STATE_NAMES

pages_bp = Blueprint("pages", __name__)


@pages_bp.route("/")
def index():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM universities")
            total = cur.fetchone()["total"]
    return render_template(
        "index.html",
        states=current_app.config["ALL_STATES"],
        total=total,
    )


@pages_bp.route("/university/<int:uid>")
def university(uid: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM universities WHERE id = %s", (uid,))
            u = cur.fetchone()
    if not u:
        abort(404)
    return render_template("university.html", u=dict(u))


@pages_bp.route("/compare")
def compare():
    return render_template("compare.html")


@pages_bp.route("/match")
def match_page():
    return render_template("match.html")


@pages_bp.route("/map")
def map_page():
    return render_template("map.html")


@pages_bp.route("/state/<state_abbr>")
def state_page(state_abbr: str):
    state_abbr = state_abbr.upper()
    state_name = STATE_NAMES.get(state_abbr, state_abbr)
    return render_template("state.html", state_abbr=state_abbr, state_name=state_name)
