"""
app/routes/universities.py
===========================
All /api/universities* and /api/university* JSON endpoints.
Routes are intentionally thin: they parse the request and delegate
all query logic to the service layer.
"""

from flask import (
    Blueprint, 
    request, 
    jsonify, 
    current_app
)
from app.services.university_service import (
    get_universities_flat,
    get_state_summary,
    search_universities,
    get_university_detail,
    get_universities_compare,
    get_stats,
)
from app.services.program_service import (
    get_programs,
    set_featured_program,
    reset_programs,
    update_program_urls,
)

universities_bp = Blueprint("universities", __name__)


# ── University listings ───────────────────────────────────────

@universities_bp.route("/api/universities/all")
def api_universities_all():
    """Fast flat endpoint — all universities with key facts for client-side filtering."""
    try:
        state = request.args.get("state", "").strip()
        return jsonify(get_universities_flat(state_filter=state))
    except Exception as e:
        import traceback
        # universities_bp.logger.error(f"/api/universities/all error:\n{traceback.format_exc()}")  # type: ignore[attr-defined]
        current_app.logger.error(f"/api/universities/all error:\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@universities_bp.route("/api/state_summary")
def api_state_summary():
    """Per-state university counts broken out by Carnegie tier."""
    return jsonify(get_state_summary())


@universities_bp.route("/api/universities")
def api_universities():
    """Paginated, filtered, sorted university search."""
    params   = request.args
    page     = max(1, int(params.get("page", 1)))
    sort_by  = params.get("sort", "name")
    sort_dir = params.get("dir", "asc")
    q        = params.get("q", "").strip()
    state    = params.get("state", "").strip()

    return jsonify(
        search_universities(
            q=q,
            state=state,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            filter_params=params.to_dict(),
        )
    )


@universities_bp.route("/api/stats")
def api_stats():
    """Global aggregate stats for the header/stats bar."""
    return jsonify(get_stats())


# ── Single university ─────────────────────────────────────────

@universities_bp.route("/api/university/<int:uid>")
def api_university(uid: int):
    """Full university detail including facts, rankings, awards, and Carnegie data."""
    u = get_university_detail(uid)
    if not u:
        return jsonify({"error": "not found"}), 404
    return jsonify(u)


@universities_bp.route("/api/compare")
def api_compare():
    """
    Return full detail for up to 4 universities by ID.
    Replaces the old anti-pattern of calling app.test_client() internally.
    """
    ids = [int(x) for x in request.args.getlist("ids") if x.isdigit()][:4]
    if not ids:
        return jsonify([])
    return jsonify(get_universities_compare(ids))


@universities_bp.route("/api/compare_fast")
def api_compare_fast():
    """
    Lightweight compare endpoint (facts + Carnegie only, no rankings).
    Also used by the compare page for side-by-side cards.
    """
    ids = [int(x) for x in request.args.getlist("ids") if x.isdigit()][:4]
    if not ids:
        return jsonify([])
    return jsonify(get_universities_compare(ids))


# ── Programs ──────────────────────────────────────────────────

@universities_bp.route("/api/university/<int:uid>/programs")
def api_university_programs(uid: int):
    """Return programs for a university, or a redirect hint if none exist."""
    result = get_programs(uid)
    if isinstance(result, dict) and result.get("error") == "University not found":
        return jsonify(result), 404
    return jsonify(result)


@universities_bp.route("/api/university/<int:uid>/programs/set-featured", methods=["POST"])
def api_set_featured(uid: int):
    body      = request.get_json(silent=True) or {}
    prog_id   = body.get("program_id")
    rep_note  = body.get("reputation_note", "")
    if not prog_id:
        return jsonify({"error": "program_id required"}), 400
    set_featured_program(uid, prog_id, rep_note)
    return jsonify({"ok": True})


@universities_bp.route("/api/university/<int:uid>/programs/update-urls", methods=["POST"])
def api_update_program_urls(uid: int):
    result = update_program_urls(uid)
    if "error" in result:
        return jsonify(result), 404
    return jsonify(result)


@universities_bp.route("/api/university/<int:uid>/programs/reset", methods=["POST"])
def api_reset_programs(uid: int):
    reset_programs(uid)
    return jsonify({"ok": True})
