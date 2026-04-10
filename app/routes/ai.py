"""
app/routes/ai.py
================
Groq-powered AI endpoints.
"""

from flask import Blueprint, request, jsonify
from app.services.ai_service import analyze_universities, match_universities

ai_bp = Blueprint("ai", __name__)


@ai_bp.route("/api/analyze", methods=["POST"])
def api_analyze():
    """Generate a structured HTML comparison report for a set of universities."""
    body    = request.get_json(silent=True) or {}
    context = str(body.get("context", "")).strip()

    if not context:
        return jsonify({"error": "No university data provided"}), 400

    result = analyze_universities(context)
    if "error" in result:
        status = 503 if "API key" in result["error"] else 500
        return jsonify(result), status

    return jsonify(result)


@ai_bp.route("/api/match_ai", methods=["POST"])
def api_match_ai():
    """Generate a personalised recommendation for a student's top university matches."""
    body      = request.get_json(silent=True) or {}
    prefs     = str(body.get("preferences", "")).strip()
    top_names = str(body.get("top_names", "")).strip()

    if not prefs or not top_names:
        return jsonify({"error": "Missing preferences or top matches"}), 400

    result = match_universities(prefs, top_names)
    if "error" in result:
        status = 503 if "API key" in result["error"] else 500
        return jsonify(result), status

    return jsonify(result)
