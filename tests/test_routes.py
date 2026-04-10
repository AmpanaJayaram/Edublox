"""
tests/test_routes.py
====================
Integration-style tests for every route in the UniSearch API.

Each test uses the `client` fixture (Flask test client) and patches the
relevant service function so no real DB or Groq calls are made.

Covered blueprints
------------------
universities_bp  /api/universities/all, /api/universities, /api/stats,
                 /api/state_summary, /api/university/<id>, /api/compare,
                 /api/compare_fast, /api/university/<id>/programs
ai_bp            /api/analyze, /api/match_ai
"""

import pytest
from unittest.mock import patch


# ═══════════════════════════════════════════════════════════════
# /api/universities/all
# ═══════════════════════════════════════════════════════════════

class TestUniversitiesAll:

    def test_returns_list(self, client, sample_uni_list):
        with patch("app.routes.universities.get_universities_flat", return_value=sample_uni_list):
            r = client.get("/api/universities/all")
        assert r.status_code == 200
        data = r.get_json()
        assert isinstance(data, list)
        assert len(data) == 2

    def test_state_filter_passed_through(self, client, sample_uni_list):
        with patch("app.routes.universities.get_universities_flat", return_value=sample_uni_list) as m:
            client.get("/api/universities/all?state=Texas")
        m.assert_called_once_with(state_filter="Texas")

    def test_empty_list_is_200(self, client):
        with patch("app.routes.universities.get_universities_flat", return_value=[]):
            r = client.get("/api/universities/all")
        assert r.status_code == 200
        assert r.get_json() == []

    def test_service_exception_returns_500(self, client):
        with patch("app.routes.universities.get_universities_flat", side_effect=RuntimeError("db down")):
            r = client.get("/api/universities/all")
        assert r.status_code == 500
        assert "error" in r.get_json()


# ═══════════════════════════════════════════════════════════════
# /api/universities  (paginated search)
# ═══════════════════════════════════════════════════════════════

class TestUniversitiesSearch:

    def _mock_result(self, items=None, total=0):
        return {"universities": items or [], "total": total, "page": 1, "pages": 1}

    def test_basic_request_200(self, client):
        with patch("app.routes.universities.search_universities", return_value=self._mock_result()):
            r = client.get("/api/universities")
        assert r.status_code == 200

    def test_response_has_required_keys(self, client, sample_uni_list):
        payload = self._mock_result(sample_uni_list, total=2)
        with patch("app.routes.universities.search_universities", return_value=payload):
            r = client.get("/api/universities?q=Texas&state=Texas&page=1")
        data = r.get_json()
        assert "universities" in data
        assert "total" in data

    def test_query_params_forwarded(self, client):
        with patch("app.routes.universities.search_universities", return_value=self._mock_result()) as m:
            client.get("/api/universities?q=MIT&state=Massachusetts&sort=name&dir=desc&page=2")
        _, kwargs = m.call_args
        assert kwargs["q"] == "MIT"
        assert kwargs["state"] == "Massachusetts"
        assert kwargs["sort_by"] == "name"
        assert kwargs["sort_dir"] == "desc"
        assert kwargs["page"] == 2

    def test_page_defaults_to_1(self, client):
        with patch("app.routes.universities.search_universities", return_value=self._mock_result()) as m:
            client.get("/api/universities")
        _, kwargs = m.call_args
        assert kwargs["page"] == 1

    def test_page_clamps_to_1_for_zero(self, client):
        with patch("app.routes.universities.search_universities", return_value=self._mock_result()) as m:
            client.get("/api/universities?page=0")
        _, kwargs = m.call_args
        assert kwargs["page"] == 1


# ═══════════════════════════════════════════════════════════════
# /api/stats
# ═══════════════════════════════════════════════════════════════

class TestStats:

    def test_returns_dict(self, client):
        fake_stats = {"total": 3500, "states": 50, "avg_acceptance": 62.4}
        with patch("app.routes.universities.get_stats", return_value=fake_stats):
            r = client.get("/api/stats")
        assert r.status_code == 200
        assert r.get_json()["total"] == 3500


# ═══════════════════════════════════════════════════════════════
# /api/state_summary
# ═══════════════════════════════════════════════════════════════

class TestStateSummary:

    def test_returns_200(self, client):
        summary = [{"state": "Texas", "count": 120}]
        with patch("app.routes.universities.get_state_summary", return_value=summary):
            r = client.get("/api/state_summary")
        assert r.status_code == 200
        assert r.get_json() == summary


# ═══════════════════════════════════════════════════════════════
# /api/university/<id>
# ═══════════════════════════════════════════════════════════════

class TestUniversityDetail:

    def test_known_id_returns_200(self, client, sample_uni):
        with patch("app.routes.universities.get_university_detail", return_value=sample_uni):
            r = client.get("/api/university/1")
        assert r.status_code == 200
        data = r.get_json()
        assert data["id"] == 1
        assert data["name"] == "University of Texas at Austin"

    def test_unknown_id_returns_404(self, client):
        with patch("app.routes.universities.get_university_detail", return_value=None):
            r = client.get("/api/university/999999")
        assert r.status_code == 404
        assert r.get_json()["error"] == "not found"

    def test_response_has_core_keys(self, client, sample_uni):
        with patch("app.routes.universities.get_university_detail", return_value=sample_uni):
            r = client.get("/api/university/1")
        data = r.get_json()
        for key in ("id", "name", "state", "control"):
            assert key in data, f"Missing key: {key}"


# ═══════════════════════════════════════════════════════════════
# /api/compare  and  /api/compare_fast
# ═══════════════════════════════════════════════════════════════

class TestCompare:

    @pytest.mark.parametrize("endpoint", ["/api/compare", "/api/compare_fast"])
    def test_returns_list_of_universities(self, client, sample_uni_list, endpoint):
        with patch("app.routes.universities.get_universities_compare", return_value=sample_uni_list):
            r = client.get(f"{endpoint}?ids=1&ids=2")
        assert r.status_code == 200
        assert isinstance(r.get_json(), list)
        assert len(r.get_json()) == 2

    @pytest.mark.parametrize("endpoint", ["/api/compare", "/api/compare_fast"])
    def test_no_ids_returns_empty_list(self, client, endpoint):
        with patch("app.routes.universities.get_universities_compare", return_value=[]):
            r = client.get(endpoint)
        assert r.status_code == 200
        assert r.get_json() == []

    @pytest.mark.parametrize("endpoint", ["/api/compare", "/api/compare_fast"])
    def test_caps_at_4_ids(self, client, endpoint):
        with patch("app.routes.universities.get_universities_compare", return_value=[]) as m:
            client.get(f"{endpoint}?ids=1&ids=2&ids=3&ids=4&ids=5")
        called_ids = m.call_args[0][0]
        assert len(called_ids) <= 4

    @pytest.mark.parametrize("endpoint", ["/api/compare", "/api/compare_fast"])
    def test_non_digit_ids_ignored(self, client, endpoint):
        with patch("app.routes.universities.get_universities_compare", return_value=[]) as m:
            client.get(f"{endpoint}?ids=1&ids=abc&ids=2")
        called_ids = m.call_args[0][0]
        assert called_ids == [1, 2]


# ═══════════════════════════════════════════════════════════════
# /api/university/<id>/programs
# ═══════════════════════════════════════════════════════════════

class TestPrograms:

    def test_returns_programs_list(self, client):
        fake_programs = [{"id": 10, "name": "Computer Science", "level": "Bachelor's"}]
        with patch("app.routes.universities.get_programs", return_value=fake_programs):
            r = client.get("/api/university/1/programs")
        assert r.status_code == 200
        assert isinstance(r.get_json(), list)

    def test_university_not_found_returns_404(self, client):
        with patch("app.routes.universities.get_programs",
                   return_value={"error": "University not found"}):
            r = client.get("/api/university/999/programs")
        assert r.status_code == 404


# ═══════════════════════════════════════════════════════════════
# /api/analyze
# ═══════════════════════════════════════════════════════════════

class TestAnalyze:

    def test_valid_context_returns_result(self, client):
        with patch("app.routes.ai.analyze_universities",
                   return_value={"result": "<div>Analysis</div>"}):
            r = client.post("/api/analyze", json={"context": "University A vs B"})
        assert r.status_code == 200
        assert "result" in r.get_json()

    def test_missing_context_returns_400(self, client):
        r = client.post("/api/analyze", json={})
        assert r.status_code == 400
        assert "error" in r.get_json()

    def test_empty_context_string_returns_400(self, client):
        r = client.post("/api/analyze", json={"context": "   "})
        assert r.status_code == 400

    def test_groq_error_returns_500(self, client):
        with patch("app.routes.ai.analyze_universities",
                   return_value={"error": "Something went wrong"}):
            r = client.post("/api/analyze", json={"context": "some context"})
        assert r.status_code == 500

    def test_missing_api_key_returns_503(self, client):
        with patch("app.routes.ai.analyze_universities",
                   return_value={"error": "Groq API key missing"}):
            r = client.post("/api/analyze", json={"context": "some context"})
        assert r.status_code == 503

    def test_no_json_body_returns_400(self, client):
        r = client.post("/api/analyze", data="not json", content_type="text/plain")
        assert r.status_code == 400


# ═══════════════════════════════════════════════════════════════
# /api/match_ai
# ═══════════════════════════════════════════════════════════════

class TestMatchAI:

    def test_valid_request_returns_result(self, client):
        with patch("app.routes.ai.match_universities",
                   return_value={"result": "Great fit!"}):
            r = client.post("/api/match_ai", json={
                "preferences": "I like research",
                "top_names":   "MIT, Stanford",
            })
        assert r.status_code == 200
        assert r.get_json()["result"] == "Great fit!"

    def test_missing_preferences_returns_400(self, client):
        r = client.post("/api/match_ai", json={"top_names": "MIT"})
        assert r.status_code == 400

    def test_missing_top_names_returns_400(self, client):
        r = client.post("/api/match_ai", json={"preferences": "research"})
        assert r.status_code == 400

    def test_both_fields_empty_returns_400(self, client):
        r = client.post("/api/match_ai", json={"preferences": "", "top_names": ""})
        assert r.status_code == 400

    def test_groq_error_returns_500(self, client):
        with patch("app.routes.ai.match_universities",
                   return_value={"error": "Groq request failed: timeout"}):
            r = client.post("/api/match_ai", json={
                "preferences": "research",
                "top_names":   "MIT",
            })
        assert r.status_code == 500

    def test_missing_api_key_returns_503(self, client):
        with patch("app.routes.ai.match_universities",
                   return_value={"error": "Groq API key missing"}):
            r = client.post("/api/match_ai", json={
                "preferences": "research",
                "top_names":   "MIT",
            })
        assert r.status_code == 503
