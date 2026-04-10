"""
tests/test_services.py
======================
Unit tests for the service layer.  No Flask app, no HTTP — pure logic.

Covered services
----------------
ai_service     analyze_universities(), match_universities()
               (full coverage without real Groq calls)

university_service  infer_control() via text_utils (pure function — no DB)

Notes
-----
university_service DB functions (get_universities_flat, search_universities,
get_university_detail, etc.) are tested indirectly through the route tests in
test_routes.py.  Direct unit tests for those functions require a more complete
DB cursor mock that returns real column descriptions; add them here as the
service layer stabilises.
"""

import pytest
from unittest.mock import MagicMock, patch


# ═══════════════════════════════════════════════════════════════
# ai_service — analyze_universities()
# ═══════════════════════════════════════════════════════════════

class TestAnalyzeUniversities:

    def _make_groq_response(self, content: str):
        mock_resp = MagicMock()
        mock_resp.choices[0].message.content = content
        return mock_resp

    def test_returns_result_key_on_success(self):
        from app.services.ai_service import analyze_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_groq_response(
            "<div class='ai-section'>Analysis here</div>"
        )

        with patch("app.services.ai_service.groq_client", mock_client):
            result = analyze_universities("University A data")

        assert "result" in result
        assert "error" not in result

    def test_no_groq_client_returns_error(self):
        from app.services.ai_service import analyze_universities

        with patch("app.services.ai_service.groq_client", None):
            result = analyze_universities("some context")

        assert "error" in result
        assert "API key" in result["error"]

    def test_groq_exception_returns_error(self):
        from app.services.ai_service import analyze_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = Exception("connection refused")

        with patch("app.services.ai_service.groq_client", mock_client):
            result = analyze_universities("some context")

        assert "error" in result
        assert "connection refused" in result["error"]

    def test_markdown_bold_converted_to_html(self):
        from app.services.ai_service import analyze_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_groq_response(
            "**Bold text** and *italic text*"
        )

        with patch("app.services.ai_service.groq_client", mock_client):
            result = analyze_universities("context")

        assert "<strong>Bold text</strong>" in result["result"]
        assert "<em>italic text</em>" in result["result"]

    def test_empty_groq_response_falls_back_gracefully(self):
        from app.services.ai_service import analyze_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_groq_response(None)

        with patch("app.services.ai_service.groq_client", mock_client):
            result = analyze_universities("context")

        assert "result" in result
        assert result["result"] == "Analysis unavailable."

    def test_uses_correct_model(self):
        from app.services.ai_service import analyze_universities, _MODEL

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_groq_response("ok")

        with patch("app.services.ai_service.groq_client", mock_client):
            analyze_universities("context")

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert call_kwargs["model"] == _MODEL

    def test_passes_context_in_user_message(self):
        from app.services.ai_service import analyze_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_groq_response("ok")

        with patch("app.services.ai_service.groq_client", mock_client):
            analyze_universities("MIT vs Stanford comparison data")

        messages = mock_client.chat.completions.create.call_args[1]["messages"]
        user_content = next(m["content"] for m in messages if m["role"] == "user")
        assert "MIT vs Stanford comparison data" in user_content


# ═══════════════════════════════════════════════════════════════
# ai_service — match_universities()
# ═══════════════════════════════════════════════════════════════

class TestMatchUniversities:

    def _make_groq_response(self, content: str):
        mock_resp = MagicMock()
        mock_resp.choices[0].message.content = content
        return mock_resp

    def test_returns_result_on_success(self):
        from app.services.ai_service import match_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_groq_response(
            "You should consider MIT for its research strengths."
        )

        with patch("app.services.ai_service.groq_client", mock_client):
            result = match_universities("research-focused", "MIT, Stanford")

        assert "result" in result
        assert "error" not in result

    def test_no_groq_client_returns_error(self):
        from app.services.ai_service import match_universities

        with patch("app.services.ai_service.groq_client", None):
            result = match_universities("research", "MIT")

        assert "error" in result
        assert "API key" in result["error"]

    def test_groq_exception_returns_error_with_prefix(self):
        from app.services.ai_service import match_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = Exception("timeout")

        with patch("app.services.ai_service.groq_client", mock_client):
            result = match_universities("research", "MIT")

        assert "error" in result
        assert "Groq request failed" in result["error"]

    def test_preferences_and_names_forwarded_to_prompt(self):
        from app.services.ai_service import match_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_groq_response("ok")

        with patch("app.services.ai_service.groq_client", mock_client):
            match_universities("loves small liberal arts", "Williams, Amherst, Bowdoin")

        messages = mock_client.chat.completions.create.call_args[1]["messages"]
        combined = " ".join(m["content"] for m in messages)
        assert "loves small liberal arts" in combined
        assert "Williams, Amherst, Bowdoin" in combined

    def test_result_is_stripped_string(self):
        from app.services.ai_service import match_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_groq_response(
            "   Some recommendation.   "
        )

        with patch("app.services.ai_service.groq_client", mock_client):
            result = match_universities("prefs", "names")

        assert result["result"] == "Some recommendation."

    def test_temperature_is_set(self):
        from app.services.ai_service import match_universities

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = self._make_groq_response("ok")

        with patch("app.services.ai_service.groq_client", mock_client):
            match_universities("prefs", "names")

        kwargs = mock_client.chat.completions.create.call_args[1]
        assert "temperature" in kwargs


# ═══════════════════════════════════════════════════════════════
# text_utils — infer_control()  (pure function, no mocking needed)
# ═══════════════════════════════════════════════════════════════

class TestInferControl:
    """
    infer_control() is a pure string-classification function.
    These tests document the expected mapping and guard against regressions.
    """

    @pytest.fixture(autouse=True)
    def _import(self):
        from app.utils.text_utils import infer_control
        self.infer_control = infer_control

    def test_state_university_returns_public(self):
        assert self.infer_control("Texas State University") == "Public"

    def test_university_of_returns_public(self):
        assert self.infer_control("University of Michigan") == "Public"

    def test_a_and_m_returns_public(self):
        assert self.infer_control("Texas A&M University") == "Public"

    def test_military_returns_public(self):
        assert self.infer_control("United States Naval Academy") == "Public"

    def test_private_name_returns_empty_string(self):
        # infer_control only detects public patterns — private = ""
        assert self.infer_control("Harvard University") == ""
        assert self.infer_control("Duke University") == ""

    def test_case_insensitive(self):
        assert self.infer_control("STATE UNIVERSITY OF NEW YORK") == "Public"
        assert self.infer_control("university of texas") == "Public"

    def test_unknown_returns_empty_string(self):
        result = self.infer_control("Unrecognised Institution Type")
        assert result == ""

    def test_empty_string_does_not_raise(self):
        result = self.infer_control("")
        assert isinstance(result, str)

    def test_none_equivalent_does_not_raise(self):
        result = self.infer_control("None")
        assert isinstance(result, str)


# ═══════════════════════════════════════════════════════════════
# config — environment helpers (no DB connection needed)
# ═══════════════════════════════════════════════════════════════

class TestConfig:

    def test_missing_env_var_raises_environment_error(self, monkeypatch):
        monkeypatch.delenv("DB_HOST", raising=False)
        # Re-importing config re-runs load_dotenv but _require_env is a function
        from app.config import _require_env
        with pytest.raises(EnvironmentError, match="DB_HOST"):
            _require_env("DB_HOST")

    def test_int_env_raises_on_non_integer(self, monkeypatch):
        monkeypatch.setenv("SOME_INT", "not_a_number")
        from app.config import _int_env
        with pytest.raises(EnvironmentError, match="must be an integer"):
            _int_env("SOME_INT")

    def test_safe_int_env_returns_default_when_missing(self, monkeypatch):
        monkeypatch.delenv("MISSING_INT", raising=False)
        from app.config import _safe_int_env
        assert _safe_int_env("MISSING_INT", 99) == 99

    def test_safe_int_env_returns_value_when_present(self, monkeypatch):
        monkeypatch.setenv("MY_PAGE_SIZE", "50")
        from app.config import _safe_int_env
        assert _safe_int_env("MY_PAGE_SIZE", 24) == 50

    def test_safe_int_env_returns_default_for_blank(self, monkeypatch):
        monkeypatch.setenv("BLANK_INT", "  ")
        from app.config import _safe_int_env
        assert _safe_int_env("BLANK_INT", 10) == 10
