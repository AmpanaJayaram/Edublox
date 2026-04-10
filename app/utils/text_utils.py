"""
app/utils/text_utils.py
=======================
Text helpers used across services and routes.
"""

import re

# Patterns strongly associated with public institutions.
_PUBLIC_RE = re.compile(
    r"state university|university of |state college|"
    r"\ba&m\b|\ba&t\b|community college|technical college|"
    r"polytechnic|school of mines|naval|military|"
    r"air force|coast guard|merchant marine",
    re.IGNORECASE,
)


def infer_control(name: str) -> str:
    """
    Return 'Public' if the institution name matches known public-institution
    patterns, otherwise return an empty string.
    """
    if not name:
        return ""
    if _PUBLIC_RE.search(name):
        return "Public"
    return ""
