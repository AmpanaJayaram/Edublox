"""
app/services/ai_service.py
==========================
Groq LLM calls for university analysis and match recommendations.
Routes call these functions; all prompt engineering lives here.
"""

import re
from app.extensions import groq_client

_MODEL = "llama-3.3-70b-versatile"


def analyze_universities(context: str) -> dict:
    """
    Generate a structured HTML comparison report for a set of universities.

    Returns {"result": "<html string>"} on success,
    or {"error": "..."} on failure / missing client.
    """
    if not groq_client:
        return {"error": "Groq API key missing. Add GROQ_API_KEY to your .env file."}

    try:
        chat = groq_client.chat.completions.create(
            model=_MODEL,
            max_tokens=1800,
            temperature=0.4,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior university admissions counselor writing a detailed, "
                        "professional comparison report for a prospective student. "
                        "Your response must use this EXACT HTML structure with no deviations:\n\n"
                        "<div class='ai-section'><div class='ai-section-title'>📊 University Overviews</div>"
                        "</div>\n"
                        "<div class='ai-section'><div class='ai-section-title'>🏆 Category Winners</div>"
                        "</div>\n"
                        "<div class='ai-section'><div class='ai-section-title'>💡 Key Insights</div>"
                        "</div>\n"
                        "<div class='ai-verdict'><div class='ai-verdict-title'>✦ Final Recommendation</div>"
                        "</div>"
                    ),
                },
                {
                    "role": "user",
                    "content": f"""Compare these universities for a prospective student and write a detailed professional analysis:

{context}

Write the full analysis using this exact HTML structure:

<div class='ai-section'>
<div class='ai-section-title'>📊 University Overviews</div>
<div class='ai-uni-block'>
<div class='ai-uni-name'>[University Name]</div>
<div class='ai-uni-desc'>[3-4 sentences covering what type of university it is, its strengths, student body characteristics, and notable programs or research.]</div>
<div class='ai-uni-stats'>
<span class='ai-stat-chip'>[Key stat 1]</span>
<span class='ai-stat-chip'>[Key stat 2]</span>
<span class='ai-stat-chip'>[Key stat 3]</span>
</div>
</div>
</div>

<div class='ai-section'>
<div class='ai-section-title'>🏆 Category Winners</div>
<div class='ai-winner-row'><span class='ai-winner-cat'>Best Value</span><span class='ai-winner-name'>[University]</span><span class='ai-winner-why'>[1 sentence why]</span></div>
<div class='ai-winner-row'><span class='ai-winner-cat'>Best for Research</span><span class='ai-winner-name'>[University]</span><span class='ai-winner-why'>[1 sentence why]</span></div>
<div class='ai-winner-row'><span class='ai-winner-cat'>Best Career Outcomes</span><span class='ai-winner-name'>[University]</span><span class='ai-winner-why'>[1 sentence why]</span></div>
<div class='ai-winner-row'><span class='ai-winner-cat'>Most Selective</span><span class='ai-winner-name'>[University]</span><span class='ai-winner-why'>[1 sentence why]</span></div>
<div class='ai-winner-row'><span class='ai-winner-cat'>Best Graduation Rate</span><span class='ai-winner-name'>[University]</span><span class='ai-winner-why'>[1 sentence why]</span></div>
</div>

<div class='ai-section'>
<div class='ai-section-title'>💡 Key Insights</div>
<div class='ai-insight'>⚡ [Important insight]</div>
<div class='ai-insight'>⚡ [Second insight]</div>
<div class='ai-insight'>⚡ [Third insight]</div>
</div>

<div class='ai-verdict'>
<div class='ai-verdict-title'>✦ Final Recommendation</div>
[2-3 sentences giving a clear recommendation.]
</div>

Use the actual data provided. Be specific and helpful.""",
                },
            ],
        )

        text = chat.choices[0].message.content or "Analysis unavailable."
        text = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", text)
        text = re.sub(r"\*(.*?)\*",     r"<em>\1</em>",         text)
        return {"result": text}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


def match_universities(prefs: str, top_names: str) -> dict:
    """
    Generate a personalised recommendation paragraph for a student's top matches.

    Returns {"result": "<text>"} on success,
    or {"error": "..."} on failure / missing client.
    """
    if not groq_client:
        return {"error": "Groq API key missing. Add GROQ_API_KEY to your .env file."}

    try:
        completion = groq_client.chat.completions.create(
            model=_MODEL,
            temperature=0.5,
            max_tokens=500,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a friendly university advisor. "
                        "Write in 2-3 short clear paragraphs with no markdown bullets or headers. "
                        "Be warm, specific, encouraging, and practical."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"A student has these preferences: {prefs}\n\n"
                        f"Top matching universities found: {top_names}\n\n"
                        "Write a personalized 2-3 paragraph recommendation explaining why these "
                        "universities are a good fit and what to look for. Mention 2-3 specific "
                        "universities by name and what makes them special for this student's goals. "
                        "Keep it under 200 words and end with one actionable next step."
                    ),
                },
            ],
        )

        result = completion.choices[0].message.content.strip()
        return {"result": result}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": f"Groq request failed: {str(e)}"}
