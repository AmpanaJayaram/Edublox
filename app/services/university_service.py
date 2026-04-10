"""
app/services/university_service.py
===================================
All database query logic for university data.
Routes call these functions and handle only request parsing + JSON responses.
"""

import math
from app.db.connection import get_conn
from app.config import PAGE_SIZE
from app.utils.constants import (
    SORT_COLS, FILTER_MAP, FACTS_KEY, CARNEGIE_INT_FIELDS,
)
from app.utils.text_utils import infer_control


# ── States ────────────────────────────────────────────────────

def get_all_states() -> list[str]:
    """Return a sorted list of state abbreviations that have universities."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT state FROM universities
                WHERE state IS NOT NULL AND state != ''
                ORDER BY state
            """)
            return [r["state"] for r in cur.fetchall()]


# ── Fast flat listing (client-side filtering) ─────────────────

def get_universities_flat(state_filter: str = "") -> list[dict]:
    """
    Return a flat list of all universities with their key facts.
    Used by the client-side filter/search on the map and index views.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:

            # Check which optional columns exist on carnegie_classifications
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'carnegie_classifications'
            """)
            cc_cols = {r["column_name"] for r in cur.fetchall()}

            tier_expr    = "cc.tier_label"          if "tier_label"          in cc_cols else "'T3'"
            control_expr = "cc.institution_control" if "institution_control" in cc_cols else "''"

            where = "WHERE u.state = %s" if state_filter else ""
            sql = f"""
                SELECT u.id,
                       u.name,
                       u.city,
                       u.state,
                       u.school_url,
                       COALESCE({tier_expr}, 'T3')  AS tier,
                       COALESCE({control_expr}, '') AS control
                FROM universities u
                LEFT JOIN carnegie_classifications cc ON cc.university_id = u.id
                {where}
                ORDER BY u.name
            """
            cur.execute(sql, [state_filter] if state_filter else [])
            rows = cur.fetchall()

            result = [
                {
                    "id":         int(r["id"]),
                    "name":       r["name"]       or "",
                    "city":       r["city"]        or "",
                    "state":      r["state"]       or "",
                    "school_url": r["school_url"]  or "",
                    "tier":       r["tier"]        or "T3",
                    "control":    r["control"]     or "",
                }
                for r in rows
            ]

            # Attach key facts in a second query (faster than per-row subqueries)
            if result:
                ids      = [r["id"] for r in result]
                id_index = {r["id"]: i for i, r in enumerate(result)}
                fmt      = ",".join(["%s"] * len(ids))
                cur.execute(
                    f"""
                    SELECT university_id, label, value_numeric
                    FROM university_facts
                    WHERE university_id IN ({fmt})
                      AND label IN %s
                    """,
                    ids + [tuple(FACTS_KEY.keys())],
                )
                for fr in cur.fetchall():
                    uid = int(fr["university_id"])
                    key = FACTS_KEY.get(fr["label"])
                    val = fr["value_numeric"]
                    if key and uid in id_index and val is not None:
                        idx = id_index[uid]
                        if key not in result[idx]:
                            result[idx][key] = float(val)

    return result


# ── State summary (map view) ──────────────────────────────────

def get_state_summary() -> dict:
    """Return tier counts per state for the map view."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.state,
                       COUNT(*) AS total,
                       COUNT(CASE WHEN cc.tier_label = 'T1' THEN 1 END) AS t1,
                       COUNT(CASE WHEN cc.tier_label = 'T2' THEN 1 END) AS t2,
                       COUNT(CASE WHEN cc.tier_label = 'T3' THEN 1 END) AS t3
                FROM universities u
                LEFT JOIN carnegie_classifications cc ON cc.university_id = u.id
                WHERE u.state IS NOT NULL AND length(u.state) = 2
                GROUP BY u.state
                ORDER BY u.state
            """)
            rows = cur.fetchall()
    return {
        r["state"]: {
            "total": r["total"],
            "t1": r["t1"],
            "t2": r["t2"],
            "t3": r["t3"],
        }
        for r in rows
    }


# ── Paginated search ──────────────────────────────────────────

def search_universities(
    q: str = "",
    state: str = "",
    sort_by: str = "name",
    sort_dir: str = "asc",
    page: int = 1,
    filter_params: dict | None = None,
) -> dict:
    """
    Return a paginated, filtered, sorted list of universities.

    filter_params keys match FILTER_MAP (acc_min, acc_max, tuition_min, …).
    Returns {"results": [...], "total": int, "page": int, "pages": int}.
    """
    filter_params = filter_params or {}

    where    = ["1=1"]
    args     = []
    fact_joins  = ""
    join_args   = []

    if q:
        where.append("(u.name ILIKE %s OR u.city ILIKE %s OR u.state ILIKE %s)")
        pct = f"%{q}%"
        args += [pct, pct, pct]

    if state:
        where.append("u.state = %s")
        args.append(state)

    for i, (param_key, (section, label, op)) in enumerate(FILTER_MAP.items()):
        val = filter_params.get(param_key)
        if val:
            try:
                val_f = float(val)
                alias = f"f{i}"
                fact_joins += f"""
                    JOIN university_facts {alias}
                      ON {alias}.university_id = u.id
                     AND {alias}.section       = %s
                     AND {alias}.label         = %s
                     AND {alias}.value_numeric {op} %s
                """
                join_args += [section, label, val_f]
            except (ValueError, TypeError):
                pass

    sort_col  = SORT_COLS.get(sort_by, "u.name")
    sort_null = "NULLS LAST" if sort_dir == "asc" else "NULLS FIRST"
    order     = f"{sort_col} {sort_dir.upper()} {sort_null}"
    where_str = " AND ".join(where)
    all_args  = join_args + args
    offset    = (page - 1) * PAGE_SIZE

    count_sql = f"""
        SELECT COUNT(DISTINCT u.id)
        FROM universities u {fact_joins}
        WHERE {where_str}
    """
    data_sql = f"""
        SELECT DISTINCT
            u.id, u.name, u.city, u.state, u.school_url, u.catalog_url,
            (SELECT value         FROM university_facts WHERE university_id=u.id AND label='Acceptance Rate'    LIMIT 1) AS acceptance_rate,
            (SELECT value         FROM university_facts WHERE university_id=u.id AND label='In-State Tuition'  LIMIT 1) AS in_state_tuition,
            (SELECT value         FROM university_facts WHERE university_id=u.id AND label='Total Enrollment'  LIMIT 1) AS student_size,
            (SELECT value         FROM university_facts WHERE university_id=u.id AND label='Average SAT Score' LIMIT 1) AS average_sat,
            (SELECT value_numeric FROM university_facts WHERE university_id=u.id AND label='Acceptance Rate'   LIMIT 1) AS acceptance_rate_num,
            (SELECT value_numeric FROM university_facts WHERE university_id=u.id AND label='In-State Tuition'  LIMIT 1) AS tuition_num,
            (SELECT value_numeric FROM university_facts WHERE university_id=u.id AND label='Total Enrollment'  LIMIT 1) AS size_num
        FROM universities u {fact_joins}
        WHERE {where_str}
        ORDER BY {order}
        LIMIT %s OFFSET %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, all_args)
            total = cur.fetchone()["count"]
            cur.execute(data_sql, all_args + [PAGE_SIZE, offset])
            rows = [dict(r) for r in cur.fetchall()]

    return {
        "results": rows,
        "total":   total,
        "page":    page,
        "pages":   max(1, math.ceil(total / PAGE_SIZE)),
    }


# ── University detail ─────────────────────────────────────────

def get_university_detail(uid: int) -> dict | None:
    """
    Return a fully hydrated university dict (facts, rankings, awards, carnegie),
    or None if the university doesn't exist.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("SELECT * FROM universities WHERE id = %s", (uid,))
            u = cur.fetchone()
            if not u:
                return None
            u = dict(u)

            # Facts grouped by section
            cur.execute("""
                SELECT section, label, value, value_numeric,
                       source_url, extracted_at, extractor, confidence, notes
                FROM university_facts
                WHERE university_id = %s
                ORDER BY section, label
            """, (uid,))
            facts_by_section: dict = {}
            for f in cur.fetchall():
                f = dict(f)
                sec = f.pop("section")
                if f.get("extracted_at"):
                    f["extracted_at"] = f["extracted_at"].strftime("%Y-%m-%d")
                if f.get("value_numeric") is not None:
                    f["value_numeric"] = float(f["value_numeric"])
                if f.get("confidence") is not None:
                    f["confidence"] = float(f["confidence"])
                facts_by_section.setdefault(sec, []).append(f)
            u["facts"] = facts_by_section

            # Rankings + awards
            try:
                cur.execute("""
                    SELECT type, description, source_url,
                           source_url_publisher, source_url_university,
                           publisher_name, publisher_confidence, evidence_snippet,
                           extracted_at
                    FROM university_rankings
                    WHERE university_id = %s
                    ORDER BY type, id
                """, (uid,))
            except Exception:
                conn.rollback()
                cur.execute("""
                    SELECT type, description, source_url, extracted_at
                    FROM university_rankings
                    WHERE university_id = %s
                    ORDER BY type, id
                """, (uid,))
            rankings_raw = cur.fetchall()
            u["rankings"] = [dict(r) for r in rankings_raw if r["type"] == "ranking"]
            u["awards"]   = [dict(r) for r in rankings_raw if r["type"] == "award"]

            # Carnegie classification
            u = _attach_carnegie(conn, cur, u)

    return u


def get_universities_compare(ids: list[int]) -> list[dict]:
    """
    Return a list of hydrated university dicts for the compare view.
    Replaces the old anti-pattern of calling app.test_client() internally.
    """
    results = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            for uid in ids:
                cur.execute(
                    "SELECT id, name, city, state, school_url FROM universities WHERE id = %s",
                    (uid,),
                )
                u = cur.fetchone()
                if not u:
                    continue
                u = dict(u)

                cur.execute("""
                    SELECT section, label, value, value_numeric, source_url, confidence
                    FROM university_facts
                    WHERE university_id = %s
                    ORDER BY section, label
                """, (uid,))
                facts: dict = {}
                for f in cur.fetchall():
                    f = dict(f)
                    sec = f.pop("section")
                    facts.setdefault(sec, []).append({
                        **f,
                        "value_numeric": float(f["value_numeric"]) if f["value_numeric"] is not None else None,
                        "confidence":    float(f["confidence"])    if f["confidence"]    is not None else None,
                    })
                u["facts"] = facts

                try:
                    cur.execute("""
                        SELECT tier_label, institution_control, institutional_classification,
                               research_spending, research_doctorates, faculty_count,
                               total_enrollment, basic_classification, designations
                        FROM carnegie_classifications WHERE university_id = %s
                    """, (uid,))
                    cc = cur.fetchone()
                    if cc:
                        cc = dict(cc)
                        for k in CARNEGIE_INT_FIELDS:
                            if cc.get(k) is not None:
                                cc[k] = int(cc[k])
                        if not cc.get("institution_control"):
                            cc["institution_control"] = infer_control(u.get("name", ""))
                        u["tier"]    = cc.get("tier_label", "T3")
                        u["carnegie"] = cc
                    else:
                        u["tier"]                      = "T3"
                        u["carnegie"]                  = None
                        u["institution_type_inferred"] = infer_control(u.get("name", ""))
                except Exception:
                    conn.rollback()
                    u["tier"]     = "T3"
                    u["carnegie"] = None

                results.append(u)

    return results


# ── Global stats ──────────────────────────────────────────────

def get_stats() -> dict:
    """Return high-level counts shown on the stats bar."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM universities")
            total = cur.fetchone()["n"]

            cur.execute(
                "SELECT COUNT(DISTINCT state) AS n FROM universities WHERE state IS NOT NULL"
            )
            states = cur.fetchone()["n"]

            cur.execute(
                "SELECT AVG(value_numeric) AS avg FROM university_facts WHERE label='Acceptance Rate'"
            )
            avg_acc = cur.fetchone()["avg"]

            cur.execute("""
                SELECT COUNT(DISTINCT university_id) AS n
                FROM carnegie_classifications
                WHERE institutional_classification IS NOT NULL
            """)
            carnegie_count = cur.fetchone()["n"]

    return {
        "total":          total,
        "states":         states,
        "avg_acceptance": round(float(avg_acc), 1) if avg_acc else None,
        "carnegie_count": carnegie_count,
    }


# ── Private helpers ───────────────────────────────────────────

def _attach_carnegie(conn, cur, u: dict) -> dict:
    """Add carnegie classification data to a university dict in place."""
    try:
        cur.execute("""
            SELECT id, university_id, institutional_classification,
                   basic_classification, carnegie_page_url,
                   extracted_at, confidence, match_method,
                   tier_label, tier_reason, evidence_snippet, notes,
                   research_spending, research_doctorates,
                   institution_control, designations,
                   faculty_count, total_enrollment, dorm_capacity
            FROM carnegie_classifications WHERE university_id = %s
        """, (u["id"],))
    except Exception:
        conn.rollback()
        try:
            cur.execute("SELECT * FROM carnegie_classifications WHERE university_id = %s", (u["id"],))
        except Exception:
            conn.rollback()
            cur.execute("SELECT NULL LIMIT 0")

    cc = cur.fetchone()
    if cc:
        cc = dict(cc)
        if cc.get("extracted_at") and hasattr(cc["extracted_at"], "strftime"):
            cc["extracted_at"] = cc["extracted_at"].strftime("%Y-%m-%d")
        if cc.get("confidence") is not None:
            cc["confidence"] = float(cc["confidence"])
        for k in CARNEGIE_INT_FIELDS:
            if cc.get(k) is not None:
                cc[k] = int(cc[k])
        if not cc.get("institution_control"):
            cc["institution_control"] = infer_control(u.get("name", ""))
        u["carnegie"] = cc
        u["tier"]     = cc.get("tier_label", "T3")
    else:
        u["carnegie"] = None
        u["tier"]     = "T3"
        inferred = infer_control(u.get("name", ""))
        if inferred:
            u["institution_type_inferred"] = inferred

    return u
