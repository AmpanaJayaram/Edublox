"""
app/utils/constants.py
======================
Static lookup tables and query mappings shared across routes and services.
"""

# ── State abbreviation → full name ────────────────────────────

STATE_NAMES: dict[str, str] = {
    "AL": "Alabama",        "AK": "Alaska",         "AZ": "Arizona",
    "AR": "Arkansas",       "CA": "California",     "CO": "Colorado",
    "CT": "Connecticut",    "DE": "Delaware",       "FL": "Florida",
    "GA": "Georgia",        "HI": "Hawaii",         "ID": "Idaho",
    "IL": "Illinois",       "IN": "Indiana",        "IA": "Iowa",
    "KS": "Kansas",         "KY": "Kentucky",       "LA": "Louisiana",
    "ME": "Maine",          "MD": "Maryland",       "MA": "Massachusetts",
    "MI": "Michigan",       "MN": "Minnesota",      "MS": "Mississippi",
    "MO": "Missouri",       "MT": "Montana",        "NE": "Nebraska",
    "NV": "Nevada",         "NH": "New Hampshire",  "NJ": "New Jersey",
    "NM": "New Mexico",     "NY": "New York",       "NC": "North Carolina",
    "ND": "North Dakota",   "OH": "Ohio",           "OK": "Oklahoma",
    "OR": "Oregon",         "PA": "Pennsylvania",   "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota",   "TN": "Tennessee",
    "TX": "Texas",          "UT": "Utah",           "VT": "Vermont",
    "VA": "Virginia",       "WA": "Washington",     "WV": "West Virginia",
    "WI": "Wisconsin",      "WY": "Wyoming",        "DC": "District of Columbia",
}

# ── Sort column expressions ───────────────────────────────────
# Maps URL sort param → SQL expression used in ORDER BY.

SORT_COLS: dict[str, str] = {
    "name": "u.name",
    "acceptance_rate":      "(SELECT value_numeric FROM university_facts WHERE university_id=u.id AND label='Acceptance Rate' LIMIT 1)",
    "in_state_tuition":     "(SELECT value_numeric FROM university_facts WHERE university_id=u.id AND label='In-State Tuition' LIMIT 1)",
    "student_size":         "(SELECT value_numeric FROM university_facts WHERE university_id=u.id AND label='Total Enrollment' LIMIT 1)",
    "average_sat":          "(SELECT value_numeric FROM university_facts WHERE university_id=u.id AND label='Average SAT Score' LIMIT 1)",
    "graduation_rate_4yr":  "(SELECT value_numeric FROM university_facts WHERE university_id=u.id AND label='4-Year Graduation Rate' LIMIT 1)",
    "median_earnings_10yr": "(SELECT value_numeric FROM university_facts WHERE university_id=u.id AND label='Median Earnings (10yr)' LIMIT 1)",
}

# ── Filter map ────────────────────────────────────────────────
# Maps URL query param → (facts section, facts label, SQL operator).

FILTER_MAP: dict[str, tuple[str, str, str]] = {
    "acc_min":     ("admissions",   "Acceptance Rate",        ">="),
    "acc_max":     ("admissions",   "Acceptance Rate",        "<="),
    "tuition_min": ("tuition",      "In-State Tuition",       ">="),
    "tuition_max": ("tuition",      "In-State Tuition",       "<="),
    "size_min":    ("student_life", "Total Enrollment",       ">="),
    "size_max":    ("student_life", "Total Enrollment",       "<="),
    "sat_min":     ("admissions",   "Average SAT Score",      ">="),
    "sat_max":     ("admissions",   "Average SAT Score",      "<="),
    "grad_min":    ("outcomes",     "4-Year Graduation Rate", ">="),
    "grad_max":    ("outcomes",     "4-Year Graduation Rate", "<="),
}

# ── Facts key map ─────────────────────────────────────────────
# Maps university_facts.label → JSON response key used in the flat endpoint.

FACTS_KEY: dict[str, str] = {
    "Acceptance Rate":      "acceptance_rate",
    "In-State Tuition":     "in_state_tuition",
    "Out-of-State Tuition": "out_of_state_tuition",
    "Total Enrollment":     "student_size",
    "Median Earnings (10yr)": "median_earnings",
}

# ── Carnegie numeric fields ───────────────────────────────────
# Fields that must be cast to int when returned from carnegie_classifications.

CARNEGIE_INT_FIELDS: tuple[str, ...] = (
    "research_spending",
    "research_doctorates",
    "faculty_count",
    "total_enrollment",
    "dorm_capacity",
)
