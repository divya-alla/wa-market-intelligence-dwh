"""
ETL Pipeline: Seattle Regional Business License Data
Extracts from data.seattle.gov Socrata API, applies per-record jitter, saves Parquet staging layer.
"""

import re
import logging
import requests
import numpy as np
import pandas as pd
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_URL      = "https://data.seattle.gov/resource/wnbq-64tb.json"
RECORD_LIMIT = 50_000
OUTPUT_PATH  = "stg_wa_businesses.parquet"
JITTER_RANGE = 0.004
RNG_SEED     = 42

TARGET_CITIES = [
    "BOTHELL", "LYNNWOOD", "EVERETT", "WOODINVILLE",
    "REDMOND", "KIRKLAND", "SEATTLE", "BELLEVUE",
]

# Static city-level centroids used as base coordinates for all records.
CITY_CENTROIDS = {
    "SEATTLE":     (47.6062, -122.3321),
    "BELLEVUE":    (47.6101, -122.2015),
    "KIRKLAND":    (47.6815, -122.2087),
    "REDMOND":     (47.6740, -122.1215),
    "BOTHELL":     (47.7623, -122.2054),
    "LYNNWOOD":    (47.8209, -122.3151),
    "EVERETT":     (47.9790, -122.2021),
    "WOODINVILLE": (47.7543, -122.1638),
}

# Priority-ordered classification rules — first match wins.
NAICS_RULES = [
    ("exact",  "611620", "Kids Activities & Education"),
    ("exact",  "611610", "Kids Activities & Education"),
    ("exact",  "624410", "Kids Activities & Education"),
    ("exact",  "711510", "Art Workshops & Creative"),
    ("exact",  "541430", "Art Workshops & Creative"),
    ("exact",  "711130", "Art Workshops & Creative"),
    ("exact",  "561920", "Event & Party Businesses"),
    ("exact",  "532284", "Event & Party Businesses"),
    ("exact",  "711310", "Event & Party Businesses"),
    ("exact",  "532289", "Event & Party Businesses"),
    ("exact",  "561720", "Professional Cleaning Services"),
    ("exact",  "561740", "Professional Cleaning Services"),
    ("exact",  "713940", "Health, Wellness & Fitness"),
    ("exact",  "812112", "Health, Wellness & Fitness"),
    ("exact",  "812199", "Health, Wellness & Fitness"),
    ("prefix", "722",    "Restaurants & Food Services"),
    ("prefix", "44",     "Retail & Boutiques"),
    ("prefix", "45",     "Retail & Boutiques"),
]


def classify_naics(code: str) -> str:
    code = str(code).strip() if code else ""
    for match_type, value, category in NAICS_RULES:
        if match_type == "exact"  and code == value:          return category
        if match_type == "prefix" and code.startswith(value): return category
    return "Other Local Services"


def clean_zip(raw) -> Optional[str]:
    if pd.isna(raw) or not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))[:5]
    return digits if len(digits) == 5 else None


def _find_col(columns, *keywords, exclude=()) -> Optional[str]:
    """Return the first column name containing any keyword and none of the exclude terms."""
    for col in columns:
        lower = col.lower()
        if any(k in lower for k in keywords) and not any(e in lower for e in exclude):
            return col
    return None


def fetch_businesses() -> pd.DataFrame:
    city_list = ", ".join(f"'{c}'" for c in TARGET_CITIES)
    params = {
        "$limit": RECORD_LIMIT,
        "$where": f"city in({city_list})",
    }
    log.info("Fetching up to %d records from %s ...", RECORD_LIMIT, API_URL)
    try:
        resp = requests.get(API_URL, params=params, timeout=120)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        log.error("API request timed out after 120 s — check connectivity and retry")
        raise
    except requests.exceptions.HTTPError as exc:
        log.error("HTTP error from API: %s", exc)
        raise
    except requests.exceptions.RequestException as exc:
        log.error("API request failed: %s", exc)
        raise

    data = resp.json()
    log.info("Retrieved %d raw records", len(data))
    return pd.DataFrame(data)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]
    cols = df.columns.tolist()

    # ── Identifiers ───────────────────────────────────────────────────────────
    ubi_col = _find_col(cols, "ubi", "license_number", "business_id")
    df["ubi"] = df[ubi_col].astype(str).str.strip() if ubi_col else df.index.astype(str)

    # ── Business name ─────────────────────────────────────────────────────────
    name_col = _find_col(
        cols, "trade_name", "businessname", "business_legal_name",
        "business_name", "name", exclude=("naics",),
    )
    df["business_name"] = df[name_col].astype(str).str.strip() if name_col else ""

    # ── City ──────────────────────────────────────────────────────────────────
    df["city"] = df["city"].str.upper().str.strip() if "city" in cols else ""

    # ── ZIP: standardize to exactly 5 digits ──────────────────────────────────
    zip_col = _find_col(cols, "zip")
    df["zip_code"] = df[zip_col].apply(clean_zip) if zip_col else None

    # ── NAICS ─────────────────────────────────────────────────────────────────
    naics_col = _find_col(
        cols, "primary_naics_code", "naicscode", "naics_code", "naics",
        exclude=("desc", "description"),
    )
    naics_desc_col = _find_col(cols, "naics_description", "naicsdescription", "naicsdesc")
    df["naics_code"]         = df[naics_col].astype(str).str.strip()      if naics_col      else ""
    df["naics_description"]  = df[naics_desc_col].astype(str).str.strip() if naics_desc_col else ""
    df["strategic_category"] = df["naics_code"].apply(classify_naics)

    # ── Dates & status ────────────────────────────────────────────────────────
    date_col = _find_col(cols, "license_start_date", "opendate", "open_date", "startdate")
    df["open_date"] = pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT

    status_col = _find_col(cols, "license_status", "status")
    df["status"] = df[status_col].str.upper().str.strip() if status_col else "UNKNOWN"

    # ── Geospatial: city centroid + per-record random jitter ──────────────────
    # Jitter is non-negotiable: it spreads coincident points for density analysis.
    rng        = np.random.default_rng(RNG_SEED)
    base_lat   = df["city"].map(lambda c: CITY_CENTROIDS.get(c, (None, None))[0])
    base_lon   = df["city"].map(lambda c: CITY_CENTROIDS.get(c, (None, None))[1])
    jitter_lat = rng.uniform(-JITTER_RANGE, JITTER_RANGE, len(df))
    jitter_lon = rng.uniform(-JITTER_RANGE, JITTER_RANGE, len(df))
    df["latitude"]  = base_lat + jitter_lat
    df["longitude"] = base_lon + jitter_lon

    # ── Drop rows missing essential fields ────────────────────────────────────
    before = len(df)
    df = df.dropna(subset=["latitude", "longitude", "zip_code"])
    log.info(
        "Dropped %d rows (unknown city / missing zip) → %d remain",
        before - len(df), len(df),
    )

    keep = ["ubi", "business_name", "city", "zip_code",
            "latitude", "longitude", "naics_code", "naics_description",
            "strategic_category", "open_date", "status"]
    return df[[c for c in keep if c in df.columns]].reset_index(drop=True)


def run() -> None:
    raw     = fetch_businesses()
    staging = transform(raw)
    staging.to_parquet(OUTPUT_PATH, compression="snappy", index=False)
    log.info("Saved %d records → %s", len(staging), OUTPUT_PATH)
    log.info(
        "Strategic category breakdown:\n%s",
        staging["strategic_category"].value_counts().to_string(),
    )


if __name__ == "__main__":
    run()
