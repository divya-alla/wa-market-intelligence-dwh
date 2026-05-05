"""
ETL Pipeline: WA State Business License Data
Extracts from Socrata API, classifies semantically, saves as Parquet staging layer.
"""

import re
import logging
import requests
import pandas as pd
from typing import Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_URL      = "https://data.wa.gov/resource/4wur-kfnr.json"
RECORD_LIMIT = 15_000
OUTPUT_PATH  = "stg_wa_businesses.parquet"

TARGET_CITIES = [
    "BOTHELL", "LYNNWOOD", "EVERETT", "WOODINVILLE", "REDMOND",
    "KIRKLAND", "SEATTLE", "SPOKANE", "TACOMA", "VANCOUVER",
]

# Priority-ordered rules: first match wins.
# 611610 appears in both Kids and Art specs — Kids takes priority here.
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
        if match_type == "exact"   and code == value:       return category
        if match_type == "prefix"  and code.startswith(value): return category
    return "Other Local Services"


def extract_lat_lon(location_val) -> Tuple[Optional[float], Optional[float]]:
    """Handle both GeoJSON Point and Socrata human_address formats."""
    if not isinstance(location_val, dict):
        return None, None
    try:
        # GeoJSON Point: {"type": "Point", "coordinates": [lon, lat]}
        if location_val.get("type") == "Point":
            coords = location_val.get("coordinates", [])
            if len(coords) >= 2:
                return float(coords[1]), float(coords[0])
        # Socrata flat format: {"latitude": "47.x", "longitude": "-122.x"}
        lat = location_val.get("latitude") or location_val.get("lat")
        lon = location_val.get("longitude") or location_val.get("lon")
        return (float(lat) if lat else None, float(lon) if lon else None)
    except (ValueError, TypeError):
        return None, None


def clean_zip(raw) -> Optional[str]:
    if pd.isna(raw) or not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))[:5]
    return digits if len(digits) == 5 else None


def _find_col(columns, *keywords, exclude=()) -> Optional[str]:
    """Return first column whose name contains any keyword (and none of exclude)."""
    for col in columns:
        if any(k in col for k in keywords) and not any(e in col for e in exclude):
            return col
    return None


def fetch_businesses() -> pd.DataFrame:
    city_list = ", ".join(f"'{c}'" for c in TARGET_CITIES)
    params = {
        "$limit":  RECORD_LIMIT,
        "$where":  f"upper(city) IN ({city_list})",
        "$order":  "opendate DESC",
    }
    log.info("Fetching up to %d records from %s...", RECORD_LIMIT, API_URL)
    resp = requests.get(API_URL, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    log.info("Retrieved %d raw records", len(data))
    return pd.DataFrame(data)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]
    cols = df.columns.tolist()

    # ── Geospatial ──────────────────────────────────────────────────────────
    if "location" in cols:
        latlon         = df["location"].apply(extract_lat_lon)
        df["latitude"]  = latlon.apply(lambda x: x[0])
        df["longitude"] = latlon.apply(lambda x: x[1])
    for coord in ("latitude", "longitude"):
        if coord not in df.columns:
            df[coord] = None
        else:
            df[coord] = pd.to_numeric(df[coord], errors="coerce")

    # ── Identifiers ──────────────────────────────────────────────────────────
    ubi_col  = _find_col(cols, "ubi")
    df["ubi"] = df[ubi_col] if ubi_col else None

    # ── ZIP ──────────────────────────────────────────────────────────────────
    zip_col      = _find_col(cols, "zip")
    df["zip_code"] = df[zip_col].apply(clean_zip) if zip_col else None

    # ── Business name ─────────────────────────────────────────────────────────
    name_col         = _find_col(cols, "businessname", "business_name", "name", exclude=("naics",))
    df["business_name"] = df[name_col].astype(str).str.strip() if name_col else ""

    # ── City & county ─────────────────────────────────────────────────────────
    df["city"]   = df["city"].str.upper().str.strip()   if "city"   in cols else ""
    county_col   = _find_col(cols, "county")
    df["county"] = df[county_col].str.upper().str.strip() if county_col else ""

    # ── NAICS ─────────────────────────────────────────────────────────────────
    naics_col      = _find_col(cols, "naicscode", "naics_code", "naics", exclude=("desc",))
    naics_desc_col = _find_col(cols, "naicsdescription", "naics_description", "naicsdesc")
    df["naics_code"]        = df[naics_col].astype(str).str.strip()        if naics_col      else ""
    df["naics_description"] = df[naics_desc_col].astype(str).str.strip()   if naics_desc_col else ""
    df["strategic_category"] = df["naics_code"].apply(classify_naics)

    # ── Open date & status ────────────────────────────────────────────────────
    date_col    = _find_col(cols, "opendate", "open_date", "licensestart", "startdate")
    df["open_date"] = pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT

    status_col  = _find_col(cols, "status")
    df["status"] = df[status_col].str.upper().str.strip() if status_col else "UNKNOWN"

    # ── Drop rows missing essential fields ────────────────────────────────────
    before = len(df)
    df = df.dropna(subset=["ubi", "latitude", "longitude", "zip_code"])
    log.info("Dropped %d rows missing UBI / coordinates / zip  (%d remain)", before - len(df), len(df))

    keep = ["ubi", "business_name", "city", "zip_code", "county",
            "latitude", "longitude", "naics_code", "naics_description",
            "strategic_category", "open_date", "status"]
    return df[[c for c in keep if c in df.columns]].reset_index(drop=True)


def run() -> None:
    raw     = fetch_businesses()
    staging = transform(raw)
    staging.to_parquet(OUTPUT_PATH, compression="snappy", index=False)
    log.info("Saved %d records → %s", len(staging), OUTPUT_PATH)
    log.info("Strategic category breakdown:\n%s",
             staging["strategic_category"].value_counts().to_string())


if __name__ == "__main__":
    run()
