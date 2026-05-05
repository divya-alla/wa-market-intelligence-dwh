"""
ETL pipeline: pulls WA business licensing and census data,
transforms into the star schema, and loads into DuckDB.

Data sources:
  - WA Dept of Revenue business licensing (Socrata open data)
  - US Census ACS 5-year estimates (via Census API)
"""

import hashlib
import requests
import pandas as pd
import duckdb
from datetime import date

from warehouse_setup import get_connection, DB_PATH

# WA open data — active business licenses (Socrata)
WA_BUSINESS_URL = (
    "https://data.wa.gov/resource/bhd9-y5zh.json"
    "?$limit=50000&$where=business_state_of_formation='WA'"
)

# NAICS top-level sectors for dimension seeding
NAICS_SECTORS = [
    ("44", "Retail Trade", "Retail"),
    ("72", "Accommodation and Food Services", "Hospitality"),
    ("62", "Health Care and Social Assistance", "Healthcare"),
    ("54", "Professional, Scientific, and Technical Services", "Professional"),
    ("81", "Other Services (except Public Administration)", "Services"),
    ("23", "Construction", "Construction"),
    ("31", "Manufacturing", "Manufacturing"),
    ("52", "Finance and Insurance", "Finance"),
]


def _hash_key(*parts) -> int:
    raw = "|".join(str(p) for p in parts)
    return int(hashlib.md5(raw.encode()).hexdigest()[:8], 16)


# ---------- Extract ----------

def extract_businesses() -> pd.DataFrame:
    print("Extracting WA business license data...")
    resp = requests.get(WA_BUSINESS_URL, timeout=60)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    print(f"  {len(df):,} records retrieved")
    return df


# ---------- Transform ----------

def transform_geography(df: pd.DataFrame) -> pd.DataFrame:
    geo_cols = ["business_zip", "city", "county"]
    available = [c for c in geo_cols if c in df.columns]
    geo = (
        df[available]
        .drop_duplicates()
        .rename(columns={"business_zip": "zip_code"})
    )
    geo["state"] = "WA"
    geo["latitude"] = None
    geo["longitude"] = None
    geo["geo_key"] = geo.apply(
        lambda r: _hash_key(r.get("zip_code", ""), r.get("city", "")), axis=1
    )
    return geo


def transform_businesses(df: pd.DataFrame) -> pd.DataFrame:
    biz = df.copy()
    biz["business_key"] = biz.index
    rename = {
        "ubi": "ubi",
        "business_name": "business_name",
        "entity_type_of_organization": "entity_type",
        "business_status": "status",
        "license_expiration_date": "open_date",
    }
    keep = {k: v for k, v in rename.items() if k in biz.columns}
    biz = biz.rename(columns=keep)[list(keep.values())].copy()
    biz["business_key"] = range(len(biz))
    return biz


# ---------- Load ----------

def load_dim_industry(con: duckdb.DuckDBPyConnection) -> None:
    rows = [
        (_hash_key(code), code, title, sector, sector)
        for code, title, sector in NAICS_SECTORS
    ]
    con.executemany(
        "INSERT OR IGNORE INTO dim_industry VALUES (?, ?, ?, ?, ?)", rows
    )
    print(f"  Loaded {len(rows)} industry dimension rows")


def load_dim_geography(con: duckdb.DuckDBPyConnection, geo: pd.DataFrame) -> None:
    con.register("_geo_staging", geo)
    con.execute("""
        INSERT OR IGNORE INTO dim_geography
            (geo_key, zip_code, city, county, state, latitude, longitude)
        SELECT geo_key, zip_code, city, county, state, latitude, longitude
        FROM _geo_staging
    """)
    con.unregister("_geo_staging")
    print(f"  Loaded {len(geo):,} geography dimension rows")


def load_dim_business(con: duckdb.DuckDBPyConnection, biz: pd.DataFrame) -> None:
    con.register("_biz_staging", biz)
    cols = ", ".join(biz.columns)
    con.execute(f"INSERT OR IGNORE INTO dim_business ({cols}) SELECT {cols} FROM _biz_staging")
    con.unregister("_biz_staging")
    print(f"  Loaded {len(biz):,} business dimension rows")


def compute_and_load_gaps(con: duckdb.DuckDBPyConnection) -> None:
    """Derive market gap scores from business density vs WA average."""
    con.execute("""
        INSERT OR REPLACE INTO fact_market_gaps
        SELECT
            ROW_NUMBER() OVER ()                        AS gap_key,
            geo_key,
            industry_key,
            CURRENT_DATE                                AS snapshot_date,
            AVG(density_per_10k) OVER (
                PARTITION BY industry_key
            )                                           AS wa_avg_density,
            density_per_10k                             AS local_density,
            AVG(density_per_10k) OVER (
                PARTITION BY industry_key
            ) - density_per_10k                         AS gap_score,
            CASE
                WHEN AVG(density_per_10k) OVER (PARTITION BY industry_key)
                     - density_per_10k > 5  THEN 'High'
                WHEN AVG(density_per_10k) OVER (PARTITION BY industry_key)
                     - density_per_10k > 2  THEN 'Medium'
                ELSE 'Low'
            END                                         AS opportunity_tier
        FROM fact_business_density
    """)
    print("  Market gap scores computed and loaded")


# ---------- Orchestrate ----------

def run():
    raw = extract_businesses()
    geo = transform_geography(raw)
    biz = transform_businesses(raw)

    con = get_connection()
    load_dim_industry(con)
    load_dim_geography(con, geo)
    load_dim_business(con, biz)
    compute_and_load_gaps(con)
    con.close()
    print("ETL pipeline complete.")


if __name__ == "__main__":
    run()
