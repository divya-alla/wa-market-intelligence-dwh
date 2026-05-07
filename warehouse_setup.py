"""
Star Schema Data Warehouse v2: Seattle Regional Business Intelligence
Reads staging Parquet, builds star schema with population data,
and creates the MSI + Opportunity Score view (v_market_gap).
"""

import logging
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH      = "wa_business_intelligence.db"
STAGING_PATH = "stg_wa_businesses.parquet"

# Official estimated city populations (US Census / city sources).
POPULATION_MAP = {
    "SEATTLE":     801_000,
    "BELLEVUE":    155_000,
    "EVERETT":     114_000,
    "KIRKLAND":     97_000,
    "REDMOND":      87_000,
    "BOTHELL":      54_000,
    "LYNNWOOD":     43_000,
    "WOODINVILLE":  14_000,
}


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def build_warehouse() -> None:
    con = duckdb.connect(DB_PATH)
    log.info("Connected to %s", DB_PATH)

    con.execute(f"""
        CREATE OR REPLACE VIEW stg AS
        SELECT * FROM read_parquet('{STAGING_PATH}')
    """)

    # Build the population VALUES clause for inline joining.
    pop_values = ", ".join(f"('{city}', {pop})" for city, pop in POPULATION_MAP.items())

    # ── dim_locations ─────────────────────────────────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_locations AS
        WITH pop_data AS (
            SELECT city, estimated_population
            FROM (VALUES {pop_values}) AS t(city, estimated_population)
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY s.city, s.zip_code) AS location_key,
            s.city,
            s.zip_code,
            COALESCE(pd.estimated_population, 0)            AS estimated_population,
            CAST(AVG(s.latitude)  AS DOUBLE)                AS centroid_lat,
            CAST(AVG(s.longitude) AS DOUBLE)                AS centroid_lon
        FROM stg s
        LEFT JOIN pop_data pd ON s.city = pd.city
        WHERE s.city IS NOT NULL AND s.zip_code IS NOT NULL
        GROUP BY s.city, s.zip_code, pd.estimated_population
    """)
    log.info("dim_locations   → %d rows", _count(con, "dim_locations"))

    # ── dim_industries ────────────────────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE dim_industries AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY naics_code) AS industry_key,
            naics_code,
            naics_description,
            strategic_category
        FROM (
            SELECT DISTINCT naics_code, naics_description, strategic_category
            FROM stg
        )
    """)
    log.info("dim_industries  → %d rows", _count(con, "dim_industries"))

    # ── fact_market_activity ──────────────────────────────────────────────────
    # Per-record lat/lon preserved here so the map renders individual jittered dots.
    con.execute("""
        CREATE OR REPLACE TABLE fact_market_activity AS
        SELECT
            ROW_NUMBER() OVER ()              AS fact_key,
            s.ubi,
            dl.location_key,
            di.industry_key,
            s.business_name,
            s.naics_description,
            CAST(s.latitude  AS DOUBLE)       AS latitude,
            CAST(s.longitude AS DOUBLE)       AS longitude,
            CAST(s.open_date AS DATE)         AS open_date,
            s.status
        FROM stg s
        JOIN dim_locations  dl
          ON  s.city     = dl.city
          AND s.zip_code = dl.zip_code
        JOIN dim_industries di
          ON  s.naics_code        = di.naics_code
          AND s.strategic_category = di.strategic_category
    """)
    log.info("fact_market_activity → %d rows", _count(con, "fact_market_activity"))

    # ── v_market_gap ──────────────────────────────────────────────────────────
    # opportunity_score = population / (competitor_count + 1)
    #   Higher score → fewer competitors per resident → better entry window.
    # density_per_10k   = competitors per 10,000 city residents.
    # msi_score         = zip count / regional average (< 0.8 High Opp, > 1.2 Saturated).
    con.execute("""
        CREATE OR REPLACE VIEW v_market_gap AS
        WITH zip_category_counts AS (
            SELECT
                dl.city,
                dl.zip_code,
                dl.estimated_population,
                di.strategic_category,
                COUNT(*)             AS competitor_count,
                AVG(dl.centroid_lat) AS latitude,
                AVG(dl.centroid_lon) AS longitude
            FROM fact_market_activity fma
            JOIN dim_locations  dl ON fma.location_key = dl.location_key
            JOIN dim_industries di ON fma.industry_key = di.industry_key
            GROUP BY dl.city, dl.zip_code, dl.estimated_population, di.strategic_category
        ),
        regional_averages AS (
            SELECT
                strategic_category,
                AVG(competitor_count) AS regional_avg_count
            FROM zip_category_counts
            GROUP BY strategic_category
        )
        SELECT
            zcc.city,
            zcc.zip_code,
            zcc.strategic_category,
            zcc.competitor_count,
            zcc.estimated_population,
            ROUND(ra.regional_avg_count, 2)                                                              AS regional_avg_count,
            ROUND(zcc.competitor_count / NULLIF(ra.regional_avg_count, 0), 3)                            AS msi_score,
            ROUND(CAST(zcc.estimated_population AS DOUBLE) / (zcc.competitor_count + 1), 1)              AS opportunity_score,
            ROUND(CAST(zcc.competitor_count AS DOUBLE) / NULLIF(zcc.estimated_population, 0) * 10000, 2) AS density_per_10k,
            CASE
                WHEN zcc.competitor_count / NULLIF(ra.regional_avg_count, 0) < 0.8  THEN 'High Opportunity'
                WHEN zcc.competitor_count / NULLIF(ra.regional_avg_count, 0) <= 1.2 THEN 'Balanced Market'
                ELSE 'Highly Saturated'
            END AS market_gap_status,
            zcc.latitude,
            zcc.longitude
        FROM zip_category_counts zcc
        JOIN regional_averages ra USING (strategic_category)
        ORDER BY zcc.city, zcc.zip_code, zcc.strategic_category
    """)
    log.info("v_market_gap view created")

    con.close()
    log.info("Warehouse build complete → %s", DB_PATH)


if __name__ == "__main__":
    build_warehouse()
