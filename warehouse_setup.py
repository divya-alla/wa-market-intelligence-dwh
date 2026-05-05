"""
Star Schema Data Warehouse: WA Business Intelligence
Reads the staging Parquet, builds dimension/fact tables, and creates the MSI view.
"""

import logging
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH      = "wa_business_intelligence.db"
STAGING_PATH = "stg_wa_businesses.parquet"


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def build_warehouse() -> None:
    con = duckdb.connect(DB_PATH)
    log.info("Connected to %s", DB_PATH)

    con.execute(f"""
        CREATE OR REPLACE VIEW stg AS
        SELECT * FROM read_parquet('{STAGING_PATH}')
    """)

    # ── dim_locations ─────────────────────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE dim_locations AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY city, zip_code) AS location_key,
            city,
            zip_code,
            county,
            CAST(AVG(latitude)  AS DOUBLE) AS latitude,
            CAST(AVG(longitude) AS DOUBLE) AS longitude
        FROM stg
        WHERE city IS NOT NULL AND zip_code IS NOT NULL
        GROUP BY city, zip_code, county
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
    con.execute("""
        CREATE OR REPLACE TABLE fact_market_activity AS
        SELECT
            s.ubi,
            dl.location_key,
            di.industry_key,
            s.business_name,
            CAST(s.open_date AS DATE) AS open_date,
            s.status
        FROM stg s
        JOIN dim_locations  dl
          ON  s.city     = dl.city
          AND s.zip_code  = dl.zip_code
        JOIN dim_industries di
          ON  s.naics_code        = di.naics_code
          AND s.strategic_category = di.strategic_category
    """)
    log.info("fact_market_activity → %d rows", _count(con, "fact_market_activity"))

    # ── v_market_gap_analysis (MSI view) ─────────────────────────────────────
    # MSI = zip_category_count / regional_avg_count_for_category
    # < 0.8  → High Opportunity
    # 0.8–1.2 → Balanced Market
    # > 1.2  → Highly Saturated
    con.execute("""
        CREATE OR REPLACE VIEW v_market_gap_analysis AS
        WITH zip_category_counts AS (
            SELECT
                dl.city,
                dl.zip_code,
                di.strategic_category,
                COUNT(*)                           AS competitor_count,
                AVG(CAST(dl.latitude  AS DOUBLE))  AS latitude,
                AVG(CAST(dl.longitude AS DOUBLE))  AS longitude
            FROM fact_market_activity  fma
            JOIN dim_locations   dl ON fma.location_key  = dl.location_key
            JOIN dim_industries  di ON fma.industry_key  = di.industry_key
            GROUP BY dl.city, dl.zip_code, di.strategic_category
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
            ROUND(ra.regional_avg_count, 2)                                         AS regional_avg_count,
            ROUND(zcc.competitor_count / NULLIF(ra.regional_avg_count, 0), 3)       AS msi_score,
            CASE
                WHEN zcc.competitor_count / NULLIF(ra.regional_avg_count, 0) < 0.8
                     THEN 'High Opportunity'
                WHEN zcc.competitor_count / NULLIF(ra.regional_avg_count, 0) <= 1.2
                     THEN 'Balanced Market'
                ELSE      'Highly Saturated'
            END                                                                      AS market_gap_status,
            zcc.latitude,
            zcc.longitude
        FROM zip_category_counts zcc
        JOIN regional_averages ra USING (strategic_category)
        ORDER BY zcc.city, zcc.zip_code, zcc.strategic_category
    """)
    log.info("v_market_gap_analysis view created")

    con.close()
    log.info("Warehouse build complete → %s", DB_PATH)


if __name__ == "__main__":
    build_warehouse()
