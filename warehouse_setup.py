"""
Star schema setup for WA market intelligence data warehouse.
Run once to initialize the DuckDB database and all dimension/fact tables.
"""

import duckdb


DB_PATH = "wa_market.db"


def get_connection():
    return duckdb.connect(DB_PATH)


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.executemany("CREATE TABLE IF NOT EXISTS {} ({})".format(tbl, cols) for tbl, cols in TABLES.items())


TABLES = {
    "dim_date": """
        date_key        INTEGER PRIMARY KEY,
        full_date       DATE,
        year            INTEGER,
        quarter         INTEGER,
        month           INTEGER,
        month_name      VARCHAR,
        day_of_week     INTEGER
    """,
    "dim_geography": """
        geo_key         INTEGER PRIMARY KEY,
        zip_code        VARCHAR,
        city            VARCHAR,
        county          VARCHAR,
        state           VARCHAR DEFAULT 'WA',
        latitude        DOUBLE,
        longitude       DOUBLE
    """,
    "dim_industry": """
        industry_key    INTEGER PRIMARY KEY,
        naics_code      VARCHAR,
        naics_title     VARCHAR,
        sector          VARCHAR,
        subsector       VARCHAR
    """,
    "dim_business": """
        business_key    INTEGER PRIMARY KEY,
        ubi             VARCHAR UNIQUE,
        business_name   VARCHAR,
        entity_type     VARCHAR,
        status          VARCHAR,
        open_date       DATE
    """,
    "fact_business_density": """
        density_key     INTEGER PRIMARY KEY,
        date_key        INTEGER REFERENCES dim_date(date_key),
        geo_key         INTEGER REFERENCES dim_geography(geo_key),
        industry_key    INTEGER REFERENCES dim_industry(industry_key),
        business_count  INTEGER,
        population      INTEGER,
        density_per_10k DOUBLE,
        median_income   DOUBLE
    """,
    "fact_market_gaps": """
        gap_key             INTEGER PRIMARY KEY,
        geo_key             INTEGER REFERENCES dim_geography(geo_key),
        industry_key        INTEGER REFERENCES dim_industry(industry_key),
        snapshot_date       DATE,
        wa_avg_density      DOUBLE,
        local_density       DOUBLE,
        gap_score           DOUBLE,
        opportunity_tier    VARCHAR
    """,
}


def populate_dim_date(con: duckdb.DuckDBPyConnection, start: str = "2018-01-01", end: str = "2025-12-31") -> None:
    con.execute(f"""
        INSERT OR IGNORE INTO dim_date
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
            d                                       AS full_date,
            year(d)                                 AS year,
            quarter(d)                              AS quarter,
            month(d)                                AS month,
            monthname(d)                            AS month_name,
            dayofweek(d)                            AS day_of_week
        FROM generate_series(DATE '{start}', DATE '{end}', INTERVAL 1 DAY) t(d)
    """)


def setup():
    con = get_connection()
    for table_name, columns in TABLES.items():
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
    populate_dim_date(con)
    con.close()
    print(f"Warehouse initialized at {DB_PATH}")


if __name__ == "__main__":
    setup()
