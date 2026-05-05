"""
Streamlit Dashboard — WA Market Gap Analysis Engine
Run: streamlit run app.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

DB_PATH = "wa_business_intelligence.db"

st.set_page_config(
    page_title="WA Market Intelligence",
    page_icon="📊",
    layout="wide",
)

st.title("Washington State Market Gap Analysis Engine")
st.caption("Identify underserved markets and high-opportunity zones across Washington State.")

# ── Database connection ───────────────────────────────────────────────────────

@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=True)


def q(sql: str, params: list = None) -> pd.DataFrame:
    con = get_con()
    return con.execute(sql, params or []).df()


# Verify warehouse is ready before rendering anything else
try:
    q("SELECT 1 FROM v_market_gap_analysis LIMIT 1")
except Exception:
    st.error(
        "**Warehouse not found or not built yet.** Run the pipeline first:\n\n"
        "```bash\n"
        "python etl_pipeline.py\n"
        "python warehouse_setup.py\n"
        "```"
    )
    st.stop()

# ── Cascading sidebar filters ─────────────────────────────────────────────────

with st.sidebar:
    st.header("Market Filters")

    cities = q("SELECT DISTINCT city FROM v_market_gap_analysis ORDER BY city")["city"].tolist()
    selected_city = st.selectbox("1. City", cities)

    zips = q(
        "SELECT DISTINCT zip_code FROM v_market_gap_analysis WHERE city = ? ORDER BY zip_code",
        [selected_city],
    )["zip_code"].tolist()
    selected_zip = st.selectbox("2. Zip Code", zips)

    categories = q(
        "SELECT DISTINCT strategic_category FROM v_market_gap_analysis "
        "WHERE city = ? AND zip_code = ? ORDER BY strategic_category",
        [selected_city, selected_zip],
    )["strategic_category"].tolist()
    selected_category = st.selectbox("3. Strategic Category", categories)

# ── Load filtered data ────────────────────────────────────────────────────────

gap_row = q(
    "SELECT * FROM v_market_gap_analysis "
    "WHERE city = ? AND zip_code = ? AND strategic_category = ? LIMIT 1",
    [selected_city, selected_zip, selected_category],
)

competitors = q(
    """
    SELECT
        fma.business_name,
        dl.city,
        dl.zip_code,
        CAST(dl.latitude  AS DOUBLE) AS latitude,
        CAST(dl.longitude AS DOUBLE) AS longitude,
        fma.open_date,
        fma.status
    FROM fact_market_activity fma
    JOIN dim_locations  dl ON fma.location_key = dl.location_key
    JOIN dim_industries di ON fma.industry_key = di.industry_key
    WHERE dl.city = ? AND dl.zip_code = ? AND di.strategic_category = ?
    ORDER BY fma.open_date DESC
    """,
    [selected_city, selected_zip, selected_category],
)

if gap_row.empty:
    st.warning("No data available for this selection.")
    st.stop()

# ── KPI Scorecards ────────────────────────────────────────────────────────────

msi       = float(gap_row["msi_score"].iloc[0])
status    = str(gap_row["market_gap_status"].iloc[0])
count     = int(gap_row["competitor_count"].iloc[0])
reg_avg   = float(gap_row["regional_avg_count"].iloc[0])

STATUS_COLOR = {
    "High Opportunity": "normal",   # renders green
    "Balanced Market":  "off",
    "Highly Saturated": "inverse",  # renders red
}

st.subheader(f"{selected_category}  |  {selected_city}, {selected_zip}")

k1, k2, k3 = st.columns(3)

with k1:
    st.metric(
        label="Competitor Count",
        value=f"{count:,}",
        help="Active businesses in this zip code and category.",
    )

with k2:
    st.metric(
        label="Regional Average",
        value=f"{reg_avg:.1f}",
        help="Average competitor count across all zip codes for this category.",
    )

with k3:
    st.metric(
        label="Market Gap Status",
        value=status,
        delta=f"MSI {msi:.2f}",
        delta_color=STATUS_COLOR.get(status, "off"),
        help="MSI < 0.8 = High Opportunity  |  0.8–1.2 = Balanced  |  > 1.2 = Highly Saturated",
    )

st.divider()

# ── Geospatial Competitor Map ─────────────────────────────────────────────────

map_df = competitors.dropna(subset=["latitude", "longitude"])

if not map_df.empty:
    color_map = {
        "High Opportunity": "#2a9d8f",
        "Balanced Market":  "#f4a261",
        "Highly Saturated": "#e63946",
    }
    marker_color = color_map.get(status, "#457b9d")

    fig = px.scatter_mapbox(
        map_df,
        lat="latitude",
        lon="longitude",
        hover_name="business_name",
        hover_data={
            "city":      True,
            "zip_code":  True,
            "open_date": True,
            "latitude":  False,
            "longitude": False,
        },
        zoom=11,
        height=480,
        title=f"Active Competitors — {selected_category} in {selected_zip}",
        mapbox_style="carto-positron",
    )
    fig.update_traces(marker=dict(size=10, color=marker_color, opacity=0.85))
    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No geocoded records available to map for this selection.")

# ── Competitor Table ──────────────────────────────────────────────────────────

st.subheader("Direct Competitors")

display = competitors[["business_name", "city", "zip_code", "open_date", "status"]].copy()
display.columns = ["Business Name", "City", "Zip Code", "License Date", "Status"]
display["License Date"] = pd.to_datetime(display["License Date"], errors="coerce").dt.date

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Business Name": st.column_config.TextColumn(width="large"),
        "License Date":  st.column_config.DateColumn(),
    },
)
