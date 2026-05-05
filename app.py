"""
Streamlit dashboard — WA Market Gap Analysis
Run: streamlit run app.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

from warehouse_setup import DB_PATH

st.set_page_config(
    page_title="WA Market Intelligence",
    page_icon="📊",
    layout="wide",
)


@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


def load_gaps(con, sector_filter: str, tier_filter: list[str]) -> pd.DataFrame:
    sector_clause = f"AND i.sector = '{sector_filter}'" if sector_filter != "All" else ""
    tier_clause = (
        "AND g.opportunity_tier IN ({})".format(
            ", ".join(f"'{t}'" for t in tier_filter)
        )
        if tier_filter
        else ""
    )
    return con.execute(f"""
        SELECT
            g.opportunity_tier,
            g.gap_score,
            g.local_density,
            g.wa_avg_density,
            geo.city,
            geo.county,
            geo.zip_code,
            geo.latitude,
            geo.longitude,
            i.sector,
            i.naics_title
        FROM fact_market_gaps g
        JOIN dim_geography geo ON g.geo_key = geo.geo_key
        JOIN dim_industry  i   ON g.industry_key = i.industry_key
        WHERE 1=1
          {sector_clause}
          {tier_clause}
        ORDER BY g.gap_score DESC
    """).df()


def load_sectors(con) -> list[str]:
    return ["All"] + con.execute(
        "SELECT DISTINCT sector FROM dim_industry ORDER BY sector"
    ).df()["sector"].tolist()


# ---------- Layout ----------

st.title("Washington State Market Gap Analysis")
st.caption("Identify underserved markets across WA counties and industries.")

con = get_con()

with st.sidebar:
    st.header("Filters")
    sectors = load_sectors(con)
    selected_sector = st.selectbox("Industry Sector", sectors)
    selected_tiers = st.multiselect(
        "Opportunity Tier",
        ["High", "Medium", "Low"],
        default=["High", "Medium"],
    )

df = load_gaps(con, selected_sector, selected_tiers)

if df.empty:
    st.warning("No data found. Run `warehouse_setup.py` then `etl_pipeline.py` first.")
    st.stop()

# KPI row
col1, col2, col3 = st.columns(3)
col1.metric("Markets Identified", f"{len(df):,}")
col2.metric("Avg Gap Score", f"{df['gap_score'].mean():.2f}")
col3.metric("High-Opportunity Zones", f"{(df['opportunity_tier'] == 'High').sum():,}")

st.divider()

# Map
has_coords = df["latitude"].notna() & df["longitude"].notna()
if has_coords.any():
    fig_map = px.scatter_mapbox(
        df[has_coords],
        lat="latitude",
        lon="longitude",
        color="opportunity_tier",
        size="gap_score",
        hover_name="city",
        hover_data={"county": True, "sector": True, "gap_score": ":.2f"},
        color_discrete_map={"High": "#e63946", "Medium": "#f4a261", "Low": "#2a9d8f"},
        mapbox_style="carto-positron",
        zoom=6,
        center={"lat": 47.5, "lon": -120.5},
        title="Market Opportunity Map — Washington State",
    )
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info("Lat/lon data not yet loaded — showing table view only.")

# Bar chart — gap by county
fig_bar = px.bar(
    df.groupby("county")["gap_score"].mean().reset_index().sort_values("gap_score", ascending=False).head(15),
    x="county",
    y="gap_score",
    title="Top 15 Counties by Average Market Gap Score",
    labels={"gap_score": "Gap Score", "county": "County"},
    color="gap_score",
    color_continuous_scale="Reds",
)
st.plotly_chart(fig_bar, use_container_width=True)

# Raw table
with st.expander("Raw Data"):
    st.dataframe(df, use_container_width=True)
