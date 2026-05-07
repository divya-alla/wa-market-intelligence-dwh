"""
Seattle Regional Business Intelligence Dashboard
Run: streamlit run app.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import date

DB_PATH = "wa_business_intelligence.db"

st.set_page_config(
    page_title="Seattle BI Dashboard",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Slate & Teal dark theme ───────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp                              { background-color: #0f1923; color: #e2e8f0; }
    [data-testid="stSidebar"]           { background-color: #1a2535; border-right: 1px solid #2d3f55; }
    [data-testid="stMetric"]            { background-color: #1e2d3d; border: 1px solid #2d4a6a;
                                          border-radius: 8px; padding: 16px !important; }
    [data-testid="stMetricLabel"] p     { color: #64b5c4 !important; font-size: 0.78rem !important;
                                          text-transform: uppercase; letter-spacing: 0.06em; }
    [data-testid="stMetricValue"]       { color: #e2e8f0 !important; font-weight: 700 !important; }
    h1, h2, h3                          { color: #e2e8f0 !important; }
    hr                                  { border-color: #2d3f55 !important; }
    .stSelectbox label                  { color: #64b5c4 !important; font-weight: 600; }
    [data-testid="stSidebar"] h2        { color: #14b8a6 !important; }
    [data-testid="stDataFrame"]         { background-color: #1a2535; }
    .title-bar                          { border-left: 4px solid #14b8a6; padding-left: 14px; margin-bottom: 1.2rem; }
    .title-bar h1                       { margin: 0; font-size: 1.75rem; }
    .title-bar p                        { color: #64b5c4; margin: 4px 0 0 0; font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)

# ── Database ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=True)


def q(sql: str, params: list = None) -> pd.DataFrame:
    return get_con().execute(sql, params or []).df()


try:
    q("SELECT 1 FROM v_market_gap LIMIT 1")
except Exception:
    st.error(
        "**Warehouse not found.** Run the pipeline first:\n\n"
        "```\npython etl_pipeline.py\npython warehouse_setup.py\n```"
    )
    st.stop()

# ── Page header ───────────────────────────────────────────────────────────────
st.markdown("""
<div class="title-bar">
    <h1>Seattle Regional Business Intelligence</h1>
    <p>Market Gap Analysis · Competitor Density · Opportunity Scoring</p>
</div>
""", unsafe_allow_html=True)

# ── Session-state initialisation ──────────────────────────────────────────────
all_cities = q("SELECT DISTINCT city FROM v_market_gap ORDER BY city")["city"].tolist()

if "sel_city" not in st.session_state:
    st.session_state.sel_city = all_cities[0] if all_cities else None
if "sel_zip" not in st.session_state:
    st.session_state.sel_zip = "All"
if "sel_category" not in st.session_state:
    st.session_state.sel_category = "All"


def _on_city_change():
    """Cascade: reset both dependent filters when city changes."""
    st.session_state.sel_zip      = "All"
    st.session_state.sel_category = "All"


def _on_zip_change():
    """Cascade: reset category when zip changes."""
    st.session_state.sel_category = "All"


# ── Sidebar: cascading filters ────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## Market Filters")
    st.markdown("---")

    selected_city = st.selectbox(
        "City",
        options=all_cities,
        key="sel_city",
        on_change=_on_city_change,
    )

    zip_rows    = q("SELECT DISTINCT zip_code FROM v_market_gap WHERE city = ? ORDER BY zip_code",
                    [selected_city])
    zip_options = ["All"] + zip_rows["zip_code"].tolist()
    if st.session_state.sel_zip not in zip_options:
        st.session_state.sel_zip = "All"

    selected_zip = st.selectbox(
        "ZIP Code",
        options=zip_options,
        key="sel_zip",
        on_change=_on_zip_change,
    )

    if selected_zip == "All":
        cat_rows = q(
            "SELECT DISTINCT strategic_category FROM v_market_gap "
            "WHERE city = ? ORDER BY strategic_category",
            [selected_city],
        )
    else:
        cat_rows = q(
            "SELECT DISTINCT strategic_category FROM v_market_gap "
            "WHERE city = ? AND zip_code = ? ORDER BY strategic_category",
            [selected_city, selected_zip],
        )
    cat_options = ["All"] + cat_rows["strategic_category"].tolist()
    if st.session_state.sel_category not in cat_options:
        st.session_state.sel_category = "All"

    selected_category = st.selectbox(
        "Business Category",
        options=cat_options,
        key="sel_category",
    )

    st.markdown("---")
    st.markdown(
        "<small style='color:#64b5c4'>Changing City resets ZIP & Category.<br>"
        "Changing ZIP resets Category.</small>",
        unsafe_allow_html=True,
    )

# ── Build parameterised WHERE clauses ─────────────────────────────────────────
gap_parts  = ["city = ?"];  gap_params  = [selected_city]
biz_parts  = ["dl.city = ?"]; biz_params = [selected_city]

if selected_zip != "All":
    gap_parts.append("zip_code = ?");    gap_params.append(selected_zip)
    biz_parts.append("dl.zip_code = ?"); biz_params.append(selected_zip)

if selected_category != "All":
    gap_parts.append("strategic_category = ?");    gap_params.append(selected_category)
    biz_parts.append("di.strategic_category = ?"); biz_params.append(selected_category)

gap_where = " AND ".join(gap_parts)
biz_where = " AND ".join(biz_parts)

# ── Aggregated KPI data ───────────────────────────────────────────────────────
gap_df = q(f"""
    SELECT
        SUM(competitor_count)   AS total_competitors,
        AVG(density_per_10k)    AS avg_density,
        AVG(opportunity_score)  AS avg_opp_score
    FROM v_market_gap
    WHERE {gap_where}
""", gap_params)

# ── Individual business records for map + table ───────────────────────────────
businesses = q(f"""
    SELECT
        fma.business_name,
        fma.naics_description,
        di.strategic_category,
        dl.city,
        dl.zip_code,
        fma.latitude,
        fma.longitude,
        fma.open_date,
        fma.status
    FROM fact_market_activity fma
    JOIN dim_locations  dl ON fma.location_key = dl.location_key
    JOIN dim_industries di ON fma.industry_key = di.industry_key
    WHERE {biz_where}
    ORDER BY fma.open_date DESC
""", biz_params)

if gap_df.empty or businesses.empty:
    st.warning("No data for this selection. Try broadening your filters.")
    st.stop()

# ── KPI calculations ──────────────────────────────────────────────────────────
total_competitors = int(gap_df["total_competitors"].iloc[0] or 0)
avg_density       = float(gap_df["avg_density"].iloc[0]       or 0.0)
avg_opp_score     = float(gap_df["avg_opp_score"].iloc[0]     or 0.0)

today         = pd.Timestamp(date.today())
biz_dated     = businesses.dropna(subset=["open_date"]).copy()
biz_dated["open_date"] = pd.to_datetime(biz_dated["open_date"], errors="coerce")
avg_tenure_yrs = ((today - biz_dated["open_date"]).dt.days.mean() / 365.25
                  if not biz_dated.empty else 0.0)

if   avg_opp_score >= 5_000: opp_rating, opp_color = "Excellent", "normal"
elif avg_opp_score >= 1_000: opp_rating, opp_color = "Good",      "normal"
elif avg_opp_score >=   200: opp_rating, opp_color = "Fair",      "off"
else:                         opp_rating, opp_color = "Saturated", "inverse"

# ── KPI cards ─────────────────────────────────────────────────────────────────
label = selected_city
if selected_zip      != "All": label += f"  ·  {selected_zip}"
if selected_category != "All": label += f"  ·  {selected_category}"
st.markdown(f"### {label}")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Total Competitors", f"{total_competitors:,}",
              help="Active licensed businesses matching your current filters.")
with c2:
    st.metric("Density per 10k Residents", f"{avg_density:.1f}",
              help="Competitor count per 10,000 city residents. Higher = more crowded market.")
with c3:
    st.metric("Avg Market Tenure", f"{avg_tenure_yrs:.1f} yrs",
              help="Average years businesses have been operating in this selection.")
with c4:
    st.metric("Opportunity Rating", opp_rating,
              delta=f"Score {avg_opp_score:,.0f}",
              delta_color=opp_color,
              help="Population ÷ (Competitors + 1). Higher = fewer rivals per resident = stronger entry window.")

st.divider()

# ── Competitor density map ─────────────────────────────────────────────────────
CATEGORY_COLORS = {
    "Kids Activities & Education":    "#14b8a6",
    "Art Workshops & Creative":       "#a78bfa",
    "Event & Party Businesses":       "#f59e0b",
    "Professional Cleaning Services": "#60a5fa",
    "Health, Wellness & Fitness":     "#34d399",
    "Restaurants & Food Services":    "#f87171",
    "Retail & Boutiques":             "#fb923c",
    "Other Local Services":           "#94a3b8",
}

map_df = businesses.dropna(subset=["latitude", "longitude"])

if not map_df.empty:
    fig = px.scatter_mapbox(
        map_df,
        lat="latitude",
        lon="longitude",
        color="strategic_category",
        color_discrete_map=CATEGORY_COLORS,
        hover_name="business_name",
        hover_data={
            "naics_description":  True,
            "city":               True,
            "zip_code":           True,
            "open_date":          True,
            "latitude":           False,
            "longitude":          False,
            "strategic_category": False,
        },
        zoom=11,
        center={"lat": map_df["latitude"].mean(), "lon": map_df["longitude"].mean()},
        height=520,
        mapbox_style="carto-darkmatter",
        title="Business Competitor Density Map",
        labels={"strategic_category": "Category"},
    )
    fig.update_traces(marker=dict(size=7, opacity=0.80))
    fig.update_layout(
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        paper_bgcolor="#0f1923",
        font_color="#e2e8f0",
        legend=dict(bgcolor="#1a2535", bordercolor="#2d3f55", font=dict(color="#e2e8f0")),
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No geocoded records available for this selection.")

# ── Business directory table ──────────────────────────────────────────────────
st.subheader("Business Directory")

display = businesses[
    ["business_name", "naics_description", "strategic_category",
     "city", "zip_code", "open_date", "status"]
].copy()
display.columns = ["Business Name", "NAICS Description", "Category",
                   "City", "ZIP", "License Date", "Status"]
display["License Date"] = pd.to_datetime(display["License Date"], errors="coerce").dt.date

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Business Name":     st.column_config.TextColumn(width="large"),
        "NAICS Description": st.column_config.TextColumn(width="medium"),
        "License Date":      st.column_config.DateColumn(),
    },
)
