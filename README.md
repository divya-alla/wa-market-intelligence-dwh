# Seattle Regional Business Intelligence Dashboard
### A Market Gap Analysis Tool for Business Owners

---

## What This Tool Does

This dashboard pulls live business license data from the City of Seattle's open data portal and shows you — visually and numerically — **how crowded or open the market is** in 8 cities across the Seattle metro area:

> Bothell · Lynnwood · Everett · Woodinville · Redmond · Kirkland · Seattle · Bellevue

You can use it to answer questions like:
- *"If I open a yoga studio in Kirkland, how many competitors am I walking into?"*
- *"Which ZIP codes in Bellevue are underserved for kids' activities?"*
- *"How long have competing businesses been operating — is this a mature or young market?"*

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Download and process the data (takes ~2 minutes)
python etl_pipeline.py

# 3. Build the analytics warehouse
python warehouse_setup.py

# 4. Launch the dashboard
streamlit run app.py
```

Open your browser to `http://localhost:8501`.

---

## How to Use the Dashboard

### Step 1 — Choose your market with the sidebar filters

The three filters on the left work in a cascade:

| Filter | What it does |
|---|---|
| **City** | Narrows the analysis to one of the 8 metro cities. Changing city automatically resets ZIP and Category. |
| **ZIP Code** | Drills into a specific postal zone within the city. Use "All" to see the whole city. |
| **Business Category** | Focuses on your industry (e.g., "Health, Wellness & Fitness"). Use "All" to see every category. |

> **Tip:** Start broad (city only), look at the map to find hot spots, then drill into the ZIP that interests you.

---

## Understanding the 4 KPI Cards

### 1. Total Competitors
The raw count of active licensed businesses matching your filter. This is your direct competitive set.

- **Low number** → less noise in the market, but also possibly low demand.
- **High number** → proven demand exists, but you'll need a strong differentiation strategy.

---

### 2. Density per 10k Residents
*Competitors ÷ City Population × 10,000*

This normalizes headcount against the city's population, so you can fairly compare a large city like Seattle to a small one like Woodinville.

| Density | What it means |
|---|---|
| < 5 | Very sparse — strong room to enter |
| 5 – 20 | Moderate competition |
| 20 – 50 | Crowded — you need a clear niche |
| > 50 | Saturated — consider an adjacent category |

---

### 3. Avg Market Tenure
The average number of years that competing businesses have been operating (measured from their license start date to today).

- **High tenure (8+ years)** → established players with loyal customer bases. Barrier to entry is higher, but it also proves the market is sustainable.
- **Low tenure (< 3 years)** → a younger, more dynamic market. Competitors haven't locked in customers yet, but the market may still be finding its footing.

---

### 4. Opportunity Rating
Derived from the **Opportunity Score** (explained below). This is your single headline verdict:

| Rating | Opportunity Score | What it means |
|---|---|---|
| **Excellent** | ≥ 5,000 | Very few competitors relative to population. Strong entry window. |
| **Good** | ≥ 1,000 | Healthy market with room for new entrants. |
| **Fair** | ≥ 200 | Competitive but not saturated — differentiation is key. |
| **Saturated** | < 200 | High competitor density. Requires a clear USP or niche focus. |

---

## Understanding the Opportunity Score

The **Opportunity Score** is the core formula of this dashboard:

```
Opportunity Score = City Population ÷ (Competitor Count + 1)
```

The `+ 1` prevents division-by-zero and gives a small boost to entirely empty markets.

**How to interpret it:**

A score of **2,000** means there are roughly 2,000 residents for every business in that category and ZIP. A score of **100** means only 100 residents per business — a very crowded segment.

**Example:**
> Kirkland (97,000 residents) has 12 yoga studios in ZIP 98033.
> Opportunity Score = 97,000 ÷ (12 + 1) = **7,461** → Rating: Excellent

---

## Understanding the MSI Score (Market Saturation Index)

The **MSI Score** compares a specific ZIP code's competitor count against the regional average for that same category:

```
MSI Score = (Competitors in this ZIP) ÷ (Regional Average for this Category)
```

| MSI Range | Status | Meaning |
|---|---|---|
| < 0.8 | **High Opportunity** | Below-average competition for the region |
| 0.8 – 1.2 | **Balanced Market** | Roughly average competition |
| > 1.2 | **Highly Saturated** | Above-average concentration of competitors |

**Why MSI matters:** The Opportunity Score tells you the absolute picture; the MSI tells you the *relative* picture. A ZIP with 50 restaurants might have a low Opportunity Score but an MSI of 0.6 — meaning it actually has *fewer* restaurants than similarly sized ZIPs nearby. That's actionable intelligence.

---

## Reading the Map

The map shows every individual business as a dot, color-coded by category. Dots are spread slightly (jittered) so overlapping businesses in the same location are all visible rather than stacking on top of each other.

**Hover over any dot** to see:
- Business name
- Exact NAICS (industry) description
- City and ZIP code
- License start date

**Dense clusters** → high competition zones.  
**Sparse areas within a city** → potential underserved neighborhoods.

---

## Data Source & Freshness

Data is pulled live from the **City of Seattle Open Data Portal** each time you run `etl_pipeline.py`. Licenses represent active businesses at the time of the pull.

- Dataset: Seattle Business Licenses (`wnbq-64tb`)
- Up to 50,000 records per pull
- Re-run the pipeline monthly for fresh competitive intelligence.

---

## Glossary

| Term | Definition |
|---|---|
| **NAICS Code** | North American Industry Classification System — the official government code for a business type |
| **Strategic Category** | A simplified grouping of NAICS codes used in this dashboard (e.g., all NAICS codes for yoga, gyms, and wellness grouped under "Health, Wellness & Fitness") |
| **MSI** | Market Saturation Index — relative competitor density vs. regional average |
| **Opportunity Score** | Population per competitor; higher is better for new entrants |
| **Jitter** | A small random offset applied to map coordinates so overlapping businesses are all visible |
