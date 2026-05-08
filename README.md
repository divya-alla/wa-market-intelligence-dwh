# Seattle Regional Business Intelligence Dashboard

Regional market intelligence platform for analyzing business density, competitive saturation, and market opportunity across the Seattle metro area using public business license data.

Built an end-to-end analytics pipeline that ingests live business license records from the Seattle Open Data API, transforms and categorizes NAICS data, stores curated datasets in a local warehouse, and serves interactive insights through a Streamlit dashboard.

---

## Features

- Processed and analyzed 50K+ business license records
- Built automated ETL pipeline for ingestion and transformation
- Implemented market opportunity and saturation scoring models
- Developed interactive geographic competitor visualization
- Created ZIP-level and category-level competitive analysis
- Normalized NAICS classifications into strategic business categories

### Supported Cities
- Seattle
- Bellevue
- Redmond
- Kirkland
- Bothell
- Woodinville
- Lynnwood
- Everett

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Processing | Python, Pandas, NumPy |
| Storage | SQLite |
| Visualization | Streamlit, Plotly, PyDeck |
| Data Source | Seattle Open Data API |

---

## Architecture

```text
Seattle Open Data API
        ↓
Python ETL Pipeline
        ↓
Data Cleaning + Categorization
        ↓
SQLite Analytics Warehouse
        ↓
Streamlit Dashboard
```

---

## Project Structure

```text
├── app.py
├── etl_pipeline.py
├── warehouse_setup.py
├── requirements.txt
├── data/
└── warehouse/
```

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Run the Application

### 1. Run ETL Pipeline

```bash
python etl_pipeline.py
```

### 2. Build Analytics Warehouse

```bash
python warehouse_setup.py
```

### 3. Launch Dashboard

```bash
streamlit run app.py
```

Application runs locally at:

```text
http://localhost:8501
```

---

## Core Metrics

### Opportunity Score

Measures population-to-competitor ratio.

```text
Opportunity Score = Population / (Competitor Count + 1)
```

### Market Saturation Index (MSI)

Benchmarks ZIP-level competition against regional category averages.

```text
MSI = ZIP Competitor Count / Regional Category Average
```

### Density per 10K Residents

```text
Density = (Competitor Count / Population) * 10,000
```

---

## Data Source

Seattle Open Data Portal  
Dataset: Seattle Business Licenses (`wnbq-64tb`)

---

## Future Enhancements

- Historical trend analysis
- Scheduled pipeline orchestration
- Census demographic integration
- PostgreSQL warehouse migration
- Dockerized deployment
- Multi-region support
