# NYC 311 Service Request Pipeline

A production-style data engineering pipeline that ingests, transforms, validates, and loads NYC 311 service request data into PostgreSQL.

---

## Overview

This pipeline pulls live service request data from the NYC Open Data API (Socrata), cleans and validates it, and loads it into a local PostgreSQL database. It was built as a learning project to develop real data engineering skills — medallion architecture, upsert patterns, data quality checks, and CLI-driven pipeline orchestration. The pipeline is designed to be run on a schedule, with each run safely upserting new and updated records without duplicating existing data.

---

## Architecture

    NYC Open Data API (Socrata)
            ↓
       Bronze Layer (raw parquet)       ← raw API response, no changes
            ↓
       Silver Layer (cleaned parquet)   ← typed, nulls handled, columns dropped
            ↓
       Validation (data quality checks) ← critical checks raise, warnings logged
            ↓
       PostgreSQL (Docker)              ← upserted on unique_key

---

## Tech Stack

| Tool | Why |
|---|---|
| Python | Core language for the pipeline |
| requests | Makes HTTP calls to the Socrata API |
| pandas | DataFrame operations for transform and validate |
| pyarrow / parquet | Columnar storage for bronze and silver layers — preserves types, smaller than CSV |
| SQLAlchemy | Connects Python to PostgreSQL, enables upsert pattern |
| PostgreSQL + Docker | Local database, containerized so setup is reproducible on any machine |
| Click | Builds the CLI so the whole pipeline runs with one command |
| pytest | Tests transform and validate logic against a fake DataFrame |

---

## Setup & Running

**1. Clone the repo**
```bash
git clone https://github.com/stoor812/nyc-311-pipeline.git
cd nyc-311-pipeline
```

**2. Create and activate virtual environment**
```bash
python -m venv venv
source venv/Scripts/activate  # Windows
source venv/bin/activate       # Mac/Linux
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Set up your `.env` file**

Create a `.env` file in the project root:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nyc_data
DB_USER=pipeline
DB_PASSWORD=pipeline
```

**5. Start PostgreSQL with Docker**
```bash
docker compose up -d
```

**6. Run the pipeline**
```bash
python -m src.pipeline.cli run
```

With a custom record limit:
```bash
python -m src.pipeline.cli run --limit 1000
```

**7. Run tests**
```bash
python -m pytest tests/ -v
```

---

## Project Structure

```
nyc-311-pipeline/
├── data/
│   ├── bronze/        # raw parquet files from API
│   └── silver/        # cleaned and typed parquet files
├── src/
│   └── pipeline/
│       ├── ingest.py      # fetches data from Socrata API, saves to bronze
│       ├── transform.py   # casts types, drops columns, saves to silver
│       ├── validate.py    # data quality checks before loading
│       ├── load.py        # upserts silver data into PostgreSQL
│       └── cli.py         # CLI entry point, orchestrates all phases
├── tests/
│   └── test_pipeline.py   # pytest tests for transform and validate
├── docker-compose.yml     # PostgreSQL container definition
├── requirements.txt       # Python dependencies
└── .env.example           # template for environment variables
```

---

## Key Concepts

- **Medallion architecture** — separating raw (bronze) and cleaned (silver) data into distinct layers makes each stage independently inspectable and replayable
- **Upsert over replace** — using `ON CONFLICT DO UPDATE` means the pipeline is safe to run repeatedly without duplicating or wiping data
- **Validation as a pipeline gate** — critical failures raise and stop the pipeline before bad data reaches the database; warnings log but allow the run to continue
- **Functions over scripts** — structuring each phase as importable functions rather than top-level scripts makes the code testable and composable through the CLI