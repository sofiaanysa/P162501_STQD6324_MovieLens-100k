# MovieLens 100k Data Pipeline: Apache Spark & Cassandra

| | |
|---|---|
| **Name** | SOFIA ANNISA BINTI JAIRI |
| **Student ID** | P162501 |
| **Course** | STQD6324 Data Management — Assignment 2 (25%) |
| **Semester** | 2, 2025/2026 |

---

## Project Overview

This project implements a **big data pipeline** on the [MovieLens 100k dataset](https://grouplens.org/datasets/movielens/100k/) using **Apache Spark** and **Apache Cassandra**. Raw data files are ingested as Spark RDDs, transformed into DataFrames, queried with Spark SQL, and results are persisted to Cassandra for validation.

Five analytical tasks are performed:

- **Task i** — Average rating for each movie
- **Task ii** — Top 10 highest-rated movies (minimum 5 ratings)
- **Task iii** — Active users (≥ 50 ratings) and their favourite genre
- **Task iv** — All users under 20 years old
- **Task v** — Scientists aged between 30 and 40

---

## Repository Structure

```
P162501_STQD6324_MovieLens_100k/
├── P162501_STQD6324_MovieLens_100k.ipynb   # Main Jupyter Notebook
├── data/
│   ├── u.data                               # 100,000 ratings
│   ├── u.user                               # 943 user profiles
│   └── u.item                               # 1,682 movie records
└── README.md                                # This file
```

---

## Dataset

| Property | Detail |
|---|---|
| **Name** | MovieLens 100k |
| **Source** | https://grouplens.org/datasets/movielens/100k/ |
| **Ratings** | 100,000 (scale 1–5) |
| **Users** | 943 |
| **Movies** | 1,682 |

| File | Delimiter | Columns |
|---|---|---|
| `u.data` | Tab | user_id, movie_id, rating, timestamp |
| `u.user` | Pipe | user_id, age, gender, occupation, zip_code |
| `u.item` | Pipe | movie_id, title, release_date, IMDb_URL, 19 genre flags |

---

## Methodology

### 1. Environment Setup
Install PySpark, cassandra-driver, pandas, and matplotlib. Set Java 17 and initialise a Spark session. Download and start Cassandra 4.1.3.

### 2. Data Ingestion
Load each file via `SparkContext.textFile()` into RDDs. Parse and cast fields, then convert to Spark DataFrames using `.map().toDF()`.

### 3. Data Cleaning & Preprocessing
- Filter malformed rows (wrong field count) during RDD parsing
- Remove nulls with `dropna()`
- Trim whitespace from string fields
- Derive a `genres[]` list column from the 19 binary genre flags in `u.item`

### 4. Exploratory Analysis
Register all DataFrames as Spark SQL temporary views. Run exploratory queries: most-rated movies, most active users, occupation distribution.

### 5. Analytical Tasks (i–v)
All five tasks executed using **Spark SQL**, each with a chart and written interpretation.

### 6. Cassandra Integration
Write result DataFrames to Cassandra using the Python `cassandra-driver` with prepared statements. Read each table back into Spark for validation. Confirm row counts via CQL.

---

## Cassandra Schema

Keyspace: `movielens_ks` · `SimpleStrategy` · replication factor: `1`

| Table | Primary Key | Task |
|---|---|---|
| `movie_avg_ratings` | `movie_id` | i |
| `top10_movies` | `movie_id` | ii |
| `active_user_genres` | `(user_id, genre)` | iii |
| `young_users` | `user_id` | iv |
| `scientists` | `user_id` | v |

---

## How to Reproduce

### Option A: Google Colab (Recommended)

1. Open `P162501_STQD6324_MovieLens_100k.ipynb` in [Google Colab](https://colab.research.google.com/)
2. Upload `u.data`, `u.user`, and `u.item` using the 📁 sidebar (files land in `/content/`)
3. Run all cells in order (`Runtime → Run all`)

> After any runtime restart, always re-run from Cell 1 — the `JAVA_HOME` environment variable does not survive a reset.

### Option B: Run Locally

**Step 1 — Install dependencies:**
```bash
pip install pyspark==4.0.2 cassandra-driver pandas matplotlib jupyter
```

**Step 2 — Start Cassandra:**
```bash
bin/cassandra -R
```

**Step 3 — Launch the notebook:**
```bash
jupyter notebook P162501_STQD6324_MovieLens_100k.ipynb
```

**Step 4 — Run all cells in order.**

> Ensure Java 17 is installed and `JAVA_HOME` points to it before running.

### Reproducibility Notes
- Dataset paths are resolved automatically — the notebook checks `/content/` first, then falls back to the working directory.
- No manual Cassandra schema setup is needed — all `CREATE KEYSPACE` and `CREATE TABLE` statements run inside the notebook.

---

## Dependencies

| Package | Purpose |
|---|---|
| `pyspark` | RDDs, DataFrames, Spark SQL |
| `cassandra-driver` | Cassandra I/O (write & read) |
| `pandas` | Bridge between Cassandra driver and Spark |
| `matplotlib` | Visualisations for each task |

---

## Environment

| Component | Version | Notes |
|---|---|---|
| Python | 3.12 | |
| PySpark | 4.0.2 | |
| Java | OpenJDK **17** |  Required for PySpark 4.x |
| Cassandra | 4.1.3 | Runs under Java 11 (separate subprocess) |
| cassandra-driver | 3.28+ | |

> ** Java Version Warning**  
> PySpark 4.0 requires Java 17. Running under Java 11 causes `JAVA_GATEWAY_EXITED`.  
> The notebook sets `JAVA_HOME` to Java 17 automatically before Spark starts.  
> Cassandra is launched in a separate subprocess with its own Java 11 `JAVA_HOME`, so both coexist cleanly.

---

## Notebook Structure

| Section | Description |
|---|---|
| Milestone 1 | Environment setup: Java 17, packages, Cassandra, Spark session |
| Milestone 2 | Data ingestion: `textFile()` → RDDs → DataFrames |
| Milestone 3 | Data cleaning: null removal, malformed row filtering, genre list derivation |
| Milestone 4 | Exploratory analysis with Spark SQL |
| Milestone 5 | Analytical tasks (i–v) with visualisations and interpretations |
| Milestone 6 | Cassandra write, read-back validation, CQL row-count check |
