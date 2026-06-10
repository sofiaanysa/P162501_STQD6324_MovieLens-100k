<div align="center">

# MovieLens 100k Data Pipeline
### STQD6324 Data Management — Assignment 2

**Apache Spark · Apache Cassandra · Python**

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.0.2-orange?logo=apache-spark&logoColor=white)
![Java](https://img.shields.io/badge/Java-OpenJDK%2017-red?logo=openjdk&logoColor=white)
![Cassandra](https://img.shields.io/badge/Cassandra-4.1.3-1287B1?logo=apache-cassandra&logoColor=white)
![Platform](https://img.shields.io/badge/Platform-Google%20Colab-F9AB00?logo=googlecolab&logoColor=white)

</div>

---

##  Table of Contents

- [Overview](#overview)
- [Analytical Tasks](#analytical-tasks)
- [Repository Structure](#repository-structure)
- [Environment & Dependencies](#environment--dependencies)
- [Dataset](#dataset)
- [How to Run](#how-to-run)
- [Pipeline Architecture](#pipeline-architecture)
- [Cassandra Schema](#cassandra-schema)
- [Notebook Structure](#notebook-structure)
- [Technical Requirements Checklist](#technical-requirements-checklist)
- [Known Issues & Fixes](#known-issues--fixes)

---

## Overview

This project implements a Python-based big data pipeline using **Apache Spark** and **Apache Cassandra** to analyse the [MovieLens 100k dataset](https://grouplens.org/datasets/movielens/100k/). Raw data files are ingested as Spark RDDs, transformed into DataFrames, queried with Spark SQL, and persisted to Cassandra for validation.

**Student:** SOFIA ANNISA BINTI JAIRI ·
**Matric ID:** P162501 ·
**Course:** STQD6324 Data Management · 

---

## Analytical Tasks

| # | Task | Method |
|---|---|---|
| i | Average rating for each movie | Spark SQL (`AVG` + `JOIN`) |
| ii | Top 10 movies with highest average ratings *(min 5 ratings)* | Spark SQL (`HAVING`, `ORDER BY`, `LIMIT`) |
| iii | Users who rated ≥ 50 movies and their favourite genre | DataFrame API (`explode`, Window `rank`) |
| iv | All users under 20 years old | Spark SQL (`WHERE age < 20`) |
| v | Scientists aged 30–40 | Spark SQL (`WHERE occupation = 'scientist' AND age BETWEEN 30 AND 40`) |

Each task includes a **bar/histogram visualisation** and a **written interpretation** in the notebook.

---

## Repository Structure

```
.
├── P162501_STQD6324_MovieLens_100k.ipynb   ← Main notebook (all code + analysis)
├── README.md                                ← This file
└── data/
    ├── u.data    ← 100,000 ratings
    ├── u.user    ← 943 user profiles
    └── u.item    ← 1,682 movie records
```

> The `data/` folder is for local runs. On Google Colab, upload files directly to `/content/`.

---

## Environment & Dependencies

| Component | Version | Notes |
|---|---|---|
| Python | 3.12 | Colab default |
| PySpark | **4.0.2** | Installed via `pip` |
| Java | **OpenJDK 17** |  Required for PySpark 4.x — see note below |
| Apache Cassandra | 4.1.3 | Downloaded & started by the notebook |
| cassandra-driver | 3.28+ | Python driver for Cassandra I/O |
| pandas | 2.x | DataFrame bridge for Cassandra reads |
| matplotlib | 3.x | Visualisations |

> ** Java Version Warning**  
> PySpark 4.0 compiles its JARs to Java class file version **61.0** (Java 17).  
> Running under Java 11 (max class version 55.0) raises `UnsupportedClassVersionError`, which appears as `JAVA_GATEWAY_EXITED`.  
> The notebook automatically sets `JAVA_HOME` to Java 17 before Spark starts.  
> Cassandra itself uses Java 11 (started in a subprocess with its own `JAVA_HOME`). Both runtimes coexist cleanly.

---

## Dataset

**Source:** [https://grouplens.org/datasets/movielens/100k/](https://grouplens.org/datasets/movielens/100k/)

| File | Delimiter | Rows | Columns |
|---|---|---|---|
| `u.data` | Tab (`\t`) | 100,000 | user_id, movie_id, rating (1–5), timestamp |
| `u.user` | Pipe (`\|`) | 943 | user_id, age, gender, occupation, zip_code |
| `u.item` | Pipe (`\|`) | 1,682 | movie_id, title, release_date, IMDb_URL, 19 genre flags |

The 19 genre flag columns in `u.item` are binary (0/1): `unknown`, `Action`, `Adventure`, `Animation`, `Childrens`, `Comedy`, `Crime`, `Documentary`, `Drama`, `Fantasy`, `Film_Noir`, `Horror`, `Musical`, `Mystery`, `Romance`, `Sci_Fi`, `Thriller`, `War`, `Western`.

---

## How to Run

### Option A — Google Colab *(Recommended)*

1. **Open the notebook** in Google Colab  
   *(File → Upload notebook → select `P162501_STQD6324_MovieLens_100k.ipynb`)*

2. **Upload the three dataset files**  
   Click the folder icon in the left sidebar → Upload → select `u.data`, `u.user`, `u.item`  
   They will land in `/content/` which the notebook checks automatically.

3. **Run all cells in order** (`Runtime → Run all`)  
   The notebook is fully self-contained:
   - Installs all Python packages
   - Kills stale Java processes and sets Java 17
   - Downloads and starts Cassandra 4.1.3
   - Creates the Spark session
   - Runs all five analytical tasks
   - Writes results to Cassandra and validates

> **After a runtime restart**, always re-run from Cell 1. The `JAVA_HOME` environment variable does not survive a Colab runtime reset.

---

### Option B — Local Machine

#### Prerequisites

```bash
# Java 17
sudo apt install openjdk-17-jdk   # Ubuntu/Debian
brew install openjdk@17           # macOS

# Python packages
pip install pyspark==4.0.2 cassandra-driver pandas matplotlib

# Cassandra 4.1.3 — download from:
# https://cassandra.apache.org/download/
```

#### Steps

```bash
# 1. Start Cassandra
bin/cassandra -R

# 2. Clone the repo and add data files
git clone <your-repo-url>
cd <repo-folder>
# Copy u.data, u.user, u.item into a data/ subfolder

# 3. Launch the notebook
jupyter notebook P162501_STQD6324_MovieLens_100k.ipynb
```

In the notebook, update the `resolve_path()` candidate lists in **Milestone 2** to include your local paths if needed.

---

## Pipeline Architecture

```
Dataset Files
─────────────
  u.data  ──┐
  u.user  ──┤──► sparkContext.textFile()
  u.item  ──┘         │
                       ▼
                  Raw RDDs
                  (one row per line)
                       │
               .map() / .filter()
                       │
                       ▼
              Spark DataFrames
         ┌─────────────────────────┐
         │  ratings_df             │
         │  users_df               │
         │  items_df (+ genres[])  │
         └─────────────────────────┘
                       │
          createOrReplaceTempView()
                       │
                       ▼
                  Spark SQL
         ┌──────────────────────────────────┐
         │  Task i   — avg rating/movie     │
         │  Task ii  — top 10 movies        │
         │  Task iii — active user genres   │
         │  Task iv  — users under 20       │
         │  Task v   — scientists 30–40     │
         └──────────────────────────────────┘
                       │
            cassandra-driver (Python)
                       │
                       ▼
         Cassandra Keyspace: movielens_ks
         ┌──────────────────────────────────┐
         │  movie_avg_ratings               │
         │  top10_movies                    │
         │  active_user_genres              │
         │  young_users                     │
         │  scientists                      │
         └──────────────────────────────────┘
                       │
         Read back → Spark DataFrames
                       │
                       ▼
                  Validation 
```

---

## Cassandra Schema

Keyspace: `movielens_ks` · Strategy: `SimpleStrategy` · Replication factor: `1`

```cql
CREATE TABLE movie_avg_ratings (
    movie_id    int PRIMARY KEY,
    movie_title text,
    avg_rating  double,
    num_ratings int
);

CREATE TABLE top10_movies (
    movie_id    int PRIMARY KEY,
    movie_title text,
    avg_rating  double,
    num_ratings int
);

CREATE TABLE active_user_genres (
    user_id     int,
    genre       text,
    genre_count int,
    PRIMARY KEY (user_id, genre)
);

CREATE TABLE young_users (
    user_id    int PRIMARY KEY,
    age        int,
    gender     text,
    occupation text,
    zip_code   text
);

CREATE TABLE scientists (
    user_id    int PRIMARY KEY,
    age        int,
    gender     text,
    occupation text,
    zip_code   text
);
```

---

## Notebook Structure

| Milestone | Section | Key Actions |
|---|---|---|
| **1** | Environment Setup | Kill stale Java · Set Java 17 · Install packages · Start Cassandra · Start Spark |
| **2** | Data Ingestion | `textFile()` → RDDs → `.map().toDF()` → DataFrames |
| **3** | Cleaning & Preprocessing | Null checks · Malformed row filtering · `genres[]` column derivation |
| **4** | Exploratory Analysis | Spark SQL: most rated movies, active users, occupation distribution |
| **5** | Analytical Tasks (i–v) | Five tasks, each with Spark SQL query + chart + interpretation |
| **6** | Cassandra Integration | Create schema · Write DataFrames · Read back · CQL row-count validation |

---

## Technical Requirements Checklist

- [x] **Req 1** — Import required Python libraries (PySpark, cassandra-driver, pandas, matplotlib)
- [x] **Req 2** — Load MovieLens files via `SparkContext.textFile()`
- [x] **Req 3** — Create RDD objects from raw dataset
- [x] **Req 4** — Transform RDDs into Spark DataFrames via `.map().toDF()`
- [x] **Req 5** — Data cleaning: null removal, malformed row filtering, genre list derivation
- [x] **Req 6** — Analytical queries using Apache Spark SQL (all 5 tasks)
- [x] **Req 7** — Write processed DataFrames into Cassandra keyspace/tables
- [x] **Req 8** — Read Cassandra tables back into Spark DataFrames for validation
- [x] **Req 9** — Visualisations (histogram + bar charts) for each analytical task
- [x] **Req 10** — Written interpretations and discussion for each task

---

## Known Issues & Fixes

### `JAVA_GATEWAY_EXITED` on Spark startup

**Cause:** PySpark 4.0's JARs target Java 17 (class file version 61.0). Java 11 only understands up to version 55.0, so the JVM exits immediately with `UnsupportedClassVersionError`.

**Fix:** Set `JAVA_HOME` to the Java 17 installation *before* any PySpark import. Cell 2 in the notebook does this automatically:
```python
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.17.0-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
```

---

### Maven download crashes Spark startup (`spark.jars.packages`)

**Cause:** Using `spark.jars.packages` to load the Spark-Cassandra connector triggers a Maven download at JVM launch time. If Colab's network blocks or throttles Maven Central, the gateway exits before sending its port number.

**Fix:** The notebook does **not** use `spark.jars.packages`. Instead, all Cassandra I/O is handled by the Python `cassandra-driver`, which installs cleanly via `pip` and runs independently of Spark.

---

### Stale Java process blocks the gateway port

**Cause:** A previous failed Spark session can leave a Java process holding the gateway socket, preventing a new session from binding.

**Fix:** `!pkill -f java` is run in Cell 1 before any Spark code — automatically clearing any lingering processes.

---

<div align="center">
<sub>STQD6324 Data Management · Universiti Kebangsaan Malaysia · Semester 2, 2025/2026</sub>
</div>
