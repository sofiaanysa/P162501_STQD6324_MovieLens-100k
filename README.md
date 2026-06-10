# STQD6324 Data Management — Assignment 2
### MovieLens 100k Pipeline: Apache Spark & Cassandra

**Student:** Sofia Annisa Binti Jairi · P162501  
**Semester:** 2, 2025/2026

---

## Environment

| Component | Version |
|---|---|
| Python | 3.12 |
| PySpark | 4.0.2 |
| Java | OpenJDK 17  |
| Cassandra | 4.1.3 |
| cassandra-driver | 3.28+ |
| pandas | 2.x |
| matplotlib | 3.x |

>  PySpark 4.0 requires **Java 17**. Java 11 will cause `JAVA_GATEWAY_EXITED`. The notebook handles this automatically.

---

## Dataset

Download from: https://grouplens.org/datasets/movielens/100k/

| File | Description |
|---|---|
| `u.data` | 100,000 ratings |
| `u.user` | 943 user profiles |
| `u.item` | 1,682 movie records |

---

## How to Run (Google Colab)

1. Open `P162501_STQD6324_MovieLens_100k.ipynb` in Colab
2. Upload `u.data`, `u.user`, and `u.item` using the 📁 sidebar
3. Run all cells in order (`Runtime → Run all`)

> After any runtime restart, always re-run from Cell 1.

---

## Analytical Tasks

| Task | Description |
|---|---|
| i | Average rating for each movie |
| ii | Top 10 highest-rated movies (min 5 ratings) |
| iii | Active users (≥ 50 ratings) and their favourite genre |
| iv | All users under 20 years old |
| v | Scientists aged between 30 and 40 |

---

## Cassandra Keyspace

Keyspace: `movielens_ks`

| Table | Task |
|---|---|
| `movie_avg_ratings` | i |
| `top10_movies` | ii |
| `active_user_genres` | iii |
| `young_users` | iv |
| `scientists` | v |
