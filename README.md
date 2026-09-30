# SellerLens — Marketplace Seller Scorecard

**Domain:** Retail & E-Commerce
**Capstone topic:** 01 (COMFORTABLE) — Databricks & Snowflake Capstone

## Problem
An online marketplace pays 40 third-party sellers monthly but has no single
trusted view of who's actually reliable — revenue alone rewards sellers who
ship late.

## Pipeline
Medallion architecture in Databricks (Delta Lake), served through Snowflake:

| File | Purpose |
|---|---|
| `00_generator.py` | Generates the seeded synthetic dataset (seed 42): 40 sellers, 6,000 orders, 15,000 items, 6,000 reviews |
| `01_bronze.py` | Lands raw CSVs into Delta tables as strings, with provenance columns |
| `02_silver.py` | Casts types, dedupes orders, quarantines orphan items, defines "late" once |
| `03_gold.py` | Aggregates to one row per seller per month: orders, revenue, late_rate, avg_review_score |
| `04_snowflake_setup.sql` | Warehouse, database, stage, table, Stream/Task, read-only role |
| `05_snowflake_queries.sql` | The three business questions (top sellers, late-delivery by state, review/lateness trend) |

## Results (from a live run)
- Bronze: 40 / 6,120 / 15,000 / 6,000 rows
- Silver: orders deduped 6,120 → 6,000; 90 orphan items quarantined; 200 NULL prices
- Gold: 240 rows (40 sellers × 6 months), revenue reconciles exactly to Silver
- All 10 "escalating" sellers show late rate spiking 36–96% from April onward,
  matched by a review-score drop the same month
- Snowflake load verified idempotent (repeat COPY INTO loads 0 files);
  read-only ANALYST_ROLE verified via a denied DELETE

## Author
[Subhashree Pattnaik] — [23053088]
