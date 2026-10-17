
# NYC Taxi Analytics Pipeline

A real-time, streaming data engineering pipeline built on Databricks that ingests, cleans, and analyzes over **50 million NYC Yellow Taxi trip records**, using the medallion architecture (Bronze -> Silver -> Gold) with PySpark, Structured Streaming, Auto Loader, and Delta Lake.

## Overview

A transportation analytics company receives raw taxi trip files every month, but has no system to process them - files just accumulate in storage. This project builds the missing piece: a streaming pipeline that automatically detects new files, cleans and enriches the data, and produces business-ready tables answering three real analytics questions, all without ever reprocessing data it has already seen.

## Results at a Glance

| Metric | Value |
|---|---|
| Total trip records processed | 50.7 million |
| Months of data ingested | 7 (Jan-Jul 2019) |
| Data quality rules applied | 6, each documented with reasoning |
| Derived columns added in Silver | 7 |
| Average cleaning removal rate | ~1.1% - 1.4%, consistent across every month tested |
| Gold tables (business questions answered) | 3 |
| Charts with written interpretation | 3 |
| Incremental load tests run | 4 separate rounds, zero duplicate rows every time |

## The Problem

The BI team needs three specific questions answered from this data, and the pipeline must handle new files arriving every month without manual rework:
1. **Demand patterns** - When is taxi demand highest? Does it differ between weekdays and weekends?
2. **Payment & tipping** - Does payment method affect tipping behavior?
3. **Vendor comparison** - How do the two taxi meter vendors compare?

## My Approach

I designed this as a 3-layer medallion pipeline, where each layer has exactly one job:

- **Bronze** ingests data exactly as it arrives - no transformations, no judgment calls - so there's always an untouched copy to fall back on.
- **Silver** is where all the quality decisions happen: explicit rules, each with a documented reason, plus derived columns and decoded labels, so Gold never has to guess what a number means.
- **Gold** answers the actual business questions with fair, bias-aware metrics - for example, I compare weekday vs. weekend demand using average trips *per day*, not raw totals, since the dataset naturally contains more weekdays than weekend days in any 3-month window.

Every layer streams using `trigger(availableNow=True)` with its own checkpoint, so the entire pipeline can be re-run safely at any time - re-running never creates duplicates, and a new month's file is picked up automatically with zero code changes.

## Architecture

Source files (NYC TLC taxi data, 7 months: Jan-Jul 2019)
&nbsp;&nbsp;&nbsp;&nbsp;copied in, simulating files "arriving over time"
&nbsp;&nbsp;&nbsp;&nbsp;v
Landing zone (a folder in a Unity Catalog Volume)
&nbsp;&nbsp;&nbsp;&nbsp;Auto Loader + checkpoint + trigger(availableNow=True)
&nbsp;&nbsp;&nbsp;&nbsp;v
BRONZE - raw data exactly as it arrived + source_file + ingested_at (streaming, Delta)
&nbsp;&nbsp;&nbsp;&nbsp;readStream from Bronze, own checkpoint
&nbsp;&nbsp;&nbsp;&nbsp;v
SILVER - typed, quality-filtered, 7 derived columns, decoded labels (streaming, Delta)
&nbsp;&nbsp;&nbsp;&nbsp;groupBy + aggregations
&nbsp;&nbsp;&nbsp;&nbsp;v
GOLD - 3 named tables, one per business question (Delta, SQL-queryable)
&nbsp;&nbsp;&nbsp;&nbsp;Spark SQL + display()
&nbsp;&nbsp;&nbsp;&nbsp;v
Charts + written business interpretations

## Tech Stack
- **Platform:** Databricks (Free Edition, serverless compute)
- **Processing:** PySpark, Spark SQL
- **Streaming:** Structured Streaming, Databricks Auto Loader
- **Storage:** Delta Lake, Unity Catalog (catalog -> schema -> volume/table)
- **Version control:** Git, GitHub (via Databricks Git folders)

## Pipeline Deep Dive

### Bronze - Streaming Ingestion
Auto Loader watches the landing zone and streams any new CSV file into a raw Delta table, untouched, tagging every row with `source_file` and `ingested_at`. A checkpoint guarantees fault tolerance: re-running the pipeline, or recovering from a crash, never reprocesses a file twice.

### Silver - Cleaning and Enrichment
Reads Bronze as a stream (never from the raw files directly) and applies 6 explicit rules:

| Rule | What it checks | Why |
|---|---|---|
| R1 | trip_distance > 0 | A 0 or negative distance isn't a real trip |
| R2 | trip_distance <= 300 miles | Removes clear GPS/sensor errors |
| R3 | dropoff_time > pickup_time | A trip can't end before it starts |
| R4 | fare_amount >= 0 | Negative fares are refunds/adjustments, not trips |
| R5 | trip_minutes <= 360 | Removes trips with broken duration data |
| R6 | pickup month matches the file's own month | Keeps the rule valid for any future month, not just the original 3 |

Adds 7 derived columns (`pickup_date`, `pickup_hour`, `day_type`, `tip_percent`, `avg_speed_mph`, `payment_method`, `vendor_name`) and decodes `payment_type`/`VendorID` into readable labels. About 1.1% of raw rows are removed, consistently, across every month tested.

### Gold - Business Aggregations

**Q1 - Demand by hour.** Peak demand on weekdays hits at 6pm (~17,500 trips/day average), driven by a clear two-peak commute pattern (morning + evening rush). Weekend demand peaks at a similar hour (~14,300 trips/day average) but with one smooth curve, since there's no commute pattern to drive a second peak.

**Q2 - Payment & tipping.**

| Payment method | Share of trips | Avg tip % | % of trips with any tip |
|---|---|---|---|
| Credit card | 73.1% | 24.9% | 95.6% |
| Cash | 26.5% | ~0% (not recorded) | ~0% (not recorded) |
| No charge / Dispute | 0.5% | ~0% | ~0% |

Key insight: cash tips are handed directly to the driver and never touch the metering system, so the 0% figure reflects a data visibility gap, not real rider behavior - a distinction worth calling out any time this kind of data is used to judge "who tips more."

**Q3 - Vendor comparison.**

| Vendor | Share of trips | Avg fare | Avg tip % | Avg speed (mph) |
|---|---|---|---|---|
| VeriFone | 61.3% | $12.84 | 17.7% | 12.3 |
| Creative Mobile Technologies | 37.9% | $12.34 | 19.1% | 14.6 |
| Unknown vendor | 0.8% | $11.83 | 17.5% | 10.9 |

The average speed difference is likely explained by which routes/areas each vendor's cabs happened to serve, not real differences in driving performance - correlation isn't causation.

### Incremental Load Proof
Started the pipeline on 3 months (Jan-Mar 2019, 22.5M rows), then simulated new data arriving in 3 separate rounds with zero code changes:

| Round | Files added | Bronze rows added | Silver rows added | Duplicates created |
|---|---|---|---|---|
| 1 | April | 7,433,139 | 7,352,834 | 0 |
| 2 | May | 7,565,261 | 7,477,579 | 0 |
| 3 | June + July (together) | 13,251,443 | 13,061,218 | 0 |

Every round proved the pipeline handles new files correctly - including multiple files landing at once - without any code modification.

## Charts

Charts were built with Spark SQL + `display()` inside `05_analytics`. Screenshots below (see `charts/` folder):

![Demand by hour](charts/demand_by_hour.png)
*Weekday demand shows a clear two-peak commute pattern; weekend demand is a single smooth curve.*

![Tipping by payment method](charts/payment_tipping.png)
*Credit card tipping is visible and significant; cash tipping is structurally invisible to the data, not necessarily absent in reality.*

![Vendor comparison](charts/vendor_comparison.png)
*VeriFone handles the majority of trips; fare and tipping are close between the two main vendors.*

## Key Engineering Decisions
- **Why keep Bronze raw?** If a Silver rule is ever found to be wrong, Bronze is the only safe source to recompute from - without it, there's no way to correct a mistake without re-fetching the original files.
- **Why `availableNow` instead of a continuous stream?** It's the only trigger supported on Databricks serverless compute, and it's cheaper and more predictable for a project that doesn't need to run 24/7.
- **Why per-day averages instead of raw totals in Gold?** A 3-month window has more weekdays than weekend days, so comparing raw totals would make weekdays look artificially busier than they really are per day.

Full reasoning for all 8 of the project's design questions is documented in `07_design_decisions`.

## Skills Demonstrated
PySpark DataFrame transformations - Spark SQL - Structured Streaming - Databricks Auto Loader - checkpointing and fault tolerance - Delta Lake (ACID writes, schema enforcement/evolution, time travel) - Unity Catalog (catalogs, schemas, volumes) - medallion architecture design - data quality rule design - fair/bias-aware metric design - data visualization and business communication - Git/GitHub version control

## Repository Structure
| Notebook | Purpose |
|---|---|
| `00_config` | All paths and table names centralized in one place |
| `00_playground` | Scratch/practice notebook (not part of the graded pipeline) |
| `01_setup_landing_zone` | Creates schema/volume, copies source files into the landing zone |
| `02_bronze_ingestion` | Auto Loader streaming ingestion into Bronze |
| `03_silver_cleaning` | Data quality rules, derived columns, decoded labels |
| `04_gold_aggregations` | 3 Gold tables, one per business question, plus a sample CSV export |
| `05_analytics` | Charts and written business interpretations |
| `06_incremental_test` | Proof that new months are processed incrementally with no duplicates |
| `07_design_decisions` | My own-words answers to the project's design questions |
| `sample_outputs/` | Small CSV exports of the 3 Gold tables, for quick browsing without running the pipeline |

## How to Run
Notebooks are numbered and designed to run in order, top to bottom, on fresh Databricks serverless compute:
`00_config` -> `01_setup_landing_zone` -> `02_bronze_ingestion` -> `03_silver_cleaning` -> `04_gold_aggregations` -> `05_analytics` -> `06_incremental_test`

## About This Project
Built from zero prior experience with Databricks, Spark, or streaming pipelines, with a deliberate focus on understanding every design decision rather than just making the code run - including working through fairness in metric design, data quality tradeoffs, and the limits of what the data can and can't actually prove.