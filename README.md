
# NYC Taxi Analytics Pipeline

A real-time, streaming data engineering pipeline built on Databricks. It ingests NYC Yellow Taxi trip data, cleans and enriches it, and produces business-ready analytics tables and charts - designed and built from scratch using PySpark, Spark SQL, Structured Streaming, Auto Loader, and Delta Lake.

## The Problem

A transportation analytics company receives raw taxi trip files every month, but the files just pile up in storage with no pipeline to process them. The business intelligence team has three questions they need answered from this data, and they need a pipeline that keeps working automatically as new months of data keep arriving - without reprocessing everything from scratch every time.

## The Solution

I designed and built a streaming pipeline using the **medallion architecture** (Bronze -> Silver -> Gold), where each new file dropped into a landing zone folder is automatically detected, ingested, cleaned, and rolled up into analytics tables, with zero manual intervention and zero duplicate processing.

### Business Questions Answered
1. **Demand patterns** - When is taxi demand highest during the day? Does this differ between weekdays and weekends?
2. **Payment & tipping** - Does payment method affect tipping behavior?
3. **Vendor comparison** - How do the two taxi meter vendors compare across key metrics?

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

## What I Built

**Bronze layer (raw ingestion):** Auto Loader watches a landing zone folder and streams any new CSV file straight into a raw Delta table, untouched, tagging each row with its source file name and ingestion timestamp. A dedicated checkpoint guarantees that re-running the pipeline never reprocesses a file twice, and that a crash or restart resumes exactly where it left off.

**Silver layer (cleaning and enrichment):** Reads Bronze as a stream (not from the raw files), applies 6 explicit, documented data quality rules (e.g. trip distance must be positive, dropoff must be after pickup, pickup date must match the file's own month), and adds 7 derived columns including `tip_percent`, `avg_speed_mph`, and decoded `payment_method` / `vendor_name` labels. About 1.1% of raw rows are removed, with every rule's reasoning documented.

**Gold layer (business aggregations):** Three named Delta tables, one per business question, each built with fair, bias-aware metrics (for example, comparing weekday vs. weekend demand using average trips per day, not raw totals, since the two groups have a different number of calendar days).

**Analytics layer:** Three charts built with Spark SQL + `display()` (a line chart for hourly demand, bar charts for payment method and vendor comparison), each with a written interpretation of what the data shows and why.

**Incremental proof:** Started the pipeline with 3 months of data (Jan-Mar 2019), then simulated new months arriving over time by dropping in April, then May, then June and July together - re-running the same pipeline each time with zero code changes, and proving with before/after row counts that only the new data was processed.

## Key Results
- Processed over **50 million** taxi trip records across 7 months of data
- Silver cleaning rules consistently remove about **1.1%** of raw rows across every month tested
- Incremental loading proven correct across **4 separate rounds** (April, May, June+July together) with **zero duplicate rows** in any round
- Discovered and documented a real data quality insight: cash tips are not recorded in the source data (drivers receive them directly), which makes "card riders tip more" a data limitation rather than a true behavioral finding

## Design Decisions
See `07_design_decisions` for my own-words answers to the project's architecture and data design questions, covering topics like fault tolerance, checkpoint behavior, schema evolution, and Delta Lake time travel.

## Repository Structure
| Notebook | Purpose |
|---|---|
| `00_config` | All paths and table names centralized in one place |
| `00_playground` | Scratch/practice notebook (not part of the graded pipeline) |
| `01_setup_landing_zone` | Creates schema/volume, copies source files into the landing zone |
| `02_bronze_ingestion` | Auto Loader streaming ingestion into Bronze |
| `03_silver_cleaning` | Data quality rules, derived columns, decoded labels |
| `04_gold_aggregations` | 3 Gold tables, one per business question |
| `05_analytics` | Charts and written business interpretations |
| `06_incremental_test` | Proof that new months are processed incrementally with no duplicates |
| `07_design_decisions` | My own-words answers to the project's design questions |

## How to Run
Notebooks are numbered and designed to run in order, top to bottom, on fresh Databricks serverless compute:
`00_config` -> `01_setup_landing_zone` -> `02_bronze_ingestion` -> `03_silver_cleaning` -> `04_gold_aggregations` -> `05_analytics` -> `06_incremental_test`

## About This Project
Built as a hands-on learning project to go from zero prior experience with Databricks, Spark, and streaming data pipelines to a complete, working, production-style pipeline - with a focus on understanding every design choice, not just making the code run.