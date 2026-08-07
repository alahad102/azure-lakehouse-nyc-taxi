# Azure Lakehouse — NYC Taxi Data Pipeline

An end-to-end Azure data lakehouse processing **19.9 million NYC taxi trips** through a medallion architecture, with schema harmonisation across two source systems, a two-tier data quality model, a dbt star schema with SCD Type 2 history, and 34 automated tests.

Built as a portfolio project, but with production constraints deliberately kept: real cost management, real schema drift, real bugs found and fixed at scale.

---

## Architecture

```
NYC TLC (public HTTP)
        │
        │  Azure Data Factory — ForEach + Copy, managed identity auth
        ▼
┌───────────────────────────────────────────────────────────────┐
│  ADLS Gen2  ·  stlakehousenyctx2  ·  hierarchical namespace   │
│                                                               │
│  landing ──▶ bronze ──▶ silver ──▶ gold                       │
│  (parquet)   (Delta)    (Delta)    (Delta)                    │
└───────────────────────────────────────────────────────────────┘
        │           │          │           │
        │      Auto Loader  PySpark      dbt
        │      (Databricks) (Databricks) (SQL warehouse)
        │
        └── Unity Catalog governs all four layers
```

| Layer | Contents | Rows |
|---|---|---|
| **landing** | Raw parquet, byte-identical to source | 9 files |
| **bronze** | `bronze_yellow_trips`, `bronze_green_trips`, `bronze_taxi_zones` | 19,898,800 |
| **silver** | `silver_trips`, `silver_trips_quarantine`, quality metrics | 19,453,731 clean |
| **gold** | `fct_trips` + 4 dimensions incl. SCD2 | 19,453,731 |

---

## Tech stack

Azure Data Factory · ADLS Gen2 · Azure Databricks · Unity Catalog · Delta Lake · PySpark · dbt-databricks · Power BI *(pending)* · GitHub Actions *(pending)*

---

## The data

NYC Taxi & Limousine Commission trip records, January–June 2023 — two services with genuinely different schemas:

| | Yellow | Green |
|---|---|---|
| Service | Medallion street-hail, mostly Manhattan | Boro taxis, outer boroughs |
| Columns | 19 | 20 |
| Pickup column | `tpep_pickup_datetime` | `lpep_pickup_datetime` |
| Unique columns | `airport_fee` | `ehail_fee`, `trip_type` |
| Rows | 19,493,620 | 405,180 |
| Mean trip distance | 3.91 mi | 8.55 mi |

One row per completed trip. Zone IDs (1–265) resolve to boroughs via TLC's separate `taxi_zone_lookup.csv`, loaded as a reference table.

---

## Pipeline

### Ingestion — Azure Data Factory

A parameterised pipeline pulls monthly parquet files from TLC over HTTP into the `landing` container.

- **ForEach loop** over a file array, `batchCount` 4, retry 2 — scaling from 1 month to 6 is an array edit, not a pipeline change
- **Managed identity**, not an account key. ADF defaults to a key, which tests successfully and would commit an encrypted secret to a public repo. The committed linked-service JSON contains only a URL.
- Version-controlled under `/adf` via ADF's git integration

### Bronze — Auto Loader

Yellow and Green land in **separate** Delta tables. Merging them here would require renaming columns to a common schema — that's harmonisation, and harmonisation is Silver's job. Bronze stays faithful to what each source sent.

Every row carries lineage metadata: `_source_file`, `_ingested_at`, `_run_id`, `_rescued_data`.

Auto Loader runs as a Structured Streaming job with `trigger(availableNow=True)` and separate schema/checkpoint locations per source. Re-running the notebook ingests zero rows — idempotency proven by re-run, not asserted.

**Directory listing mode, deliberately.** File notification mode provisions Event Grid and a Storage Queue to solve a problem that doesn't exist at 9 files.

### Silver — PySpark

Where two messy sources become one trustworthy table.

**Schema harmonisation** handles three distinct differences — names, column count, and *column order*. The ordering difference is the dangerous one: `passenger_count` sits at position 4 in Yellow, `store_and_fwd_flag` at position 4 in Green. A positional `union()` would write passenger counts into a text column with **no error raised**. Resolved with `unionByName(allowMissingColumns=True)`.

**Two-tier validation.** Not every data problem invalidates a trip:

| Tier | Example | Action |
|---|---|---|
| Fatal | dropoff before pickup, negative distance | quarantine |
| Field-level | `passenger_count` null | keep row, set flag |

Quarantining on the second tier would have discarded ~68,000 genuine trips over one optional metadata field the driver didn't enter.

**Nothing is deleted.** Rejected rows go to `silver_trips_quarantine` with a recorded reason. Every Bronze row appears in exactly one Silver table — a verifiable claim, not an assertion that the data is clean.

**Silver overwrites; Bronze appends.** Silver is derived and must be fully reproducible from Bronze. That property was cashed in twice during development.

### Gold — dbt star schema

| Model | Grain | Rows |
|---|---|---|
| `fct_trips` | one trip | 19,453,731 |
| `dim_date` | one day | 181 |
| `dim_location` | one zone | 266 |
| `dim_vendor` | one vendor | 3 |
| `dim_vendor_scd2` | one vendor **version** | 5 |

**Date range derived, not hardcoded** — scaling 1 month → 6 took `dim_date` from 31 to 181 rows with zero edits.

**Unknown member (-1) in `dim_location`.** Trips with unmatched zone IDs route there via `coalesce`, so an inner join can't silently drop them. It currently matches zero rows and is kept anyway — a defensive path that costs nothing and currently catches nothing is correct design.

*(Related finding: 38,069 trips show an "Unknown" borough. That is **not** the synthetic member — location IDs 264 and 265 are TLC's own Unknown/N/A designations. Verified `-1` matches zero rows, which proves every ID in the data resolves against the lookup.)*

**Reversals surfaced as separate measures**, not netted or excluded:

| Measure | Definition |
|---|---|
| `gross_revenue` | `SUM(total_amount) WHERE NOT is_reversal` |
| `net_revenue` | `SUM(total_amount)` — reversals subtract naturally |

Keeps the fact table at transaction grain and lets measures encode business logic, rather than baking a filter into the load. Also enables the invariant `net_revenue <= gross_revenue`.

### SCD Type 2 — vendor contract history

Vendor contract tiers are tracked with full history via **dbt snapshots**, so a trip is always attributed to the contract that was in effect *when it happened*.

```
Jan ──────── Feb ──────── Mar ──────── May ──────── Jun
V1: Standard ─────────────────────────▶│ Premium ────▶
V2: Standard ─────────────▶│ Premium ───────────────▶
V6:            (no data)   Standard ─────────────────▶
```

`fct_trips` carries `vendor_version_key`, resolved by point-in-time join at build time:

```sql
left join dim_vendor_scd2 v
  on  t.vendor_id = v.vendor_id
  and t.pickup_datetime >= v.effective_start_date
  and t.pickup_datetime <  v.effective_end_date
```

Consumers then join on a single equality. The temporal logic is resolved once, not re-derived in every query.

**The measured difference:**

| Join method | Premium revenue | Standard revenue |
|---|---|---|
| Current dimension only (Type 1) | $550,915,401.34 | $195,686.44 |
| **Point-in-time (Type 2)** | **$340,111,360.44** | **$210,999,727.34** |

**$210,804,040.90 — 38.3% of total revenue — would be attributed to the wrong contract tier** by a Type 1 dimension. Trip counts tie exactly across both methods (19,453,022 non-reversal trips), confirming no fan-out and no dropped rows.

#### Design notes

- **Half-open intervals** (`start <= t < end`). Inclusive bounds on both sides would let a trip at a boundary instant match two versions and double-count its fare.
- **`coalesce(dbt_valid_to, '9999-12-31')`** — dbt writes NULL for the current row, and `pickup_datetime < NULL` evaluates to NULL, silently dropping every current-period trip from the join.
- **`left join`, not inner** — an unmatched trip stays visible with a null key, and a `not_null` test makes the gap loud. This is how vendor 6 was found (below).
- **Surrogate keys where history exists, natural keys elsewhere.** `dim_date`, `dim_location`, and `dim_vendor` are static, build in parallel, and use natural keys with relationship tests enforcing integrity. `dim_vendor_scd2` requires a surrogate key, because one `vendor_id` maps to several rows.
- **The tier changes were engineered.** TLC data contains no slowly-changing vendor attribute. Each change is a one-line commit in `seeds/vendor_master.csv`, so the history is auditable rather than asserted.

---

## Bugs found and fixed

Four are worth reading. Each was found by a check, not by luck.

### 1. Deduplication would have corrupted revenue

163 apparent duplicate pairs. Rather than calling `dropDuplicates()`, the full rows were inspected: each pair was identical on six key columns and **opposite in `total_amount`** (`-4.00` / `+4.00`).

Verified, not assumed — all 163 groups summed to exactly zero, and zero groups had matching totals. These are **fare charges and their reversals**, not duplicate writes.

`dropDuplicates()` keeps one arbitrary row per group. It would have retained either the charge or the refund, **non-deterministically**, distorting revenue differently on each run.

The bug was in the key, not the data. Adding `total_amount`, `vendor_id`, and `payment_type` resolved all 163. Dedup remains in the pipeline at zero removals, as a guard against future re-ingestion errors.

### 2. Hardcoded date validation quarantined 97% of the data

A validation rule with literal date bounds worked fine on one month. At six months it flagged 16,390,645 rows.

Fixed by deriving each row's valid window from its own `_source_file` — lineage metadata added for provenance, now doing functional work. Scales to any number of files with no edits.

Post-fix count: **440 rows**, about 73 per month. That's a genuine TLC quirk — trips whose timestamps fall outside the month their file claims — not a rule artifact.

### 3. Parquet physical type drift silently nulled 6 columns across 16M rows

TLC changed physical Parquet types between monthly releases. Spark will not coerce across physical types; it raises `PARQUET_COLUMN_DATA_TYPE_MISMATCH` or, under Auto Loader's rescue mode, diverts the values into `_rescued_data`.

Six columns went null across 16 million rows. **No error.**

The signal was a data quality metric reading exactly **100.0%** missing on two independent sources on the same date. Real-world quality issues are rarely exactly 100% — that's a pipeline signature, not a source problem.

Recovered by parsing `_rescued_data` in Silver with `coalesce` and `get_json_object`. Fixed in Silver, not Bronze: Bronze's job is fidelity to source.

*Instrumentation built while the data was clean was the only thing that caught it when the data wasn't.*

### 4. Hand-typed reference data drifted from source data

`vendor_master.csv` was written from TLC's published data dictionary, which lists VendorID 1 and 2.

The data contains a **vendor 6** — undocumented, 4,132 trips, first appearing 2023-02-01. It flowed into `dim_vendor` automatically (built with `SELECT DISTINCT` from the data) so every existing test passed. It was absent only from the hand-typed seed.

Caught by the `left join` plus a null count on `vendor_version_key`. An inner join would have deleted 4,132 rows and reported success.

Now guarded by a `not_null` test — if TLC adds a vendor 7, the build fails instead of silently producing nulls.

---

## Testing

**34 dbt tests, all passing.**

| Type | Count | Catches |
|---|---|---|
| `not_null` | 16 | missing keys and measures |
| `relationships` | 5 | orphaned foreign keys |
| `unique` | 4 | duplicate dimension keys |
| `accepted_values` | 3 | invalid enum values |
| `accepted_range` | 1 | non-positive trip distance |
| **singular** | **1** | **overlapping SCD2 versions** |

The relationship tests carry real weight: the star schema uses natural keys rather than build-time surrogate joins, so **these tests are the referential integrity mechanism** — there is no database constraint doing it.

The singular test asserts an invariant no built-in can express: for any vendor, no two versions may be valid simultaneously. An overlap would cause a point-in-time join to match multiple dimension rows for one fact row and silently double-count revenue.

Its sensitivity was verified by deliberately inverting the boundary operator (`<` → `<=`), which produced 2 false positives — confirming the test discriminates on exactly the condition that matters, rather than passing because it is too loose to fail.

---

## Measured results

| Metric | Value |
|---|---|
| Bronze rows ingested | 19,898,800 |
| Silver clean | 19,453,731 |
| Silver quarantined | 445,069 (2.24%) |
| True duplicates removed | 0 (163 investigated, all reversals) |
| Rows flagged `passenger_count_missing` | 68,366 |
| Fact rows with unresolved vendor version | 0 |
| Gross revenue (Jan–Jun 2023) | $551,111,087.78 |
| SCD2 revenue misattribution avoided | $210,804,040.90 (38.3%) |
| dbt tests | 34 passing |
| Source files ingested | 9 (47.7 MB) |

---

## Cost management

Running on Pay-As-You-Go, not a sponsored subscription — cost is a real constraint and part of the engineering.

- **A NAT Gateway idle-cost incident** ($5.42) traced to Secure Cluster Connectivity, which provisions a NAT Gateway billed hourly whether or not compute runs. Workspace rebuilt with SCC disabled; idle cost now ≈ $0/day.
- **Serverless SQL warehouse for dbt**, all-purpose cluster only for notebooks. Different jobs, different compute, ~4 minutes of startup avoided per query.
- Budget alert at $30/month with 50% / 90% thresholds.
- Cluster terminated at the close of every session.

---

## Repository structure

```
├── adf/                    ADF pipeline, datasets, linked services (JSON)
├── dbt/nyctaxi_gold/
│   ├── models/             star schema + SCD2 dimension
│   ├── seeds/              vendor_master.csv (SCD2 source)
│   ├── snapshots/          snap_vendor_master.yml
│   └── tests/              singular invariant tests
├── ingestion/              upload_to_landing.py
├── notebooks/
│   ├── 01_setup_external_locations.py
│   ├── 02_bronze_autoloader.py
│   └── 03_silver_transformations.py
└── docs/                   diagrams and screenshots
```

---

## Status

| Step | |
|---|---|
| 1–6 · Ingestion, Bronze, Silver, Gold | ✅ |
| 7 · SCD Type 2 | ✅ |
| 8 · Orchestration (Lakeflow Jobs) | ⬜ |
| 9 · Unity Catalog governance + column masking | ⬜ |
| 10 · CI/CD (GitHub Actions, two-tier) | ⬜ |
| 11 · Optimization experiment (Z-ORDER, before/after) | ✅ measured — [see write-up](docs/optimization_experiment.md) |
| 12 · Power BI dashboard | ⬜ |
| 13 · Architecture diagram, screenshots, demo | ⬜ |

**Known gaps:** `silver_trips_quarantine` is an orphan node in the dbt DAG — a `fct_data_quality` model would close it. Key Vault is provisioned but not yet wired into Databricks secret scopes. The ADF pipeline runs on demand; no scheduled trigger yet.
Z-ORDER on `fct_trips` does not survive a rebuild — `CREATE OR REPLACE TABLE` writes files in query-emission order, discarding the clustering, so OPTIMIZE must run as a task after the dbt tasks in the orchestrated job.

---

*Built by [Abdullah Al Ahad Khan](https://github.com/alahad102) — MS Computer Science, Prairie View A&M University.*