# Optimization Experiment — OPTIMIZE and Z-ORDER on `fct_trips`

**Date:** August 7, 2026
**Table:** `nyctaxi.gold.fct_trips` — 19,453,731 rows, 23 columns
**Compute:** Serverless SQL warehouse, Databricks Runtime 18.x (Photon)

---

## Summary

Z-ORDER on `pickup_date_key` reduced files scanned from **5 to 1** and increased table size by **3.6%**. Query runtime was unchanged within measurement noise.

**The technique worked as designed. The table is too small to benefit from it.**

This is a negative result, reported as one. The measurement discipline required to establish it is documented below, because three separate caching layers will each produce a convincing but meaningless speedup if you don't defeat them.

---

## Baseline

`DESCRIBE DETAIL` before any change:

| Property | Value |
|---|---|
| `numFiles` | 5 |
| `sizeInBytes` | 330,228,141 (315 MB) |
| Avg file size | 66.0 MB |
| File size range | 13.7 MB – 110.0 MB |
| `partitionColumns` | `[]` |

**No small file problem.** 5 files at ~66 MB is already well-packed.

The reason is architectural: dbt builds Gold with `CREATE OR REPLACE TABLE`, which discards the table and rewrites it in a single operation. It never appends. Small files accumulate in tables written *incrementally* — streaming sinks, hourly batches, row-level updates. A full-rebuild pattern never gives them the chance.

The file size *spread* was uneven, though — one 13.7 MB runt against a 110 MB file. Uneven sizes hurt parallelism, since one task runs long after the others finish.

---

## Measurement methodology

Three caching layers each produce a false result. All three had to be handled.

| Layer | Symptom | Handling |
|---|---|---|
| **Warehouse cold start** | First query took 5.228s | Warm up before measuring |
| **Result cache** | `0/0 tasks`, `0 rows read`, `0 bytes read` | Vary query text per run |
| **Disk cache** (local SSD) | Second read faster than first | Accept; compare cold-to-cold only |

### The traps, in order

**Cold start.** The first run of the benchmark query took 5.228s. Repeating it immediately gave 0.249s. Nothing about the table changed — the warehouse had gone idle and the first query paid startup cost.

Reporting 5.2s as a baseline and ~0.4s as the post-OPTIMIZE result would have shown a fabricated 13× improvement.

**Result cache.** `SET use_cached_result = false` works, but did not persist across executions in the SQL Editor — repeated runs kept coming back with `0/0 tasks completed`, indicating no work was done at all.

The reliable workaround is to change the query text, since the result cache keys on the exact statement:

```sql
SELECT count(*), sum(total_amount) /* r1 */
FROM nyctaxi.gold.fct_trips
WHERE pickup_date_key = 20230315;
```

Then `/* r2 */`, `/* r3 */`. A comment changes the text without changing the semantics.

**Reading the `Tasks` column is the check.** `0/0 completed` means the result came from cache and the run must be discarded. Runtime alone does not reveal this.

### What to measure

**Files scanned, not runtime.**

Runtime at this scale is dominated by scheduling and network overhead, not I/O. Run-to-run variance (0.27s – 0.52s) exceeds the effect being measured. Files scanned comes from the query plan and is deterministic.

---

## Benchmark query

```sql
SELECT count(*), sum(total_amount)
FROM nyctaxi.gold.fct_trips
WHERE pickup_date_key = 20230315;
```

Returns 119,859 trips — one day, about 0.6% of the table.

### Before Z-ORDER

| Run | Tasks | Runtime | Rows read | Bytes read |
|---|---|---|---|---|
| 1 | 5/5 | 0.296s | 119,859 | 7.86 MB |
| 2 | 0/0 | 0.368s | 0 | 0 B *(cached — discarded)* |
| 3 | 5/5 | 0.269s | 119,859 | 7.86 MB |

**All 5 files opened.** Every file's `pickup_date_key` min/max range spanned the full six months, so none could be ruled out.

**Rows read equalled rows returned**, and bytes read was 7.86 MB of a 315 MB table — 2.5%. Column pruning was already doing significant work: the query touches 2 of 23 columns, and Parquet reads only those from disk regardless of row order.

---

## The change

```sql
OPTIMIZE nyctaxi.gold.fct_trips
ZORDER BY (pickup_date_key);
```

Runtime: 19.3s. 27 tasks. 326.26 MB written.

### Reported metrics

| | Before | After |
|---|---|---|
| Files | 5 | **4** |
| Total size | 330,228,141 | **342,112,593** |
| Min file size | 13.7 MB | 81.6 MB |
| Max file size | 110.0 MB | 90.0 MB |
| Avg file size | 66.0 MB | 85.5 MB |

Also reported: `partitionsOptimized: 0`, `numOutputCubes: 1`, `strategyName: minCubeSize(107374182400)`.

The entire table fit inside a single Z-order cube — the minimum cube size is 100 GB, and this table is 0.3 GB.

### The table got 3.6% larger

Sorting rows by `pickup_date_key` **broke the natural clustering of every other column**. Parquet compresses best when adjacent rows hold similar values; the rows arrived from Silver already loosely grouped in ways that correlated across columns. Re-sorting by date scattered that.

**This is the Z-ORDER tradeoff, measured: you pay in storage and compression to buy data skipping.** With ZSTD compression the cost here was 11.9 MB.

---

## After Z-ORDER

Single-date query, three uncached runs:

| Run | Tasks | Runtime | Rows read | Bytes read |
|---|---|---|---|---|
| r1 | 1/1 | 0.519s | 119,859 | 8.09 MB |
| r2 | 1/1 | 0.397s | 119,859 | 8.09 MB |
| r3 | 1/1 | 0.411s | 119,859 | 8.09 MB |

**1 task instead of 5.** Three of the four files were eliminated by their min/max statistics and never opened. The skip is deterministic across runs.

### Range query

```sql
SELECT count(*), sum(total_amount)
FROM nyctaxi.gold.fct_trips
WHERE pickup_date_key BETWEEN 20230301 AND 20230307;
```

| Query | Rows returned | Tasks | Bytes read | Runtime |
|---|---|---|---|---|
| 1 day | 119,859 | 1/1 | 8.09 MB | ~0.41s |
| **7 days** | **766,937** | **1/1** | **8.09 MB** | ~0.42s |

**6.4× more rows returned for identical work.** The whole week lives inside one Z-ordered file, so the query reads that file and filters within it. At 8 MB the read is effectively free — returning six times more rows costs nothing measurable.

---

## Result

| Metric | Before | After | Change |
|---|---|---|---|
| **Files scanned** | **5 of 5** | **1 of 4** | **−80%** |
| Bytes read | 7.86 MB | 8.09 MB | +2.9% |
| Rows read | 119,859 | 119,859 | — |
| Table size | 330.2 MB | 342.1 MB | +3.6% |
| File count | 5 | 4 | −1 |
| File size spread | 13.7–110 MB | 81.6–90.0 MB | evened out |
| Runtime | ~0.28s | ~0.41s | within noise |

**Files scanned dropped 80%. Nothing else improved, and two things got marginally worse.**

### Why the file-skipping win didn't translate

**Too few files.** Data skipping saves you from opening files. With 4 files, the ceiling is skipping 3. Meaningful gains appear at hundreds or thousands of files, where skipping 95% turns minutes into seconds.

**Column pruning already dominated.** The query reads 2 of 23 columns. That happens regardless of row order and was already cutting the read to 2.5% of the table. Z-ORDER can only improve on what column pruning leaves behind, and there wasn't much.

**The sort cost compression.** Bytes read went *up* 2.9%, tracking the 3.6% storage inflation — the single file now read is proportionally larger than the equivalent fraction was before.

### When this would have paid off

- Table in the tens or hundreds of GB, producing hundreds of files
- Queries selecting many columns, where column pruning can't carry the load
- High-cardinality filter columns where the natural row order gives no useful ordering
- Repeated selective queries — a BI dashboard filtering by date, for instance

None of those describe this table today.

---

## Z-ORDER is not persistent

`DESCRIBE DETAIL` reports `clusteringColumns: []` after the Z-ORDER completed.

That field tracks **liquid clustering** (`CLUSTER BY`), a separate and newer feature. Z-ORDER leaves no marker in table properties — the only record is the `zOrderBy` parameter in the `DESCRIBE HISTORY` entry for the OPTIMIZE operation.

**Practical consequence: any subsequent write undoes it.** The next `dbt run --select fct_trips` issues `CREATE OR REPLACE TABLE`, producing files in whatever order the query emits them. The clustering is gone.

**Z-ORDER is a one-time physical rearrangement, not a table setting.** In a pipeline that rebuilds the table on every run, OPTIMIZE has to run *after* the rebuild, every time — which is why it belongs as a task in the orchestrated job rather than as a manual command.

---

## Time travel and retention

`DESCRIBE HISTORY nyctaxi.gold.fct_trips` shows the table's full lifecycle, recorded automatically:

| Version | Date | Operation | Rows | Files |
|---|---|---|---|---|
| 0 | Jul 30 | CREATE OR REPLACE | 3,063,498 | 1 |
| 1 | Aug 1 | CREATE OR REPLACE | 19,453,731 | 3 |
| 2 | Aug 7, 17:21 | CREATE OR REPLACE | 19,453,731 | 5 |
| 3 | Aug 7, 17:57 | CREATE OR REPLACE | 19,453,731 | 5 |
| 4 | Aug 7, 18:35 | OPTIMIZE | — | 4 |

Version 0 is the 1-month build. Version 1 is the 6-month scale-up. Versions 2 and 3 bracket today's vendor 6 fix.

### Reproducing a past bug

Versions 2 and 3 are the rebuild before and after vendor 6 was added to the SCD2 seed:

```sql
SELECT count(*) - count(vendor_version_key) AS unmatched
FROM nyctaxi.gold.fct_trips VERSION AS OF 2;
-- 4132

SELECT count(*) - count(vendor_version_key) AS unmatched
FROM nyctaxi.gold.fct_trips VERSION AS OF 3;
-- 0
```

The bug and its fix, both queryable from the live table. No backup, no staging copy — just a version number.

### History outlives the data

Querying version 0 failed:

```
[DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION]
Cannot time travel beyond delta.deletedFileRetentionDuration (168 HOURS)
```

168 hours = 7 days. Version 0 was written 9 days earlier, and its Parquet files have already been cleaned up.

**The log entry survives; the data does not.** `DESCRIBE HISTORY` will happily list versions you can no longer read. The practical answer to "how far back can you time travel?" is the retention duration, not the length of the history.

---

## VACUUM — not run, deliberately

`VACUUM` deletes files no longer referenced by any retained version.

It was **not run**, for two reasons:

**There is nothing meaningful to reclaim.** The 7-day retention has already expired the pre-August-1 versions. Files from versions 2–4 total under a gigabyte — a few cents per month in ADLS.

**The retained versions have current value.** Versions 2 and 3 are the vendor 6 bug and its fix, and they're actively useful as a reproducible test case.

The tradeoff to understand: **storage cost versus how far back you can reproduce a past state.** Setting retention below 7 days requires explicitly disabling a safety check, because a long-running query reading an older version would fail mid-execution when its files disappear.

---

## What this experiment demonstrates

**Measure before optimizing.** The initial hypothesis — a small file problem — was wrong. `DESCRIBE DETAIL` disproved it in one query, before any changes were made.

**Defeat the caches, or measure nothing.** Cold start alone would have manufactured a 13× improvement. The result cache produced runs doing literally zero work while reporting plausible runtimes.

**Report the metric that carries signal.** Files scanned is deterministic and comes from the query plan. Runtime at this scale is noise.

**A negative result is a result.** Z-ORDER behaved exactly as documented. The table is simply too small for it to matter — and knowing *why* is more useful than a speedup that only exists because the second run hit a warm cache.