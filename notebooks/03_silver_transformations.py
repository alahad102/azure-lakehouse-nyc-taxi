# Databricks notebook source
from pyspark.sql import functions as F

yellow = spark.table("nyctaxi.bronze.bronze_yellow_trips")
green  = spark.table("nyctaxi.bronze.bronze_green_trips")

def profile(df, name, pickup, dropoff):
    total = df.count()
    checks = {
        "negative_fare":        F.col("fare_amount") < 0,
        "zero_or_neg_distance": F.col("trip_distance") <= 0,
        "passenger_zero":       F.col("passenger_count") == 0,
        "passenger_over_8":     F.col("passenger_count") > 8,
        "passenger_null":       F.col("passenger_count").isNull(),
        "dropoff_before_pickup": F.col(dropoff) <= F.col(pickup),
        "outside_jan_2023":     (F.col(pickup) < "2023-01-01") | (F.col(pickup) >= "2023-02-01"),
        "null_pickup_location": F.col("PULocationID").isNull(),
    }
    print(f"\n=== {name} | total rows: {total:,} ===")
    for label, cond in checks.items():
        n = df.filter(cond).count()
        print(f"  {label:<24} {n:>10,}  ({n/total*100:6.3f}%)")

profile(yellow, "YELLOW", "tpep_pickup_datetime", "tpep_dropoff_datetime")
profile(green,  "GREEN",  "lpep_pickup_datetime", "lpep_dropoff_datetime")

# COMMAND ----------

for name, df in [("yellow", yellow), ("green", green)]:
    n = df.filter(F.col("_rescued_data").isNotNull()).count()
    print(f"{name}: {n:,} rows with rescued data")

# COMMAND ----------

def fatal_rules(pickup, dropoff):
    return (
        (F.col("fare_amount") < 0)
        | (F.col("trip_distance") <= 0)
        | (F.col(dropoff) <= F.col(pickup))
        | (F.col(pickup) < "2023-01-01")
        | (F.col(pickup) >= "2023-02-01")
    )

for name, df, pu, do in [
    ("YELLOW", yellow, "tpep_pickup_datetime", "tpep_dropoff_datetime"),
    ("GREEN",  green,  "lpep_pickup_datetime", "lpep_dropoff_datetime"),
]:
    total = df.count()
    bad   = df.filter(fatal_rules(pu, do)).count()
    print(f"{name}: {bad:,} of {total:,} quarantined ({bad/total*100:.3f}%) -> {total-bad:,} clean")

# COMMAND ----------

dup_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
            "PULocationID", "DOLocationID", "trip_distance", "fare_amount"]

total   = yellow.count()
distinct = yellow.select(*dup_cols).distinct().count()
print(f"YELLOW: {total:,} rows, {distinct:,} distinct combos -> {total-distinct:,} potential duplicates")

# COMMAND ----------

dup_keys = (yellow.groupBy(*dup_cols)
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count")))

print("Duplicate groups:", dup_keys.count())
dup_keys.show(10, truncate=False)

# COMMAND ----------

sample = dup_keys.limit(1).collect()[0]
cond = None
for c in dup_cols:
    eq = F.col(c).eqNullSafe(sample[c])
    cond = eq if cond is None else (cond & eq)

yellow.filter(cond).select(
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "trip_distance",
    "fare_amount", "tip_amount", "total_amount", "payment_type"
).show(truncate=False)

# COMMAND ----------

dup_full = dup_keys.join(yellow, on=dup_cols, how="inner")

# Do the amounts within each group sum to ~zero? (refund pairs cancel)
sums = (dup_full.groupBy(*dup_cols)
    .agg(F.sum("total_amount").alias("net"),
         F.countDistinct("total_amount").alias("distinct_totals"),
         F.count("*").alias("n")))

print("groups netting to zero:", sums.filter(F.abs(F.col("net")) < 0.01).count())
print("groups with identical total_amount:", sums.filter(F.col("distinct_totals") == 1).count())
print("total groups:", sums.count())

# COMMAND ----------

dup_cols_v2 = dup_cols + ["total_amount", "VendorID", "payment_type"]
t = yellow.count()
d = yellow.select(*dup_cols_v2).distinct().count()
print(f"YELLOW with corrected key: {t-d:,} remaining duplicates")

# COMMAND ----------

green_dup_cols = ["lpep_pickup_datetime", "lpep_dropoff_datetime",
                  "PULocationID", "DOLocationID", "trip_distance", "fare_amount"]
green_dup_cols_v2 = green_dup_cols + ["total_amount", "VendorID", "payment_type"]

t = green.count()
print(f"GREEN naive key:     {t - green.select(*green_dup_cols).distinct().count():,} duplicates")
print(f"GREEN corrected key: {t - green.select(*green_dup_cols_v2).distinct().count():,} duplicates")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_extract, to_date, add_months

# ---- helper: normalize any remaining mixed-case column names -------------

def lower_cols(df):
    for c in df.columns:
        if c != c.lower() and not c.startswith("_"):
            df = df.withColumnRenamed(c, c.lower())
    return df

# ---- 1. Harmonize each source to a common schema -------------------------

y = (spark.table("nyctaxi.bronze.bronze_yellow_trips")
    .withColumn("service_type", F.lit("yellow"))
    .withColumnRenamed("VendorID", "vendor_id")
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("RatecodeID", "ratecode_id")
    .withColumnRenamed("PULocationID", "pu_location_id")
    .withColumnRenamed("DOLocationID", "do_location_id")
    .withColumn("ehail_fee", F.lit(None).cast("double"))
    .withColumn("trip_type", F.lit(None).cast("int")))

g = (spark.table("nyctaxi.bronze.bronze_green_trips")
    .withColumn("service_type", F.lit("green"))
    .withColumnRenamed("VendorID", "vendor_id")
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("RatecodeID", "ratecode_id")
    .withColumnRenamed("PULocationID", "pu_location_id")
    .withColumnRenamed("DOLocationID", "do_location_id")
    .withColumn("ehail_fee", F.col("ehail_fee").cast("double"))
    .withColumn("airport_fee", F.lit(None).cast("double")))

y = lower_cols(y)
g = lower_cols(g)

# ---- 2. Union BY NAME, never positional ---------------------------------
# Column ORDER differs between the two sources. A positional union() would
# silently misalign values. unionByName matches on name instead.

from pyspark.sql.functions import get_json_object, coalesce

def from_rescue(field, cast_to):
    return get_json_object(F.col("_rescued_data"), f"$.{field}").cast(cast_to)

trips = (trips
    .withColumn("vendor_id",       coalesce(F.col("vendor_id"),       from_rescue("VendorID", "long")))
    .withColumn("passenger_count", coalesce(F.col("passenger_count"), from_rescue("passenger_count", "double")))
    .withColumn("ratecode_id",     coalesce(F.col("ratecode_id"),     from_rescue("RatecodeID", "double")))
    .withColumn("pu_location_id",  coalesce(F.col("pu_location_id"),  from_rescue("PULocationID", "long")))
    .withColumn("do_location_id",  coalesce(F.col("do_location_id"),  from_rescue("DOLocationID", "long")))
    .withColumn("payment_type",    coalesce(F.col("payment_type"),    from_rescue("payment_type", "long")))
    .withColumn("airport_fee",     coalesce(F.col("airport_fee"),
                                            from_rescue("Airport_fee", "double"),
                                            from_rescue("airport_fee", "double")))
    .withColumn("ehail_fee",       coalesce(F.col("ehail_fee"),       from_rescue("ehail_fee", "double")))
    .withColumn("trip_type",       coalesce(F.col("trip_type"),       from_rescue("trip_type", "double")))
)

# ---- 3. Derive the valid period from each row's own source file ----------
# Earlier this rule hardcoded January 2023, which quarantined 16.4M rows
# (97% of all quarantined) the moment Feb-Jun data arrived. Deriving the
# window from the _source_file lineage column scales to any number of months
# with no edits. Same class of trap as a hardcoded dim_date range.
#   "yellow_tripdata_2023-04.parquet" -> 2023-04-01
#   valid window: [2023-04-01, 2023-05-01)

trips = trips.withColumn(
    "_file_period",
    to_date(regexp_extract(F.col("_source_file"), r"(\d{4}-\d{2})", 1), "yyyy-MM")
)

# ---- 4. Validate ---------------------------------------------------------
# when().when() chain records WHICH rule failed, not just that one did.
# First match wins, so counts show the PRIMARY failure reason.

fatal = (
      F.when(F.col("fare_amount") < 0, "negative_fare")
       .when(F.col("trip_distance") <= 0, "zero_or_negative_distance")
       .when(F.col("dropoff_datetime") <= F.col("pickup_datetime"), "dropoff_before_pickup")
       .when((F.col("pickup_datetime") < F.col("_file_period")) |
             (F.col("pickup_datetime") >= add_months(F.col("_file_period"), 1)),
             "pickup_outside_file_period")
)

trips = (trips
    .withColumn("_quarantine_reason", fatal)
    .withColumn("passenger_count_missing", F.col("passenger_count").isNull())
    .withColumn("is_reversal", F.col("total_amount") < 0))

clean      = trips.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
quarantine = trips.filter(F.col("_quarantine_reason").isNotNull())

print(f"clean:      {clean.count():,}")
print(f"quarantine: {quarantine.count():,}")
print(f"columns:    {len(clean.columns)}")

# COMMAND ----------

(quarantine.groupBy("_quarantine_reason")
    .count().orderBy(F.desc("count")).show(truncate=False))

# COMMAND ----------

# Both sources present, in expected proportion?
clean.groupBy("service_type").count().show()

# Quarantine breakdown by reason and source
(quarantine.groupBy("service_type", "_quarantine_reason")
    .count().orderBy("service_type", F.desc("count")).show(truncate=False))

# Flags behaving as expected
print("passenger_count_missing:", clean.filter(F.col("passenger_count_missing")).count())
print("is_reversal:            ", clean.filter(F.col("is_reversal")).count())

# COMMAND ----------

(clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("nyctaxi.silver.silver_trips"))

(quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("nyctaxi.silver.silver_trips_quarantine"))

print("written")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT service_type, COUNT(*) AS rows,
# MAGIC        ROUND(SUM(total_amount), 2) AS revenue,
# MAGIC        ROUND(AVG(trip_distance), 2) AS avg_miles
# MAGIC FROM nyctaxi.silver.silver_trips
# MAGIC GROUP BY service_type;

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("nyctaxi.silver.silver_trips")
quar   = spark.table("nyctaxi.silver.silver_trips_quarantine")

clean_by_source = silver.groupBy("service_type").agg(
    F.count("*").alias("rows_clean"),
    F.sum(F.col("passenger_count_missing").cast("int")).alias("passenger_count_missing"),
    F.sum(F.col("is_reversal").cast("int")).alias("reversal_rows"))

quar_by_reason = quar.groupBy("service_type", "_quarantine_reason").agg(
    F.count("*").alias("rows_quarantined"))

metrics = (clean_by_source.join(
        quar_by_reason.groupBy("service_type")
                      .agg(F.sum("rows_quarantined").alias("rows_quarantined")),
        on="service_type", how="outer")
    .withColumn("rows_total", F.col("rows_clean") + F.col("rows_quarantined"))
    .withColumn("quarantine_pct",
                F.round(F.col("rows_quarantined") / F.col("rows_total") * 100, 3))
    .withColumn("measured_at", F.current_timestamp()))

metrics.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nyctaxi.silver.silver_quality_metrics")

quar_by_reason.withColumn("measured_at", F.current_timestamp()) \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("nyctaxi.silver.silver_quality_by_reason")

metrics.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(pickup_datetime, 'yyyy-MM') AS month,
# MAGIC        service_type,
# MAGIC        COUNT(*) AS trips,
# MAGIC        SUM(CAST(passenger_count_missing AS INT)) AS missing,
# MAGIC        ROUND(SUM(CAST(passenger_count_missing AS INT)) / COUNT(*) * 100, 1) AS pct_missing
# MAGIC FROM nyctaxi.silver.silver_trips
# MAGIC GROUP BY 1, 2 ORDER BY 1, 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM nyctaxi.bronze.bronze_yellow_trips
# MAGIC WHERE _rescued_data IS NOT NULL;

# COMMAND ----------

from pyspark.sql import functions as F
(spark.table("nyctaxi.bronze.bronze_yellow_trips")
    .filter(F.col("_rescued_data").isNotNull())
    .select("_source_file", "_rescued_data")
    .limit(3).show(truncate=False))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _source_file, COUNT(*) AS rows,
# MAGIC        SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS rescued
# MAGIC FROM nyctaxi.bronze.bronze_yellow_trips
# MAGIC GROUP BY _source_file ORDER BY _source_file;

# COMMAND ----------

for m in ["01", "05"]:
    p = f"abfss://landing@stlakehousenyctx2.dfs.core.windows.net/yellow_tripdata_2023-{m}.parquet"
    print(f"\n=== 2023-{m} ===")
    spark.read.parquet(p).printSchema()

# COMMAND ----------

for m in ["01", "05"]:
    p = f"abfss://landing@stlakehousenyctx2.dfs.core.windows.net/green_tripdata_2023-{m}.parquet"
    print(f"\n=== green 2023-{m} ===")
    spark.read.parquet(p).printSchema()