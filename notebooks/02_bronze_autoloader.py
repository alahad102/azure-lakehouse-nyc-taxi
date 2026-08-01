# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
import uuid

LANDING = "abfss://landing@stlakehousenyctx2.dfs.core.windows.net/"
BRONZE  = "abfss://bronze@stlakehousenyctx2.dfs.core.windows.net/"
run_id  = str(uuid.uuid4())

# NYC TLC changes column types between monthly releases. Jan 2023 declares
# VendorID/PULocationID/DOLocationID as long and passenger_count/RatecodeID
# as double; May 2023 declares them as integer and long. Without hints,
# Auto Loader infers from whichever file it sees first and rescues the rest.
# Hint the WIDER type so both variants coerce cleanly.

df_yellow = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{BRONZE}_schemas/yellow")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.schemaHints", YELLOW_HINTS)
    .option("pathGlobFilter", "yellow_tripdata_*.parquet")
    .load(LANDING)
    .withColumn("_source_file", col("_metadata.file_name"))
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_run_id", lit(run_id))
)

(df_yellow.writeStream
    .format("delta")
    .option("checkpointLocation", f"{BRONZE}_checkpoints/yellow")
    .trigger(availableNow=True)
    .toTable("nyctaxi.bronze.bronze_yellow_trips")
    .awaitTermination()
)

print("Yellow ingestion complete. Run ID:", run_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count FROM nyctaxi.bronze.bronze_yellow_trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VendorID, tpep_pickup_datetime, total_amount,
# MAGIC        _source_file, _ingested_at, _run_id
# MAGIC FROM nyctaxi.bronze.bronze_yellow_trips
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _run_id, COUNT(*) AS rows
# MAGIC FROM nyctaxi.bronze.bronze_yellow_trips
# MAGIC GROUP BY _run_id;

# COMMAND ----------

df_green = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{BRONZE}_schemas/green")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.schemaHints", GREEN_HINTS)
    .option("pathGlobFilter", "green_tripdata_*.parquet")
    .load(LANDING)
    .withColumn("_source_file", col("_metadata.file_name"))
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_run_id", lit(run_id))
)

(df_green.writeStream
    .format("delta")
    .option("checkpointLocation", f"{BRONZE}_checkpoints/green")
    .trigger(availableNow=True)
    .toTable("nyctaxi.bronze.bronze_green_trips")
    .awaitTermination()
)

print("Green ingestion complete.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _source_file, COUNT(*) AS rows,
# MAGIC        SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS rescued,
# MAGIC        SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) AS null_passengers
# MAGIC FROM nyctaxi.bronze.bronze_yellow_trips
# MAGIC GROUP BY _source_file ORDER BY _source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM nyctaxi.bronze.bronze_green_trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE nyctaxi.bronze.bronze_yellow_trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE nyctaxi.bronze.bronze_green_trips;

# COMMAND ----------

from pyspark.sql import functions as F

zones = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("abfss://landing@stlakehousenyctx2.dfs.core.windows.net/taxi_zone_lookup.csv")
    .withColumn("_source_file", F.lit("taxi_zone_lookup.csv"))
    .withColumn("_ingested_at", F.current_timestamp()))

# normalize column names to snake_case
for c in zones.columns:
    if not c.startswith("_"):
        zones = zones.withColumnRenamed(c, c.lower())

(zones.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("nyctaxi.bronze.bronze_taxi_zones"))

print(f"rows: {zones.count()}")
zones.show(5, truncate=False)
zones.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'yellow' AS svc, COUNT(*) AS rows FROM nyctaxi.bronze.bronze_yellow_trips
# MAGIC UNION ALL
# MAGIC SELECT 'green', COUNT(*) FROM nyctaxi.bronze.bronze_green_trips;

# COMMAND ----------

BRONZE = "abfss://bronze@stlakehousenyctx2.dfs.core.windows.net/"

spark.sql("DROP TABLE IF EXISTS nyctaxi.bronze.bronze_yellow_trips")
spark.sql("DROP TABLE IF EXISTS nyctaxi.bronze.bronze_green_trips")

for p in ["_checkpoints/yellow", "_checkpoints/green",
          "_schemas/yellow", "_schemas/green"]:
    try:
        dbutils.fs.rm(BRONZE + p, recurse=True)
        print("removed", p)
    except Exception as e:
        print("skip", p, e)

# COMMAND ----------

spark.table("nyctaxi.bronze.bronze_yellow_trips").printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
(spark.table("nyctaxi.bronze.bronze_yellow_trips")
    .filter(F.col("_source_file") == "yellow_tripdata_2023-05.parquet")
    .select("_rescued_data").limit(2).show(truncate=False))

# COMMAND ----------

print(dbutils.fs.ls("abfss://bronze@stlakehousenyctx2.dfs.core.windows.net/_schemas/yellow"))

# COMMAND ----------

jan = "abfss://landing@stlakehousenyctx2.dfs.core.windows.net/yellow_tripdata_2023-01.parquet"
may = "abfss://landing@stlakehousenyctx2.dfs.core.windows.net/yellow_tripdata_2023-05.parquet"

jan_schema = spark.read.parquet(jan).schema

test = spark.read.schema(jan_schema).parquet(may)
test.select("VendorID", "passenger_count", "RatecodeID",
            "PULocationID", "DOLocationID", "airport_fee").show(5)