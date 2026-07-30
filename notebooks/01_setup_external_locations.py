# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `ext-nyctaxi-bronze`
# MAGIC URL 'abfss://bronze@stlakehousenyctx2.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `dbw_lakehouse_nyctx_7405610338249631`);
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `ext-nyctaxi-silver`
# MAGIC URL 'abfss://silver@stlakehousenyctx2.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `dbw_lakehouse_nyctx_7405610338249631`);
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `ext-nyctaxi-gold`
# MAGIC URL 'abfss://gold@stlakehousenyctx2.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `dbw_lakehouse_nyctx_7405610338249631`);

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS nyctaxi
# MAGIC MANAGED LOCATION 'abfss://bronze@stlakehousenyctx2.dfs.core.windows.net/managed';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS nyctaxi.bronze
# MAGIC MANAGED LOCATION 'abfss://bronze@stlakehousenyctx2.dfs.core.windows.net/managed';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS nyctaxi.silver
# MAGIC MANAGED LOCATION 'abfss://silver@stlakehousenyctx2.dfs.core.windows.net/managed';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS nyctaxi.gold
# MAGIC MANAGED LOCATION 'abfss://gold@stlakehousenyctx2.dfs.core.windows.net/managed';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER EXTERNAL LOCATION `ext-nyctaxi-bronze` SET STORAGE CREDENTIAL cred_nyctaxi_storage;
# MAGIC ALTER EXTERNAL LOCATION `ext-nyctaxi-silver` SET STORAGE CREDENTIAL cred_nyctaxi_storage;
# MAGIC ALTER EXTERNAL LOCATION `ext-nyctaxi-gold`   SET STORAGE CREDENTIAL cred_nyctaxi_storage;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION `ext-nyctaxi-landing`;

# COMMAND ----------

files = dbutils.fs.ls("abfss://landing@stlakehousenyctx2.dfs.core.windows.net/")
for f in files:
    print(f.name, "|", round(f.size / 1024 / 1024, 2), "MB")