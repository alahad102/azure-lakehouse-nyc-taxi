# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Unity Catalog Setup: Credential, External Locations, Catalog
# MAGIC
# MAGIC One-time infrastructure setup for the NYC TLC lakehouse. Run this once per workspace.
# MAGIC
# MAGIC ## Prerequisites (must be done before running this notebook)
# MAGIC
# MAGIC **1. ADLS Gen2 storage account with hierarchical namespace enabled**
# MAGIC `stlakehousenyctx2`, containers: `landing`, `bronze`, `silver`, `gold`
# MAGIC
# MAGIC **2. Grant the Databricks Access Connector data-plane access to the storage account**
# MAGIC
# MAGIC Azure RBAC separates *control plane* (manage the resource) from *data plane* (read/write the
# MAGIC data inside it). Being subscription Owner grants the former, not the latter. The Access
# MAGIC Connector's system-assigned managed identity needs an explicit data-plane role:
# MAGIC
# MAGIC ```
# MAGIC Storage account -> Access Control (IAM) -> Add role assignment
# MAGIC   Role:   Storage Blob Data Contributor
# MAGIC   Member: Managed identity -> Access Connector for Azure Databricks
# MAGIC           -> unity-catalog-access-connector
# MAGIC ```
# MAGIC
# MAGIC Verify it landed on the right identity (the portal picker lists several similar-looking
# MAGIC identities). Note that `az role assignment list` displays the **appId** while
# MAGIC `az resource show ... identity.principalId` returns the **objectId** — these differ for the
# MAGIC same identity, so resolve both with `az ad sp show --id <either-id>` before concluding
# MAGIC there's a mismatch.
# MAGIC
# MAGIC **3. Create the storage credential (UI — see next cell for why)**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storage credential — must be created via the UI
# MAGIC
# MAGIC `CREATE STORAGE CREDENTIAL` DDL fails with `PARSE_SYNTAX_ERROR` on an all-purpose cluster;
# MAGIC that statement requires a SQL warehouse. Create it through the UI instead:
# MAGIC
# MAGIC ```
# MAGIC Catalog -> External Data -> Credentials -> Create credential
# MAGIC   Credential Type:  Storage credential
# MAGIC   Name:             cred_nyctaxi_storage
# MAGIC   Authentication:   Azure Managed Identity
# MAGIC   Access Connector ID:
# MAGIC     /subscriptions/<sub-id>/resourceGroups/<databricks-managed-rg>
# MAGIC       /providers/Microsoft.Databricks/accessConnectors/unity-catalog-access-connector
# MAGIC   User-assigned managed identity ID:  (leave blank)
# MAGIC ```
# MAGIC
# MAGIC **Leave the user-assigned field blank on purpose.** Blank means the connector's
# MAGIC *system-assigned* identity is used — that's the one holding the Storage Blob Data
# MAGIC Contributor role. Filling it in would point at `dbmanagedidentity`, which has no such role.
# MAGIC
# MAGIC ### Do not reuse the workspace default credential
# MAGIC
# MAGIC Databricks auto-creates a credential named `dbw_lakehouse_nyctx_<workspace-id>` on
# MAGIC deployment. It points at the same access connector, and external locations created with it
# MAGIC will be accepted **and will even pass the "Test connection" check** — but any real data read
# MAGIC fails:
# MAGIC
# MAGIC ```
# MAGIC PERMISSION_DENIED: The credential '<workspace-default>' is a workspace default credential
# MAGIC that is only allowed to access data in the following paths:
# MAGIC 'abfss://unity-catalog-storage@dbstorage.../...'
# MAGIC ```
# MAGIC
# MAGIC **A credential's allowed paths are a control separate from the underlying identity's IAM
# MAGIC roles.** Two independent gates; both must pass. The identity had full access the whole time —
# MAGIC the credential object itself was scoped to workspace-internal storage.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All four external locations, created directly with the correct credential.
# MAGIC -- URL pattern: abfss://<container>@<storage-account>.dfs.core.windows.net/
# MAGIC --   abfss = secure ADLS Gen2 driver
# MAGIC --   .dfs. = hierarchical namespace endpoint (vs .blob. for flat namespace)
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `ext-nyctaxi-landing`
# MAGIC URL 'abfss://landing@stlakehousenyctx2.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL cred_nyctaxi_storage);
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `ext-nyctaxi-bronze`
# MAGIC URL 'abfss://bronze@stlakehousenyctx2.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL cred_nyctaxi_storage);
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `ext-nyctaxi-silver`
# MAGIC URL 'abfss://silver@stlakehousenyctx2.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL cred_nyctaxi_storage);
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `ext-nyctaxi-gold`
# MAGIC URL 'abfss://gold@stlakehousenyctx2.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL cred_nyctaxi_storage);

# COMMAND ----------

# MAGIC %md
# MAGIC ## File events: deliberately left disabled
# MAGIC
# MAGIC If an external location is created through the UI, the "Test connection" step will report
# MAGIC `File Events Resource Provision — Failed` with a 403. That is expected here and safe to skip.
# MAGIC
# MAGIC Auto Loader has two file-discovery modes:
# MAGIC
# MAGIC | Mode | How it works | When it wins |
# MAGIC |---|---|---|
# MAGIC | **Directory listing** (default) | Lists the folder each run, diffs against the checkpoint | Small-to-moderate file counts |
# MAGIC | **File notification** | Event Grid pushes new-file events to a Storage Queue; Auto Loader reads the queue | Hundreds of thousands of files, where listing itself becomes the bottleneck |
# MAGIC
# MAGIC File notification requires the managed identity to *provision* Azure resources — that needs
# MAGIC control-plane roles (`EventGrid EventSubscription Contributor`, `Storage Queue Data
# MAGIC Contributor`, `Storage Account Contributor`) beyond the data-plane role granted above.
# MAGIC
# MAGIC At this file volume, listing overhead is negligible and notification mode would provision
# MAGIC billable resources to solve a problem that doesn't exist. Directory listing is the correct
# MAGIC choice here.
# MAGIC
# MAGIC Note: file events must also be **disabled** before a location's credential can be changed —
# MAGIC `ALTER EXTERNAL LOCATION ... SET STORAGE CREDENTIAL` fails with
# MAGIC `UC_EXTERNAL_LOCATION_OP_NOT_ALLOWED` while a managed file event queue is bound to it.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Expect 5 rows: the four above, plus the Databricks-managed workspace default location
# MAGIC -- (named after the workspace ID). Do not modify that one — resources named after the
# MAGIC -- workspace ID are Databricks-owned.
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog and schemas
# MAGIC
# MAGIC Unity Catalog addresses everything as `catalog.schema.table` — roughly
# MAGIC `server -> database -> table`. One catalog per project, one schema per medallion layer.
# MAGIC
# MAGIC **`MANAGED LOCATION` is required here.** Without it:
# MAGIC
# MAGIC ```
# MAGIC INVALID_STATE: Metastore storage root URL does not exist
# MAGIC ```
# MAGIC
# MAGIC The metastore has no default storage root configured, so managed tables have nowhere to live.
# MAGIC
# MAGIC **This is also the better design, not just a workaround.** Managed tables let UC handle
# MAGIC layout and optimization, but pointing `MANAGED LOCATION` at *my own* storage account means
# MAGIC every byte survives workspace deletion — which matters because the operational rule for this
# MAGIC project is to delete the Databricks workspace when stepping away for more than a few days.
# MAGIC
# MAGIC UC only accepts a `MANAGED LOCATION` that falls inside a registered external location, which
# MAGIC is why the previous cell is a hard prerequisite for this one.

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

# MAGIC %md
# MAGIC ## Verification
# MAGIC
# MAGIC The two cells below are the real end-to-end test. `DESCRIBE` confirms the metadata is
# MAGIC correct; the file listing confirms Spark can actually *read* the storage — which is the part
# MAGIC the workspace default credential silently failed.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- credential_name should read: cred_nyctaxi_storage
# MAGIC DESCRIBE EXTERNAL LOCATION `ext-nyctaxi-landing`;

# COMMAND ----------

# Live data-plane read. If this prints the parquet files, the full chain works:
# Spark -> access connector -> system-assigned identity -> Storage Blob Data Contributor -> ADLS

files = dbutils.fs.ls("abfss://landing@stlakehousenyctx2.dfs.core.windows.net/")
for f in files:
    print(f.name, "|", round(f.size / 1024 / 1024, 2), "MB")

# Expected:
#   green_tripdata_2023-01.parquet  | 1.36 MB
#   yellow_tripdata_2023-01.parquet | 45.46 MB