# Databricks notebook source
# MAGIC %md
# MAGIC ### Configure Mount Point
# MAGIC **Prerequisites**
# MAGIC - Azure App registration - Service Principal
# MAGIC   - Blob Storage Contributor Role
# MAGIC - Create Databricks secret for the access key

# COMMAND ----------

container_name = "hms-schema-managed"
storage_account_name = "stbalazsdev01"
scope_name = "balazs_test_mount_migration"
key_name = "appregbalazsdev01_secret"
application_id = "b958dbf4-1f97-4ef9-b339-e052591f41f5"
directory_id = "874cd0d6-f21a-4c6e-8239-51287476f635"
mount_name = f"/mnt/{container_name}"

# COMMAND ----------

##Unmount blob storage
#dbutils.fs.unmount("/mnt/unity_test")

# COMMAND ----------

if any(mount.mountPoint == f"{mount_name}" for mount in dbutils.fs.mounts()):
  print("Mount point already mounted")
else: 
  # Mount Blob Storage
  configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{application_id}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=f"{scope_name}",
                                                                       key=f"{key_name}"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"
          }

  # Optionally, you can add <directory-name> to the source URI of your mount point.
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"{mount_name}",
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/hms-schema-managed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Demo Environment

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Managed HMS Database on mount point

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS hms_schema_managed CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE hms_schema_managed LOCATION 'dbfs:/mnt/hms-schema-managed/db'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create HMS Managed Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hms_schema_managed.managed_table AS
# MAGIC SELECT * FROM samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %md
# MAGIC ###

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create HMS External table

# COMMAND ----------

spark.read.table("samples.nyctaxi.trips").write.format("delta").save("abfss://unity-test@stbalazsdev01.dfs.core.windows.net/delta/nyctaxi/trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access to the ADLS w/o mount point
# MAGIC **Prerequisites**
# MAGIC - Azure App registration - Service Principal
# MAGIC   - Blob Storage Contributor Role
# MAGIC - Create Databricks secret for the access key
# MAGIC
# MAGIC **NOTE**: If you want to access the given location from other notebook, you should add as Spark Config for you cluster

# COMMAND ----------

container_name = "ext-test"
storage_account_name = "stbalazsdev01"
scope_name = "balazs_test_mount_migration"
key_name = "appregbalazsdev01_secret"
application_id = "b958dbf4-1f97-4ef9-b339-e052591f41f5"
directory_id = "874cd0d6-f21a-4c6e-8239-51287476f635"

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", 
  "OAuth")
spark.conf.set(
  f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", 
  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(
  f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", 
  f"{application_id}")
spark.conf.set(
  f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", 
  dbutils.secrets.get(scope=f"{scope_name}",key=f"{key_name}"))
spark.conf.set(
  f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", 
  f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.your_sync_schema.your_external_table_w_adls_loc_tpch_customer
# MAGIC LOCATION "abfss://unity-test@stbalazsdev01.dfs.core.windows.net/delta/nyctaxi/trips"
# MAGIC AS
# MAGIC select * from samples.tpch.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.your_sync_schema.your_external_table_w_adls_loc_tpch_nation
# MAGIC LOCATION "abfss://unity-test@stbalazsdev01.dfs.core.windows.net/delta/tpch/nation"
# MAGIC AS
# MAGIC select * from samples.tpch.nation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.your_sync_schema.your_external_table_w_mount_loc_nyctaxi_trips USING DELTA LOCATION "/mnt/unity-test/delta/nyctaxi/trips/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hive_metastore.your_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.your_schema.your_external_table USING JSON LOCATION "/mnt/contbalazsdev01_blob/world-cup-test/WorldCup-2022-04-28_03_44_30.json";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear dev catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS balazs_dev.bronze.your_external_table_w_mount_loc_tpch_supplier_partition

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS balazs_dev.bronze.your_external_table_w_mount_loc_tpch_supplier_partition_cloned

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS balazs_dev.ext_test.your_external_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS balazs_dev.gold.your_external_table_w_mount_loc_tpch_supplier

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS balazs_dev.managed_test.managed_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS balazs_dev.sync CASCADE
