# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore External Tables outside of DBFS with mounted file paths to UC External Tables
# MAGIC
# MAGIC This notebook will migrate all external tables (or a single) outside of DBFS with mounted file paths in a given schema from the Hive metastore to a UC catalog.
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with **spark.databricks.sql.initial.catalog.name set to hive_metastore** or the base catalog where the external tables will be pulled
# MAGIC - **External Tables on DBFS** - this means the files reside completely within DBFS and the only way forward for these are to recreate them via CLONE (*clone_hms_table_to_uc_managed* notebook) or CTAS (*ctas_hms_table_to_uc_managed* notebook). Since if we leave the files in DBFS anybody can read the files of the table.
# MAGIC
# MAGIC **CREATE TABLE LIKE COPY LOCATION**
# MAGIC
# MAGIC You can create Unity Catalog table(s) from your **External** HMS table(s) without any data movement. With the `CRAETE TABLE LIKE COPY LOCATION` command the location of the HMS table will be copied over as metadata to the Unity Catalog table. Data stays as is.
# MAGIC
# MAGIC **Migration away from Mounts points**
# MAGIC
# MAGIC There is no support for mount points with Unity Catalog. Existing mount points should be upgraded to External Locations.
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have external location(s) inplace for the table(s)' mounted file path(s) that you want to migrate.
# MAGIC - You have `CREATE EXTERNAL TABLE` privileges on the external location(s)
# MAGIC - You have the right privileges on the UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `CREATE TABLE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("target_table", "", "Target UC Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")
# Table type mustn't be changed
table_type = "EXTERNAL"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import (get_hms_table_description, get_mounted_tables_dict,check_mountpoint_existance_as_externallocation, migrate_hms_external_table_to_uc_external)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed tables descriptions if the `Source Table`  parameter is empty
# MAGIC - Get a managed table description if the `Source Table` is filled

# COMMAND ----------

external_tables_descriptions = get_hms_table_description(spark, source_schema, source_table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter for table(s) with mounted file path(s) only

# COMMAND ----------

mounted_tables_descriptions = get_mounted_tables_dict(external_tables_descriptions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking whether the mounted table(s)' mount file path(s) exist as external location(s)
# MAGIC
# MAGIC If a table's mount file path doesn't exist in an external location path, an error will be thrown with the missing path.
# MAGIC
# MAGIC **Note**: Before run, please double check your mounts and external locations

# COMMAND ----------

check_mountpoint_existance_as_externallocation(spark, dbutils, mounted_tables_descriptions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrating hive_metastore External Tables to UC External Tables without data movement using the CREATE TABLE LIKE COPY LOCATION command
# MAGIC
# MAGIC Available options:
# MAGIC - Migrate all external tables with mounted file paths from the given `Source Schema` to the given `Target Catalog` and `Target Schema`. 
# MAGIC   - Applicable if the `Source Table` is empty.
# MAGIC - Migrate an external table with mounted file path from the given hive metastore `Source Schema` and `Source Table` to the given `Target Catalog` and `Target Schema`.
# MAGIC   - Applicable if the `Source Table` is filled.
# MAGIC   - If `Target Table` is empty, the `Source Table`'s name is given to the Unity Catalog table.
# MAGIC
# MAGIC **Note**: Equality check between the legacy HMS table(s) and the UC table(s) will run

# COMMAND ----------

migrate_hms_external_table_to_uc_external(spark, mounted_tables_descriptions, target_catalog, target_schema, target_table)
