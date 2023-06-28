# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore Tables to UC Managed Tables using DEEP CLONE
# MAGIC
# MAGIC This notebook will migrate all managed or external tables (or a single) from a Hive metastore to a UC catalog.
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with spark.databricks.sql.initial.catalog.name set to hive_metastore or the base catalog where the tables will be pulled for cloning
# MAGIC
# MAGIC **Cloning:**
# MAGIC
# MAGIC You can create a copy of an existing Delta Lake table on Azure Databricks at a specific version using the clone command. Clones can be either deep or shallow. For migrating HMS tables to UC managed tables, we are using *deep clone*.
# MAGIC
# MAGIC A *deep clone* is a clone that copies the source table data to the clone target in addition to the metadata of the existing table. Additionally, stream metadata is also cloned such that a stream that writes to the Delta table can be stopped on a source table and continued on the target of a clone from where it left off.
# MAGIC
# MAGIC The metadata that is cloned includes: schema, partitioning information, invariants, nullability. For deep clones only, stream and COPY INTO metadata are also cloned. Metadata not cloned are the table description and user-defined commit metadata.
# MAGIC
# MAGIC **Note:**
# MAGIC - Deep clones do not depend on the source from which they were cloned, but are expensive to create because a deep clone copies the data as well as the metadata.
# MAGIC - Cloning with replace to a target that already has a table at that path creates a Delta log if one does not exist at that path. You can clean up any existing data by running vacuum.
# MAGIC - If an existing Delta table exists, a new commit is created that includes the new metadata and new data from the source table. This new commit is incremental, meaning that only new changes since the last clone are committed to the table.
# MAGIC - Cloning a table is not the same as Create Table As Select or CTAS. A clone copies the metadata of the source table in addition to the data. Cloning also has simpler syntax: you don’t need to specify partitioning, format, invariants, nullability and so on as they are taken from the source table.
# MAGIC - A cloned table has an independent history from its source table. Time travel queries on a cloned table will not work with the same inputs as they work on its source table.
# MAGIC
# MAGIC **Source**: https://learn.microsoft.com/en-gb/azure/databricks/delta/clone
# MAGIC
# MAGIC **Before you start the migration**, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have the right privileges on the target UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `CREATE TABLE`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_table_type", "Choose MANAGED OR EXTERNAL", "Source Table(s) Type")
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("target_table", "", "Target UC Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_table_type = dbutils.widgets.get("source_table_type")
source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_hms_table_description, clone_hms_table_to_uc_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed or external tables descriptions if the `Source Table`  parameter is empty
# MAGIC - Get a managed or external table description if the `Source Table` is filled
# MAGIC
# MAGIC

# COMMAND ----------

tables_descriptions = get_hms_table_description(spark, source_schema, source_table, source_table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrating hive_metastore Tables to UC Managed Tables with data movement using the DEEP CLONE command
# MAGIC
# MAGIC Available options:
# MAGIC - Migrate all managed or external tables from the given `Source Schema` to the given `Target Catalog` and `Target Schema`. 
# MAGIC   - Applicable if the `Source Table` is empty.
# MAGIC - Migrate a managed or external table from the given hive metastore `Source Schema` and `Source Table` to the given `Target Catalog` and `Target Schema`.
# MAGIC   - Applicable if the `Source Table` is filled.
# MAGIC   - If `Target Table` is empty, the `Source Table`'s name is given to the Unity Catalog table.
# MAGIC

# COMMAND ----------

clone_hms_table_to_uc_managed(spark, tables_descriptions, target_catalog, target_schema, target_table)
