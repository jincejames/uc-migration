# Databricks notebook source
# MAGIC %md
# MAGIC ### Set widgets and get their values

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import modules

# COMMAND ----------

from utils.table_utils import (sync_hms_external_table_to_uc_external)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the SYNC TABLE command to upgrade individual HMS external table (source_table) to external table in Unity Catalog.

# COMMAND ----------

sync_hms_external_table_to_uc_external(spark, source_schema, source_table, target_catalog, target_schema)