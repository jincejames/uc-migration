# Databricks notebook source
# MAGIC %md
# MAGIC ### Set widgets and get their values

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("target_table", "", "Target UC Table")

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")
table_type = "MANAGED"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import modules

# COMMAND ----------

from utils.table_utils import (get_hms_table_description, clone_hms_managed_table_to_uc_managed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the tables' descriptions of the schema

# COMMAND ----------

managed_tables_descriptions = get_hms_table_description(spark, source_schema, source_table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Migrate Managed Tables to UC as Managed Tables and check equality

# COMMAND ----------

clone_hms_managed_table_to_uc_managed(spark, mounted_tables_descriptions, target_catalog, target_schema, target_table)