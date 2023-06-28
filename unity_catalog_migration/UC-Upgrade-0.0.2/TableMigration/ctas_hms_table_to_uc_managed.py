# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore Tables to UC Managed Tables using CTAS (CREATE TABLE AS SELECT)
# MAGIC
# MAGIC This notebook will migrate all managed or external tables (or a single) from a Hive metastore to a UC catalog.
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with spark.databricks.sql.initial.catalog.name set to hive_metastore or the base catalog where the tables will be pulled
# MAGIC
# MAGIC **CTAS (CREATE TABLE AS SELECT)**
# MAGIC - Populate a new table with records from the existing table based on the *SELECT STATEMENT*.
# MAGIC - It involves data movement.
# MAGIC
# MAGIC **Note**:
# MAGIC - Doesn't copy the metadata of the source table in addition to the data. For that use the *clone_hms_to_uc_managed* notebook.
# MAGIC - Need to specify partitioning, format, invariants, nullability, and so on as they are not taken from the source table.
# MAGIC
# MAGIC **Before you start the migration**, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have the right privileges on the target UC catalog and schema securable objects
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
dbutils.widgets.text("source_table_type", "Choose MANAGED OR EXTERNAL", "Source Table(s) Type")
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("target_table", "", "Target UC Table")
dbutils.widgets.text("select_statement", "", "SELECT Statement")
dbutils.widgets.text("partition_clause", "", "PARTITION BY Clause")
dbutils.widgets.text("options_clause", "", "OPTIONS Clause")

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
select_statement = dbutils.widgets.get("select_statement")
partition_clause = dbutils.widgets.get("partition_clause")
options_clause = dbutils.widgets.get("options_clause")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_hms_table_description, ctas_hms_table_to_uc_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed tables descriptions if the `Source Table`  parameter is empty
# MAGIC - Get a managed table description if the `Source Table` is filled

# COMMAND ----------

tables_descriptions = get_hms_table_description(spark, source_schema, source_table, source_table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Migrate hive_metastore Managed Tables to UC Managed Tables with data movement using the CTAS command
# MAGIC
# MAGIC Available options:
# MAGIC - Migrate **all** managed tables from the given `Source Schema` to the given `Target Catalog` and `Target Schema`. 
# MAGIC   - Applicable if the `Source Table` is empty.
# MAGIC - Migrate **single** managed table from the given hive metastore `Source Schema` and `Source Table` to the given `Target Catalog` and `Target Schema`.
# MAGIC   - Applicable if the `Source Table` is filled.
# MAGIC   - If `Target Table` is empty, the `Source Table`'s name is given to the Unity Catalog table.
# MAGIC   - Available CTAS parameters:
# MAGIC     - `SELECT Statement` SELECT and FROM syntax not needed
# MAGIC     - `PARTITION BY clause` column names separated by comma (PARTITION BY syntax not needed)
# MAGIC     - `OPTIONS` including TBLPROPRETIES and COMMENT (OPTIONS syntax not needed)

# COMMAND ----------

ctas_hms_table_to_uc_managed(spark, 
                            tables_descriptions, 
                            target_catalog, 
                            target_schema, 
                            target_table, 
                            select_statement, 
                            partition_clause, 
                            options_clause
                            )
