# Databricks notebook source
# MAGIC %md
# MAGIC # Migrate Parquet data with CONVERT TO DELTA
# MAGIC
# MAGIC The CONVERT TO DELTA SQL command performs a one-time conversion for Parquet and Iceberg tables to Delta Lake tables.
# MAGIC
# MAGIC **Note**
# MAGIC - For Databricks Runtime 11.2 and above, CONVERT TO DELTA automatically infers partitioning information for tables registered to the metastore, eliminating the requirement to manually specify partitions.
# MAGIC
# MAGIC **Supports**:
# MAGIC   - Parquet format
# MAGIC   - Parquet as Hive format
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC   - Check out the notebook logic
# MAGIC   - If you are running this notebook for a Unity Catalog table
# MAGIC     - You have the right privileges on the UC catalog and schema securable objects
# MAGIC       - `USE CATALOG`
# MAGIC       - `USE SCHEMA`
# MAGIC       - `MODIFY TABLE`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema(s). 
# MAGIC * **`Table(s)`** (optional): 
# MAGIC   - The name of the source HMS table. Multiple tables should be given as follows "table_1, table_2". If filled only the given table(s) will be pulled otherwise all the tables.
# MAGIC * **`Catalog`** (mandatory):
# MAGIC   - The name of the catalog.   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("table", "", "Table(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
# Variable mustn't be changed
table_type = "table"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_table_description, convert_parquet_table_to_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all tables descriptions if the `Table(s)` parameter is empty
# MAGIC - Get the given managed table(s) description if the `Table(s)` is filled

# COMMAND ----------

tables_descriptions = get_table_description(spark, catalog, schema, table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converting Parquet to Delta

# COMMAND ----------

# Create empty convert status list
convert_status_list = []
# Iterate through table descriptions
for table_details in tables_descriptions:
    # Convert
    convert_status = convert_parquet_table_to_delta(spark, dbutils, table_details)
    # Append convert status list
    convert_status_list.append([convert_status.full_name, convert_status.status_code, convert_status.status_description])
    # If status code FAILED, exit
    if convert_status.status_code == "FAILED":
      dbutils.notebook.exit(convert_status_list)
if convert_status.status_code == "SUCCESS":
  dbutils.notebook.exit(convert_status_list)
