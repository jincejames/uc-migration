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
# MAGIC ## Widget parameters
# MAGIC * **`Source Table(s) Type`** (mandatory):
# MAGIC   - Type of the source tables either MANAGED or EXTERNAL.
# MAGIC * **`Source Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema.
# MAGIC * **`Source Table`** (optional): 
# MAGIC   - The name of the source HMS table. If filled only the given table will be pulled otherwise all the tables based on the `Source Table(s) Type`.
# MAGIC * **`Create Target UC Catalog`** (optional): 
# MAGIC   - Fill with `Y` if you want to create the catalog that you give in the `Target UC Catalog`.
# MAGIC   - Prerequisite:
# MAGIC     - `CREATE CATALOG` privilege.
# MAGIC * **`Target UC Catalog`** (mandatory):
# MAGIC   - The name of the target catalog.
# MAGIC * **`Target UC Catalog Location`** (optional):
# MAGIC   - If `Create Target UC Catalog` is filled with `Y`. You can add the a default location (managed) for the catalog.
# MAGIC   - Prerequisite:
# MAGIC     - `CREATE MANAGED STORAGE` privilege on the external location
# MAGIC * **`Create Target UC Schema`** (optional):
# MAGIC    - Fill with `Y` if you want to create the schema that you give in the `Target UC Schema`.
# MAGIC   - Prerequisite:
# MAGIC     - `CREATE SCHEMA` privilege on the `Target UC Catalog`.
# MAGIC * **`Target UC Catalog Comment`** (optional):
# MAGIC   - If `Create Target UC Catalog` is filled with `Y`. You can add a description to your catalog.
# MAGIC * **`Target UC Schema`** (mandatory):
# MAGIC   - The name of the target schema.
# MAGIC * **`Target UC Schema Location`** (optional):
# MAGIC   - If `Create Target UC Schema` is filled with `Y`. You can add the a default location (managed) for the schema.
# MAGIC   - **Note**:
# MAGIC     - If you add location to the Create Catalog and the Create Schema at the same time, the schema's managed location will be used.
# MAGIC   - Prerequisite:
# MAGIC     - `CREATE MANAGED STORAGE` privilege on the external location
# MAGIC * **`Target UC Schema Comment`** (optional):
# MAGIC   - If `Create Target UC Schema` is filled with `Y`. You can add a description to your Schema.
# MAGIC * **`Target UC Table`** (optional):
# MAGIC   - Only applicable if the `Source Table` is filled, then a name can be given for the Target UC Table. Otherwise, the `Source Table` name will be used.
# MAGIC * **`SELECT Statement`** (optional):
# MAGIC   - User-defined column list with or without transformations (SELECT and FROM syntax not needed)
# MAGIC * **`PARTITION BY Clause`** (optional):
# MAGIC   - Column names separated by comma (PARTITION BY syntax not needed)
# MAGIC * **`OPTIONS Clause`** (optional):
# MAGIC   - Iincluding TBLPROPRETIES and COMMENT (OPTIONS syntax not needed)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_table_type", "Choose MANAGED OR EXTERNAL", "Source Table(s) Type")
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("create_target_catalog", "", "Create Target UC Catalog")
dbutils.widgets.text("target_catalog_comment", "", "Target UC Catalog Comment")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_catalog_location", "", "Target UC Catalog Location")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("create_target_schema", "", "Create Target UC Schema")
dbutils.widgets.text("target_schema_comment", "", "Target UC Schema Comment")
dbutils.widgets.text("target_schema_location", "", "Target UC Schema Location")
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
create_target_catalog = dbutils.widgets.get("create_target_catalog")
target_catalog_comment = dbutils.widgets.get("target_catalog_comment")
target_catalog = dbutils.widgets.get("target_catalog")
target_catalog_location = dbutils.widgets.get("target_catalog_location")
create_target_schema = dbutils.widgets.get("create_target_schema")
target_schema_comment = dbutils.widgets.get("target_schema_comment")
target_schema = dbutils.widgets.get("target_schema")
target_schema_location = dbutils.widgets.get("target_schema_location")
target_table = dbutils.widgets.get("target_table")
select_statement = dbutils.widgets.get("select_statement")
partition_clause = dbutils.widgets.get("partition_clause")
options_clause = dbutils.widgets.get("options_clause")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_hms_table_description, ctas_hms_table_to_uc_managed
from utils.common_utils import create_uc_catalog, create_uc_schema

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
# MAGIC ## Create Catalog 
# MAGIC **Only** if the `Create Target UC Catalog` parameter is **`Y`**
# MAGIC - You have the `CREATE CATALOG` privilege
# MAGIC - You can create the catalog on a default location (managed location)
# MAGIC   - If `Target UC Catalog Location` is filled with the right path 
# MAGIC   - You have an external location
# MAGIC   - If you have `CREATE MANAGED STORAGE` privilege on the external location
# MAGIC   - (Optional) Use `Target UC Catalog Comment` to add a catalog description

# COMMAND ----------

if create_target_catalog:
  create_uc_catalog(spark, target_catalog, target_schema, target_catalog_location, target_catalog_comment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema 
# MAGIC **Only** if the `Create Schema` parameter is **`Y`**
# MAGIC - You have the `CREATE SCHEMA` privilege on the applicable catalog
# MAGIC - You can create the schema on a default location (managed location)
# MAGIC   - If `Schema Location` is filled with the right path
# MAGIC     - **Note**: If you add a location for catalog and schema either, the schema location will be used.
# MAGIC   - You have an external location
# MAGIC   - If you have the `CREATE MANAGED STORAGE` privilege on the applicable external location
# MAGIC   - (Optional) Use `Target UC Schema Comment` to add a schema description
# MAGIC

# COMMAND ----------

if create_target_schema:
  create_uc_schema(spark, target_catalog, target_schema, target_schema_location, target_schema_comment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrate hive_metastore Managed Tables to UC Managed Tables with data movement using the CTAS command
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
