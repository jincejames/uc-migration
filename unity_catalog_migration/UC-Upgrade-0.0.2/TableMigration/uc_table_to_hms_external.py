# Databricks notebook source
# MAGIC %md
# MAGIC # Create Hive Metastore External Table(s) from Unity Catalog Table(s)
# MAGIC
# MAGIC This notebook will create all external table(s) in a given schema from the Unity Catalog to Hive Metastore.
# MAGIC
# MAGIC **Before you start the migration**, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have the right privileges on the source UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `SELECT TABLE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC * **`Source Table(s) Type`** (mandatory):
# MAGIC   - Type of the source tables either MANAGED or EXTERNAL.
# MAGIC * **`Source UC Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema.
# MAGIC * **`Source UC Table(s)`** (optional): 
# MAGIC   - The name of the source UC table(s). If filled only the given external table(s) will be pulled otherwise all tables that are either **MANAGED** or **EXTERNAL** based on the given `Source UC Table Type`.
# MAGIC * **`Create Target HMS Schema`** (optional):
# MAGIC    - Fill with `Y` if you want to create the schema that you give in the `Target HMS Schema`.
# MAGIC * **`Target HMS Schema`** (mandatory):
# MAGIC   - The name of the target schema.
# MAGIC * **`Target HMS Schema Location`** (optional):
# MAGIC   - If `Create Target HMS Schema` is filled with `Y`. You can add the default location (managed) for the schema.
# MAGIC   - **Note**:
# MAGIC     - You need to have the appropriate configuration in place on cluster level to access your Storage Account.
# MAGIC * **`Target HMS Schema Comment`** (optional):
# MAGIC   - If `Create Target HMS Schema` is filled with `Y`. You can add a description to your Schema.
# MAGIC * **`Target HMS Table`** (optional):
# MAGIC   - Only applicable if the `Source Table` is filled with a **single table name**, then a name can be given for the Target UC Table. Otherwise, the `Source Table` name will be used.   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("table_type", "", "Source UC Table(s) Type")
dbutils.widgets.text("source_uc_catalog", "", "Source UC Catalog")
dbutils.widgets.text("source_uc_schema", "", "Source UC Schema")
dbutils.widgets.text("source_uc_table", "", "Source UC Table(s)")
dbutils.widgets.text("target_hms_schema", "", "Target HMS Schema")
dbutils.widgets.text("create_target_hms_schema", "", "Create Target HMS Schema")
dbutils.widgets.text("target_hms_schema_comment", "", "Target HMS Schema Comment")
dbutils.widgets.text("target_hms_schema_location", "", "Target HMS Schema Location")
dbutils.widgets.text("target_hms_table", "", "Target HMS Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

table_type = dbutils.widgets.get("table_type")
source_catalog = dbutils.widgets.get("source_uc_catalog")
source_schema = dbutils.widgets.get("source_uc_schema")
source_table = dbutils.widgets.get("source_uc_table")
create_target_schema = dbutils.widgets.get("create_target_hms_schema")
target_schema_comment = dbutils.widgets.get("target_hms_schema_comment")
target_schema = dbutils.widgets.get("target_hms_schema")
target_schema_location = dbutils.widgets.get("target_hms_schema_location")
target_table = dbutils.widgets.get("target_hms_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_table_description, create_hms_external_from_uc_table
from utils.common_utils import create_hms_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the Unity Catalog table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed or external tables descriptions if the `Source UC Table(s)`  parameter is empty
# MAGIC - Get the given managed or external table(s) description if the `Source UC Table(s)` is filled

# COMMAND ----------

tables_descriptions = get_table_description(spark, source_catalog, source_schema, source_table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create HMS Schema 
# MAGIC **Only** if the `Create Target HMS Schema` parameter is **`Y`**
# MAGIC - You can create the schema on a default location (managed location)
# MAGIC   - If `Schema Location` is filled with the right path
# MAGIC     - **Note**: If you add a location for schema, the schema location will be used when you create a table without a location in the schema.
# MAGIC   - **Prerequisite**:
# MAGIC     - You need to have the appropriate configuration in place on cluster level to access your Storage Account
# MAGIC   - (Optional) Use `Target HMS Schema Comment` to add a schema description
# MAGIC

# COMMAND ----------

if create_target_schema:
  create_hms_schema(spark, target_schema, target_schema_location, target_schema_comment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating hive_metastore External Tables from UC Tables without data movement using the CREATE TABLE LOCATION command
# MAGIC
# MAGIC Available options:
# MAGIC - Create all tables with mounted file paths from the given `Source Schema` to the given `Target Catalog` and `Target Schema`. 
# MAGIC   - Applicable if the `Source UC Table(s)` is empty.
# MAGIC - Create the external or managed table(s) from the given Unity Catalog `Source UC Catalog`.`Source Schema` and `Source Table` to the Hive Metastore and `Target HMS Schema`.
# MAGIC   - Applicable if the `Source Table` is filled.
# MAGIC   - If `Target HMS Table` is empty, the `Source UC Table(s)`'s name is given to the Unity Catalog table. Only applicable if the `Source UC Table(s)` gets a single table name.
# MAGIC
# MAGIC **Note**: Equality check between the legacy HMS table(s) and the UC table(s) will run

# COMMAND ----------

create_hms_external_from_uc_table(spark, tables_descriptions, target_schema, target_table)
