# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore Tables outside of DBFS to UC External Tables
# MAGIC
# MAGIC This notebook will migrate external and managed table(s) outside of DBFS with mounted file path(s) in a given schema from the Hive Metastore to a UC catalog.
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with **spark.databricks.sql.initial.catalog.name set to hive_metastore** or the base catalog where the external tables will be pulled
# MAGIC - **Tables on DBFS** - this means the files reside completely within DBFS and the only way forward for these are to recreate them via CLONE (*clone_hms_table_to_uc_managed* notebook) or CTAS (*ctas_hms_table_to_uc_managed* notebook). Since if we leave the files in DBFS anybody can read the files of the table.
# MAGIC
# MAGIC **CREATE OR REPLACE TABLE LOCATION**
# MAGIC
# MAGIC **IMPORTANT**:
# MAGIC - Only supported for **Delta Lake tables**
# MAGIC
# MAGIC You can create Unity Catalog table(s) from your **External** and **Managed** HMS table(s) without any data movement. With the `CREATE OR REPLACE TABLE LOCATION` command the table metadata will be recreated in the Hive Metastore and Unity Catalog. It keeps the history of the table in HMS and UC as well. It doesn't require any data movement.
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
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Source Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema(s). Multiple schemas should be given as follows "schema_1, schema_2". If filled only the given schema(s) will be pulled otherwise all the schemas.
# MAGIC     - **Note**:
# MAGIC       - If multiple schemas are given, `Source Table(s)` have to leave empty.
# MAGIC * **`Source Table(s)`** (optional): 
# MAGIC   - The name of the source HMS table. Multiple tables should be given as follows "table_1, table_2". If filled only the given table(s) will be pulled otherwise all the tables.
# MAGIC     - Only applicable if a single `Source Schema(s)` is given.
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
# MAGIC   - Only applicable if the `Source Table(s)` is filled with a **single table name**, then a name can be given for the Target UC Table. Otherwise, the `Source Table(s)` name will be used.   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source Schema(s)")
dbutils.widgets.text("source_table", "", "Source Table(s)")
dbutils.widgets.dropdown("create_target_catalog", "N", ["N", "Y"], "Create Target UC Catalog")
dbutils.widgets.text("target_catalog_comment", "", "Target UC Catalog Comment")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_catalog_location", "", "Target UC Catalog Location")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.dropdown("create_target_schema", "N", ["N", "Y"], "Create Target UC Schema")
dbutils.widgets.text("target_schema_comment", "", "Target UC Schema Comment")
dbutils.widgets.text("target_schema_location", "", "Target UC Schema Location")
dbutils.widgets.text("target_table", "", "Target UC Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

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
# Source catalog mustn't be changed
source_catalog = "hive_metastore"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_table_description,  create_or_replace_external_table
from utils.common_utils import create_uc_catalog, create_uc_schema, get_schema_detail

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed tables descriptions if the `Source Table(s)`  parameter is empty
# MAGIC - Get the given managed table(s) description if the `Source Table(s)` is filled

# COMMAND ----------

tables_descriptions = get_table_description(spark, source_catalog, source_schema, source_table, "")

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

if create_target_catalog == "Y":
  create_uc_catalog(spark, target_catalog, target_schema, target_catalog_location, target_catalog_comment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema 
# MAGIC **Only** if the `Create Target UC Schema` parameter is **`Y`**
# MAGIC - You have the `CREATE SCHEMA` privilege on the applicable catalog
# MAGIC - You can create the schema on a default location (managed location) in Unity Catalog
# MAGIC   - You have an external location
# MAGIC   - If you have the `CREATE MANAGED STORAGE` privilege on the applicable external location
# MAGIC   - If `Target UC Schema` is not given:
# MAGIC     - Dynamically creates the schema:
# MAGIC       - If the source schema is in **DBFS**, it is being created in the default location
# MAGIC       - If the source schema is in a **mount point**, it is being created in the abfss path of the mount point
# MAGIC       - if the source schema is in **abfss path**, it is being created in that path.
# MAGIC     - If the `Target UC Schema` is given, it is created with that name.
# MAGIC       - If `Target UC Schema Location` is filled with the right path, it is being created in that location.
# MAGIC       - Otherwise, it is being created in the default location.
# MAGIC     - **Note**: If you add a location for catalog and schema either, the schema location will be used.
# MAGIC   - (Optional) Use `Target UC Schema Comment` to add a schema description
# MAGIC     - Only applicable if a single source schema is added as `Source Schema(s)`. 
# MAGIC

# COMMAND ----------

if create_target_schema == "Y":

    # If there is no target schema
    if source_schema and not target_schema and not target_schema_location:
      
      # Get the schema details
      schema_detail = get_schema_detail(spark, dbutils, source_schema)
      
      # Set target schema variables
      target_schema = getattr(schema_detail, "database")
      target_schema_location = getattr(schema_detail, "external_location")+f"/{target_schema}"
      target_schema_comment = f"Migrated from hive_metastore.{source_schema} within the same location."
      
      # Create Unity Catalog target schema
      create_hms_schema(spark, target_catalog, target_schema, target_schema_location, target_schema_comment)
    
    # Target schema is given
    elif source_schema and target_schema:
      
      # Create Unity Catalog target schema
      create_hms_schema(spark, target_schema, target_schema_location, target_schema_comment)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrating Hive Metastore Tables outside of DBFS to UC External Tables without data movement using the CREATE OR REPLACE TABLE LOCATION command
# MAGIC
# MAGIC Available options:
# MAGIC - Create all Delta tables outside of DBFS from the given `Source Schema` to the given `Target UC Catalog`.
# MAGIC   -  and `Target Schema` if given
# MAGIC   - or `Source Schema` is used as target schema.
# MAGIC   - Applicable if the `Source Table(s)` is empty.
# MAGIC - Create the external or managed table(s) from the given Hive Metastore `Source Schema` and `Source Table` to the `Target UC Catalog` and `Target UC Schema`.
# MAGIC   - Applicable if the `Source Table` is filled.
# MAGIC   - If `Target HMS Table` is empty, the `Source UC Table(s)`'s name is given to the Unity Catalog table. Only applicable if the `Source UC Table(s)` gets a single table name.
# MAGIC
# MAGIC **Note**: Equality check between the legacy HMS table(s) and the UC table(s) will run

# COMMAND ----------

# Create schema list from a string list
if source_schema.find(",") and not target_schema:
  # The input is a list of schemas in a string and no target schema
  schema_list = source_schema.split(", ")
elif source_schema and not target_schema:
  # The input is a single schema and no target schema
  schema_list = source_schema
elif target_schema:
  # The input is a single schema in the target schema
  schema_list = target_schema

# Iterate through the list of schemas
for schema in schema_list:

  create_or_replace_external_table(spark, dbutils, tables_descriptions, target_catalog, schema, target_table)
