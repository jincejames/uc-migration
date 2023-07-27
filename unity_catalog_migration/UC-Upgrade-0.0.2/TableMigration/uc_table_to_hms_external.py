# Databricks notebook source
# MAGIC %md
# MAGIC # Create Hive Metastore External Table(s) from Unity Catalog Table(s) Using CREATE OR REPLACE TABLE LOCATION
# MAGIC
# MAGIC This notebook will create External table(s) in a given schema from the Unity Catalog to Hive Metastore.
# MAGIC
# MAGIC **CREATE OR REPLACE TABLE LOCATION**
# MAGIC
# MAGIC **IMPORTANT**:
# MAGIC - Only supported for **Delta Lake tables**
# MAGIC
# MAGIC You can create Unity Catalog table(s) from your **External** and **Managed** HMS table(s) without any data movement. With the `CREATE OR REPLACE TABLE LOCATION` command the table metadata will be recreated in the Hive Metastore and Unity Catalog. It keeps the history of the table if it is **Delta**. It doesn't require any data movement.
# MAGIC
# MAGIC **Before you start the migration**, please double-check the followings:
# MAGIC
# MAGIC - Create an [Azure Service Principal](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/service-principals#step-1-create-an-azure-service-principal-in-your-azure-account) and grant Storage Blob Data Contributor Role
# MAGIC   - Use [Secret Scope](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes)
# MAGIC - [Azure credentials](https://learn.microsoft.com/en-gb/azure/databricks/storage/azure-storage#connect-to-azure-data-lake-storage-gen2-or-blob-storage-using-azure-credentials) should be set in cluster-level
# MAGIC - Check out the notebook logic
# MAGIC - You have the right privileges on the source UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `SELECT TABLE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC * **`Source UC Schema`** (mandatory): 
# MAGIC   -  The name of the source HMS schema.
# MAGIC * **`Source UC Table(s)`** (optional): 
# MAGIC   - The name of the source HMS table. Multiple tables should be given as follows "table_1, table_2". If filled only the given table(s) will be pulled otherwise all the tables.
# MAGIC * **`Create Target HMS Schema`** (optional):
# MAGIC    - Fill with `Y` if you want to create the schema that you give in the `Target HMS Schema`.
# MAGIC * **`Target HMS Schema`** (optional):
# MAGIC   - The name of the target schema. If not given, the `Source UC Schema` is used.
# MAGIC * **`Target HMS Schema Location`** (optional):
# MAGIC   - If `Create Target HMS Schema` is filled with `Y`. You can add the default location (managed) for the schema.
# MAGIC   - **Note**:
# MAGIC     - You need to have the appropriate configuration in place on cluster level to access your Storage Account.
# MAGIC * **`Target HMS Schema Comment`** (optional):
# MAGIC   - If `Create Target HMS Schema` is filled with `Y`. You can add a description to your Schema.
# MAGIC * **`Target HMS Table`** (optional):
# MAGIC   - Only applicable if the `Source UC Table(s)` is filled with a **single table name**, then a name can be given for the Target UC Table. Otherwise, the `Source UC Table(s)` name will be used.   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_uc_catalog", "", "Source UC Catalog")
dbutils.widgets.text("source_uc_schema", "", "Source UC Schema")
dbutils.widgets.text("source_uc_table", "", "Source UC Table(s)")
dbutils.widgets.text("target_hms_schema", "", "Target HMS Schema")
dbutils.widgets.dropdown("create_target_hms_schema", "N", ["N", "Y"], "Create Target HMS Schema")
dbutils.widgets.text("target_hms_schema_comment", "", "Target HMS Schema Comment")
dbutils.widgets.text("target_hms_schema_location", "", "Target HMS Schema Location")
dbutils.widgets.text("target_hms_table", "", "Target HMS Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_uc_catalog")
source_schema = dbutils.widgets.get("source_uc_schema")
source_table = dbutils.widgets.get("source_uc_table")
create_target_schema = dbutils.widgets.get("create_target_hms_schema")
target_schema_comment = dbutils.widgets.get("target_hms_schema_comment")
target_schema = dbutils.widgets.get("target_hms_schema")
target_schema_location = dbutils.widgets.get("target_hms_schema_location")
target_table = dbutils.widgets.get("target_hms_table")
# Mustn't change the target catalog variable
target_catalog = "hive_metastore"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_table_description, create_or_replace_external_table
from utils.common_utils import create_hms_schema, get_schema_detail

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the Unity Catalog table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed or external tables descriptions if the `Source UC Table(s)`  parameter is empty
# MAGIC - Get the given managed or external table(s) description if the `Source UC Table(s)` is filled

# COMMAND ----------

tables_descriptions = get_table_description(spark, source_catalog, source_schema, source_table, "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema 
# MAGIC **Only** if the `Create Target HMS Schema` parameter is **`Y`**
# MAGIC - To create a schema in Hive Metastore, you just need to have access to it.
# MAGIC   - If `Target HMS Schema` and `Target UC Schema Location` are not given:
# MAGIC     - Dynamically creates the schema in the location of the `Source UC Schema`'s location and adds the name of the `Source UC Schema` as a subfolder.
# MAGIC   - If `Target HMS Schema` is given:
# MAGIC       - If `Target HMS Schema Location` is filled with the right path, it is being created in that location.
# MAGIC       - Otherwise, it is being created in the default location.
# MAGIC     - **Note**: If you add a location for catalog and schema either, the schema location will be used.
# MAGIC   - (Optional) Use `Target HMS Schema Comment` to add a schema description
# MAGIC     - Only applicable if the `Target HMS Schema` is given.
# MAGIC

# COMMAND ----------

if create_target_schema == "Y":

    # If there is no target schema
    if source_schema and not target_schema and not target_schema_location:
      
      # Get the schema details
      schema_detail = get_schema_detail(spark, dbutils, source_schema)
      
      # Set target schema variables
      target_schema = getattr(schema_detail, "database")
      target_schema_location = getattr(schema_detail, "external_location")+f"/{source_schema}"
      target_schema_comment = f"Migrated from hive_metastore.{source_schema} within the same location."
      
      # Create Unity Catalog target schema
      create_hms_schema(spark, target_catalog, target_schema, target_schema_location, target_schema_comment)
    
    # Target schema is given
    elif source_schema and target_schema:
      
      # Create Unity Catalog target schema
      create_hms_schema(spark, target_schema, target_schema_location, target_schema_comment)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating hive_metastore External Tables from UC Tables without data movement using the CREATE OR REPLACE TABLE LOCATION command
# MAGIC
# MAGIC Available options:
# MAGIC - Create all Delta tables outside of DBFS from the given `Source Schema` to the given `Target Catalog`.
# MAGIC   -  and `Target Schema` if given
# MAGIC   - or `Source Schema` is used as target schema.
# MAGIC   - Applicable if the `Source UC Table(s)` is empty.
# MAGIC - Create the external or managed table(s) from the given Unity Catalog `Source UC Catalog`.`Source Schema` and `Source Table` to the Hive Metastore and `Target HMS Schema`.
# MAGIC   - Applicable if the `Source Table` is filled.
# MAGIC   - If `Target HMS Table` is empty, the `Source UC Table(s)`'s name is given to the Unity Catalog table. Only applicable if the `Source UC Table(s)` gets a single table name.
# MAGIC
# MAGIC **Note**: Equality check between the legacy HMS table(s) and the UC table(s) will run

# COMMAND ----------

create_or_replace_external_table(spark, dbutils, tables_descriptions, target_catalog, target_schema, target_table)
