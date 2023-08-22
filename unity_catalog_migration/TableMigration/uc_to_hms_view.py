# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Unity Catalog view(s) to Hive Metastore view(s)
# MAGIC
# MAGIC This notebook will migrate view(s) from the Unity Catalog catalog and Hive Metastore
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with **spark.databricks.sql.initial.catalog.name set to hive_metastore** or the base catalog where the external tables will be pulled
# MAGIC
# MAGIC **Custom sync function**
# MAGIC
# MAGIC Syncing views between two catalogs.
# MAGIC
# MAGIC **Functionality**:
# MAGIC - Replacing the target view and recreating it if:
# MAGIC   - The source and target view definitions are different
# MAGIC - Create the view if it doesn't exist
# MAGIC
# MAGIC **IMPORTANT**:
# MAGIC - The **Schema(s) has to exist with the same name in Hive Metastore**
# MAGIC - The **tables with the same name have to exist in Hive Metastore within their same schema** as in the Unity Catalog
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have the right privileges on the source UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `SELECT TABLE` / `SELECT VIEW` 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Source UC Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema(s).
# MAGIC * **`Source UC View(s)`** (optional): 
# MAGIC   - The name of the source HMS view. Multiple views should be given as follows "view_1, view_2". If filled only the given view(s) will be pulled otherwise all the views.
# MAGIC * **`Create Target HMS Schema`** (optional):
# MAGIC    - Fill with `Y` if you want to create the schema that you give in the `Target HMS Schema`.
# MAGIC * **`Target HMS Schema`** (mandatory):
# MAGIC   - The name of the target schema.
# MAGIC * **`Target HMS Schema Location`** (optional):
# MAGIC   - If `Create Target HMS Schema` is filled with `Y`. You can add the default location (managed) for the schema.
# MAGIC * **`Target HMS Schema Comment`** (optional):
# MAGIC   - If `Create Target HMS Schema` is filled with `Y`. You can add a description to your Schema.  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_catalog", "", "Source UC Catalog")
dbutils.widgets.text("source_schema", "", "Source UC Schema")
dbutils.widgets.text("source_view", "", "Source UC View")
dbutils.widgets.dropdown("create_target_schema", "N", ["N", "Y"], "Create Target HMS Schema")
dbutils.widgets.text("target_schema", "", "Target HMS Schema")
dbutils.widgets.text("target_schema_comment", "", "Target HMS Schema Comment")
dbutils.widgets.text("target_schema_location", "", "Target HMS Schema Location")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
source_view = dbutils.widgets.get("source_view")
create_target_schema = dbutils.widgets.get("create_target_schema")
target_schema_comment = dbutils.widgets.get("target_schema_comment")
target_schema =  dbutils.widgets.get("target_schema")
target_schema_location = dbutils.widgets.get("target_schema_location")
# Variables mustn't be changed
target_catalog = "hive_metastore"
table_type = "view"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_table_description, sync_view
from utils.common_utils import create_hms_schema, get_schema_detail

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
      target_schema_comment = f"Created from {source_catalog}.{source_schema} within the same location."
      
      # Create Hive Metastore target schema
      create_hms_schema(spark, target_schema, target_schema_location, target_schema_comment)
    
    # Target schema is given
    elif source_schema and target_schema:
      
      # Create Hive Metastore target schema
      create_hms_schema(spark, target_schema, target_schema_location, target_schema_comment)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore vies(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all views descriptions if the `Source HMS View(s)` parameter is empty
# MAGIC - Get the given view(s) description if the `Source HMS View(s)` is filled

# COMMAND ----------

view_descriptions = get_table_description(spark, source_catalog, source_schema, source_view, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync the Unity Catalog view(s) to Hive Metastore
# MAGIC
# MAGIC **Functionality**:
# MAGIC - Replacing the target view and recreating it if:
# MAGIC   - The source and target view definitions are different
# MAGIC - Create the view if it doesn't exist
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC - The **Schema(s) has to exist with the same name in Hive Metastore**
# MAGIC - The **tables with the same name have to exist in Hive Metastore within their same schema** as in the Unity Catalog

# COMMAND ----------

# Create empty sync status list
sync_status_list = []
# Iterate through the view descriptions
for view_details in view_descriptions:
  # Sync view
  sync_status = sync_view(spark, view_details, target_catalog)
  # If sync status code FAILED, exit notebook
  if sync_status.sync_status_code == "FAILED":
    dbutils.notebook.exit([sync_status.sync_status_code, sync_status.sync_status_description])

if sync_status.sync_status_code == "SUCCESS":
  dbutils.notebook.exit([sync_status.sync_status_code, sync_status.sync_status_description])
