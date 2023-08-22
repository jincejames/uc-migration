# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Hive Metastore View(s) to UC view(s)
# MAGIC
# MAGIC This notebook will migrate view(s) from the Hive Metastore to a UC catalog.
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
# MAGIC - Currently, it is **only migrating views to a single target catalog**
# MAGIC - The **Schema(s) has to exist with the same name in Unity Catalog**
# MAGIC - The **tables with the same name have to exist in Unity Catalog within their same schema** as in the Hive Metastore
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have migrated all the required schema(s) with the same name(s)
# MAGIC - You have migrated all the required table(s) with the same name(s) for the view(s)
# MAGIC - You have the right privileges on the UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `CREATE TABLE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Source HMS Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema(s).
# MAGIC * **`Source HMS View(s)`** (optional): 
# MAGIC   - The name of the source HMS view. Multiple views should be given as follows "view_1, view_2". If filled only the given view(s) will be pulled otherwise all the views.
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
# MAGIC * **`Target UC Catalog Comment`** (optional):
# MAGIC   - If `Create Target UC Catalog` is filled with `Y`. You can add a description to your catalog.
# MAGIC * **`Create Target UC Schema`** (optional):
# MAGIC    - Fill with `Y` if you want to create the schema that you give in the `Target UC Schema`.
# MAGIC   - Prerequisite:
# MAGIC     - `CREATE SCHEMA` privilege on the `Target UC Catalog`.
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source HMS Schema")
dbutils.widgets.text("source_view", "", "Source HMS View")
dbutils.widgets.dropdown("create_target_catalog", "N", ["N", "Y"], "Create Target UC Catalog")
dbutils.widgets.text("target_catalog_comment", "", "Target UC Catalog Comment")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_catalog_location", "", "Target UC Catalog Location")
dbutils.widgets.dropdown("create_target_schema", "N", ["N", "Y"], "Create Target UC Schema")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("target_schema_comment", "", "Target UC Schema Comment")
dbutils.widgets.text("target_schema_location", "", "Target UC Schema Location")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_view = dbutils.widgets.get("source_view")
create_target_catalog = dbutils.widgets.get("create_target_catalog")
target_catalog_comment = dbutils.widgets.get("target_catalog_comment")
target_catalog = dbutils.widgets.get("target_catalog")
target_catalog_location = dbutils.widgets.get("target_catalog_location")
create_target_schema = dbutils.widgets.get("create_target_schema")
target_schema_comment = dbutils.widgets.get("target_schema_comment")
target_schema =  dbutils.widgets.get("target_schema")
target_schema_location = dbutils.widgets.get("target_schema_location")
# Variables mustn't be changed
source_catalog = "hive_metastore"
table_type = "view"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_table_description, sync_view
from utils.common_utils import create_uc_catalog, create_uc_schema, get_schema_detail

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
      create_uc_schema(spark, target_catalog, target_schema, target_schema_location, target_schema_comment)
    
    # Target schema is given
    elif source_schema and target_schema:
      
      # Create Unity Catalog target schema
      create_uc_schema(spark, target_catalog, target_schema, target_schema_location, target_schema_comment)
    

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

view_descriptions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync the Hive Metastore view(s) to Unity Catalog
# MAGIC
# MAGIC **Functionality**:
# MAGIC - Replacing the target view and recreating it if:
# MAGIC   - The source and target view definitions are different
# MAGIC - Create the view if it doesn't exist
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC - The **Schema(s) has to exist with the same name in Unity Catalog**
# MAGIC - The **tables with the same name have to exist in Unity Catalog within their same schema** as in the Hive Metastore

# COMMAND ----------

# Create empty sync status list
sync_status_list = []
# Iterate through the view descriptions
for view_details in view_descriptions:
  # Sync view
  sync_status = sync_view(spark, view_details, target_catalog)
  print(sync_status)
  # Append sync status list
  sync_status_list.append([sync_status.source_object_type, sync_status.source_object_full_name, sync_status.target_object_full_name, sync_status.sync_status_code, sync_status.sync_status_description])
  # If sync status code FAILED, exit notebook
  if sync_status.sync_status_code == "FAILED":
    dbutils.notebook.exit(sync_status_list)

# Exit notebook
dbutils.notebook.exit(sync_status_list)
