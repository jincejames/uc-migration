# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore External Tables outside of DBFS with mounted file paths to UC External Tables
# MAGIC
# MAGIC This notebook will migrate external table(s) outside of DBFS with mounted file path(s) in a given schema from the Hive Metastore to a UC catalog.
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with **spark.databricks.sql.initial.catalog.name set to hive_metastore** or the base catalog where the external tables will be pulled
# MAGIC - **External Tables on DBFS** - this means the files reside completely within DBFS and the only way forward for these are to recreate them via CLONE (*clone_hms_table_to_uc_managed* notebook) or CTAS (*ctas_hms_table_to_uc_managed* notebook). Since if we leave the files in DBFS anybody can read the files of the table.
# MAGIC
# MAGIC **CREATE TABLE LIKE COPY LOCATION**
# MAGIC
# MAGIC You can create Unity Catalog table(s) from your **External** HMS table(s) without any data movement. With the `CRAETE TABLE LIKE COPY LOCATION` command the location of the HMS table will be copied over as metadata to the Unity Catalog table. Data stays as is.
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
# MAGIC   - The name of the source HMS schema.
# MAGIC * **`Source Table(s)`** (optional): 
# MAGIC   - The name of the source HMS table. Multiple tables should be given as follows "table_1, table_2". If filled only the given external table(s) will be pulled otherwise all external tables.
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
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table(s)")
dbutils.widgets.text("create_target_catalog", "", "Create Target UC Catalog")
dbutils.widgets.text("target_catalog_comment", "", "Target UC Catalog Comment")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_catalog_location", "", "Target UC Catalog Location")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("create_target_schema", "", "Create Target UC Schema")
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
# Table type mustn't be changed
table_type = "EXTERNAL"
# Source catalog mustn't be changed
source_catalog = "hive_metastore"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import (get_table_description, get_mounted_tables_dict,check_mountpoint_existance_as_externallocation, migrate_hms_external_table_to_uc_external)
from utils.common_utils import create_uc_catalog, create_uc_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed tables descriptions if the `Source Table(s)`  parameter is empty
# MAGIC - Get the given managed table(s) description if the `Source Table(s)` is filled

# COMMAND ----------

external_tables_descriptions = get_table_description(spark, source_catalog, source_schema, source_table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter for table(s) with mounted file path(s) only

# COMMAND ----------

mounted_tables_descriptions = get_mounted_tables_dict(dbutils, external_tables_descriptions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking whether the mounted table(s)' mount file path(s) exist as external location(s)
# MAGIC
# MAGIC If a table's mount file path doesn't exist in an external location path, an error will be thrown with the missing path.
# MAGIC
# MAGIC **Note**: Before run, please double check your mounts and external locations

# COMMAND ----------

check_mountpoint_existance_as_externallocation(spark, dbutils, mounted_tables_descriptions)

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
# MAGIC ## Migrating hive_metastore External Tables to UC External Tables without data movement using the CREATE TABLE LIKE COPY LOCATION command
# MAGIC
# MAGIC Available options:
# MAGIC - Migrate all external tables with mounted file paths from the given `Source Schema` to the given `Target Catalog` and `Target Schema`. 
# MAGIC   - Applicable if the `Source Table(s)` is empty.
# MAGIC - Migrate external table(s) with mounted file path(s) from the given Hive Metastore `Source Schema` and `Source Table(s)` to the given `Target Catalog` and `Target Schema`.
# MAGIC   - Applicable if the `Source Table(s)` is filled.
# MAGIC   - If `Target Table` is empty, the `Source Table(s)`'s name is given to the Unity Catalog table. Only applicable, if a single source table name is given.
# MAGIC
# MAGIC **Note**: Equality check between the legacy HMS table(s) and the UC table(s) will run

# COMMAND ----------

migrate_hms_external_table_to_uc_external(spark, mounted_tables_descriptions, target_catalog, target_schema, target_table)
