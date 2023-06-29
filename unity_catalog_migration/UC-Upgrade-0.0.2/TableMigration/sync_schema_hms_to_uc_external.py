# Databricks notebook source
# MAGIC %md
# MAGIC # Seamlessly Upgrade Hive Metastore External Tables outside of DBFS with mounted file paths in a given schema to UC External Table using SYNC SCHEMA
# MAGIC
# MAGIC This notebook will seamlessly migrate eligible external tables outside of DBFS with a mounted file path in a given schema from the Hive metastore to a UC catalog.
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with **spark.databricks.sql.initial.catalog.name set to hive_metastore** or the base catalog where the external tables will be pulled
# MAGIC - **External Tables on DBFS** - this means the files reside completely within DBFS and the only way forward for these are to recreate them via CLONE (*hms-external-to-uc-managed* notebook). Since if we leave the files in DBFS anybody can read the files of the table.
# MAGIC
# MAGIC **SYNC** command helps you migrate your existing Hive metastore to the Unity Catalog metastore and also helps to keep both your metastores in sync on an ongoing basis until you completely migrate all your dependent applications from Hive metastore to the Unity Catalog metastore.
# MAGIC
# MAGIC It abstracts all the complexities of migrating a schema and external tables from the Hive metastore to the Unity Catalog metastore and keeping them in sync. Once executed, it analyses the source and target tables or schemas and performs the below operations:
# MAGIC
# MAGIC 1. If the target table does not exist, the sync operation creates a target table with the same name as the source table in the provided target schema. The owner of the target table will default to the user who is running the SYNC command
# MAGIC 2. If the target table exists, and if the table is determined to be created by a previous SYNC command or upgraded via Web Interface, the sync operation will update the table such that its schema matches with the schema of the source table.
# MAGIC
# MAGIC **`SYNC SCHEMA`**: It upgrades all eligible tables in a Schema from Hive metastore to the Unity Catalog metastore.
# MAGIC
# MAGIC **Migration away from Mounts points**
# MAGIC
# MAGIC There is no support for mount points with Unity Catalog. Existing mount points should be upgraded to External Locations.
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have external location(s) in place for the table(s)' mounted file path(s(s) that you want to migrate.
# MAGIC - You have `CREATE EXTERNAL TABLE` privileges on the external location(s)
# MAGIC - You have the right privileges on the UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `CREATE TABLE`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Source Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema.
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("create_target_catalog", "", "Create Target UC Catalog")
dbutils.widgets.text("target_catalog_comment", "", "Target UC Catalog Comment")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_catalog_location", "", "Target UC Catalog Location")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("create_target_schema", "", "Create Target UC Schema")
dbutils.widgets.text("target_schema_comment", "", "Target UC Schema Comment")
dbutils.widgets.text("target_schema_location", "", "Target UC Schema Location")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
create_target_catalog = dbutils.widgets.get("create_target_catalog")
target_catalog_comment = dbutils.widgets.get("target_catalog_comment")
target_catalog = dbutils.widgets.get("target_catalog")
target_catalog_location = dbutils.widgets.get("target_catalog_location")
create_target_schema = dbutils.widgets.get("create_target_schema")
target_schema_comment = dbutils.widgets.get("target_schema_comment")
target_schema = dbutils.widgets.get("target_schema")
target_schema_location = dbutils.widgets.get("target_schema_location")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import sync_hms_schema_to_uc_schema
from utils.common_utils import create_uc_catalog, create_uc_schema

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
# MAGIC %md
# MAGIC ## Using the SYNC SCHEMA command to upgrade all eligible HMS external tables in the given schema to external tables in Unity Catalog.
# MAGIC
# MAGIC  You can use it to create new tables in the given `Target Catalog` and `Target Schema` in Unity Catalog from the given hive_metastore `Target Schema` as well as update the Unity Catalog tables when the source tables' metadata in hive_metastore are changed.
# MAGIC
# MAGIC **Please be aware** that if there is at least a table in the given `Source Schema` that is eligible for using the `SYNC SCHEMA` command, error won't be thrown for those that are not.
# MAGIC
# MAGIC **Important**: You need to run `SYNC SCHEMA` periodically if you want to keep seamlessly upgrading the UC tables with the HMS tables in any metadata changes. 
# MAGIC
# MAGIC  **Note**: Equality check between the legacy HMS table(s) and the UC table(s) will run

# COMMAND ----------

sync_hms_schema_to_uc_schema(spark, source_schema, target_catalog, target_schema)
