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
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import sync_hms_schema_to_uc_schema

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
