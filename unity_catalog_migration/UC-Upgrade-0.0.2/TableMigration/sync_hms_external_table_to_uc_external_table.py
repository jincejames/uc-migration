# Databricks notebook source
# MAGIC %md
# MAGIC # Seamlessly Upgrade Hive Metastore External Table outside of DBFS with mounted file paths to UC External Table using SYNC
# MAGIC
# MAGIC This notebook will seamlessly migrate a given external table outside of DBFS with a mounted file path in a given schema from the Hive metastore to a UC catalog.
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
# MAGIC **`SYNC TABLE`**: It upgrades a table from Hive metastore to the Unity Catalog metastore
# MAGIC
# MAGIC **Migration away from Mounts points**
# MAGIC
# MAGIC There is no support for mount points with Unity Catalog. Existing mount points should be upgraded to External Locations.
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have an external location in place for the table's mounted file path(s) that you want to migrate.
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
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import sync_hms_external_table_to_uc_external

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using the SYNC TABLE command to upgrade individual HMS external table (source_table) to external table in Unity Catalog.
# MAGIC
# MAGIC  You can use it to create a new table in the given `Target Catalog` and `Target Schema` in Unity Catalog from the existing hive_metastore table as well as update the Unity Catalog table when the source table's metadata in hive_metastore is changed.
# MAGIC
# MAGIC **Please be aware** that if the given `Source Table` is not eligible for using `SYNC TABLE` command then an error will be thrown.
# MAGIC
# MAGIC **Important**: You need to run `SYNC TABLE` periodically if you want to keep seamlessly upgrading the UC table with the HMS table in any metadata changes. 
# MAGIC
# MAGIC  **Note**: Equality check between the legacy HMS table(s) and the UC table(s) will run

# COMMAND ----------

sync_hms_external_table_to_uc_external(spark, source_schema, source_table, target_catalog, target_schema)
