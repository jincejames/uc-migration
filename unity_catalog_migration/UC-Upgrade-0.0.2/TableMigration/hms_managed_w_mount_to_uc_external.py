# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore Managed Tables outside of DBFS with mounted file path to UC External Tables
# MAGIC
# MAGIC This notebook will migrate all managed tables (or a single) outside of DBFS with mounted file paths in a given schema from the Hive metastore to a UC catalog. 
# MAGIC
# MAGIC Managed tables cannot be migrated to Unity Catalog as Managed Tables without data movement, but as External Tables, they can. This is when the **parent database has its location set to external paths, e.g. a mounted path** from the object store.
# MAGIC
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with **spark.databricks.sql.initial.catalog.name set to hive_metastore** or the base catalog where the external tables will be pulled
# MAGIC - **Managed Tables on DBFS** - this means the files reside completely within DBFS and the only way forward for these are to recreate them via CLONE (*clone_hms_table_to_uc_managed* notebook) or CTAS (*ctas_hms_table_to_uc_managed* notebook)
# MAGIC
# MAGIC **CREATE TABLE LIKE COPY LOCATION**
# MAGIC
# MAGIC You can create Unity Catalog table(s) from your HMS table(s) without any data movement. With the `CRAETE TABLE LIKE COPY LOCATION` command the location of the HMS table will be copied over as metadata to the Unity Catalog table. Data stays as is.
# MAGIC
# MAGIC **Migration away from Mounts points**
# MAGIC
# MAGIC There is no support for mount points with Unity Catalog. Existing mount points should be upgraded to External Locations.
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic.
# MAGIC - You have external location(s) inplace for the table(s)' mounted file path(s) that you want to migrate.
# MAGIC - You have `CREATE EXTERNAL TABLE` privileges on the external location(s)
# MAGIC - You have the right privileges on the UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `CREATE TABLE`

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
dbutils.widgets.text("target_table", "", "Target UC Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
# Pass source_schema variable to the spark context
spark.conf.set("source_schema", str(source_schema))
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")
# Table type mustn't be changed
table_type = "MANAGED"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import (get_hms_table_description, get_mounted_tables_dict,   check_mountpoint_existance_as_externallocation,                              migrate_hms_external_table_to_uc_external)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed tables descriptions if the `Source Table`  parameter is empty
# MAGIC - Get a managed table description if the `Source Table` is filled

# COMMAND ----------

managed_tables_descriptions = get_hms_table_description(spark, source_schema, source_table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change the table(s) from Managed to External
# MAGIC
# MAGIC Hive metastore Mangaged Tables cannot be migrated to UC without data movement hence we need to change the table from managed to external. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pass the list of table(s) to be changed to the Spark Context

# COMMAND ----------

# Create string list from list of managed tables
managed_tables_str = ", ".join([r["Table"] for r in managed_tables_descriptions])
# Pass managed table string list to spark context
spark.conf.set("managed_tables_str", managed_tables_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the list of table(s) to be changed from the Spark Context

# COMMAND ----------

# MAGIC %scala
# MAGIC // Read managed tables string from spark context
# MAGIC val managed_tables_str = (spark.conf.get("managed_tables_str"))
# MAGIC // Create list of managed tables from string list
# MAGIC val managed_tables_list: List[String] =  managed_tables_str.split(", ").map(_.trim).toList

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute the change script in Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
# MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
# MAGIC import org.apache.spark.sql.AnalysisException
# MAGIC
# MAGIC // Iterate through the managed tables and change them to external tables
# MAGIC for (table <- managed_tables_list) {
# MAGIC   val tableName = table
# MAGIC   val schemaName = spark.conf.get("source_schema")
# MAGIC
# MAGIC   try {
# MAGIC     val oldTable: CatalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(schemaName)))
# MAGIC     val alteredTable: CatalogTable = oldTable.copy(tableType = CatalogTableType.EXTERNAL)
# MAGIC     spark.sessionState.catalog.alterTable(alteredTable)
# MAGIC     println(s"The table $schemaName.$tableName' has been changed to EXTERNAL table.")
# MAGIC   } catch {
# MAGIC     case _: AnalysisException => println(s"The table '$tableName' does not exist in database '$schemaName'.")
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter for table(s) with mounted file path(s) only

# COMMAND ----------

mounted_tables_descriptions = get_mounted_tables_dict(managed_tables_descriptions)

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
# MAGIC ## Migrating hive_metastore External Tables to UC External Tables without data movement using the CREATE TABLE LIKE COPY LOCATION command
# MAGIC
# MAGIC Available options:
# MAGIC - Migrate all external tables with mounted file paths from the given `Source Schema` to the given `Target Catalog` and `Target Schema`. 
# MAGIC   - Applicable if the `Source Table` is empty.
# MAGIC - Migrate an external table with mounted file path from the given hive metastore `Source Schema` and `Source Table` to the given `Target Catalog` and `Target Schema`.
# MAGIC   - Applicable if the `Source Table` is filled.
# MAGIC   - If `Target Table` is empty, the `Source Table`'s name is given to the Unity Catalog table.
# MAGIC
# MAGIC **Note**: Equality check between the legacy HMS table(s) and the UC table(s) will run
# MAGIC

# COMMAND ----------

migrate_hms_external_table_to_uc_external(spark, mounted_tables_descriptions, target_catalog, target_schema, target_table)
