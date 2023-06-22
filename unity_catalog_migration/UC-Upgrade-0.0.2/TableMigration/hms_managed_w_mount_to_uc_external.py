# Databricks notebook source
# MAGIC %md
# MAGIC ### Set widgets and get their values

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("target_table", "", "Target UC Table")

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
# Pass source_schema variable to the spark context
spark.conf.set("source_schema", str(source_schema))
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")
table_type = "MANAGED"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import modules

# COMMAND ----------

from utils.table_utils import (get_hms_table_description, get_mounted_tables_dict,   check_mountpoint_existance_as_externallocation,                              migrate_hms_external_table_to_uc_external)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the tables' descriptions of the schema

# COMMAND ----------

managed_tables_descriptions = get_hms_table_description(spark, source_schema, source_table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change the managed to external

# COMMAND ----------

# Create string list from list of managed tables
managed_tables_str = ", ".join([r["Table"] for r in managed_tables_descriptions])
# Pass managed table string list to spark context
spark.conf.set("managed_tables_str", managed_tables_str)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Read managed tables string from spark context
# MAGIC val managed_tables_str = (spark.conf.get("managed_tables_str"))
# MAGIC // Create list of managed tables from string list
# MAGIC val managed_tables_list: List[String] =  managed_tables_str.split(", ").map(_.trim).toList

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
# MAGIC ### Get the mounted tables

# COMMAND ----------

mounted_tables_descriptions = get_mounted_tables_dict(managed_tables_descriptions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the mount point has an external location

# COMMAND ----------

check_mountpoint_existance_as_externallocation(spark, dbutils, mounted_tables_descriptions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Migrate Mounted Tables to UC and check equality

# COMMAND ----------

migrate_hms_external_table_to_uc_external(spark, mounted_tables_descriptions, target_catalog, target_schema, target_table)