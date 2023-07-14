# Databricks notebook source
# MAGIC %md
# MAGIC # Roll Back Metastore External Tables to Managed Tables
# MAGIC
# MAGIC This notebook will roll back all external tables (or single) from External to Managed that have been changed during the HMS Managed Tables to UC External Tables migration.
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with **spark.databricks.sql.initial.catalog.name set to hive_metastore** or the base catalog where the external tables will be pulled

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Source Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema.
# MAGIC * **`Source Table(s)`** (optional): 
# MAGIC   - The name(s) of the source HMS table(s). Multiple tables should be given as follows "table_1, table_2". If filled only the given external table(s) will be pulled otherwise all external tables.  

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
# Table type mustn't be changed
table_type = "EXTERNAL"
# Table properties type check mustn't be changed
table_properties_type_check = "upgraded"
# Source catalog mustn't be changed
source_catalog = "hive_metastore"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.table_utils import get_table_description, get_roll_backed_or_upgraded_table_desc_dict

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed tables descriptions if the `Source Table(s)`  parameter is empty
# MAGIC - Get the given managed table(s) description if the `Source Table(s)` is filled

# COMMAND ----------

external_table_descriptions = get_table_description(spark, source_catalog, source_schema, source_table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter for table(s) with *upgraded* Table Properties in descriptions

# COMMAND ----------

upgraded_tables_descriptions = get_roll_backed_or_upgraded_table_desc_dict(external_table_descriptions, table_properties_type_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Roll back the HMS table(s) from External to Managed
# MAGIC
# MAGIC Hive metastore Mangaged Tables cannot be migrated to UC without data movement hence we need to change the table from managed to external.
# MAGIC Therefore we need to change back the table type from External to Managed, as a roll back.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pass the list of table(s) to be changed to the Spark Context

# COMMAND ----------

# Create string list from list of upgraded tables
upgraded_tables_str = ", ".join([r["Table"] for r in external_table_descriptions if r["Table"] in upgraded_tables_descriptions])
# Pass upgraded table string list to spark context
spark.conf.set("upgraded_tables_str", upgraded_tables_str)
# Pass HMS schema to spark context
spark.conf.set("source_schema", source_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the list of table(s) to be changed from the Spark Context

# COMMAND ----------

# MAGIC %scala
# MAGIC // Read upgraded tables string from spark context
# MAGIC val upgraded_tables_str = (spark.conf.get("upgraded_tables_str"))
# MAGIC // Create list of upgraded tables from string list
# MAGIC val upgraded_tables_list: List[String] =  upgraded_tables_str.split(", ").map(_.trim).toList

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute the change script in Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
# MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
# MAGIC import org.apache.spark.sql.AnalysisException
# MAGIC
# MAGIC // Iterate through the upgraded tables and change them back to managed tables
# MAGIC for (table <- upgraded_tables_list) {
# MAGIC   val tableName = table
# MAGIC   val schemaName = spark.conf.get("source_schema")
# MAGIC   val tableProvider = "delta"
# MAGIC   val currentUser = spark.sql("SELECT current_user()").collect()(0)(0)
# MAGIC   val currentTimestamp = spark.sql("SELECT current_timestamp()").collect()(0)(0)
# MAGIC
# MAGIC   try {
# MAGIC     val oldTable: CatalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(schemaName)))
# MAGIC     val alteredTable: CatalogTable = oldTable.copy(tableType = CatalogTableType.MANAGED)
# MAGIC     spark.sessionState.catalog.alterTable(alteredTable)
# MAGIC     if (tableProvider=="delta"){
# MAGIC       println("Table provider is delta, unset TBLPROPERTIES upgrade from HMS to UC")
# MAGIC       spark.sql(s"""ALTER TABLE $schemaName.$tableName 
# MAGIC                   UNSET TBLPROPERTIES ('upgraded_to',
# MAGIC                                     'upgraded_by',
# MAGIC                                     'upgraded_at')
# MAGIC                                     """)
# MAGIC       println("Set TBLPROPERTIES with rollback details")
# MAGIC       spark.sql(s"""ALTER TABLE $schemaName.$tableName 
# MAGIC                   SET TBLPROPERTIES ('roll_backed_from' = 'EXTERNAL TO MANAGED',
# MAGIC                                     'roll_backed_by' = '$currentUser',
# MAGIC                                     'roll_backed_at' = '$currentTimestamp')
# MAGIC                                     """)                                
# MAGIC     }
# MAGIC     else{
# MAGIC       println(s"Table provider is $tableProvider, set TBLPROPERTIES with upgrade information")
# MAGIC       spark.sql(s"COMMENT ON $schemaName.$tableName IS 'Roll backed EXTERNAL TO MANAGED by $currentUser at $currentTimestamp' ")
# MAGIC     }
# MAGIC     println(s"The table $schemaName.$tableName' has been roll backed to MANAGED table.")
# MAGIC   } catch {
# MAGIC     case e: AnalysisException => println(s"AnalysisException occured: $e.")
# MAGIC   }
# MAGIC }
