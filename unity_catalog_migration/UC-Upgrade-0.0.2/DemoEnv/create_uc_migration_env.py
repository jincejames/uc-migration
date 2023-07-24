# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Unity Catalog Migration Environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC * **`Clear the migration environment?`** (mandatory):
# MAGIC   - Drop-down list. By default, it is `N` for No. 
# MAGIC     - If it is set to `Y`, when you run the cells under **Clear dev environment** section, it will clear the migration environment.
# MAGIC ### Mount parameters
# MAGIC * **`Mount Application Id`** (mandatory): 
# MAGIC   - The application (client) id of the SPN/registered APP.
# MAGIC * **`Mount Directory Id`** (mandatory):
# MAGIC   - The directory (tenant) id of the SPN/registered APP.
# MAGIC * **`Mount Key Name`** (mandatory):
# MAGIC   - The key name that is created for the secret scope.
# MAGIC * **`Mount Scope Name`** (mandatory):
# MAGIC    - The scope name that is created for the secret.
# MAGIC * **`Mount Storage Account Name`** (mandatory):
# MAGIC   - The name of the Storage Account that you want to mount.
# MAGIC * **`Mount Container Name`** (mandatory): 
# MAGIC   - The name of the container that you want to mount.
# MAGIC ### abfss parameters
# MAGIC * **`abfss Application Id`** (mandatory): 
# MAGIC   - The application (client) id of the SPN/registered APP.
# MAGIC * **`abfss Directory Id`** (mandatory):
# MAGIC   - The directory (tenant) id of the SPN/registered APP.
# MAGIC * **`abfss Key Name`** (mandatory):
# MAGIC   - The key name that is created for the secret scope.
# MAGIC * **`abfss Scope Name`** (mandatory):
# MAGIC    - The scope name that is created for the secret.
# MAGIC * **`abfss Storage Account Name`** (mandatory):
# MAGIC   - The name of the Storage Account that you want to access through abfss.
# MAGIC * **`abfss Container Name`** (mandatory): 
# MAGIC   - The name of the container that you want to access through abfss.
# MAGIC ### Unity Catalog parameters
# MAGIC * **`Migration UC Catalog Name`** (mandatory):
# MAGIC   - The name of the Unity Catalog catalog that you want to use for the UC migration demo
# MAGIC * **`List of Principals`** (mandatory): 
# MAGIC   - The list of principals (users, groups, service principals) that you want to use for the UC migration demo.
# MAGIC     - If you like to add multiple principals: "user1, group1, user2, group2, sp1, sp2".
# MAGIC     - If you like to add a single principal: "user1".

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
# Mount widgets
dbutils.widgets.text("mount_container_name", "", "Mount Continer Name")
dbutils.widgets.text("mount_storage_account_name", "", "Mount Storage Account Name")
dbutils.widgets.text("mount_scope_name", "", "Mount Scope Name")
dbutils.widgets.text("mount_key_name", "", "Mount Key Name")
dbutils.widgets.text("mount_application_id", "", "Mount Application Id")
dbutils.widgets.text("mount_directory_id", "", "Mount Directory Id")
# abfss widgets
dbutils.widgets.text("abfss_container_name", "", "abfss Container Name")
dbutils.widgets.text("abfss_storage_account_name", "", "abfss Storage Account Name")
dbutils.widgets.text("abfss_scope_name", "", "abfss Scope Name")
dbutils.widgets.text("abfss_key_name", "", "abfss Key Name")
dbutils.widgets.text("abfss_application_id", "", "abfss Application Id")
dbutils.widgets.text("abfss_directory_id", "", "abfss Directory Id")
# UC widgets
dbutils.widgets.text("migration_catalog", "", "Migration UC Catalog Name")
dbutils.widgets.text("principal_list", "", "List of Principals")
# Clear Environment
dbutils.widgets.dropdown("clear_env","N", "NY", "Clear the migration environment?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract widgets

# COMMAND ----------

# mount variables
mount_container_name = dbutils.widgets.get("mount_container_name")
mount_storage_account_name = dbutils.widgets.get("mount_storage_account_name")
mount_scope_name = dbutils.widgets.get("mount_scope_name")
mount_key_name = dbutils.widgets.get("mount_key_name")
mount_application_id = dbutils.widgets.get("mount_application_id")
mount_directory_id = dbutils.widgets.get("mount_directory_id")

# abfss variables
abfss_container_name = dbutils.widgets.get("abfss_container_name")
abfss_storage_account_name = dbutils.widgets.get("abfss_storage_account_name")
abfss_scope_name = dbutils.widgets.get("abfss_scope_name")
abfss_key_name = dbutils.widgets.get("abfss_key_name")
abfss_application_id = dbutils.widgets.get("abfss_application_id")
abfss_directory_id = dbutils.widgets.get("abfss_directory_id")

# UC variables
migration_catalog = dbutils.widgets.get("migration_catalog")
principal_list = dbutils.widgets.get("principal_list")

# Clear environment variable
clear_env = dbutils.widgets.get("clear_env")

# mount_name mustn't be changed
mount_name = f"/mnt/{mount_container_name}"
# abfss_root_path mustn't be changed
abfss_root_path = f"abfss://{abfss_container_name}@{abfss_storage_account_name}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Point
# MAGIC **Prerequisites**
# MAGIC - [Azure App registration - Service Principal](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/service-principals#step-1-create-an-azure-service-principal-in-your-azure-account)
# MAGIC   - Assign Blob Storage Contributor Role to the SPN (for each Storage Account that you want to use)
# MAGIC - [Create Databricks secret for the access key](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmount 

# COMMAND ----------

##Unmount blob storage
#dbutils.fs.unmount(f"{mount_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create mount point if not exists

# COMMAND ----------

if any(mount.mountPoint == f"{mount_name}" for mount in dbutils.fs.mounts()):
  print("Mount point already mounted")
else: 
  # Mount Blob Storage
  mount_configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{mount_application_id}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=f"{mount_scope_name}",
                                                                       key=f"{mount_key_name}"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{mount_directory_id}/oauth2/token"
          }

  # Optionally, you can add <directory-name> to the source URI of your mount point.
  dbutils.fs.mount(
    source = f"abfss://{mount_container_name}@{mount_storage_account_name}.dfs.core.windows.net/",
    mount_point = f"{mount_name}",
    extra_configs = mount_configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check mount point

# COMMAND ----------

dbutils.fs.ls(f"{mount_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access to the ADLS w/o mount point
# MAGIC **Prerequisites**
# MAGIC - [Azure App registration - Service Principal](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/service-principals#step-1-create-an-azure-service-principal-in-your-azure-account)
# MAGIC   - Assign Blob Storage Contributor Role to the SPN (for each Storage Account that you want to use)
# MAGIC - [Create Databricks secret for the access key](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes)
# MAGIC
# MAGIC **IMPORTANT**: If you want to access the given location from other notebook, you should add as Spark Config for you cluster or to a cluster policy to access with multiple clusters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configs

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.auth.type.{abfss_storage_account_name}.dfs.core.windows.net", 
  "OAuth")
spark.conf.set(
  f"fs.azure.account.oauth.provider.type.{abfss_storage_account_name}.dfs.core.windows.net", 
  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(
  f"fs.azure.account.oauth2.client.id.{abfss_storage_account_name}.dfs.core.windows.net", 
  f"{abfss_application_id}")
spark.conf.set(
  f"fs.azure.account.oauth2.client.secret.{abfss_storage_account_name}.dfs.core.windows.net", 
  dbutils.secrets.get(scope=f"{abfss_scope_name}",key=f"{abfss_key_name}"))
spark.conf.set(
  f"fs.azure.account.oauth2.client.endpoint.{abfss_storage_account_name}.dfs.core.windows.net", 
  f"https://login.microsoftonline.com/{abfss_directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check storage access

# COMMAND ----------

dbutils.fs.ls(f"abfss://{abfss_container_name}@{abfss_storage_account_name}.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Demo Environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Managed HMS Tables on DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create HMS Managed Database on DBFS

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.managed_dbfs_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create HMS Managed Table

# COMMAND ----------

for i in range(4):
  if i == 0:
    pass
  else:
    (spark
    .read
    .table("samples.nyctaxi.trips")
    .write
    .mode("overwrite")
    .saveAsTable(f"hive_metastore.managed_dbfs_schema.dbfs_managed_table_{i}")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Managed HMS Tables outside of DBFS with mounted file paths
# MAGIC This is when the parent database has its location set to external paths, e.g. a mounted path from the cloud object storage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Managed HMS Database on mount point

# COMMAND ----------

spark.sql(f"CREATE DATABASE if not exists hive_metastore.managed_schema_outside_of_dbfs LOCATION 'dbfs:{mount_name}/db_managed_schema_outside_of_dbfs'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create HMS Managed Table

# COMMAND ----------

for i in range(4):
  if i == 0:
    pass
  else:
    (spark
    .read
    .table("samples.nyctaxi.trips")
    .write
    .mode("overwrite")
    .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs.managed_table_outside_dbfs_{i}")
    )

# COMMAND ----------

[(spark
  .read
  .table(f'samples.{row.database}.{row.tableName}')
  .write
  .format("delta")
  .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs.managed_outside_dbfs_{row.database}_{row.tableName}")
  ) for row in spark.sql("SHOW TABLES IN samples.tpch").collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create HMS External table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create External Tables on mounted path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Database for External Tables

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.external_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save sample tables as External Tables to mount point location

# COMMAND ----------

(spark
 .read
 .table("samples.nyctaxi.trips")
 .write
 .format("delta")
 .option("path", f"dbfs:{mount_name}/mount/delta/nyctaxi/trips")
 .saveAsTable("hive_metastore.external_schema.external_mount_nyctaxi_trips"))

# COMMAND ----------

[(spark
  .read
  .table(f'samples.{row.database}.{row.tableName}')
  .write
  .format("delta")
  .mode("overwrite")
  .option("path", f"dbfs:{mount_name}/mount/delta/tpch/{row.tableName}")
  .saveAsTable(f"hive_metastore.external_schema.external_mount_{row.database}_{row.tableName}")
  ) for row in spark.sql("SHOW TABLES IN samples.tpch").collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create External Tables on 'abfss' path

# COMMAND ----------

(spark
 .read
 .table("samples.nyctaxi.trips")
 .write
 .format("delta")
 .option("path", f"{abfss_root_path}/abfss/delta/nyctaxi/trips")
 .saveAsTable("hive_metastore.external_schema.external_abfss_nyctaxi_trips"))

# COMMAND ----------

[(spark
  .read
  .table(f'samples.{row.database}.{row.tableName}')
  .write
  .format("delta")
  .option("path", f"{abfss_root_path}/abfss/delta/tpch/{row.tableName}")
  .saveAsTable(f"hive_metastore.external_schema.external_abfss_{row.database}_{row.tableName}")
  ) for row in spark.sql("SHOW TABLES IN samples.tpch").collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Unity Catalog Dev Environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Unity Catalog Dev

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {migration_catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Unity Catalog Schemas

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {migration_catalog}.migrate_managed_dbfs_tables_schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {migration_catalog}.migrate_external_tables_schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {migration_catalog}.migrate_managed_tables_outside_of_dbfs_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add required permissions

# COMMAND ----------

principals = principal_list.split(",")
for principal in principals:  
  spark.sql(f"""GRANT USE_CATALOG, USE_SCHEMA, CREATE_TABLE, SELECT 
            ON CATALOG {migration_catalog}
            TO {principal}
            """)

# COMMAND ----------

# MAGIC %md
# MAGIC # Clear dev environment

# COMMAND ----------

if clear_env == "N":
  dbutils.notebook.exit(f"No need for clearing the migration environment.")
  raise 
elif clear_env == "Y":
  print("Clearing the migration environment...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear Managed Tables on DBFS

# COMMAND ----------

if clear_env == "Y": 
  spark.sql("DROP DATABASE IF EXISTS hive_metastore.managed_dbfs_schema CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear Managed Tables on outside of DBFS 

# COMMAND ----------

if clear_env == "Y":
  spark.sql("DROP DATABASE IF EXISTS hive_metastore.managed_schema_outside_of_dbfs CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear mount point

# COMMAND ----------

if clear_env == "Y":
  dbutils.fs.rm(f"dbfs:{mount_name}", recurse=True)

# COMMAND ----------

if clear_env == "Y":
  dbutils.fs.rm(f"dbfs:{mount_name}/db_managed_schema_outside_of_dbfs/", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmount

# COMMAND ----------

if clear_env == "Y":
  dbutils.fs.unmount(f"{mount_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear External Tables outside of DBFS

# COMMAND ----------

if clear_env == "Y":
  spark.sql("DROP DATABASE IF EXISTS hive_metastore.external_schema CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear 'abfss' path

# COMMAND ----------

if clear_env == "Y":
  dbutils.fs.rm(f"{abfss_root_path}/mount", recurse=True)

# COMMAND ----------

if clear_env == "Y":
  dbutils.fs.rm(f"{abfss_root_path}/abfss", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear Unity Catalog

# COMMAND ----------

if clear_env == "Y": 
  spark.sql(f"DROP CATALOG IF EXISTS {migration_catalog} CASCADE")
