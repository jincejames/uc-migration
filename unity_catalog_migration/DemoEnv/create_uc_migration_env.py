# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Unity Catalog Migration Environment
# MAGIC
# MAGIC **Creating Demo Environment for Table Migration Showcase from Hive Metastore to Unity Catalog**
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC   - You need to have an *Azure Service Principal (App Registration)* with *Storage Blob Data Contributor Role* on the given `Mount Storage Account Name` and `abfss Container Name`
# MAGIC   - You need to have the *Service Principal secret value* saved as a *Databricks Secret Scope*
# MAGIC
# MAGIC **With the notebook, you can create**:
# MAGIC   - If `Clear the migration environment?` parameter set to "N"
# MAGIC     - Managed Tables on DBFS
# MAGIC       - This is when you create a table without giving a location or the parent database doesn't have its location set to an external path
# MAGIC     - Mount point to the given container as `Mount Storage Account Name` and `Mount Container Name` parameters
# MAGIC       - Managed Tables outside of DBFS with mounted file paths
# MAGIC         - This is when the parent database has its location set to an external path, e.g. a mounted path from the cloud object storage
# MAGIC       - External Tables on outside of DBFS with mounted file paths
# MAGIC     - External Tables outside of DBFS with abfss path
# MAGIC       - Tables will be saved in the given `abfss Storage Account Name` and `abfss Container Name parameters`
# MAGIC     - Managed Tables on outside of DBFS with abfss path
# MAGIC       - This is when the parent database has its location set to an external path, e.g. to a cloud object storage like ADLS with an abfss path
# MAGIC       - **Important**:
# MAGIC         - To be able to create the database in abfss path in your Storage Account's container, you need to add the Azure Credentials to your cluster. Check cell (cmd) 35!
# MAGIC
# MAGIC **Add permissions to principals to your newly created Unity Catalog catalog**
# MAGIC   - Filling the List of Principals' parameters will add 
# MAGIC     - USE_CATALOG, USE_SCHEMA, CREATE_TABLE, SELECT privileges on *CATALOG* level
# MAGIC       - It means the given principals will have privileges to use all the schemas, create tables in all the schemas and select all the tables under the catalog 
# MAGIC
# MAGIC **Cleaning the created environment**
# MAGIC   - You can clean the environment with change the `Clear the migration environment?` parameter to "Y".
# MAGIC
# MAGIC **Working isolated from each other**:
# MAGIC   - You can use the same Service Principal or create a new one
# MAGIC   - You can use the same Storage Account or create a new one
# MAGIC   - You have to use different containers for the `Mount Container Name` and `abfss Container Name`
# MAGIC   - You have to change the Hive Metastore Database names and paths
# MAGIC   - You have to change the Unity Catalog catalog name
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Tasks
# MAGIC   - Describe what the notebook does
# MAGIC   - What needs to be changed to be able to work isolated from others
# MAGIC     - Prerequisites
# MAGIC   - Introduce SPN parameters instead of abfss and mount (consolidate into a single one)
# MAGIC   - Extend the List of Principals parameter description

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC * **`Clear the migration environment?`** (mandatory):
# MAGIC   - Drop-down list. By default, it is `N` for No. 
# MAGIC     - If it is set to `Y`, when you run the cells under **Clear dev environment** section, it will clear the migration environment.
# MAGIC ### Service Principal parameters
# MAGIC * **`Service Principal Application (Client) Id`** (mandatory): 
# MAGIC   - The application (client) id of the SPN/registered APP.
# MAGIC * **`Service Principal Directory (Tenant) Id`** (mandatory):
# MAGIC   - The directory (tenant) id of the SPN/registered APP.
# MAGIC * **`Service Principal Secret Key Name`** (mandatory):
# MAGIC   - The key name that is created for the secret scope.
# MAGIC * **`Service Principal Secret Scope Name`** (mandatory):
# MAGIC   - The scope name that is created for the secret.
# MAGIC ### Mount parameters
# MAGIC * **`Mount Storage Account Name`** (mandatory):
# MAGIC   - The name of the Storage Account that you want to mount.
# MAGIC * **`Mount Container Name`** (mandatory): 
# MAGIC   - The name of the container that you want to mount.
# MAGIC ### abfss parameters
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
# MAGIC     - Gives USE_CATALOG, USE_SCHEMA, CREATE_TABLE, SELECT privileges on *CATALOG* level
# MAGIC       - It means the given principals will have privileges to use all the schemas, create tables in all the schemas and select all the tables under the catalog 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
# Service Principal widgets
dbutils.widgets.text("sp_scope_name", "", "Service Principal Secret Scope Name")
dbutils.widgets.text("sp_key_name", "", "Service Principal Secret Key Name")
dbutils.widgets.text("sp_application_id", "", "Service Principal Application (Client) Id")
dbutils.widgets.text("sp_directory_id", "", "Service Principal Directory (Tenant) Id")
# Mount widgets
dbutils.widgets.text("mount_container_name", "", "Mount Container Name")
dbutils.widgets.text("mount_storage_account_name", "", "Mount Storage Account Name")
# abfss widgets
dbutils.widgets.text("abfss_container_name", "", "abfss Container Name")
dbutils.widgets.text("abfss_storage_account_name", "", "abfss Storage Account Name")
# UC widgets
dbutils.widgets.text("migration_catalog", "", "Migration UC Catalog Name")
dbutils.widgets.text("principal_list", "", "List of Principals")
# Clear Environment
dbutils.widgets.dropdown("clear_env","N", "NY", "Clear the migration environment?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract widgets

# COMMAND ----------

# service principal variables
sp_scope_name = dbutils.widgets.get("sp_scope_name")
sp_key_name = dbutils.widgets.get("sp_key_name")
sp_application_id = dbutils.widgets.get("sp_application_id")
sp_directory_id = dbutils.widgets.get("sp_directory_id")

# mount variables
mount_container_name = dbutils.widgets.get("mount_container_name")
mount_storage_account_name = dbutils.widgets.get("mount_storage_account_name")

# abfss variables
abfss_container_name = dbutils.widgets.get("abfss_container_name")
abfss_storage_account_name = dbutils.widgets.get("abfss_storage_account_name")

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
# MAGIC ### Create mount point if not exists

# COMMAND ----------

if clear_env == "N":
  if any(mount.mountPoint == f"{mount_name}" for mount in dbutils.fs.mounts()):
    print("Mount point already mounted")
  else: 
    # Mount Blob Storage
    mount_configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": f"{sp_application_id}",
            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=f"{sp_scope_name}",
                                                                        key=f"{sp_key_name}"),
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{sp_directory_id}/oauth2/token"
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

if clear_env == "N":
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

if clear_env == "N":
  spark.conf.set(
    f"fs.azure.account.auth.type.{abfss_storage_account_name}.dfs.core.windows.net", 
    "OAuth")
  spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{abfss_storage_account_name}.dfs.core.windows.net", 
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{abfss_storage_account_name}.dfs.core.windows.net", 
    f"{sp_application_id}")
  spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{abfss_storage_account_name}.dfs.core.windows.net", 
    dbutils.secrets.get(scope=f"{sp_scope_name}",key=f"{sp_key_name}"))
  spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{abfss_storage_account_name}.dfs.core.windows.net", 
    f"https://login.microsoftonline.com/{sp_directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check storage access

# COMMAND ----------

if clear_env == "N":
  dbutils.fs.ls(abfss_root_path)

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

if clear_env == "N":
  spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.managed_dbfs_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create HMS Managed Table

# COMMAND ----------

if clear_env == "N":
  for i in range(4):
    if i == 0:
      pass
    else:
      # Create table
      (spark
      .read
      .table("samples.nyctaxi.trips")
      .write
      .mode("overwrite")
      .saveAsTable(f"hive_metastore.managed_dbfs_schema.dbfs_managed_table_{i}")
      )
      # Create view
      (spark.sql(f"CREATE OR REPLACE VIEW hive_metastore.managed_dbfs_schema.vw_dbfs_managed_table_{i} AS SELECT * FROM hive_metastore.managed_dbfs_schema.dbfs_managed_table_{i} LIMIT 100"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Managed HMS Tables outside of DBFS *with* mounted file paths
# MAGIC This is when the parent database has its location set to external paths, e.g. a mounted path from the cloud object storage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Managed HMS Database on mount point

# COMMAND ----------

if clear_env == "N":
  spark.sql(f"CREATE DATABASE if not exists hive_metastore.managed_schema_outside_of_dbfs_mount LOCATION 'dbfs:{mount_name}/managed_schema_outside_of_dbfs_mount'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create HMS Managed Table

# COMMAND ----------

if clear_env == "N":
  for i in range(4):
    if i == 0:
      pass
    else:
      # Delta table
      (spark
      .read
      .table("samples.nyctaxi.trips")
      .write
      .mode("overwrite")
      .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs_mount.managed_table_outside_dbfs_{i}")
      )
      # Create view
      (spark.sql(f"CREATE OR REPLACE VIEW hive_metastore.managed_schema_outside_of_dbfs_mount.vw_managed_table_outside_dbfs_{i} AS SELECT * FROM hive_metastore.managed_schema_outside_of_dbfs_mount.managed_table_outside_dbfs_{i} LIMIT 100"))

      # Parquet table
      (spark
      .read
      .table("samples.nyctaxi.trips")
      .write
      .format("parquet")
      .mode("overwrite")
      .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs_mount.managed_table_outside_dbfs_parquet_{i}")
      )
      # CSV table
      (spark
      .read
      .table("samples.nyctaxi.trips")
      .write
      .format("csv")
      .mode("overwrite")
      .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs_mount.managed_table_outside_dbfs_csv_{i}")
      )

# COMMAND ----------

if clear_env == "N":
  [(spark
    .read
    .table(f'samples.{row.database}.{row.tableName}')
    .write
    .format("delta")
    .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs_mount.managed_outside_dbfs_{row.database}_{row.tableName}")
    ) for row in spark.sql("SHOW TABLES IN samples.tpch").collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Managed HMS Tables outside of DBFS *without* mounted file paths
# MAGIC This is when the parent database has its location set to external paths, e.g. to a cloud object storage like ADLS
# MAGIC
# MAGIC **Prerequisites**
# MAGIC   - You need to create a secret scope and a secret key for the Service Principal's secret
# MAGIC   - Credentials need to be added to your cluster like:
# MAGIC     - ```
# MAGIC       fs.azure.account.auth.type.<your_storage_account_name>.dfs.core.windows.net OAuth
# MAGIC       fs.azure.account.oauth2.client.secret.<your_storage_account_name>.dfs.core.windows.net {{secrets/<your_secret_scope_name>/<your_sercet_key_name>}}
# MAGIC       fs.azure.account.oauth2.client.id.<storage_account_name>.dfs.core.windows.net <your_service_principal_client_id>
# MAGIC       fs.azure.account.oauth2.client.endpoint.<storage_account_name>.dfs.core.windows.net https://login.microsoftonline.com/<your_service_principal_directory_id>/oauth2/token
# MAGIC       fs.azure.account.oauth.provider.type.<storage_account_name>.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
# MAGIC       ```
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Managed HMS Database on 'abfss' location

# COMMAND ----------

if clear_env == "N":
  spark.sql(f"CREATE DATABASE if not exists hive_metastore.managed_schema_outside_of_dbfs_abfss LOCATION '{abfss_root_path}/managed_schema_outside_of_dbfs_abfss'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create HMS Managed Table

# COMMAND ----------

if clear_env == "N":
  for i in range(4):
    if i == 0:
      pass
    else:
      # Delta table
      (spark
      .read
      .table("samples.nyctaxi.trips")
      .write
      .mode("overwrite")
      .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs_abfss.managed_table_outside_dbfs_{i}")
      )
      # Create view
      (spark.sql(f"CREATE OR REPLACE VIEW hive_metastore.managed_schema_outside_of_dbfs_abfss.vw_managed_table_outside_dbfs_{i} AS SELECT * FROM hive_metastore.managed_schema_outside_of_dbfs_abfss.managed_table_outside_dbfs_{i} LIMIT 100"))

      # Parquet table
      (spark
      .read
      .table("samples.nyctaxi.trips")
      .write
      .format("parquet")
      .mode("overwrite")
      .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs_abfss.managed_table_outside_dbfs_parquet_{i}")
      )
      # CSV table
      (spark
      .read
      .table("samples.nyctaxi.trips")
      .write
      .format("csv")
      .mode("overwrite")
      .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs_abfss.managed_table_outside_dbfs_csv_{i}")
      )

# COMMAND ----------

if clear_env == "N":
  [(spark
    .read
    .table(f'samples.{row.database}.{row.tableName}')
    .write
    .format("delta")
    .saveAsTable(f"hive_metastore.managed_schema_outside_of_dbfs_abfss.managed_outside_dbfs_{row.database}_{row.tableName}")
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

if clear_env == "N":
  spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.external_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save sample tables as External Tables to mount point location

# COMMAND ----------

if clear_env == "N":
  (spark
  .read
  .table("samples.nyctaxi.trips")
  .write
  .format("delta")
  .option("path", f"dbfs:{mount_name}/mount/delta/nyctaxi/trips")
  .saveAsTable("hive_metastore.external_schema.external_mount_nyctaxi_trips"))

# COMMAND ----------

if clear_env == "N":
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

if clear_env == "N":
  # Delta
  (spark
  .read
  .table("samples.nyctaxi.trips")
  .write
  .format("delta")
  .mode("overwrite")
  .option("path", f"{abfss_root_path}/abfss/delta/nyctaxi/trips")
  .saveAsTable("hive_metastore.external_schema.external_abfss_nyctaxi_trips"))
  # Create view
  (spark.sql(f"CREATE OR REPLACE VIEW hive_metastore.external_schema.vw_external_abfss_nyctaxi_trips AS SELECT * FROM hive_metastore.external_schema.external_abfss_nyctaxi_trips LIMIT 100"))
  # Parquet
  (spark
  .read
  .table("samples.nyctaxi.trips")
  .write
  .format("parquet")
  .option("path", f"{abfss_root_path}/abfss/delta/nyctaxi/trips_parquet")
  .saveAsTable("hive_metastore.external_schema.external_abfss_nyctaxi_trips_parquet"))
  # CSV
  (spark
  .read
  .table("samples.nyctaxi.trips")
  .write
  .format("csv")
  .option("path", f"{abfss_root_path}/abfss/delta/nyctaxi/trips_csv")
  .saveAsTable("hive_metastore.external_schema.external_abfss_nyctaxi_trips_csv"))

# COMMAND ----------

if clear_env == "N":
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
# MAGIC ## Create Hive Metastore Database for sync Hive Metastore tables to Unity Catalog

# COMMAND ----------

if clear_env == "N":
  spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.external_schema_from_uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Unity Catalog Dev

# COMMAND ----------

if clear_env == "N":
  spark.sql(f"CREATE CATALOG IF NOT EXISTS {migration_catalog} MANAGED LOCATION 'abfss://managed-unity-migration-env@rsv0datasolutions0kbca.dfs.core.windows.net/{migration_catalog}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Unity Catalog Schemas

# COMMAND ----------

if clear_env == "N":
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {migration_catalog}.managed_dbfs_schema")

# COMMAND ----------

if clear_env == "N":
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {migration_catalog}.external_schema")

# COMMAND ----------

if clear_env == "N":
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {migration_catalog}.managed_schema_outside_of_dbfs_mount")

# COMMAND ----------

if clear_env == "N":
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {migration_catalog}.managed_schema_outside_of_dbfs_abfss")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add required permissions

# COMMAND ----------

if clear_env == "N":
  principals = principal_list.split(",")
  for principal in principals:  
    spark.sql(f"""GRANT USE_CATALOG, USE_SCHEMA, CREATE_TABLE, SELECT 
              ON CATALOG `{migration_catalog}`
              TO `{principal}`
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
  spark.sql("DROP DATABASE IF EXISTS hive_metastore.managed_schema_outside_of_dbfs_mount CASCADE")

# COMMAND ----------

if clear_env == "Y":
  spark.sql("DROP DATABASE IF EXISTS hive_metastore.managed_schema_outside_of_dbfs_abfss CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear External Tables on outside of DBFS

# COMMAND ----------

if clear_env == "Y":
  spark.sql("DROP DATABASE IF EXISTS hive_metastore.external_schema CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear External Schema that used for sync from Unity Catalog

# COMMAND ----------

if clear_env == "Y":
  spark.sql(f"DROP DATABASE IF EXISTS hive_metastore.external_schema_from_uc CASCADE")

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
