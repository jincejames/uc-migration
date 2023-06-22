from os.path import commonprefix

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

import pyspark.sql.functions as F
import pyspark.sql.utils as U

def get_hms_table_description(spark: SparkSession, source_schema: str, source_table: str, table_type: str) -> list:
  """
  Get the Descriptions of HMS tables
  
  Parameters:
    spark: Active SparkSession
    source_schema: The string of the hive_metastore source schema
    source_table: The string of the hive_metastore source table. All the tables in the given source schema will be used if not given.
    table_type: The type of the hive_metastore source table (e.g. EXTERNAL, MANAGED)
  
  Returns:
    The list of dictionaries of the tables' descriptions. Including 'Location', 'Database', 'Table', 'Type', and 'Provider'.
  """

  if not source_schema:
    raise ValueError("Source Schema is empty")

  # Set hive_metastore variables
  hms_catalog = "hive_metastore"
  hms_schema = source_schema
  hms_table = source_table

  # List variable for table descriptions
  table_descriptions = []

  if not hms_table:
    # Read all tables in the given schema to DataFrame
    hms_tables = (spark
                  .sql(f"show tables in {hms_catalog}.{hms_schema}")
                  .select("tableName")
                  )
    # Get all tables from Hive Metastore for the given hive_metastore schema
    tables = list(map(lambda r: r.tableName, hms_tables.collect()))
  else:
    # Set the given HMS table
    tables = [f"{hms_table}"]

  # Loop through each table and run the describe command
  for table in tables:
      table_name = table
      try:
        # Read the Table description into DataFrame
        desc_df = (spark
                  .sql(f"DESCRIBE FORMATTED {hms_catalog}.{hms_schema}.{table_name}")
                  .filter(F.col("col_name")
                          .isin(['Location', 'Database', 'Table', 'Type', 'Provider'])
                          )
                  )
        # Create description dict
        desc_dict = {row['col_name']:row['data_type'] for row in desc_df.collect()}
        
        # Filter for table type - need to be a parameter
        if desc_dict["Type"].upper() == table_type:
        # Append the table_descriptions list with the values
          table_descriptions.append(desc_dict)
      
      except (ValueError, U.AnalysisException) as e:
        print(f"Error on {hms_catalog}.{hms_schema}.{table_name}.")
        if isinstance(e, ValueError):
          print(f"ValueError occurred: {e}")
        elif isinstance(e, U.AnalysisException):
          print(f"AnalysisException occurred: {e}")
  return table_descriptions


def get_mounted_tables_dict(table_descriptions: list) -> list:
  """
  Get the Mounted hive_metastore Tables

  Parameters:
    descriptions: The list of dictionaries of the table descriptions. Including 'Location', 'Database', 'Table', 'Type', and 'Provider'.

  Returns:
    The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Database', 'Table', 'Type', and 'Provider'.
  """
  return [d for d in table_descriptions if "/mnt" in d["Location"]]
  

def check_mountpoint_existance_as_externallocation(spark: SparkSession, dbutils: DBUtils, mounted_descriptions: list) -> None:
  """
  Checking whether the mounted tables' mount point paths exist as external location

  Parameters:
    spark: Active SparkSession
    mounted_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Database', 'Table', 'Type', and 'Provider'.
  
  """
  # Get the mounts
  mounts = dbutils.fs.mounts()
  # Iterate through the list of mounted tables' descriptions
  for r in mounted_descriptions:
    try:
      # Get the table's mounted path
      mount_table_path = r["Location"]
      mount_table = r["Table"]

      # Get the mount point and mount source (path) if the table location exists as mount source (path) and it is not a DatabricksRoot path
      mount_point, mount_source = zip(*[(mount.mountPoint, mount.source) for mount in mounts if (mount.mountPoint in mount_table_path and mount.source != "DatabricksRoot")])

      # Read the external locations to DataFrame
      external_loc_df = (spark
                         .sql("SHOW EXTERNAL LOCATIONS")
                         )
      # Get the external location name and url if the url part of any mount source (path)
      external_loc_name, external_loc_url = zip(*[(e.name, e.url) for e in external_loc_df.collect() if mount_source[0] in e.url])

      # Check whether there is only a single external location for the existing mount point
      if len(external_loc_name) > 1:
        raise ValueError(f"There are more then 1 External location: {external_loc_name}. Please check which is needed.")


      common_prefix = commonprefix([mount_source[0], external_loc_url[0]])
      mount_source_folder = mount_source[0][len(common_prefix):]
      external_loc_folder = external_loc_url[0][len(common_prefix):]
      
      # Check whether the external location url is part of the mount source (path) if not raise ValueError
      if mount_source[0] == external_loc_url[0]:
        print(f"Mount point: {mount_point[0]} with source path {mount_source[0]} has an External Location with name {external_loc_name[0]} and path {external_loc_url[0]}")
      elif mount_source[0]  == common_prefix:
        print(f"Mount point: {mount_point[0]} with source path {mount_source[0]} has a common prefix {common_prefix} with External Location with name {external_loc_name[0]} and path {external_loc_url[0]}")
        if external_loc_folder and (external_loc_folder in mount_table_path):
          print(f"External location folder {external_loc_folder} exists in the table {mount_table} mount path {mount_table_path}")
      elif external_loc_url[0] == common_prefix:
        print(f"Mount point: {mount_point[0]} with source path {mount_source[0]} has a common prefix {common_prefix} with External Location with name {external_loc_name[0]} and path {external_loc_url[0]}")
        if mount_folder and mount_folder in external_loc_url[0]:
          print(f"Mount point folder {mount_source_folder} exists in External Location {external_loc_url[0]}")
      else:
        raise ValueError(f"Mount point: {mount_point[0]} with source path {mount_source[0]} doesn't part of the external location {external_loc_namel[0]} with path {external_loc_url[0]}")

    except ValueError as e:
      raise ValueError(e)


def migrate_hms_external_table_to_uc_external(spark: SparkSession, tables_descriptions: list, target_catalog: str, target_schema: str, target_table: str) -> None:
  """
  Migrating hive_metastore External Tables to UC External Tables without data movement using the CREATE TABLE LIKE COPY LOCATION command.

  Parameters:
    spark: Active SparkSession
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Database', 'Table', 'Type', and 'Provider'.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table: The name of the target Unity Catalog table (optional). If not given all the tables from the given target schema will be checked and all of them that are external tables will be migrated to the given target catalog and target schema with the same table name as the HMS table.

  """

  if not tables_descriptions:
    raise ValueError("tables_descriptions input list is empty")
  
  # Iterate through the tables' descriptions list of dictionaries
  for r in tables_descriptions:
    
    # Set hive_metastore variables
    hms_catalog = "hive_metastore"
    hms_schema = r["Database"]
    hms_table = r["Table"]
    hms_table_provider = r["Provider"]
    
    # Set UC variables
    uc_catalog = target_catalog
    uc_schema = target_schema
    # I target UC table is not given, use the hive_metastore table
    uc_table = target_table if target_table else hms_table
    
    try:
      if not uc_catalog:
        raise ValueError(f"Target UC Catalog is not given")
      elif not uc_schema:
        raise ValueError(f"Target UC Schema is not given")
      
      # Execute Create HMS Table Like UC Table with COPY LOCATION
      spark.sql(f"""
                CREATE TABLE {uc_catalog}.{uc_schema}.{uc_table} LIKE {hms_catalog}.{hms_schema}.{hms_table} COPY LOCATION;
                """)
      print(f"Table {hms_catalog}.{hms_schema}.{hms_table} migrated to {uc_catalog}.{uc_schema}.{uc_table} using the CREATE TABLE LIKE COPY LOCATION")
      
      # Set table properties if it's delta
      if hms_table_provider.lower() == "delta":
        # Set current user and current timestamp
        current_u = spark.sql("SELECT current_user()").collect()[0][0]
        current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
        
        # Execute set table properties
        spark.sql(f"""
                  ALTER TABLE {hms_catalog}.{hms_schema}.{hms_table} 
                  SET TBLPROPERTIES ('upgraded_to' = '{uc_catalog}.{uc_schema}.{uc_table}',
                                    'upgraded_by' = 'current_u',
                                    'upgraded_at' = 'current_t')
                  """)
      else:
        print(f"Table provider is {hms_table_provider}, hence TBLPROPERTIES cannot be set")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)
        
    except (ValueError, U.AnalysisException) as e:
      if isinstance(e, ValueError):
        print(f"ValueError occurred: {e}")
      elif isinstance(e, U.AnalysisException):
        print(f"AnalysisException occurred: {e}")

def check_equality_of_hms_uc_table(spark: SparkSession, hms_schema: str, hms_table: str, uc_catalog: str, uc_schema: str, uc_table: str) -> None:
  """
  Checking the migrated UC Table equality to its source HMS table by the volume and the schema of the tables
  
  Parameters:
    spark: Active SparkSession
    hms_schema: The name of the schema in the hive_metastore
    hms_table: The name of the table in the given hive_metastore schema
    uc_catalog: The name of the target Unity Catalog catalog
    uc_schema: The name of the schema Unity Catalog schema
    uc_table: The name of the target Unity Catalog table

  """
  try:
    if not hms_schema:
      raise ValueError(f"Source hive_metastore Schema is not given")
    elif not hms_table:
      raise ValueError(f"Source hive_metastore Table is not given")
    elif not uc_catalog:
      raise ValueError(f"Target UC Catalog is not given")
    elif not uc_schema:
      raise ValueError(f"Target UC Schema is not given")
    elif not uc_table:
      raise ValueError(f"Target UC Table is not given")
    
    # Set hms_catalog variable
    hms_catalog = "hive_metastore"

    # Read hive_metastore and UC table as DataFrame
    hms = spark.read.table(f"{hms_catalog}.{hms_schema}.{hms_table}")
    uc = spark.read.table(f"{uc_catalog}.{uc_schema}.{uc_table}")

    # Extract HMS and UC schemas and volumes
    hms_schema_struct = hms.schema
    uc_schema_struct = uc.schema
    hms_count = hms.count()
    uc_count = uc.count()

    # Check schema match
    if hms_schema_struct == uc_schema_struct:
      # Check volume match
      if hms_count == uc_count:
        print(f"{hms_catalog}.{hms_schema}.{hms_table} has been migrated to {uc_catalog}.{uc_schema}.{uc_table} correctly")
      else:
        print(f"{hms_catalog}.{hms_schema}.{hms_table} count {hms_count} not equals with {uc_catalog}.{uc_schema}.{uc_table} count {uc_count}. Diff: {hms_count-uc_count}")
        raise AssertionError("Data volumes are not matching")
    else:
      print(f"{hms_catalog}.{hms_schema}.{hms_table} schema not equals with {uc_catalog}.{uc_schema}.{uc_table} schema. Diff: {list(set(hms_schema_struct)-set(uc_schema_struct))}")
      raise AssertionError("Schemas are not matching")

  except (ValueError, AssertionError) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
    elif isinstance(e, AssertionError):
      print(f"AssertionError occurred: {e}")


def sync_hms_external_table_to_uc_external(spark: SparkSession, source_schema: str, source_table: str, target_catalog: str, target_schema: str) -> None:
  """
  Using the SYNC TABLE command to upgrade individual hive_metastore external table to external table in Unity Catalog.
  You can use it to create a new table in Unity Catalog from the existing hive_metastore table as well as update the Unity Catalog table when the source tables in hive_metastore are changed.
  
  Parameters:
    spark: Active SparkSession
    source_schema: The name of the source hive_metastore schema
    source_table: The name of the source hive_metastore table in the schema
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema

  """
  try:

    if not source_schema:
      raise ValueError("source_schema input string is empty")
    elif not source_table:
      raise ValueError("source_table input string is empty")
    elif not target_catalog:
      raise ValueError("target_catalog input string is empty")
    elif not target_schema:
      raise ValueError("target_schema input string is empty")
      
    # Set hive_metastore variables
    hms_catalog = "hive_metastore"
    hms_schema = source_schema
    hms_table = source_table
    
    # Set UC variables
    uc_catalog = target_catalog
    uc_schema = target_schema
    uc_table = hms_table
      
    # Execute SYNC TABLE DRY RUN to check that the table is eligible for SYNC 
    sync_dry = spark.sql(f"""
                          SYNC TABLE {uc_catalog}.{uc_schema}.{uc_table} FROM {hms_catalog}.{hms_schema}.{hms_table} DRY RUN;
                          """)
    # Extract sync status and description
    sync_status = sync_dry.select("status_code").collect()[0][0]
    sync_description = sync_dry.select("description").collect()[0][0]
    
    if sync_status.upper() == "DRY_RUN_SUCCESS":
      # Execute the SYNC if DRY RUN was successful
      spark.sql(f"""
                SYNC TABLE {uc_catalog}.{uc_schema}.{uc_table} FROM {hms_catalog}.{hms_schema}.{hms_table};
                """)
      print(f"Table {hms_catalog}.{hms_schema}.{hms_table} is successfully synced to {uc_catalog}.{uc_schema}.{uc_table}")

      # Check the match of HMS and UC tables
      check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)

    else:
      # Raise error with the status code
      raise ValueError(f"Table {hms_catalog}.{hms_schema}.{hms_table} cannot be synced. Description: {sync_description}")
      
  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
    elif isinstance(e, U.AnalysisException):
      print(f"AnalysisException occurred: {e}")


def sync_hms_schema_to_uc_schema(spark: SparkSession, source_schema: str, target_catalog: str, target_schema: str) -> None:
  """
  Using the SYNC SCHEMA command to upgrade all eligible hive_metastore external tables in a schema to external tables in a Unity Catalog schema.
  You can use it to create new tables in Unity Catalog from existing hive_metastore tables as well as update the Unity Catalog tables when the source tables in hive_metastore are changed.
  
  Parameters:
    spark: Active SparkSession
    source_schema: The name of the source hive_metastore schema
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema

  """
  try:

    if not source_schema:
      raise ValueError("source_schema input string is empty")
    elif not target_catalog:
      raise ValueError("target_catalog input string is empty")
    elif not target_schema:
      raise ValueError("target_schema input string is empty")
      
    # Set hive_metastore variables
    hms_catalog = "hive_metastore"
    hms_schema = source_schema
    
    # Set UC variables
    uc_catalog = target_catalog
    uc_schema = target_schema
      
    # Execute SYNC SCHEMA DRY RUN to check whether there is any table is eligible for SYNC 
    sync_dry = spark.sql(f"""
                          SYNC SCHEMA {uc_catalog}.{uc_schema} FROM {hms_catalog}.{hms_schema} DRY RUN;
                          """)
    if any("DRY_RUN_SUCCESS" == status for status in [sc.status_code for sc in sync_dry.select("status_code").collect()]):
      # Execute the SYNC SCHEMA if DRY RUN was successful
      synced_df = spark.sql(f"""
                            SYNC SCHEMA {uc_catalog}.{uc_schema} FROM {hms_catalog}.{hms_schema};
                            """)
      
      # Check the match of HMS and UC tables that have been successfully synced
      for table in synced_df.filter("status_code = 'SUCCESS'").collect():
        print(f"Table {hms_catalog}.{hms_schema}.{table['source_name']} has successfully synced to {uc_catalog}.{uc_schema}.{table['target_name']}")
        
        # Check the match of HMS and UC tables that have been successfully synced
        check_equality_of_hms_uc_table(spark, hms_schema, table["source_name"], uc_catalog, uc_schema, table["target_name"])

    else:
      # Raise error with the status code
      raise ValueError(f"There is no eligible table in {hms_catalog}.{hms_schema} to SYNC to Unity Catalog")
      
  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
    elif isinstance(e, U.AnalysisException):
      print(f"AnalysisException occurred: {e}")


def ctas_hms_managed_table_to_uc_managed(spark: SparkSession, tables_descriptions: list, target_catalog: str, target_schema: str, target_table: str) -> None:
  """
  Migrating hive_metastore Managed Tables to UC Managed Tables with data movement using the CREATE TABLE AS SELECT (CTAS) command.

  Parameters:
    spark: Active SparkSession
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Database', 'Table', 'Type', and 'Provider'.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table: The name of the target Unity Catalog table (optional). If not given all the tables from the given target schema will be checked and all of them that are external tables will be migrated to the given target catalog and target schema with the same table name as the HMS table.

  """

  if not tables_descriptions:
    raise ValueError("tables_descriptions input list is empty")
  
  # Iterate through the tables' descriptions list of dictionaries
  for r in tables_descriptions:
    
    # Set hive_metastore variables
    hms_catalog = "hive_metastore"
    hms_schema = r["Database"]
    hms_table = r["Table"]
    hms_table_provider = r["Provider"]
    
    # Set UC variables
    uc_catalog = target_catalog
    uc_schema = target_schema
    # I target UC table is not given, use the hive_metastore table
    uc_table = target_table if target_table else hms_table
    
    try:
      if not uc_catalog:
        raise ValueError(f"Target UC Catalog is not given")
      elif not uc_schema:
        raise ValueError(f"Target UC Schema is not given")
      
      # Execute CREATE TABLE AS SELECT command
      spark.sql(f"""
                CREATE TABLE {uc_catalog}.{uc_schema}.{uc_table} AS
                SELECT * FROM {hms_catalog}.{hms_schema}.{hms_table};
                """)
      print(f"Table {hms_catalog}.{hms_schema}.{hms_table} migrated to {uc_catalog}.{uc_schema}.{uc_table} with CTAS")
      
      # Set table properties if it's delta
      if hms_table_provider.lower() == "delta":
        # Set current user and current timestamp
        current_u = spark.sql("SELECT current_user()").collect()[0][0]
        current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
        
        # Execute set table properties
        spark.sql(f"""
                  ALTER TABLE {hms_catalog}.{hms_schema}.{hms_table} 
                  SET TBLPROPERTIES ('upgraded_to' = '{uc_catalog}.{uc_schema}.{uc_table}',
                                    'upgraded_by' = 'current_u',
                                    'upgraded_at' = 'current_t')
                  """)
      else:
        print(f"Table provider is {hms_table_provider}, hence TBLPROPERTIES cannot be set")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)
        
    except (ValueError, U.AnalysisException) as e:
      if isinstance(e, ValueError):
        print(f"ValueError occurred: {e}")
      elif isinstance(e, U.AnalysisException):
        print(f"AnalysisException occurred: {e}")


def clone_hms_managed_table_to_uc_managed(spark: SparkSession, tables_descriptions: list, target_catalog: str, target_schema: str, target_table: str) -> None:
  """
  Migrating hive_metastore Managed Tables to UC Managed Tables with data movement using the DEEP CLONE command.

  Parameters:
    spark: Active SparkSession
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Database', 'Table', 'Type', and 'Provider'.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table: The name of the target Unity Catalog table (optional). If not given all the tables from the given target schema will be checked and all of them that are external tables will be migrated to the given target catalog and target schema with the same table name as the HMS table.

  """

  if not tables_descriptions:
    raise ValueError("tables_descriptions input list is empty")
  
  # Iterate through the tables' descriptions list of dictionaries
  for r in tables_descriptions:
    
    # Set hive_metastore variables
    hms_catalog = "hive_metastore"
    hms_schema = r["Database"]
    hms_table = r["Table"]
    hms_table_provider = r["Provider"]
    
    # Set UC variables
    uc_catalog = target_catalog
    uc_schema = target_schema
    # I target UC table is not given, use the hive_metastore table
    uc_table = target_table if target_table else hms_table
    
    try:
      if not uc_catalog:
        raise ValueError(f"Target UC Catalog is not given")
      elif not uc_schema:
        raise ValueError(f"Target UC Schema is not given")
      
      # Execute CREATE TABLE AS SELECT command
      spark.sql(f"""
                CREATE TABLE {uc_catalog}.{uc_schema}.{uc_table}
                DEEP CLONE {hms_catalog}.{hms_schema}.{hms_table};
                """)
      print(f"Table {hms_catalog}.{hms_schema}.{hms_table} migrated to {uc_catalog}.{uc_schema}.{uc_table} using DEEP CLONE")
      
      # Set table properties if it's delta
      if hms_table_provider.lower() == "delta":
        # Set current user and current timestamp
        current_u = spark.sql("SELECT current_user()").collect()[0][0]
        current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
        
        # Execute set table properties
        spark.sql(f"""
                  ALTER TABLE {hms_catalog}.{hms_schema}.{hms_table} 
                  SET TBLPROPERTIES ('upgraded_to' = '{uc_catalog}.{uc_schema}.{uc_table}',
                                    'upgraded_by' = 'current_u',
                                    'upgraded_at' = 'current_t')
                  """)
      else:
        print(f"Table provider is {hms_table_provider}, hence TBLPROPERTIES cannot be set")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)
        
    except (ValueError, U.AnalysisException) as e:
      if isinstance(e, ValueError):
        print(f"ValueError occurred: {e}")
      elif isinstance(e, U.AnalysisException):
        print(f"AnalysisException occurred: {e}")
