from os.path import commonprefix

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

import pyspark.sql.functions as F
import pyspark.sql.utils as U

from .common_utils import get_the_object_external_location

def get_table_description(spark: SparkSession, source_catalog: str, source_schema: str, source_table: str, table_type: str) -> list:
  """
  Get the Descriptions of HMS tables to a list
  
  Parameters:
    spark: Active SparkSession
    source_catalog: The name of the source catalog.
    source_schema: The name of the source schema
    source_table: The name(s) of the source table(s) in the given schema. Should be given as a string like "table_1, table_2". All the tables in the given source schema will be used if not given.
    table_type: The type of the source table (e.g. EXTERNAL, MANAGED)
  
  Returns:
    The list of dictionaries of the tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
  """

  try:

    if not source_schema:
      raise ValueError("Source Schema is empty")

    # List variable for table descriptions
    table_descriptions = []

    if not source_table:
      # Read all tables in the given schema to DataFrame
      source_tables = (spark
                    .sql(f"show tables in {source_catalog}.{source_schema}")
                    .select("tableName")
                    )
      # Get all tables from Hive Metastore for the given hive_metastore schema
      tables = list(map(lambda r: r.tableName, source_tables.collect()))

    elif source_table.find(","):
      # The input is a list of tables in a string
      tables = source_table.split(", ")
    else:
      # The input is a single table
      tables = [f"{source_table}"]

    # Loop through each table and run the describe command
    for table in tables:
        table_name = table
        # Read the Table description into DataFrame
        desc_df = (spark
                  .sql(f"DESCRIBE FORMATTED {source_catalog}.{source_schema}.{table_name}")
                  .filter(F.col("col_name")
                          .isin(['Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'])
                          )
                  )
                  
        # Create description dict
        desc_dict = {row['col_name']:row['data_type'] for row in desc_df.collect()}
        
        # Filter for table type
        if desc_dict["Type"].upper() == table_type:
          # Append the table_descriptions list with filtered values
          table_descriptions.append(desc_dict)
        else:
          # Append the table_descriptions list with all the values
          table_descriptions.append(desc_dict)
      
  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      raise ValueError(f"ValueError occurred: {e}")
    elif isinstance(e, U.AnalysisException):
      raise U.AnalysisException(f"AnalysisException occurred: {e}")
  return table_descriptions


def get_mounted_tables_dict(dbutils: DBUtils, table_descriptions: list) -> list:
  """
  Get the Mounted hive_metastore Tables

  Parameters:
    dbutils: Databricks Utilities
    table_descriptions: The list of dictionaries of the table descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.

  Returns:
    The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
  """
  try:
    if not table_descriptions:
      raise ValueError("table_descriptions is empty")
    
    # Get mount points into a list
    mount_points = [mount.mountPoint for mount in dbutils.fs.mounts()]
  
  except ValueError as e:
    print(f"ValueError occurred: {e}")
    raise e
  return [d for d in table_descriptions if any(mount in d["Location"] for mount in mount_points)]

def get_roll_backed_or_upgraded_table_desc_dict(table_description: list, type_check: str) -> list:
  """
  Get Roll Backed or Upgraded table(s) descriptions based on the given check type

  Parameters:
    table_description: The list of dictionaries of the table descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
    type_check: Checking upgraded or roll-backed type. Acceptable inputs: 'roll_backed' for roll backed tables, 'upgraded' for upgraded tables.
  
  Returns:
    The list of dictionaries of the roll-backed or upgaded tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
  """

  try:
    
    if not table_description:
      raise ValueError("table_description is empty")
    elif not type_check:
      raise ValueError("type_check is empty")
    elif type_check not in ["roll_backed", "upgraded"]:
      raise ValueError("type_check must be either 'roll_backed' or 'upgraded")

    # Set empty output lists
    roll_backed_tables = []
    upgraded_tables = []

    for r in table_description:
      # Iterate through the list
      
      # Extract variables
      table = r["Table"]
      table_properties = r["Table Properties"].strip('][').split(', ')
      
      # Get TBLPROPERTIES Table Properties row value into dictionary
      table_properties_dict = dict(prop.split("=") for prop in [s.split(',') for s in table_properties][0])
      
      # Check table properties dictionary for roll-backed or upgraded properties
      if "roll_backed_from" in table_properties_dict:
        # Append roll-backed tables list
        roll_backed_tables.append(table)
      if "upgraded_to" in table_properties_dict:
        # Append upgraded tables list
        upgraded_tables.append(table)

  except ValueError as e:
    print(f"ValueError occurred: {e}")
    raise e

  # Check user-defined type check for upgraded or roll_backed inputs
  if type_check == "upgraded":
    # Return upgraded tables list
    return upgraded_tables
  else:
    # Return roll-backed tables list
    return roll_backed_tables
 

def check_mountpoint_existance_as_externallocation(spark: SparkSession, dbutils: DBUtils, mounted_descriptions: list) -> None:
  """
  Checking whether the mounted tables' mount point paths exist as external location

  Parameters:
    spark: Active SparkSession
    dbutils: Databricks Utilities
    mounted_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
  
  """
  # Get the mounts
  mounts = dbutils.fs.mounts()
  try:
    if not mounted_descriptions:
      raise ValueError("mounted_descriptions is empty")
  # Iterate through the list of mounted tables' descriptions
    for r in mounted_descriptions:
        # Extract table descriptions
        mount_table_path = r["Location"]
        mount_table = r["Table"]

        # Get the mount point and mount source (path) if the table location exists as mount source (path) and it is not a DatabricksRoot path
        mount_point, mount_source = zip(*[(mount.mountPoint, mount.source) for mount in mounts if ((mount.mountPoint in mount_table_path or mount.source in mount_table_path) and mount.source != "DatabricksRoot")])

        # Read the external locations to DataFrame
        external_loc_df = (spark
                          .sql("SHOW EXTERNAL LOCATIONS")
                          )
        # Get the external location name and url if the url part of any mount source (path)
        external_loc_name, external_loc_url = zip(*[(e.name, e.url) for e in external_loc_df.collect() if mount_source[0] in e.url])

        # Check whether there is only a single external location for the existing mount point
        if len(external_loc_name) > 1:
          raise ValueError(f"There are more then 1 External location: {external_loc_name}. Please check which is needed.")

        # Determine the common prefix between the mount source url and external location url
        common_prefix = commonprefix([mount_source[0], external_loc_url[0]])
        
        # Extract mount source url folder part
        mount_source_folder = mount_source[0][len(common_prefix):]
        
        # Extract external location url folder part
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
    print(f"ValueError occurred: {e}")
    raise ValueError(e)


def migrate_hms_external_table_to_uc_external(spark: SparkSession, tables_descriptions: list, target_catalog: str, target_schema: str, target_table: str) -> None:
  """
  Migrating hive_metastore External Tables to UC External Tables without data movement using the CREATE TABLE LIKE COPY LOCATION command.

  Parameters:
    spark: Active SparkSession
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table: (optional) The name of the target Unity Catalog table. If not given UC table gets the name of the original HMS table.

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
                                    'upgraded_by' = '{current_u}',
                                    'upgraded_at' = '{current_t}',
                                    'upgraded_type' = 'hms_external_to_uc_external using COPY LOCATION')
                  """)
      else:
        print(f"Table provider is {hms_table_provider}, hence TBLPROPERTIES cannot be set. Setting table comment instead.")
        spark.sql(f"COMMENT ON TABLE {hms_catalog}.{hms_schema}.{hms_table} IS 'Upgraded to {uc_catalog}.{uc_schema}.{uc_table} by {current_u} at {current_t} via hms_external_to_uc_external using COPY LOCATION.'")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)
        
    except (ValueError, U.AnalysisException) as e:
      if isinstance(e, ValueError):
        print(f"ValueError occurred: {e}")
        raise e
      elif isinstance(e, U.AnalysisException):
        print(f"AnalysisException occurred: {e}")
        raise e


def migrate_hms_table_to_uc_external(spark: SparkSession, tables_descriptions: list, target_catalog: str, target_schema: str, target_table: str) -> None:
  """
  Migrating hive_metastore External Tables to UC External Tables without data movement using the CREATE TABLE USING LOCATION command.

  Parameters:
    spark: Active SparkSession
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. IIncluding 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table: (optional) The name of the target Unity Catalog table. If not given UC table gets the name of the original HMS table.

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
                                    'upgraded_by' = '{current_u}',
                                    'upgraded_at' = '{current_t}',
                                    'upgraded_type' = 'hms_external_to_uc_external using COPY LOCATION')
                  """)
      else:
        print(f"Table provider is {hms_table_provider}, hence TBLPROPERTIES cannot be set. Setting table comment instead.")
        spark.sql(f"COMMENT ON TABLE {hms_catalog}.{hms_schema}.{hms_table} IS 'Upgraded to {uc_catalog}.{uc_schema}.{uc_table} by {current_u} at {current_t} via  hms_external_to_uc_external using COPY LOCATION.'")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)
        
    except (ValueError, U.AnalysisException) as e:
      if isinstance(e, ValueError):
        print(f"ValueError occurred: {e}")
        raise e
      elif isinstance(e, U.AnalysisException):
        print(f"AnalysisException occurred: {e}")
        raise e


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
        print(f"{hms_catalog}.{hms_schema}.{hms_table} has been created to {uc_catalog}.{uc_schema}.{uc_table} correctly")
      else:
        print(f"{hms_catalog}.{hms_schema}.{hms_table} count {hms_count} not equals with {uc_catalog}.{uc_schema}.{uc_table} count {uc_count}. Diff: {hms_count-uc_count}")
        raise AssertionError("Data volumes are not matching")
    else:
      print(f"{hms_catalog}.{hms_schema}.{hms_table} schema not equals with {uc_catalog}.{uc_schema}.{uc_table} schema. Diff: {list(set(hms_schema_struct)-set(uc_schema_struct))}")
      raise AssertionError("Schemas are not matching")

  except (ValueError, AssertionError) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
      raise e
    elif isinstance(e, AssertionError):
      print(f"AssertionError occurred: {e}")
      raise e


def sync_hms_table_to_uc_external(spark: SparkSession, source_schema: str, source_table: str, target_catalog: str, target_schema: str) -> None:
  """
  Using the SYNC TABLE command to upgrade individual or list of hive_metastore external or managed table(s) to external table(s) in Unity Catalog.
  You can use it to create the new table(s) in Unity Catalog from the existing hive_metastore table(s) as well as update the Unity Catalog table(s) when the source tables in hive_metastore are changed.
  
  Parameters:
    spark: Active SparkSession
    source_schema: The name of the source hive_metastore schema
    source_table: The name(s) of the source hive_metastore table(s) in the schema. Should be given as a string like "table_1, table_2".
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
      
    # Create table list from a string list
    if source_table.find(","):
      # The input is a list of tables in a string
      table_list = source_table.split(", ")
    else:
      # The input is a single table
      table_list = source_table

    # Iterate through the table list
    for table in table_list:

      # Set hive_metastore variables
      hms_catalog = "hive_metastore"
      hms_schema = source_schema
      hms_table = table
      
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
      raise e
    elif isinstance(e, U.AnalysisException):
      print(f"AnalysisException occurred: {e}")
      raise e


def sync_hms_schema_to_uc_schema(spark: SparkSession, source_schema: str, target_catalog: str, target_schema: str) -> None:
  """
  Using the SYNC SCHEMA command to upgrade all eligible hive_metastore external tables in the given schema(s) to external tables in a Unity Catalog schema(s).
  You can use it to create new tables in Unity Catalog from existing hive_metastore tables as well as update the Unity Catalog tables when the source tables in hive_metastore are changed.
  
  Parameters:
    spark: Active SparkSession
    source_schema: The name(s) of the source hive_metastore schema(s). Should be given as a string like "schema_1, schema_2".
    target_catalog: The name of the target Unity Catalog catalog.
    target_schema: (optional) The name of the target Unity Catalog schema. Only applicable if a single schema is given in the source_schema.

  """
  try:

    if not source_schema:
      raise ValueError("source_schema input string is empty")
    elif not target_catalog:
      raise ValueError("target_catalog input string is empty")
      
    # Create schema list from a string list
    if source_schema.find(","):
      # The input is a list of tables in a string
      schema_list = source_schema.split(", ")
    else:
      # The input is a single table
      schema_list = source_schema
    
    # Iterate through the list of schemas
    for schema in schema_list:
      
      # Set hive_metastore variables
      hms_catalog = "hive_metastore"
      hms_schema = schema
      
      # Set UC variables
      uc_catalog = target_catalog
      uc_schema = schema
        
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
        for table in synced_df.collect():
          
          if table["status_code"] == 'SUCCESS':
            
            print(f"Table {hms_catalog}.{hms_schema}.{table['source_name']} has successfully synced to {uc_catalog}.{uc_schema}.{table['target_name']}")
            
            # Check the match of HMS and UC tables that have been successfully synced
            check_equality_of_hms_uc_table(spark, hms_schema, table["source_name"], uc_catalog, uc_schema, table["target_name"])
          
          else:

            print(f"Table {hms_catalog}.{hms_schema}.{table['source_name']} has failed to sync to {uc_catalog}.{uc_schema}.{table['target_name']} with status code {table['status_code']} and description {table['description']}")

      else:
        # Raise error
        for table in sync_dry.collect():

           print(f"Table {hms_catalog}.{hms_schema}.{table['source_name']} has failed to sync to {uc_catalog}.{uc_schema}.{table['target_name']} with status code {table['status_code']} and description {table['description']}")
        
        raise ValueError(f"There is no eligible table in {hms_catalog}.{hms_schema} to SYNC to Unity Catalog.")
      
  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
      raise e
    elif isinstance(e, U.AnalysisException):
      print(f"AnalysisException occurred: {e}")
      raise e


def ctas_hms_table_to_uc_managed(spark: SparkSession, tables_descriptions: list, target_catalog: str, target_schema: str, target_table: str = "", select_statement: str = "", partition_clause: str = "", options_clause: str = "") -> None:
  """
  Migrating hive_metastore Table(s) to UC Table(s) either EXTERNAL OR MANAGED with data movement using the CREATE TABLE AS SELECT (CTAS) command.

  Parameters:
    spark: Active SparkSession
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table: (optional) The name of the target Unity Catalog table. If not given UC table gets the name of the original HMS table.
    select_statement: (optional) User-defined select statement. SELECT and FROM don't need to define.
    partition_clause: (optional) An optional clause to partition the table by a subset of columns.
    options_cluse: (optional) User-defined options including comment and tblproperties.

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
    hms_table_type = r["Type"]
    
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
      
      # Set the base CTAS statement
      ctas_stmt = f"CREATE TABLE {uc_catalog}.{uc_schema}.{uc_table}"

      if partition_clause:
        # Add PARTITIONED BY clause
        ctas_stmt += f" PARTITIONED BY ({partition_clause})"
      
      if options_clause:
        # Add OPTIONS clause
        ctas_stmt += f" OPTIONS ({options_clause})"
      
      if select_statement:
        # Add user-defined SELECT statement
        ctas_stmt += f" AS SELECT {select_statement} FROM {hms_catalog}.{hms_schema}.{hms_table};"
      else:
        # SELECT all columns as is
        ctas_stmt += f" AS SELECT * FROM {hms_catalog}.{hms_schema}.{hms_table};"

      # Execute CREATE TABLE AS SELECT command
      spark.sql(ctas_stmt)
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
                                    'upgraded_by' = '{current_u}',
                                    'upgraded_at' = '{current_t}',
                                    'upgraded_type' = 'hms_{hms_table_type}_to_uc_managed using CTAS')
                  """)
      else:
        print(f"Table provider is {hms_table_provider}, hence TBLPROPERTIES cannot be set. Setting table comment instead.")
        spark.sql(f"COMMENT ON TABLE {hms_catalog}.{hms_schema}.{hms_table} IS 'Upgraded to {uc_catalog}.{uc_schema}.{uc_table} by {current_u} at {current_t} via hms_{hms_table_type}_to_uc_managed using CTAS.'")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)
        
    except (ValueError, U.AnalysisException) as e:
      if isinstance(e, ValueError):
        print(f"ValueError occurred: {e}")
        raise e
      elif isinstance(e, U.AnalysisException):
        print(f"AnalysisException occurred: {e}")
        raise e


def clone_hms_table_to_uc_managed(spark: SparkSession, tables_descriptions: list, target_catalog: str, target_schema: str, target_table: str = "") -> None:
  """
  Migrating hive_metastore Table(s) to UC Table(s) either EXTERNAL OR MANAGED with data movement using the DEEP CLONE command.

  Parameters:
    spark: Active SparkSession
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table:(optional) The name of the target Unity Catalog table. If not given UC table gets the name of the original HMS table.

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
    hms_table_type = r["Type"]
    
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
                                    'upgraded_by' = '{current_u}',
                                    'upgraded_at' = '{current_t}',
                                    'upgraded_type' = 'hms_{hms_table_type}_to_uc_managed using CLONE')
                  """)
      else:
        print(f"Table provider is {hms_table_provider}, hence TBLPROPERTIES cannot be set. Setting table comment instead.")
        spark.sql(f"COMMENT ON TABLE {hms_catalog}.{hms_schema}.{hms_table} IS 'Upgraded to {uc_catalog}.{uc_schema}.{uc_table} by {current_u} at {current_t} via hms_{hms_table_type}_to_uc_managed using CLONE.'")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)
        
    except (ValueError, U.AnalysisException) as e:
      if isinstance(e, ValueError):
        print(f"ValueError occurred: {e}")
        raise e
      elif isinstance(e, U.AnalysisException):
        print(f"AnalysisException occurred: {e}")
        raise e


def create_or_replace_external_table(spark: SparkSession, dbutils: DBUtils,tables_descriptions: list, target_catalog: str, target_schema: str, target_table: str) -> None:
  """
  Create or Replace External Table(s) without data movement using the CREATE OR REPLACE TABLE LOCATION command.

  Parameters:
    spark: Active SparkSession
    dbutils: Databricks Utilities
    tables_descriptions: The list of dictionaries of the UC tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
    target_catalog: The name of the target catalog
    target_schema: The name of the target schema
    target_table: (optional) The name of the target table. If not given, the table gets the name of the source table. Only applicable if there is a single source table given. 

  """
  try:

    if not tables_descriptions:
      raise ValueError("tables_descriptions input list is empty")
    elif not target_catalog:
      raise ValueError("target_catalog input list is empty")
    
    # Iterate through the tables' descriptions list of dictionaries
    for r in tables_descriptions:
      
      # Set source variables
      source_catalog = r["Catalog"]
      source_schema = r["Database"]
      source_table = r["Table"]
      source_table_provider = r["Provider"]
      source_table_location = r["Location"]

      # Set target variables
      target_schema = target_schema if target_schema else source_schema
      target_table = target_table if target_table else source_table
      target_table_location = get_the_object_external_location(dbutils, "table", source_table_location)
        
      # Set Create External table using CREATE OR REPLACE LOCATION statement
      statement = f"CREATE OR REPLACE TABLE {target_catalog}.{target_schema}.{target_table} USING {source_table_provider} LOCATION '{target_table_location}' AS SELECT * FROM {source_catalog}.{source_schema}.{source_table};"

      # Execute the statement
      spark.sql(statement)

      print(f"External Table {target_catalog}.{target_schema}.{target_table} has been created from {source_catalog}.{source_schema}.{source_table} using the CREATE OR REPLACE TABLE LOCATION")
      
      # Set table properties if it's delta
      if source_table_provider.lower() == "delta":
        # Set current user and current timestamp
        current_u = spark.sql("SELECT current_user()").collect()[0][0]
        current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
        # Extract the notebook name
        notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[-1]
        
        # Execute set table properties
        if source_catalog != "hive_metastore":
          # Set table properties to created
          spark.sql(f"""
                    ALTER TABLE {source_catalog}.{source_schema}.{source_table} 
                    SET TBLPROPERTIES ('created_to' = '{target_catalog}.{target_schema}.{target_table}',
                                      'created_by' = '{current_u}',
                                      'created_at' = '{current_t}',
                                      'created_type' = '{notebook_name} CREATE OR REPLACE TABLE LOCATION')
                    """)
        else:
          # Set table properties to migrated
          spark.sql(f"""
                    ALTER TABLE {source_catalog}.{source_schema}.{source_table} 
                    SET TBLPROPERTIES ('migrated_to' = '{target_catalog}.{target_schema}.{target_table}',
                                      'migrated_by' = '{current_u}',
                                      'migrated_at' = '{current_t}',
                                      'migrated_type' = '{notebook_name} CREATE OR REPLACE TABLE LOCATION')
                    """)

      else:
        print(f"Table provider is {source_table_provider}, hence TBLPROPERTIES cannot be set. Setting table comment instead.")
        if source_catalog != "hive_metastore":
          # Set comment to created
          comment = f"Created {hms_catalog}.{uc_schema}.{uc_table} via {notebook_name} using CREATE OR REPLACE TABLE LOCATION."
        else:
          # Set comment to migrated
          comment = f"Migrated {hms_catalog}.{uc_schema}.{uc_table} via {notebook_name} using CREATE OR REPLACE TABLE LOCATION."

        spark.sql(f"COMMENT ON TABLE {source_catalog}.{source_schema}.{source_table} IS '{comment}'")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, target_schema, target_table, source_catalog, source_schema, source_table)

      # Reset target variables
      target_table = ""
      target_table_location = ""
          
  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
      raise e
    elif isinstance(e, U.AnalysisException):
      print(f"AnalysisException occurred: {e}")
      raise e
