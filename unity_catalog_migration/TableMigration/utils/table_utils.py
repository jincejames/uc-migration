from os.path import commonprefix
from dataclasses import dataclass
import traceback

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

import pyspark.sql.functions as F
import pyspark.sql.utils as U

from .common_utils import get_the_object_external_location, get_create_table_stmt


@dataclass
class SyncStatus:
    requested_object_name: str = None
    source_catalog_name: str = None
    target_catalog_name: str = None
    source_schema_name: str = None
    target_schema_name: str = None
    table_location: str = None
    source_object_full_name: str = None
    target_object_full_name: str = None
    source_object_type: str = None
    source_table_format: str = None
    source_table_schema: str = None
    source_view_text: str = None
    sync_status_code: str = None
    sync_status_description: str = None


@dataclass
class ConvertParquetToDeltaStatus:
    catalog: str = None
    schema: str = None
    table: str = None
    full_name: str = None
    status_code: str = None
    status_description: str = None


def get_table_description(spark: SparkSession, source_catalog: str, source_schema: str, source_table: str, table_type: str) -> list:
  """
  Get the Descriptions of HMS tables/views to a list
  
  Parameters:
    spark: Active SparkSession
    source_catalog: The name of the source catalog.
    source_schema: The name of the source schema
    source_table: The name(s) of the source table(s) in the given schema. Should be given as a string like "table_1, table_2". All the tables in the given source schema will be used if not given.
    table_type: The type of the source table (e.g. EXTERNAL, MANAGED)
  
  Returns:
    The list of dictionaries of the tables' descriptions.
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
        full_table_name = f"{source_catalog}.{source_schema}.{table_name}"
        print(f"Getting {full_table_name} descriptions using DESCRIBE FORMATTED command")
        # Read the Table description into DataFrame
        desc_df = (spark
                  .sql(f"DESCRIBE FORMATTED {full_table_name}")
                  )
        
        # Create description dict
        desc_dict = {row['col_name']:row['data_type'] for row in desc_df.collect()}
        
        # Filter table type
        # If table type view, filter for view type
        if table_type == "view":
          
          if desc_dict["Type"].lower() == "view":
            print(f"{desc_dict['Table']} is a view, appending...")
            # Append table_descriptions
            table_descriptions.append(desc_dict)
        
        # Filter table type to non-view  
        elif desc_dict['Type'].lower() != "view": 
            print(f"{desc_dict['Table']} is a table, appending...")
            # Append table_descriptions
            table_descriptions.append(desc_dict)
      
  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
      raise ValueError(f"ValueError occurred: {e}")
    elif isinstance(e, U.AnalysisException):
      print(f"ValueError occurred: {e}")
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
          if mount_source_folder and external_loc_url[0]:
            print(f"Mount point folder {mount_source_folder} exists in External Location {external_loc_url[0]}")
        else:
          raise ValueError(f"Mount point: {mount_point[0]} with source path {mount_source[0]} doesn't part of the external location {external_loc_name[0]} with path {external_loc_url[0]}")

  except ValueError as e:
    print(f"ValueError occurred: {e}")
    raise ValueError(e)


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

    print(f"Check source and target tables equality...")

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


def ctas_hms_table_to_uc(spark: SparkSession, dbutils: DBUtils, table_details: dict, target_catalog: str, target_schema: str, target_table: str = "", select_statement: str = "", partition_clause: str = "", options_clause: str = "", target_location: str = "") -> SyncStatus:
  """
  Migrating hive_metastore Table(s) to UC Table(s) either EXTERNAL OR MANAGED with data movement using the CREATE OR REPLACE TABLE AS SELECT (CTAS) command.

  Parameters:
    spark: Active SparkSession
    dbutils: Databricks Utilities
    table_details: The dictionary of the table descriptions.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table: (optional) The name of the target Unity Catalog table. If not given UC table gets the name of the original HMS table.
    select_statement: (optional) User-defined select statement. SELECT and FROM don't need to define.
    partition_clause: (optional) An optional clause to partition the table by a subset of columns.
    options_cluse: (optional) User-defined options including comment and tblproperties.
    target_location (optional) The target location path

  Returns:
    A SyncStatus object that contains the sync status of the table

  """
  try:

    # Set the sync_status object
    sync_status = SyncStatus()

    if not table_details:
      raise ValueError("table_details input dict is empty")
    elif not target_catalog:
      raise ValueError(f"target_catalogis not given")

    # Set hive_metastore variables
    hms_catalog = table_details["Catalog"]
    sync_status.source_catalog_name = hms_catalog
    hms_schema = table_details["Database"]
    sync_status.source_schema_name = hms_schema
    hms_table = table_details["Table"]
    sync_status.requested_object_name = hms_table
    hms_table_provider = table_details["Provider"]
    sync_status.source_table_format = hms_table_provider
    hms_table_type = table_details["Type"]
    sync_status.source_object_type = hms_table_type

    # Get HMS table DDL
    hms_table_ddl = get_create_table_stmt(spark, hms_catalog, hms_schema, hms_table)

    # Extract hms table ddl clauses
    using = hms_table_ddl.using
    options = hms_table_ddl.options
    partition_by = hms_table_ddl.partition_by
    cluster_by = hms_table_ddl.cluster_by
    location = hms_table_ddl.location
    comment = hms_table_ddl.comment
    table_properties = hms_table_ddl.table_properties

    # Set UC variables
    uc_catalog = target_catalog
    uc_schema = target_schema if target_schema else hms_schema
    # I target UC table is not given, use the hive_metastore table
    uc_table = target_table if target_table else hms_table

    # Set HMS and UC full table name
    hms_full_name = f"{hms_catalog}.{hms_schema}.{hms_table}"
    sync_status.source_object_full_name = hms_full_name
    uc_full_name = f"{uc_catalog}.{uc_schema}.{uc_table}"
    sync_status.target_object_full_name = uc_full_name

    # Clear all caches first
    spark.sql(f"REFRESH TABLE {hms_full_name}")

    print(f"Migrating with CTAS ...")

    # Set the base CTAS statement
    create_table_as_statement = f"CREATE OR REPLACE TABLE {uc_full_name}"

    # using
    if using:
        create_table_as_statement += f" USING {using}"
    elif hms_table_provider:
        create_table_as_statement += f" USING {hms_table_provider}"

    # table_clauses
    
    # OPTIONS
    if options_clause:
      create_table_as_statement += f" OPTIONS ({options_clause})"
    elif options:
        create_table_as_statement += f" OPTIONS ({options})"

    # PARTITION BY or CLUSTER BY
    if partition_clause:
      create_table_as_statement += f" PARTITIONED BY ({partition_clause})"
    elif partition_by:
        create_table_as_statement += f" PARTITIONED BY ({partition_by})"
    elif cluster_by:
        create_table_as_statement += f" CLUSTER BY ({cluster_by})"

    # LOCATION
    if target_location:
      create_table_as_statement += f" LOCATION '{target_location}'"

    # COMMENT
    if comment:
        create_table_as_statement += f" COMMENT '{comment}'"

    # TBLPROPERTIES
    if table_properties:
        create_table_as_statement += f" TBLPROPERTIES({table_properties})"

    # SELECT STATEMENT
    if not select_statement:
      select_statement = f" AS SELECT * FROM {hms_full_name};"

    # Final CTAS
    create_table_as_statement += select_statement

    print(f"Executing '{create_table_as_statement}' ...")

    spark.sql(create_table_as_statement)
    print(f"Table {hms_full_name} migrated to {uc_full_name} with CTAS")

    # Set table properties if it's delta
    if hms_table_provider.lower() == "delta":
      print("Setting TBLPROPERTIES ...")
      # Set current user and current timestamp
      current_u = spark.sql("SELECT current_user()").collect()[0][0]
      current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
      # Extract the notebook name
      notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[-1]
      
      # Set table properties statement
      tblproperties_stmt = f"""
                          ALTER TABLE {hms_full_name} 
                          SET TBLPROPERTIES ('upgraded_to' = '{uc_catalog}.{uc_schema}.{uc_table}',
                                            'upgraded_by' = '{current_u}',
                                            'upgraded_at' = '{current_t}',
                                            'upgraded_type' = '{notebook_name} using CTAS'
                                            )
                          """
      # Execute set table properties
      print(f"Executing 'tblproperties_stmt' ...")
      spark.sql(tblproperties_stmt)
    else:
      print(f"Table provider is {hms_table_provider}, hence TBLPROPERTIES cannot be set. Setting table comment instead.")
      comment_stmt = f"COMMENT ON TABLE {hms_full_name} IS 'Upgraded to {uc_full_name} by {current_u} at {current_t} via {notebook_name} using CTAS.'"
      print(f"Executing '' ...")
      spark.sql(comment_stmt)

    # Check the match of hive_metastore and UC tables
    check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)

    # Set sync status
    sync_status.sync_status_description = f"Table has been created using CTAS statement {create_table_as_statement}"
    sync_status.sync_status_code = "SUCCESS"

    print(f"Migrating table via CTAS has successfully finished")
      
  except Exception:
    sync_status.sync_status_code = "FAILED"
    sync_status.sync_status_description = str(traceback.format_exc())
    print(str(traceback.format_exc()))
  
  return sync_status


def clone_hms_table_to_uc(spark: SparkSession, dbutils: DBUtils, table_details: dict, target_catalog: str, target_schema: str, target_table: str = "", target_location: str = "") -> SyncStatus:
  """
  Migrating hive_metastore Table(s) to UC Table(s) either EXTERNAL OR MANAGED with data movement using the DEEP CLONE command.

  Parameters:
    spark: Active SparkSession
    dbutils: Databricks Utilities
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table:(optional) The name of the target Unity Catalog table. If not given UC table gets the name of the original HMS table.
  Returns:
    A SyncStatus object that contains the sync status of the table
  
  """
  try:

    # Set the sync_status object
    sync_status = SyncStatus()

    if not table_details:
        raise ValueError("table_details input list is empty")
    elif not target_catalog:
        raise ValueError("target_catalog input string is empty")

    # Set hive_metastore variables
    hms_catalog = table_details["Catalog"]
    sync_status.source_catalog_name = hms_catalog
    hms_schema = table_details["Database"]
    sync_status.source_schema_name = hms_schema
    hms_table = table_details["Table"]
    sync_status.requested_object_name = hms_table
    hms_table_provider = table_details["Provider"]
    sync_status.source_table_format = hms_table_provider
    hms_table_type = table_details["Type"]
    sync_status.source_object_type = hms_table_type
    
    # Set UC variables
    uc_catalog = target_catalog
    sync_status.target_catalog_name = uc_catalog
    uc_schema = target_schema if target_schema else hms_schema
    sync_status.target_schema_name = uc_schema
    # I target UC table is not given, use the hive_metastore table
    uc_table = target_table if target_table else hms_table

    # Set HMS table full name
    hms_full_name = f"{hms_catalog}.{hms_schema}.{hms_table}"
    sync_status.source_object_full_name = hms_full_name
    # Set UC table full name
    uc_full_name = f"{uc_catalog}.{uc_schema}.{uc_table}"
    sync_status.target_object_full_name = uc_full_name

    # Clear all caches first
    spark.sql(f"REFRESH TABLE {hms_full_name}")

    # Set DEEP CLONE COMMAND
    deep_clone_statement = f"""
                        CREATE OR REPLACE TABLE {uc_full_name}
                        DEEP CLONE {hms_full_name}
                        """

    print("Migrating with CLONE ...")
    
    # If target location is given, add LOCATION to the deep clone statement and create external table
    if target_location:
        print(f"Migrating {hms_table_type} {hms_full_name} table to External {uc_full_name} in location {target_location} using DEEP CLONE")
        deep_clone_statement += f" LOCATION '{target_location}'"
        
        sync_status.sync_status_description = f"External table has been created using DEEP CLONE"

        print(f"Executing '{deep_clone_statement}'")
        spark.sql(deep_clone_statement)

    else:
        print(f"Migrating {hms_table_type} {hms_full_name} table to Managed {uc_full_name} in location {target_location} using DEEP CLONE")
          
        sync_status.sync_status_description = f"Managed table has been created using DEEP CLONE"

        print(f"Executing '{deep_clone_statement}'")
        spark.sql(deep_clone_statement)

    # Set current user and current timestamp
    current_u = spark.sql("SELECT current_user()").collect()[0][0]
    current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
    # Extract the notebook name
    notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[-1]


    # Set table properties if it's delta
    if hms_table_provider.lower() == "delta":
        print("Setting TBLPROPERTIES ... ")
        set_table_properties_statement = f"""
                                        ALTER TABLE {hms_full_name} 
                                        SET TBLPROPERTIES ('cloned_to' = '{uc_full_name}',
                                                        'cloned_by' = '{current_u}',
                                                        'cloned_at' = '{current_t}',
                                                        'cloned_type' = '{sync_status.sync_status_description} via {notebook_name}')
                                        """
        print(f"Executing '{set_table_properties_statement}' ...")
        # Execute set table properties
        spark.sql(set_table_properties_statement)

    # Check the match of hive_metastore and UC tables
    check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)

    sync_status.sync_status_code = "SUCCESS"

    print(sync_status.sync_status_description)
  
  except Exception:
      sync_status.sync_status_code = "FAILED"
      sync_status.sync_status_description = str(traceback.format_exc())
      print(str(traceback.format_exc()))
  return sync_status


def convert_parquet_table_to_delta(spark: SparkSession, dbutils: DBUtils, table_details: dict) -> ConvertParquetToDeltaStatus:
    """
    Converts existing Parquet table(s) to a Delta table(s) in-place.
    It doesn't convert the STATISTICS of the Parquet table. Since bypassing the statistics collection during the conversion process and finish conversion faster. After the table is converted to Delta Lake, you can use OPTIMIZE ZORDER BY to reorganize the data layout and generate statistics.

    Parameters:
        spark: The active SparkSession
        dbutils: Databricks Utilities
        table_details: Dictionary of the table details
    Returns:
        A ConvertParquetToDeltaStatus that contains the convert to parquet table statuses
    """
    try:

        # Set convert_parquet_to_delta_status
        convert_parquet_to_delta_status = ConvertParquetToDeltaStatus()

        if not table_details:
            raise ValueError("table_description input list is empty")

        # Extract the table' description values
        catalog = table_details["Catalog"]
        convert_parquet_to_delta_status.catalog = catalog
        schema = table_details["Database"]
        convert_parquet_to_delta_status.schema = schema
        table = table_details["Table"]
        convert_parquet_to_delta_status.table = table
        table_provider = table_details["Provider"]
        table_partitions = (
            table_details["# Partition Information"]
            if "# Partition Information" in table_details
            else ""
        )
        table_serde = (
            table_details["Serde Library"] if "Serde Library" in table_details else ""
        )

        # Set full table name
        full_table_name = f"{catalog}.{schema}.{table}"
        convert_parquet_to_delta_status.full_name = full_table_name

        # Clear all caches first
        spark.sql(f"REFRESH TABLE {full_table_name}")

        print(f"Converting Parquet to Delta ...")

        # Check if table provider hive and it is parquet
        if table_provider.lower() == "hive":
            if (
                table_serde
                == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            ):
                print(f"Source table format is Hive with Serde Parquet. Changing to Parquet format..")                        
                table_provider = "PARQUET"

        if table_provider.lower() != "parquet":
            # Raise error if its not a Parquet table
            convert_parquet_to_delta_status.status_description = f"Table is {table_provider}, hence not a Parquet table. Conversion is not supported"
            raise ValueError(
                f"Table is {table_provider}, hence not a Parquet table. Conversion is not supported"
            )

        # Set convert to delta statement
        convert_to_delta_stmt = f"CONVERT TO DELTA {full_table_name} NO STATISTICS"

        # Execute convert to delta statement
        print(f"Executing '{convert_to_delta_stmt}' ...")
        spark.sql(convert_to_delta_stmt)

        print("Setting TBLPROPERTIES...")
        # Set current user and current timestamp
        current_u = spark.sql("SELECT current_user()").collect()[0][0]
        current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
        # Extract the notebook name
        notebook_name = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
            .rsplit("/", 1)[-1]
        )

        # Execute set table properties
        set_delta_table_properties_statement = f"""
                                            ALTER TABLE {full_table_name} 
                                            SET TBLPROPERTIES ('converted_to' = 'DELTA',
                                                              'converted_by' = '{current_u}',
                                                              'converted_at' = '{current_t}',
                                                              'converted_type' = '{notebook_name} using CONVERT TO DELTA command')
                                            """
        print(f"Executing '{set_delta_table_properties_statement}' ...")
        spark.sql(set_delta_table_properties_statement)

        # Do a final metadata sync
        print(f"Doing a final MSCK REPAIR and/or REFRESH on '{full_table_name}'")
        if catalog != "hive_metastore" and table_provider == "delta":
            msck_repair_sync_metadata_statement = (
                f"MSCK REPAIR TABLE {full_table_name} SYNC METADATA"
            )
            print(f"Executing '{msck_repair_sync_metadata_statement}'")
            spark.sql(msck_repair_sync_metadata_statement)
        else:
            if catalog == "hive_metastore" and table_partitions:
              msck_repair_statement = f"MSCK REPAIR TABLE {full_table_name}"
              print(f"Executing '{msck_repair_statement}'")
              spark.sql(msck_repair_statement)

        refresh_statement = f"REFRESH TABLE {full_table_name}"
        print(f"Executing '{refresh_statement}'")
        spark.sql(refresh_statement)

        print(
            f"Convert Parquet to Delta is successfully finished for table: {full_table_name}"
        )
        convert_parquet_to_delta_status.status_description = (
            "Convert Parquet to Delta is successfully finished"
        )
        convert_parquet_to_delta_status.status_code = "SUCCESS"

    except Exception:
        print(
            f"Convert Parquet to Delta is failed for table: {full_table_name} with error: {str(traceback.format_exc())}"
        )
        convert_parquet_to_delta_status.status_code = "FAILED"
        convert_parquet_table_to_delta.status_description = str(traceback.format_exc())

    return convert_parquet_to_delta_status


def sync_view(spark: SparkSession, view_details: dict, target_catalog: str) -> SyncStatus:
    """
    Sync views between catalogs
    
    Parameters:
        spark: The active SparkSession
        table_description: List of table(s)' descriptions. Could contain single or multiple table(s)' descriptions
        target_catalog: The name of the target catalog.
    
    Returns:
        SyncStatus object that contains the sync status of the view

    """
    try:

        # Set sync status variable from the SyncStatus object
        sync_status = SyncStatus()

        if not view_details:
            raise ValueError("view_details input dict is empty")
        elif not target_catalog:
            raise ValueError("target catalog input string is empty")
            
        #Extract view variables and set sync status
        source_catalog = view_details["Catalog"]
        sync_status.source_catalog_name = source_catalog
        source_schema = view_details["Database"]
        sync_status.source_schema_name = source_catalog
        source_view = view_details["Table"]
        sync_status.requested_object_name = source_view
        source_view_definition = view_details["View Text"]
        sync_status.source_view_text = source_view_definition

        print(f"Syncing view...")
        # TODO: Check the schemas, tables in the view definition and add the catalog name before them as a three-level namespace. Use system information schema.
        # Currently, replacing hive_metastore with the target catalog
        print(f"Replacing {source_catalog} with {target_catalog}")
        source_view_definition = source_view_definition.replace(source_catalog, target_catalog)

        # Set target variables and sync status
        target_schema = source_schema
        sync_status.target_schema_name = target_schema
        target_view = source_view

        # Set full view name and sync status
        source_full_name = f"{source_catalog}.{source_schema}.{source_view}"
        sync_status.source_object_full_name = source_full_name
        target_full_name = f"{target_catalog}.{source_schema}.{target_view}"
        sync_status.target_object_full_name = target_full_name

        print(f"Syncing view '{source_full_name}' to '{target_full_name}'")

        # Clear all caches first
        spark.sql(f"REFRESH TABLE {source_full_name}")

        # Set catalog
        use_catalog_stmt = f"USE CATALOG {target_catalog}"
        print(f"Executing {use_catalog_stmt}...")
        spark.sql(use_catalog_stmt)

        # Check if target view exists
        target_view_exists = True if [t for t in spark.sql(f"SHOW VIEWS in {target_schema}").filter((F.col("isTemporary") == 'false') & (F.col("viewName") == f'{target_view}')).collect()] else False

        # If target view exists, checking view definitions
        if target_view_exists:

            print("Target view exists. Checking for view definition change...")
            # Get target view description
            target_table_details = (spark.sql(f"DESCRIBE TABLE EXTENDED {target_full_name}"))

            # Get target view text
            target_view_definition = (target_table_details
                                      .filter("col_name = 'View Text'")
                                      .select("data_type")
                                      .collect()[0]["data_type"]
                )
            if not target_view_definition:
                raise ValueError(
                    f"View '{target_full_name}' text could not be found using DESCRIBE TABLE EXTENDED"
                )

            target_view_creation = (target_table_details
                                    .filter("col_name = 'Created Time'")
                                    .select("data_type")
                                    .collect()[0]["data_type"]
                                    )

            # If view definitions are different, set the sync status message to sync
            if target_view_definition != source_view_definition:
                print(
                    f"Target view '{target_full_name}' text '{target_view_definition}' does not match with source view '{source_full_name}' text '{source_view_definition}', marking '{target_full_name}' for drop"
                )
                sync_status.sync_status_description = f"Replace view due to differences in view text"
            else:
                print(
                    f"Views '{source_full_name}' and '{target_full_name}' have the same view text '{source_view_definition}'"
                )
                sync_status.sync_status_description = f"Views have the same view definitions, hence they are in sync"

                sync_status.sync_status_code = "SUCCESS"

                print(sync_status.sync_status_description)

                return sync_status

        # If target view doesn't exist, set sync status to creating
        if not target_view_exists:
            sync_status.sync_status_description = f"Create view since it doesn't exist."
            print(sync_status.sync_status_description)

        # Set the create or replace view statement
        create_or_replace_view_stmt = f"CREATE OR REPLACE VIEW {target_full_name} AS {source_view_definition}"
        
        print(f"Executing '{create_or_replace_view_stmt}'...")
        # Execute the create or replace view statement
        spark.sql(create_or_replace_view_stmt)

        print(f"View {source_full_name} has been created to {target_full_name} with definition {source_view_definition}")

        # Clear all caches first
        spark.sql(f"REFRESH TABLE {target_full_name}")

        sync_status.sync_status_code = "SUCCESS"

    except Exception:
        print(f"View {source_full_name} has been failed to create {target_full_name} with definition {source_view_definition}. With error: {str(traceback.format_exc())}")
        
        sync_status.sync_status_description = str(traceback.format_exc())
        sync_status.sync_status_code = "FAILED"

    return sync_status


def sync_table(spark: SparkSession, dbutils: DBUtils, table_details: dict, target_catalog: str, target_schema: str, target_table: str) -> SyncStatus:
  """
  Syncs table(s) as External Table(s) between two catalogs.

  Parameters:
    spark: Active SparkSession
    dbutils: Databricks Utilities
    table_details: The details of the table
    target_catalog: The name of the target catalog
    target_schema: The name of the target schema
    target_table: (optional) The name of the target table. If not given, the table gets the name of the source table. Only applicable if there is a single source table given.
  
  Returns:
    SyncStatus object that contains the synced 

  """
  try:

    # Set sync status variable from the SyncStatus object
    sync_status = SyncStatus()

    if not table_details:
      raise ValueError("table_details input dict is empty")
    elif not target_catalog:
      raise ValueError("target_catalog input list is empty")

    # Set source variables
    source_catalog = table_details["Catalog"]
    source_schema = table_details["Database"]
    source_table = table_details["Table"]
    source_table_type = table_details["Type"]
    source_table_provider = table_details["Provider"]
    source_table_location = table_details["Location"]
    source_table_partitions = table_details["# Partition Information"] if "# Partition Information" in table_details else ""
    source_table_serde = table_details["Serde Library"] if "Serde Library" in table_details else ""

    # Set default sync status attributes
    sync_status.requested_object_name=source_table,
    sync_status.source_catalog_name=source_catalog,
    sync_status.target_catalog_name=target_catalog,
    sync_status.source_schema_name=source_schema,
    sync_status.source_object_type=source_table_type,
    sync_status.source_table_format=source_table_provider

    # Set target variables
    target_schema = target_schema if target_schema else source_schema
    sync_status.target_schema_name = target_schema
    target_table = target_table if target_table else source_table
    target_table_location = get_the_object_external_location(dbutils, "table", source_table_location)
    sync_status.table_location = target_table_location

    # Set source and target tables full name
    source_full_name = f"{source_catalog}.{source_schema}.{source_table}"
    sync_status.source_object_full_name = source_full_name
    target_full_name = f"{target_catalog}.{target_schema}.{target_table}"
    sync_status.target_object_full_name = target_full_name

    # Clear all caches first
    spark.sql(f"REFRESH TABLE {source_full_name}")

    print(f"Syncing table...")

    # Process known hive serdes
    if source_table_provider == "hive":
        if not source_table_serde:
            raise ValueError(
                f"Table '{source_full_name}' format is 'hive' but serde could not be found"
            )
        elif (
            source_table_serde == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        ):
            print(f"Source table format is Hive with Serde Parquet. Changing to Parquet format..")
            source_table_provider = "PARQUET"
            sync_status.source_table_format = source_table_provider
        elif s_serde == "org.apache.hadoop.hive.serde2.avro.AvroSerDe":
            print(f"Source table format is Hive with Serde Avro. Changing to Avro format..")
            source_table_provider = "AVRO"
            sync_status.source_table_format = source_table_provider
        else:      
          raise NotImplementedError(
              f"Table '{source_full_name}' of format 'hive' and serde '{source_table_serde}' is not yet supported"
          )

    # Check if target table exists
    target_table_exists = True if [t for t in spark.sql(f"SHOW TABLES in {target_catalog}.{target_schema}").filter((F.col("isTemporary") == 'false') & (F.col("tableName") == f'{target_table}')).collect()] else False

    if target_table_exists:

      print("Target table exists. Checking for table changes in schema or format...")

      # Clear all caches first
      spark.sql(f"REFRESH TABLE {source_full_name}")

      # Set drop target table variable to False
      drop_target_table = False

      # Extract the Target table provider and type
      target_table_description_df = (spark.sql(f"DESCRIBE TABLE EXTENDED {target_full_name}"))
      target_table_provider = (target_table_description_df
                                .filter("col_name = 'Provider'")
                                .select("data_type")
                                .collect()[0]["data_type"]
                                ).lower()
      if not target_table_provider:
        raise ValueError(f"The table {target_full_name} format could not find using DESCRIBE TABLE EXTENDED")

      target_table_type = (target_table_description_df
                                .filter("col_name = 'Type'")
                                .select("data_type")
                                .collect()[0]["data_type"]
                                ).lower()
      if not target_table_type:
        raise ValueError(f"The table {target_full_name} type could not find using DESCRIBE TABLE EXTENDED")
      
      # If formats are different, mark the target table for drop
      if source_table_provider != target_table_provider:
        print(
                  f"Target table '{target_full_name}' format '{target_table_provider}' does not match with source table '{source_full_name}' format '{source_table_provider}', marking '{target_full_name}' for drop"
              )
        sync_status.sync_status_description = f"DROP and CREATE table due to differences in formats (target is '{target_table_provider}', source is '{source_table_provider}')"
        drop_target_table = True

      # If formats are same, continue to check for schema differences
      if not drop_target_table:

        # Get the Source and Target tables' schemas
        source_table_schema = spark.sql(f"SELECT * FROM {source_full_name} LIMIT 0").schema
        sync_status.source_table_schema = source_table_schema.json()
        target_table_schema = spark.sql(f"SELECT * FROM {target_full_name} LIMIT 0").schema
        
        # If schemas are different, mark the target table for drop
        if source_table_schema != target_table_schema:
            print(
                f"Target table '{target_full_name}' schema does not match with source table '{source_full_name}', marking '{target_full_name}' for drop"
            )

            sync_status.sync_status_description = f"DROP and CREATE TABLE due to differences in schemas (target is '{target_table_schema.json()}', source is '{source_table_schema.json()}')"

            drop_target_table = True

      # Drop the target table if format or schema are different and its an external table
      if drop_target_table:
        if target_table_type == "external":
          print(f"Dropping {target_full_name} table...")
          spark.sql(f"DROP TABLE {target_full_name}")
        
        else:
          raise ValueError(f"{target_full_name} is not an external table.")          

    # Create external table if target table doesn't exist or recently dropped
    if not target_table_exists or drop_target_table:
      
      if not target_table_exists:
        print(f"{target_full_name} doesn't exist. Creating...")
        sync_status.sync_status_description = ("Target table does not exist, CREATE a new table")
      elif drop_target_table:
        print(f"{target_full_name} exists but not matches to the {source_full_name}. Syncing...")
        sync_status.sync_status_description = ("Target table does not match to the source table, Sync target table")

      # Set current user and current timestamp
      current_u = spark.sql("SELECT current_user()").collect()[0][0]
      current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
      # Extract the notebook name
      notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[-1]
      
      # Create Delta Table
      if source_table_provider.lower() == "delta":
        
        print("Creating Delta Table")

        # Set create external delta table statement
        create_delta_table_statement = f"CREATE TABLE IF NOT EXISTS {target_full_name} USING {source_table_provider} LOCATION '{target_table_location}'"

        print(f"Executing '{create_delta_table_statement}' ...")
        spark.sql(create_delta_table_statement)
        
        # Execute set table properties
        print("Setting TBLPROPERTIES...") 
        set_delta_table_properties_statement = (f"""
                                                ALTER TABLE {source_full_name} 
                                                SET TBLPROPERTIES ('synced_to' = '{target_full_name}',
                                                                  'synced_by' = '{current_u}',
                                                                  'synced_at' = '{current_t}',
                                                                  'synced_type' = '{notebook_name} using custom sync_table function')
                                                """
                                                )
        print(f"Executing '{set_delta_table_properties_statement}' ...")
        spark.sql(set_delta_table_properties_statement)

        sync_status.sync_status_description = ("DELTA table has successfully created")

      else:
        # Create Non-Delta Table

        print(f"Creating {source_table_provider} Table")

        # Extract the source table schema as DDL
        source_table_schema_ddl = (
            spark.sql(f"SELECT * FROM {source_full_name} LIMIT 0")
            ._jdf.schema()
            .toDDL()
        )
        
        # Set create external non-delta table statement
        create_non_delta_table_statement = f"CREATE TABLE IF NOT EXISTS {target_full_name} ({source_table_schema_ddl}) USING {source_table_provider} LOCATION '{target_table_location}'"
        
        print(f"Executing '{create_non_delta_table_statement}' ...")
        spark.sql(create_non_delta_table_statement)
        
        print(f"Table provider is {source_table_provider}, hence TBLPROPERTIES cannot be set. Setting table comment instead.")
        
        # Set comment to created
        comment = f"Synced to {target_full_name} by {current_u} at {current_t} via {notebook_name} using custom sync_table function."
        comment_statement=(f"COMMENT ON TABLE {source_full_name} IS '{comment}'")
        print(f"Executing '{comment_statement}' ...")
        spark.sql(comment_statement)

        sync_status.sync_status_description = (f"{source_table_provider} table has successfully created")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, target_schema, target_table, source_catalog, source_schema, source_table)

    else:
      print(f"No action required as tables '{target_full_name}' and '{source_full_name}' are already in sync")
      sync_status.sync_status_description = ("No action required as tables are already in sync")

    # Do a final metadata sync
    print(f"Doing a final MSCK REPAIR and/or REFRESH on '{target_full_name}'")
    if target_table_exists and target_catalog != "hive_metastore" and target_table_provider == "delta":
        msck_repair_sync_metadata_statement = f"MSCK REPAIR TABLE {target_full_name} SYNC METADATA"
        print(f"Executing '{msck_repair_sync_metadata_statement}'")
        spark.sql(msck_repair_sync_metadata_statement)
    else:
      if target_catalog == "hive_metastore" and source_table_partitions:
        msck_repair_statement = f"MSCK REPAIR TABLE {target_full_name}"
        print(f"Executing '{msck_repair_statement}'")
        spark.sql(msck_repair_statement)
      
    refresh_statement = f"REFRESH TABLE {target_full_name}"
    print(f"Executing '{refresh_statement}'")
    spark.sql(refresh_statement)

    sync_status.sync_status_code = "SUCCESS"
          
  except Exception:
    sync_status.sync_status_code = "FAILED"
    sync_status.sync_status_description = str(traceback.format_exc())
  return sync_status
