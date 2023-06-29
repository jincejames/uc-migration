import re

import pyspark.sql.utils as U
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

def get_value(lst: list, idx: int, idy: int, default: str):
    try:
        return lst[idx][idy]
    except IndexError:
        return default
      
def get_a_string_between_two_markers(input_string: str, marker1: str, marker2: str) -> str:
  """
  Get a string between two markers

  Parameters:
    input_string: The input string
    marker1: The marker 1
    marker2: The marker 2

  Returns:
    The string between two markers
  """
  # Regex pattern
  regexPattern = marker1 + '(.+?)' + marker2

  # Search the string by the pattern
  str_found = re.search(regexPattern, input_string).group(1)
  
  return str_found

      
def create_uc_catalog(spark: SparkSession, catalog_name: str, schema_name: str, location: str = "", comment: str = "") -> None:
  """
  Creates Unity Catalog catalog with the given catalog name, location if applicable, and description as a comment

  Parameters:
    spark: Active SparkSession
    catalog_name: The name of the Unity Catalog catalog
    location: (Optional) Specify a location here only if you do not want managed tables in this schema to be stored in the default root storage location that was configured for the metastore
    comment: (Optional) The description of the schema

  """

  try:
    if not catalog_name:
      raise ValueError("catalog_name is empty")
    
    # The default create catalog statement
    create_catalog_stmt = f"CREATE CATALOG IF {catalog_name}"
    
    if comment: 
      # Add COMMENT to the create catalog statement
      create_catalog_stmt += f" COMMENT '{comment}'"
    
    if location:
      # Add LOCATION to the create catalog statement
      create_catalog_stmt += f" LOCATION {location}"
    
    # Execute the create catalog statement
    spark.sql(create_catalog_stmt)
    print(f"Catalog {catalog_name} has been sucessfully created with the {create_catalog_stmt} statement.")

  except (ValueError, U.AnalysisException) as e:
        if isinstance(e, ValueError):
          print(f"ValueError occurred: {e}")
          raise e
        elif isinstance(e, U.AnalysisException):
          print(f"AnalysisException occurred: {e}")
          raise e

def create_uc_schema(spark: SparkSession, catalog_name: str, schema_name: str, location: str = "", comment: str = "") -> None:
  """
  Creates Unity Catalog Schema in the given catalog with the given schema name, location if applicable, and description as a comment

  Parameters:
    spark: Active SparkSession
    catalog_name: The name of the Unity Catalog catalog
    schema_name: The name of the Unity Catalog schema
    location: (Optional) Specify a location here only if you do not want managed tables in this schema to be stored in the default root storage location that was configured for the metastore or the managed storage location specified for the catalog (if any)
    comment: (Optional) The description of the schema

  """

  try:
    if not catalog_name:
      raise ValueError("catalog_name is empty")
    elif not schema_name:
      raise ValueError("schema_name is empty")
    
    # The default create schema statement
    create_schema_stmt = f"CREATE SCHEMA {catalog_name}.{schema_name}"

    if comment: 
    
      # Add COMMENT to the create schema statement
      create_schema_stmt += f" COMMENT '{comment}'"
    
    if location:
      # Add LOCATION to the create schema statement
      create_schema_stmt += f" LOCATION {location}"
    
    # Execute the create schema statement
    spark.sql(create_schema_stmt)
    print(f"Schema {schema_name} has been sucessfully created in the {catalog_name} catalog with the {create_schema_stmt} statement.")

  except (ValueError, U.AnalysisException) as e:
        if isinstance(e, ValueError):
          print(f"ValueError occurred: {e}")
          raise e
        elif isinstance(e, U.AnalysisException):
          print(f"AnalysisException occurred: {e}")
          raise e


def create_external_location(spark: SparkSession, storage_credential: str, location_name: str, location_url: str, comment: str ="") -> None:
  """
  Creates an external location with the specified name. If a location with the same name already exists, an exception is thrown.

  Parameters:
    spark: Active SparkSession
    storage_credential: The named storage credential used to connect to this location
    location_name: The name of the external location to be created
    location_url: The location path of the cloud storage is described as an absolute URL.
    comment: (Optional) The description of the external location

  """

  try:
      if not storage_credential:
        raise ValueError("storage_credential is empty")
      elif not location_name:
        raise ValueError("location_name is empty")
      elif not location_url:
        raise ValueError("location_url is empty")
      
      # The default external location statement
      create_ext_loc_stmt = f"""
                            CREATE EXTERNAL LOCATION {location_name} URL '{location_url}'
                            WITH (STORAGE CREDENTIAL {storage_credential})
                            """
      
      if comment: 
        # Add COMMENT to the external location statement
        create_ext_loc_stmt += f" COMMENT '{comment}'"

      # Execute the create external location statement
      spark.sql(create_ext_loc_stmt)

  except (ValueError, U.AnalysisException) as e:
        if isinstance(e, ValueError):
          print(f"ValueError occurred: {e}")
          raise e
        elif isinstance(e, U.AnalysisException):
          print(f"AnalysisException occurred: {e}")
          raise e


def create_external_location_from_mount(spark: SparkSession, dbutils: DBUtils, tables_descriptions: list, storage_credential: str, location_name_prefix: str = "", mount_point_name: str = "") -> None:
  """
  Creates an external location for each top-level individual mount point with the specified prefix and container name. If a location with the same name already exists, an exception is thrown.

  Parameters:
    spark: Active SparkSession
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Database', 'Table', 'Type', and 'Provider'.
    storage_credential: The named storage credential used to connect to this location
    location_name_prefix: (Optional) Prefix for the external location name to be created
    mount_point_name: (Optional) The name of a mount point

  """

  try:
      if not storage_credential:
        raise ValueError("storage_credential is empty")
      elif not tables_descriptions:
        raise ValueError("tables_descriptions is empty")
      
      mounts = dbutils.fs.mounts()

      for r in tables_descriptions:
        # Get the table's mounted path
        mount_table_path = r["Location"]

        if mount_point_name:
          # Get the mount point and mount source (path) if the given mount point name exists, the table location exists as mount source (path) and it is not a DatabricksRoot path
          mount_point, mount_source = zip(*[(mount.mountPoint, mount.source) for mount in mounts if (mount.mountPoint in mount_table_path and mount.mountPoint == mount_point_name and mount.source != "DatabricksRoot")])
          print(mount_point, mount_source)
        else:
          # Get the mount point and mount source (path) if the table location exists as mount source (path) and it is not a DatabricksRoot path
          mount_point, mount_source = zip(*[(mount.mountPoint, mount.source) for mount in mounts if (mount.mountPoint in mount_table_path and mount.source != "DatabricksRoot")])

        if mount_source[0].startswith("wasbs"):
          raise ValueError(f"Mount point {mount_point[0]} source url {mount_source[0]} is incorrect, since its a 'wasbs' path. Please remount the mount point as 'abfss' path")
        
        if mount_source[0].startswith("abfss"):
          # Azure mount point

          # Get the external location url from the mount source url
          location_url = get_a_string_between_two_markers(mount_source[0], "//",  "/")

          # Get the container name of the mount source url
          container_name = get_a_string_between_two_markers(mount_source[0], "//",  "@")
          print(container_name)

          # The default external location statement
          create_ext_loc_stmt = f"""
                                CREATE EXTERNAL LOCATION `{location_name_prefix}_ext_loc_{container_name}` URL 'abfss://{location_url}/'
                                WITH (STORAGE CREDENTIAL `{storage_credential}`)
                                COMMENT 'Created from mount point {mount_point[0]} with mount source {mount_source[0]}'
                                """

        # Execute the create external location statement
        spark.sql(create_ext_loc_stmt)

  except (ValueError, U.AnalysisException) as e:
        if isinstance(e, ValueError):
          print(f"ValueError occurred: {e}")
          raise e
        elif isinstance(e, U.AnalysisException):
          print(f"AnalysisException occurred: {e}")
          raise e          
