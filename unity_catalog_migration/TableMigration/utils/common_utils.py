import re
import collections
from dataclasses import dataclass

import pyspark.sql.utils as U
import pyspark.sql.functions as F
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
    create_catalog_stmt = f"CREATE CATALOG IF NOT EXISTS {catalog_name}"
    
    if comment: 
      # Add COMMENT to the create catalog statement
      create_catalog_stmt += f" COMMENT '{comment}'"
    
    if location:
      # Add LOCATION to the create catalog statement
      create_catalog_stmt += f" MANAGED LOCATION '{location}'"
    
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
    create_schema_stmt = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}"

    if comment: 
    
      # Add COMMENT to the create schema statement
      create_schema_stmt += f" COMMENT '{comment}'"
    
    if location:
      # Add LOCATION to the create schema statement
      create_schema_stmt += f" MANAGED LOCATION '{location}'"
    
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
    dbutils: Databricks Utilities
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


def create_hms_schema(spark: SparkSession, schema_name: str, location: str = "", comment: str = "") -> None:
  """
  Creates Hive Metastore Schema in the given catalog with the given schema name, location if applicable, and description as a comment

  Parameters:
    spark: Active SparkSession
    schema_name: The name of the Hive Metastore schema
    location: (Optional) Specify a location here only if you do not want managed tables in this schema to be stored in the DBFS root storage location
    comment: (Optional) The description of the schema

  """

  try:
    if not schema_name:
      raise ValueError("schema_name is empty")
    
    # The default create schema statement
    create_schema_stmt = f"CREATE SCHEMA hive_metastore.{schema_name}"

    if comment: 
    
      # Add COMMENT to the create schema statement
      create_schema_stmt += f" COMMENT '{comment}'"
    
    if location:
      # Add LOCATION to the create schema statement
      create_schema_stmt += f" LOCATION {location}"
    
    # Execute the create schema statement
    spark.sql(create_schema_stmt)
    print(f"Schema {schema_name} has been sucessfully created in the hive_metastore with the {create_schema_stmt} statement.")

  except (ValueError, U.AnalysisException) as e:
        if isinstance(e, ValueError):
          print(f"ValueError occurred: {e}")
          raise e
        elif isinstance(e, U.AnalysisException):
          print(f"AnalysisException occurred: {e}")
          raise e

def get_the_object_external_location(dbutils: DBUtils, object_type: str, location: str) -> str:
  """
  Get the cloud storage path of the object location as an external location.

  Parameters:
    dbutils: Databricks Utilities
    object_type: Object type name, either 'table', 'schema', or 'catalog'.
    location: The location of the object

  Returns
    The cloud storage path as external location of the object location
  """

  try:
    if not object_type:
      raise ValueError("object_type input string is empty")
    elif not location:
      raise ValueError("location input string is empty")

    # Extract the mounts
    mounts = dbutils.fs.mounts()

    # Iterate through the mounts and return the external location
    for mount in mounts:
      if location.startswith(f"dbfs:{mount.mountPoint}/"):
        # Extract the mount point location
        external_location = location.replace(f"dbfs:{mount.mountPoint}/", mount.source).replace(f"%20", " ")
        break

      elif location[0:6] == "dbfs:/":
        
        if object_type.lower() == "table":
          # Pass, let gets the default location
          pass
        else:
          # Set database/schema/catalog location to None if it is on DBFS
          external_location = None
      
      else:
    
        # Set the external location to the current database/schema location
        external_location = location
      
    # Set external location to location if the location does not exist as mount point
    if object_type == "table":
      # Set external location if it is not DBFS
      external_location = external_location if external_location else location
      if external_location[0:6] == "dbfs:/":
        # Raise error if location is DBFS
        raise ValueError(f"The location {location} is DBFS. It needs to be migrated away with data movement")
    else:
      # Set location that calculated in the loop
      external_location

    return external_location

  except ValueError as e:
    print(f"ValueError occurred: {e}")
    raise e


# Expected output for get_schema_detail
SchemaDetails = collections.namedtuple("SchemaDetails", ["database", "location", "external_location"])


def get_schema_detail(spark: SparkSession, dbutils: DBUtils, schema_name: str) -> SchemaDetails:
    """
    Get the Schema Details
    
    Parameter:
      spark: The active SparkSession
      dbutils: Databricks Utilities
      schema_name: Name of the database/schema
    
    Returns:
      SchemaDetails of database including "database", "location", "external_location".
      
    """
    try:
      if not schema_name:
        raise ValueError("schema_name is not given")

      # Itearate through the rows of the describe schema output
      for r in spark.sql(f"describe schema {schema_name}").collect():
        if r['database_description_item'] == 'Namespace Name':
              # Extract the schema/database name
              db_name = r['database_description_value']            
        if r['database_description_item'] == 'Location':
              # Extract the schema/database location
              db_location = r['database_description_value'] 

              # Get the exact external location path of the schema/database location
              db_external_location = get_the_object_external_location(dbutils, "schema", db_location)
      
      return SchemaDetails (
          database=db_name,
          location=db_location,
          external_location=db_external_location
      )
        
    except (ValueError, U.AnalysisException) as e:
          if isinstance(e, ValueError):
            print(f"ValueError occurred: {e}")
            raise e
          elif isinstance(e, U.AnalysisException):
            print(f"AnalysisException occurred: {e}")
            raise e

@dataclass
class TableDDL:
    requested_object_name: str = None
    catalog_name: str = None
    schema_name: str = None
    original_ddl: str = None
    full_table_name: str = None
    table_schema: str = None
    using: str = None
    partition_by: str = None
    cluster_by: str = None
    table_properties: str = None
    comment: str = None
    options: str = None
    location: str = None
    dynamic_ddl: str = None


def get_create_table_stmt(spark: SparkSession, catalog: str, schema: str, table: str) -> TableDDL:
  """
  Getting the create table statement
  
  Parameters:
    spark: Active SparkSession
    catalog: The name of the catalog
    schema: The name of the schema
    table: The name of the table

  Returns:
    The TableDDL object
  """
  try:

    if not catalog:
      raise ValueError("catalog input string is empty")
    elif not schema:
      raise ValueError("schema input string is empty")
    elif not table:
      raise ValueError("table input string is empty")

    table_ddl = TableDDL()

    # Set full table name
    full_table_name = f"{catalog}.{schema}.{table}"

    # Create table definition DataFrame
    table_definition_df = (spark.sql(f"SHOW CREATE TABLE {full_table_name}")
                            .withColumn("createtab_stmt", F.regexp_replace(F.col("createtab_stmt"), "\n", " "))
                            .withColumn("full_table_name", 
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"^CREATE (TABLE|VIEW) ([A-Za-z0-9._]+) \(", 2))
                            .withColumn("location", 
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"LOCATION '([^']+)'", 1))
                            .withColumn("schema", 
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"^CREATE (TABLE|VIEW) ([A-Za-z0-9._]+) +\((.+?)\)", 3))
                            .withColumn("using",
                                        F.regexp_extract(F.col("createtab_stmt"),
                                                        r"(?<=USING\s)(\w+)(?=\s)", 0))
                            .withColumn("partition_by",
                                        F.regexp_extract(F.col("createtab_stmt"),
                                                        r"PARTITIONED BY \((.*?)\)", 1))
                            .withColumn("cluster_by",
                                        F.regexp_extract(F.col("createtab_stmt"),
                                                        r"CLUSTER BY \((.*?)\)", 1))
                            .withColumn("table_properties",
                                        F.regexp_extract(F.col("createtab_stmt"),
                                                        r"TBLPROPERTIES \(\s*(.*?)\s*\)", 1))
                            .withColumn("comment", 
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"COMMENT '([^']+)'", 1))
                            .withColumn("options",
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"OPTIONS \((.*?)\)", 1))
                            .withColumn("dynamic_ddl", 
                                        F.regexp_replace(F.col("createtab_stmt"), 
                                                        r"^CREATE (TABLE|VIEW) ([A-Za-z0-9._]+) \(", "CREATE $1 <<full_table_name>> ("))
                            .withColumn("dynamic_ddl", 
                                        F.regexp_replace(F.col("dynamic_ddl"), 
                                                        r"LOCATION '([^']+)'", "LOCATION '<<location>>'"))
                            .withColumn("dynamic_ddl", 
                                        F.regexp_replace(F.col("dynamic_ddl"), 
                                                        r"^CREATE (TABLE|VIEW) ([^(]+)\((.+?)\)", "CREATE $1 $2 (<<schema>>)"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"(?<=USING\s)(\w+)(?=\s)", " <<using>>"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"PARTITIONED BY \((.*?)\)", "PARTITIONED BY (<<partition_by>>)"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"COMMENT '([^']+)'", "COMMENT '<<comment>>'"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"CLUSTER BY \((.*?)\)", "CLUSTER BY (<<cluster_by>>)"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"TBLPROPERTIES \(\s*(.*?)\s*\)", "TBLPROPERTIES(<<table_properties>>)"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"OPTIONS \((.*?)\)", "OPTIONS (<<options>>)"))
                            )
    
    # Set table_ddl attributes

    table_ddl.requested_object_name = table
    table_ddl.catalog_name = catalog
    table_ddl.schema_name = schema
    table_ddl.original_ddl = table_definition_df.select("createtab_stmt").collect()[0][0]
    table_ddl.full_table_name = table_definition_df.select("full_table_name").collect()[0][0]
    table_ddl.table_schema = table_definition_df.select("schema").collect()[0][0]
    table_ddl.using = table_definition_df.select("using").collect()[0][0]
    table_ddl.partition_by = table_definition_df.select("partition_by").collect()[0][0]
    table_ddl.cluster_by = table_definition_df.select("cluster_by").collect()[0][0]
    table_ddl.table_properties = table_definition_df.select("table_properties").collect()[0][0]
    table_ddl.comment = table_definition_df.select("comment").collect()[0][0]
    table_ddl.options = table_definition_df.select("options").collect()[0][0]
    table_ddl.location = table_definition_df.select("location").collect()[0][0]
    table_ddl.dynamic_ddl = table_definition_df.select("dynamic_ddl").collect()[0][0]

    return table_ddl

  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
      raise e
    elif isinstance(e, U.AnalysisException):
      print(f"AnalysisException occurred: {e}")
      raise e 
  