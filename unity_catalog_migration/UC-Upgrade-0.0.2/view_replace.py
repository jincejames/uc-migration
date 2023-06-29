# Databricks notebook source
# MAGIC %sql
# MAGIC Describe table extended bronze_ai_contact_intelligence.address_likelihood_to_sell_clean 

# COMMAND ----------

class DBUtils:
    @staticmethod
    def list(filter=None):
        df = spark.sql("show databases")
        databases = df.select("databaseName").collect()
        db_names = [name[0] for name in databases]
        return (
            db_names if not filter else [db for db in db_names if re.match(filter, db)]
        )

    @staticmethod
    def list_tables(database, filter=None):
        return sqlContext.tableNames(database)

    @staticmethod
    def get_db_location(database):
        table = DBUtils.list_tables(database)[0]
        table_location = DBUtils.get_table_location(database, table)
        return os.path.dirname(table_location)

    @staticmethod
    def get_table_location(database, table):
        try: 
            df = spark.sql(f"Describe table extended {database}.{table}")
            table_info = df.collect()
        except Exception as amz:
            raise Exception("Describe table failed - " + str(amz))
        locations = [
            row["data_type"] for row in table_info if row["col_name"] == "Location"
        ]
        if len(locations) == 0: raise Exception("Location for this table doesn't exist")
        return locations[0]

    @staticmethod
    def get_table_type(database, table):
        try: 
            df = spark.sql(f"Describe table extended {database}.{table}")
            table_info = df.collect()
        except Exception as amz:
            raise Exception("Describe table failed - " + str(amz))
        table_type = [
            row["data_type"] for row in table_info if row["col_name"] == "Type"
        ]
        print(table_type)
        if len(table_type) == 0: raise Exception("Type for this table doesn't exist")
        return table_type[0]

    @staticmethod
    def get_table_version(database, table):
        table_location = DBUtils.get_table_location(database, table)
        deltaTable = DeltaTable.forPath(spark, table_location)
        history = deltaTable.history(1).collect()[0]
        return history["version"]

    @staticmethod
    def get_table_definition(database, table):
        display(spark.sql(f"show create table {database}.{table}"))

    @staticmethod
    def restore_table_to_version(database, table, version, dry_run=True):
        query = f"RESTORE {database}.{table} to VERSION AS OF {version}"
        if dry_run:
            Print.green(query)
        else:
            Print.red(query)
            spark.sql(query)

    @staticmethod
    def restore_table_to_date(database, table, date_str, dry_run=True):
        query = f"RESTORE {database}.{table} to TIMESTAMP AS OF '{date_str}'"
        if dry_run:
            Print.green(query)
        else:
            Print.red(query)
            spark.sql(query)

    @staticmethod
    def restore_table_version_relative(database, table, dry_run=True, delta=1):
        version = DBUtils.get_table_version(database, table)
        target_version = int(version) - delta
        DBUtils.restore_table_to_version(database, table, target_version, dry_run)

    @staticmethod
    def describe_db(name):
        df = spark.sql(f"describe database extended {name}")
        display(df)
        for table in sqlContext.tableNames(name):
            print(table)
        return df

    @staticmethod
    def describe_table(name):
        df = spark.sql(f"describe table extended {name}")
        display(df)
        df_data = spark.sql(f"select * from {name} limit 10")
        display(df_data)
        return df
      
    @staticmethod
    def get_view_ddl(db, table):
        sql = f"SHOW CREATE TABLE {db}.{table}"
        df = spark.sql(sql)
        #display(df)
        return df.collect()[0][0]
    
    @staticmethod
    def describe_table_history(name, limit=None):
        limit_clause = f" limit {limit}" if limit else ""
        df = spark.sql(f"DESCRIBE HISTORY {name}{limit_clause}")
        df = df.select(
            [
                "version",
                "timestamp",
                "operation",
                "operationParameters",
                "readVersion",
                "operationMetrics",
                "userMetadata",
            ]
        )
        display(df)
        return df

    @staticmethod
    def describe_table_partition(name):
        df = spark.sql(f"SHOW PARTITIONS {name}")
        display(df)

    def get_table_max_partition(name, partition_key="_partion_key"):
        return (
            spark.sql(f"SHOW PARTITIONS {name}")
            .agg(max(partition_key).alias("max_partition"))
            .collect()[0]
            .asDict()["max_partition"]
        )

    @staticmethod
    def get_table_schema(name):
        df = spark.read.table(name)
        return df.schema
    

    @staticmethod
    def print_table_schema(name):
        df = spark.read.table(name)
        return df.printSchema()

    @staticmethod
    def drop_db(db_name, dry_run=False):
        sql = f"DROP DATABASE {db_name} CASCADE"
        print(sql)
        if not dry_run:
            df = spark.sql(sql)

    @staticmethod
    def drop_table(table, dry_run=False):
        sql = f"DROP TABLE {table} if exists"
        print(sql)
        if not dry_run:
            df = spark.sql(sql)

    @staticmethod
    def clear_db(db_name, dry_run=False):
        # clear db and underlying delta table
        names = sqlContext.tableNames(db_name)
        first_table = names[0]

        df = spark.sql(f"describe table extended {db_name}.{first_table}")
        locations = df.where("col_name = 'Location'").select("data_type").collect()
        if len(locations) != 1:
            print("Error finding db location! Please check manually")
        else:
            path = locations[0][0]
            db_root = os.path.dirname(path)
            print(f"Find db root: {db_root}")
            S3Utils.rm(db_root, dry_run)
            DBUtils.drop_db(db_name, dry_run)

    @staticmethod
    def clear_table(table, dry_run=False):
        df = spark.sql(f"describe table extended {table}")
        locations = df.where("col_name = 'Location'").select("data_type").collect()
        if len(locations) != 1:
            print("Error finding db location! Please check manually")
        else:
            path = locations[0][0]
            print(f"Fount table s3 path: {path}")
            S3Utils.rm(path, dry_run)
            DBUtils.drop_table(table, dry_run)

    @staticmethod
    def _get_db_info(db_name, sort_by_name=False, reverse=False):
        result = list()
        total_row = 0

        def table_size(table):
            df = spark.sql(f"select count(*) as count from {db_name}.{table}")
            count = df.select("count").collect()[0][0]
            result.append((table, count))

        tables = sqlContext.tableNames(dbName=db_name)
        _parallel(table_size, tables)
        sort_col = 0 if sort_by_name else 1
        return sorted(result, key=lambda x: x[sort_col], reverse=reverse)

    @staticmethod
    def db_info(db_name, sort_by_name=False, reverse=False):
        """
        When sort_by_name is False, will sort by size
        """
        result = DBUtils._get_db_info(
            db_name, sort_by_name=sort_by_name, reverse=reverse
        )
        total_rows = sum(r[1] for r in result)
        print(f"     Database: {db_name}")
        print(f"         Tables: {len(result)}")
        print(f" Total rows: {total_rows:,}")
        print(60 * "-")
        for r in result:
            print(f"{r[0].ljust(40)}: {r[1]:,} ({r[1]/total_rows*100:.2f}%)")
            
    @staticmethod
    def get_view_ddl(db, table, catalog_name):
      
        spark.sql(f"use catalog {catalog_name}")
        sql = f"SHOW CREATE TABLE {catalog_name}.{db}.{table}"
        df = spark.sql(sql)
        #display(df)
        return df.collect()[0][0]
      
    @staticmethod
    def get_view_text_ddl(db, table, catalog_name):
      
        try: 
            df = spark.sql(f"Describe table extended {db}.{table}")
            table_info = df.collect()
        except Exception as amz:
            raise Exception("Describe table failed - " + str(amz))
        view_text = [
            row["data_type"] for row in table_info if row["col_name"] == "View Text"
        ]
        if len(view_text) == 0: raise Exception("Type for this table doesn't exist")
        return view_text[0]
      
    @staticmethod
    def get_view_text_catalog_ddl(db, table, catalog_name):
      
        try: 
            df = spark.sql(f"Describe table extended {db}.{table}")
            table_info = df.collect()
        except Exception as amz:
            raise Exception("Describe table failed - " + str(amz))
        view_catalog = [
            row["data_type"] for row in table_info if row["col_name"] == "View Catalog and Namespace"
        ]
        if len(view_catalog) == 0: raise Exception("Type for this table doesn't exist")
        return view_catalog[0]
            
    @staticmethod
    def newExecuteDDLS(type,db, table,location, provider, catalog_name_source, catalog_name_dest):
      try:
        if(type=='TABLE'):
          ddl=f"CREATE TABLE IF NOT EXISTS {catalog_name_dest}.{db}.{table} USING {provider} location \"{location}\""
          
        elif(type=='VIEW'):
          ddl=DBUtils.get_view_ddl(db, table, catalog_name_source).replace(f"CREATE VIEW `{catalog_name_source}`.",f"CREATE VIEW IF NOT EXISTS `{catalog_name_dest}`.")
          
        #spark.sql(f"use catalog {catalog_name_source}")
        print(ddl)
        #spark.sql(ddl)
        print(f"ddl creation sucessful for {db}.{table}")
        
      except Exception as e: 
         raise Exception("DDLS creation failed - " + str(e))
          
    @staticmethod
    def get_table_type(database, table, catalog_name):
       
        spark.sql(f"use catalog {catalog_name}")
        try: 
            df = spark.sql(f"Describe table extended {catalog_name}.{database}.{table}")
            table_info = df.collect()
        except Exception as amz:
            raise Exception("Describe table failed - " + str(amz))
        table_type = [
            row["data_type"] for row in table_info if row["col_name"] == "Type"
        ]
        if len(table_type) == 0: raise Exception("Type for this table doesn't exist")
        if ('VIEW' in table_type): 
            return "VIEW"
        elif ('MANAGED' in table_type): 
            return "MANAGED"
        elif ('EXTERNAL' in table_type): 
            return "EXTERNAL"
        return table_type[0]
      
    @staticmethod
    def get_table_type_hive(database, table):
       
      
        try: 
            df = spark.sql(f"Describe table extended {database}.{table}")
            table_info = df.collect()
        except Exception as amz:
            raise Exception("Describe table failed - " + str(amz))
        table_type = [
            row["data_type"] for row in table_info if row["col_name"] == "Type"
        ]
        if len(table_type) == 0: raise Exception("Type for this table doesn't exist")
        if ('VIEW' in table_type): 
            return "VIEW"
        elif ('MANAGED' in table_type): 
            return "MANAGED"
        elif ('EXTERNAL' in table_type): 
            return "EXTERNAL"
        return table_type[0]


# COMMAND ----------

import re
include_db_list = ['bronze_']
source_catalog = "hive_metastore"
dest_catalog = "main"
#table_map = {}
#spark.sql("USE CATALOG hive_metastore")


# for db in DBUtils.list():
#   for table in DBUtils.list_tables(db):
#     #print(f"{db}.{table}")
#     hive_table = f"{db}.{table}"
#     main_table = f"main.{db}.{table}"
#     table_map[hive_table] = main_table

with open("/dbfs/tmp/logs.txt",'w') as error:
  for db in DBUtils.list():
   if (db.startswith(tuple(include_db_list))):
    try:  
        for table in DBUtils.list_tables(db):
          try:
             table_type = DBUtils.get_table_type_hive(db, table)
          except Exception as e: 
             table_type=''
             print(f"Can't process table type for {source_catalog}.{db}.{table} as ", str(e))
             pass

          if (table_type == 'VIEW'):
        
            
            view_ddl = DBUtils.get_view_text_ddl(db, table, source_catalog)
            view_catalog = DBUtils.get_view_text_catalog_ddl(db, table, source_catalog)
            #view_ddl = replace_all(view_ddl, replacements)
            print(view_catalog)
            if view_catalog == 'main.default':
              
              alt_ddl = f"CREATE OR REPLACE VIEW {db}.{table} AS " + view_ddl
              print(f"{alt_ddl}")
      
              try:
                spark.sql(alt_ddl)
              except Exception as e:  
                  print(f"Can't process ddls for {db}.{table} as ", str(e))
                  error.write("ddls execution failed {db}.{table} - " + str(e))
                  pass
    except Exception as dbe:
         print(f"Db doesn't exist {db} ", str(dbe))
         pass
  
  
    

# COMMAND ----------

# %sql 
# USE CATALOG hive_metastore;
# CREATE OR REPLACE VIEW bronze_contacts_contact_histories.contact_histories_clean AS
#             SELECT * FROM bronze_contacts_contact_histories.contact_histories

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW vw_your_external_table_w_adls_loc_tpch_lineitem AS
# MAGIC select * from hive_metastore.your_schema.your_external_table_w_adls_loc_tpch_lineitem
# MAGIC where l_returnflag == "A"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED vw_your_external_table_w_adls_loc_tpch_lineitem

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW balazs_dev.gold.vw_your_external_table_w_adls_loc_tpch_lineitem AS
# MAGIC

# COMMAND ----------

t = ""

# COMMAND ----------

if not t:
  raise ValueError("t is empty")

# COMMAND ----------

t = 1
