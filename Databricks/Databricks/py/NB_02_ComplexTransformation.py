# Databricks notebook source
# DBTITLE 1,Metadata-Driven Ingestion Framework
# MAGIC %md
# MAGIC #### Data Transformation: Create Delta Lake Tables
# MAGIC Connect to sink instance and create Delta Lake tables with the specified name and path

# COMMAND ----------

# MAGIC %run "/Shared/MDMF/Tools/utilities"

# COMMAND ----------

# DBTITLE 1,Dependencies
import json
from pyspark.sql import functions as F
from delta.tables import *
from pyspark.sql.types import StructType

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("DataTransformationParameters", "", "")
Widget_DataTransformationParameters = dbutils.widgets.get("DataTransformationParameters")

dbutils.widgets.text("SinkGlobalParameters", "", "")
Widget_SinkGlobalParameters = dbutils.widgets.get("SinkGlobalParameters")

# COMMAND ----------

# DBTITLE 1,DataTransformationParameters to Vars
DataTransformationParameters = json.loads(Widget_DataTransformationParameters)

"""WriteMode"""
write_mode = DataTransformationParameters["WriteMode"]

"""InputParameters"""
input_parameters = json.loads(DataTransformationParameters["InputParameters"])

"""SourceDataSets"""
source_datasets_father = json.loads(DataTransformationParameters["SourceDatasets"])
#json.loads take a string as input and returns a dictionary as output.
#json.dumps take a dictionary as input and returns a string as output.
source_datasets_son = json.loads(json.dumps(source_datasets_father["sourceDatasets"]))

"""Output"""
output_path = DataTransformationParameters["OutputPath"]

"""Errors"""
errors = []
notebook_output = {}

# COMMAND ----------

# DBTITLE 1,SinkGlobalParameters to Vars
SinkGlobalParameters = json.loads(Widget_SinkGlobalParameters)

scope_name = SinkGlobalParameters['kv_scope_name']
kv_workspace_id = SinkGlobalParameters['kv_workspace_id']
kv_workspace_pk = SinkGlobalParameters['kv_workspace_pk']
ing_sink_storage_type = SinkGlobalParameters['ing_sink_storage_type']
dv_schema_container_name = SinkGlobalParameters['dv_schema_container_name']

"""dynamic values"""
storage_account_name = ''
storage_access_key_secret_name = ''
container_name = ''

# COMMAND ----------

# DBTITLE 1,Functions: *Join, Upper, Lower, Trim
def joinDatasets(df, function_name, input_parameters, DataTransformationParameters):
  query = json.loads(json.dumps(input_parameters["functions"]["joinDatasets"]["Query"]))
  df = sqlContext.sql(query)
  return df


def columnUpper(df, function_name, input_parameters, DataTransformationParameters):  
  columns_dict = json.loads(json.dumps(input_parameters["functions"][function_name]))
  columns_list = columns_dict["columns"]
  print('Executing ColumnUpper Method...' + str(columns_list))
  
  for col in df.columns:
    if len(columns_list) > 0:
      for col_user in columns_list:
        df = df.withColumn(col_user, F.upper(F.col(col_user))) # Upper specific columns
    else:
      df = df.withColumn(col, F.upper(F.col(col))) # Upper all columns  
  return df


def columnLower(df, function_name, input_parameters, DataTransformationParameters):
  columns_dict = json.loads(json.dumps(input_parameters["functions"][function_name]))
  columns_list = columns_dict["columns"]
  print('Executing ColumnLower Method...' + str(columns_list))

  for col in df.columns:
    if len(columns_list) > 0:
      for col_user in columns_list:
        df = df.withColumn(col_user, F.lower(F.col(col_user))) # Lower specific columns
    else:
      df = df.withColumn(col, F.lower(F.col(col))) # Lower all columns  
  return df
  
  
def columnTrim(df, function_name, input_parameters, DataTransformationParameters):
  columns_dict = json.loads(json.dumps(input_parameters["functions"][function_name]))
  columns_list = columns_dict["columns"]
  print('Executing ColumnTrim Method...' + str(columns_list))
  
  for col in df.columns:
    if len(columns_list) > 0:
      for col_user in columns_list:
        df = df.withColumn(col_user, F.trim(F.col(col_user))) # Trim  specific columns
    else:
      df = df.withColumn(col, F.trim(F.col(col))) # Trim all columns
  return df

# COMMAND ----------

# DBTITLE 1,*Get sink values by source module (ingestion, validation, transformation)
def get_global_values(module):
  try:
    if module.lower() == 'ingestion' or 'ingestion' in module.lower():
      storage_access_key_secret_name = SinkGlobalParameters['ing_sink_storage_secret_name']
      storage_account_name = SinkGlobalParameters['ing_sink_storage_name']      
      container_name = SinkGlobalParameters['ing_sink_container_name']
      return storage_access_key_secret_name, storage_account_name, container_name

    if module.lower() == 'validation' or 'validation' in module.lower():
      storage_access_key_secret_name = SinkGlobalParameters['dv_sink_storage_secret_name']
      storage_account_name = SinkGlobalParameters['dv_sink_storage_name']      
      container_name = SinkGlobalParameters['dv_sink_container_name']
      return storage_access_key_secret_name, storage_account_name, container_name

    if module.lower() == 'transformation' or 'transformation' in module.lower():
      storage_access_key_secret_name = SinkGlobalParameters['dt_sink_storage_secret_name']
      storage_account_name = SinkGlobalParameters['dt_sink_storage_name']      
      container_name = SinkGlobalParameters['dt_sink_container_name']
      return storage_access_key_secret_name, storage_account_name, container_name
  except Exception as ex:    
    errors.append('Error in get_global_values method: {}'.format(ex))
    print(ex)

# COMMAND ----------

# DBTITLE 1,Validate first merge
def validate_merge(SinkGlobalParameters, output_path):
  try:
    """global parameters"""
    scope_name = SinkGlobalParameters["kv_scope_name"]    
    storage_account_name = SinkGlobalParameters['dt_sink_storage_name']
    storage_access_key_secret_name = SinkGlobalParameters['dt_sink_storage_secret_name']   
    container_name = SinkGlobalParameters['dt_sink_container_name']
    
    spark.conf.set(
    "fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name), 
    dbutils.secrets.get(scope=scope_name, key=storage_access_key_secret_name))    
        
    delta_path = 'abfss://{}@{}.dfs.core.windows.net/{}'.format(container_name, storage_account_name, output_path)    
    df_delta = spark.read.format("delta").option("header","true").load(delta_path)
    
    if df_delta.rdd.isEmpty() == False:
      print('Is a valid delta for -Merge-.')
      return 'Merge'
    else:
      print('Merge is not possible (Invalid delta sink), changing mode from -Merge- to -Overwrite-.')
      return 'Overwrite'
      
  except Exception as ex:
    print('Merge is not possible (Invalid delta sink), changing mode from -Merge- to -Overwrite-.')
    return 'Overwrite'    

# COMMAND ----------

# DBTITLE 1,Read and save files
def read_source_data(scope_name, storage_access_key_secret_name, storage_account_name, container_name, source_path, source_file_format):
  try:
    spark.conf.set(
    "fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name), 
    dbutils.secrets.get(scope=scope_name, key=storage_access_key_secret_name))
    
    full_source_path = "abfss://{container}@{storage}.dfs.core.windows.net/{file}".format(container=container_name, storage=storage_account_name, file=source_path)
    
    if source_file_format.lower() == 'parquet':
      return spark.read.parquet(full_source_path)
    if source_file_format.lower() == 'delta':
      return spark.read.format("delta").option("header","true").load(full_source_path)  
  except Exception as ex:
    errors.append('Error in read_source_data method: {}'.format(ex))
    print(ex)

def save_delta_format(df, SinkGlobalParameters, output_path, write_mode, input_parameters):
  try:
    """Global parameters"""
    scope_name = SinkGlobalParameters["kv_scope_name"]  
    storage_access_key_secret_name = SinkGlobalParameters['dt_sink_storage_secret_name']   
    container_name = SinkGlobalParameters['dt_sink_container_name']
    storage_account_name = SinkGlobalParameters['dt_sink_storage_name']
    
    """Generate connection"""
    spark.conf.set(
    "fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name), 
    dbutils.secrets.get(scope=scope_name, key=storage_access_key_secret_name))
    
    """Create final path to save delta"""
    delta_path = 'abfss://{}@{}.dfs.core.windows.net/{}'.format(container_name, storage_account_name, output_path)   
    
    """input parameters"""
    delta_key_columns = input_parameters["deltaKeyColumns"] # merge
    delta_partition_columns = input_parameters["deltaPartitionColumns"] # partition
    delta_output_columns = input_parameters["deltaOutputColumns"] # output        
    
    """DeltaOutputColumns: save the result filtering by columns"""
    if len(delta_output_columns) > 0:
      df = df.select(delta_output_columns)
      
    """Validate first merge and change to overwrite"""
    if write_mode.lower() == 'merge':
      write_mode = validate_merge(SinkGlobalParameters, output_path)   
      
    """Merge: save the result merging df updates with delta base."""
    if write_mode.lower() == 'merge':
      print('Saving delta {}...'.format(write_mode))
      delta_table = DeltaTable.forPath(spark, delta_path)
      key_list = []
      
      for key in delta_key_columns:
        key_list.append('delta_table_current.{id} = df_transformed.{id}'.format(id=key))
      
      delta_table.alias('delta_table_current') \
        .merge(
          df.alias('df_transformed'), 
          ' AND '.join(key_list)
      ) \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()    
      print('Success: {} in {}'.format(write_mode, delta_path))
    else:
      """DeltaPartitionColumns: save the result with partition using overwrite or append."""
      if len(delta_partition_columns) > 0:
        print('Saving delta {} with partition {}...'.format(write_mode, delta_partition_columns))
        df.write.format('delta') \
            .option("overwriteSchema",True) \
            .partitionBy(delta_partition_columns) \
            .mode(write_mode.lower()) \
            .save(delta_path)
        print('Success: {} with partition {} in {}'.format(write_mode, delta_partition_columns, delta_path))
      else:
        """No Partition: save the result using overwrite or append."""
        print('Saving delta {} with no partition...'.format(write_mode))
        df.write.format('delta') \
            .option("overwriteSchema",True) \
            .mode(write_mode.lower()) \
            .save(delta_path)
        print('Success: {} with no partition in {}'.format(write_mode, delta_path))
  except Exception as ex:
    errors.append('Error in save_delta_format method: {}'.format(ex))
    print(ex)

# COMMAND ----------

# DBTITLE 1,*Load SourceDatasets and create temporal views
counter = 1

for dataset_x in source_datasets_son:
  """get adls values"""
  storage_access_key_secret_name, storage_account_name, container_name = get_global_values(source_datasets_son[dataset_x]["sourceModule"])
  
  """read parquet or delta source"""
  df = read_source_data(scope_name, storage_access_key_secret_name, storage_account_name, container_name, source_datasets_son[dataset_x]["sourcePath"], source_datasets_son[dataset_x]["sourceFileFormat"])

  """create temp view"""
  df.createOrReplaceTempView("dataset{}".format(counter))
  
  """clean memory (disk and cache)"""
  df.unpersist()
  
  """increase counter"""
  counter += 1

# COMMAND ----------

# DBTITLE 1,Main
try:
  """"execute functions dynamically"""
  for function_name in input_parameters["functions"]:  
    df = locals()[function_name](df, function_name, input_parameters, DataTransformationParameters)
  
  finalschema=save_to_delta_format(df,'datatransformation',storage_account_name, output_path, write_mode, input_parameters, dbname= 'dataransformation',DeltaTableName='ComplexTransformations')

except Exception as ex:
    errors.append('Error in main for statement: {}'.format(ex))

# COMMAND ----------

# DBTITLE 1,*Delete temporal views
counter = 1
for dataset_x in source_datasets_son:
  spark.catalog.dropTempView("dataset{}".format(counter))  
  counter += 1

# COMMAND ----------

# DBTITLE 1,Output
if len(errors) > 0:
  raise Exception(str(errors))
notebook_output = {'Errors':'. \n'.join(errors)}
dbutils.notebook.exit(finalschema)
