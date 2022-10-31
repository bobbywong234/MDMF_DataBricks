# Databricks notebook source
# DBTITLE 1,Imports
import json
import requests
import datetime
import hashlib
import hmac
import base64
import numpy
import dateutil
import pandas as pd
import re
from pyspark.sql import SparkSession
import functools
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import *
#from pyspark.sql import SparkSession
import time
#import pprint
import numpy as np
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Mount
def mount_to_dbfs(kv_scope_name, storage_name, key_secret_name, container_name, folder_path):
  mount_point = '/mnt/mdmf/temp/{}'.format(folder_path)  
  source_path = "wasbs://{container}@{storage}.blob.core.windows.net/{folder}".format(container=container_name, storage=storage_name, folder=folder_path)
  configs = {"fs.azure.account.key.{}.blob.core.windows.net".format(storage_name):dbutils.secrets.get(scope = kv_scope_name, key = key_secret_name)}
  #source_path = "abfss://{container}@{storage}.dfs.core.windows.net/{folder}".format(container=container_name, storage=storage_name, folder=folder_path)
  #configs = {"fs.azure.account.key.{}.dfs.core.windows.net".format(storage_name):dbutils.secrets.get(scope = kv_scope_name, key = key_secret_name)}
  
  if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    try:
      print('Mounting {}'.format(mount_point))
      dbutils.fs.mount(
        source = source_path,
        mount_point = mount_point,
        extra_configs = configs)
      print('Mounted: {} -> {}'.format(source_path, mount_point))
    except Exception as e:
      print("Error ocurred when trying to mount: {}".format(e))

  return mount_point

# COMMAND ----------

# DBTITLE 1,Unmount
def unmount_to_dbfs(mount_point):
  if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    try:
      print('Unmounting {}...'.format(mount_point))
      dbutils.fs.unmount(mount_point)
    except Exception as e:
      print("Error ocurred when trying to unmount: {}".format(e))

# COMMAND ----------

# DBTITLE 1,Format sql schema into pyspark schema
# set column schema to pyspark datatype
def format_column_schema(json_schema):
  schema = []
  for item in json_schema:
    column_name = item['name']
    column_type = item['type'].lower()
    
    if "varchar" in column_type:
      new_schema = f"{column_name} STRING"
    elif column_type == 'int':
      new_schema = f"{column_name} INTEGER"
    elif column_type == 'bigint':
      new_schema = f"{column_name} LONG"
    elif "decimal" in column_type:
      new_schema = f"{column_name} {column_type.upper()}"
    elif column_type == 'datetime':  
      new_schema = f"{column_name} TIMESTAMP"
    elif column_type == 'bit':
      new_schema = f"{column_name} BOOLEAN"
    else:
      new_schema = f"{column_name} {column_type}"
    schema.append(new_schema)
  
  schema = ', '.join(schema)
  
  return schema

#from shcema struct to sql 
def schema2sql(structschema):
  outschema=[]
  for n, row in enumerate((structschema.jsonValue())['fields']):
              data={}
              data['id'] = str(n+1)
              data['name'] = row['name']
              types=['string','decimal','double','integer','boolean','timestamp','date','binary']
              sqlt=['Varchar (255)', 'Decimal(18,4)','Decimal(18,4)','Int','Datetime','Date','Binary']
              for i in range(len(types)):
                if types[i] in row['type'].lower():
                  data['type']=sqlt[i]
                  break
                else:
                  data['type']=row['type']

              outschema.append(data)
  return outschema

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

# DBTITLE 1,Validate merge
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

# DBTITLE 1,READ PARQUET DELTA AND SAVE TO DELTA
def read_source_data(scope_name, storage_account_name, storage_access_key_secret_name, container_name, source_path, source_file_format,columnschema=None):  
  try:
    spark.conf.set(
    "fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name), 
    dbutils.secrets.get(scope=scope_name, key=storage_access_key_secret_name))
    
    #spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", dbutils.secrets.get(scope=scope_name,key=storage_access_key_secret_name))

    
    spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
    #spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    
    source_final_path = "abfss://{container}@{storage}.dfs.core.windows.net/{file}".format(container=container_name, storage=storage_account_name, file=source_path)
    
    if source_file_format.lower() == 'delta':
      #df = spark.read.schema(columnschema).format("delta").load(source_final_path)
      df = spark.read.format("delta").load(source_final_path)
    else:
      try:
        df=spark.read.schema(columnschema).parquet(source_final_path)
      except:
        df = spark.read.parquet(source_final_path)  
      
    return df 
  except Exception as ex:
    print(ex)
    errors.append('Error in read_source_data method: {}'.format(ex))

def save_to_delta_format(df, container_name,storage_account_name, output_path, write_mode, input_parameters,vacuum=168,dbname='datavalidation',DeltaTableName='DeltaTable'):
  try:
    """input parameters"""
    delta_key_columns = input_parameters["deltaKeyColumns"] # merge
    delta_partition_columns = input_parameters["deltaPartitionColumns"] # partition
    delta_output_columns = input_parameters["deltaOutputColumns"] # output
    
    table_path = 'abfss://{}@{}.dfs.core.windows.net/{}'.format(container_name, storage_account_name, output_path)
    print(table_path)
    #delta table config
    spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
    spark.sql("CREATE DATABASE IF NOT EXISTS " + dbname)
   
    """DeltaOutputColumns: save the result filtering by columns"""
    if len(delta_output_columns) > 0:
      df = df.select(delta_output_columns)
  
    """DeltaPartitionColumns: save the result by columns partitioning using append, overwrite or merge"""
    if write_mode.lower() == 'merge':
      print('Saving delta {}...'.format(write_mode))
      delta_table = DeltaTable.forPath(spark, table_path) 
      # delete the ones with more than n hours
      
      
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
      print('Done: {} '.format(write_mode))
    else:
      if len(delta_partition_columns) > 0:
        print('Saving delta {} with partition {}...'.format(write_mode, delta_partition_columns))
        spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
        df.write.format('delta') \
            .option("overwriteSchema",True) \
            .partitionBy(delta_partition_columns) \
            .mode(write_mode.lower()) \
            .save(table_path)
        
        
        print('Done: {} with partition {}.'.format(write_mode, delta_partition_columns))
      else:
        print('Saving delta {} with no partition...'.format(write_mode))
        
        df.write.format('delta') \
              .option("overwriteSchema",True) \
              .mode(write_mode.lower()) \
              .save(table_path)
      
      
      #create DELTA TABLE
        
    df.write.format('delta') \
      .option("overwriteSchema",True) \
      .partitionBy(delta_partition_columns) \
      .mode(write_mode.lower()) \
      .saveAsTable(dbname+'.'+DeltaTableName)
          
    print('Done: {} with no partition.'.format(write_mode))
    #schema for the validated df
    dfschema=schema2sql(df.schema)
    #vacuum the ones that have more than 168 hours old
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.vacuum(vacuum)
    
    return dfschema

  except Exception as ex:
    print(ex)
    errors.append('Error in save_delta_format method: {}'.format(ex))

# COMMAND ----------

# DBTITLE 1,Build API signature
def build_signature(customer_id, shared_key, date, content_length, method, content_type, resource):
  x_headers = 'x-ms-date:' + date
  string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
  authorization = ""
  try:
    bytes_to_hash = str.encode(string_to_hash,'utf-8')  
    decoded_key = base64.b64decode(shared_key)
    encoded_hash = (base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest())).decode()
    authorization = "SharedKey {}:{}".format(customer_id,encoded_hash)
  except Exception as i:
    print('Error in Utilities/build_signature: ',i)
    raise Exception(f'Error: {i}')
  return authorization

# COMMAND ----------

# DBTITLE 1,Send POST request to api/logs
def post_data(scope, customer_id, shared_key, output):
  log_type = 'MDMFDatabricksLog'
  method = 'POST'
  content_type = 'application/json'
  resource = '/api/logs'
  body = json.dumps(output)
  content_length = len(body)
  #Retrieve your Log Analytics Workspace ID from your Key Vault Databricks Secret Scope
  wks_id = dbutils.secrets.get(scope = scope, key = customer_id)
  #Retrieve your Log Analytics Primary Key from your Key Vault Databricks Secret Scope
  wks_shared_key = dbutils.secrets.get(scope = scope, key = shared_key)  
  rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
  signature = build_signature(wks_id, wks_shared_key, rfc1123date, content_length, method, content_type, resource)
  uri = 'https://' + wks_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

  headers = {
      'content-type': content_type,
      'Authorization': signature,
      'Log-Type': log_type,
      'x-ms-date': rfc1123date
  }

  response = requests.post(uri,data=body, headers=headers)
  if (response.status_code >= 200 and response.status_code <= 299):
      print ('Log Accepted')
  else:
      print ("Log Response code: {}".format(response.status_code))

# COMMAND ----------

# DBTITLE 1,Return the Storage Name from instance URL
def return_storage_name(instance_url):
  return (instance_url.split(".")[0]).split("/")[-1]

# COMMAND ----------

# DBTITLE 1,Returns ADLS full file path url
def return_file_path(container_name, storage_name, file_path):
  return "abfss://{}@{}.dfs.core.windows.net/{}".format(container_name, storage_name, file_path)

# COMMAND ----------

# DBTITLE 1,Read from ADLS
def read_from_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, storage_name, container_name, file_path, file_extension, object_schema=None, column_schema=None):  
  service_credential = dbutils.secrets.get(scope=kv_scope_name,key=service_principal_secret_name)

  spark.conf.set(f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net", "OAuth")
  spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net", service_principal_id)
  spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net", service_credential)
  spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

  spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
  
  full_file_path = return_file_path(container_name, storage_name, file_path)
  
  folder_path = file_path[0:file_path.rfind("/")]
  file_name = file_path.split("/")[-1]
  file_extension = file_extension.lower()
  file_delimiter = "|" #pipe as default
  header_flag = "true" #header as default
  #getting values from object_schema
  if object_schema:
    if ('delimiter' in object_schema):
      file_delimiter = object_schema['delimiter']
    if ('headerFlag' in object_schema):
      if object_schema['headerFlag'].upper() == 'Y' or object_schema['headerFlag'].upper() == 'TRUE':
        header_flag = "true"
      else:
        header_flag = "false"

  if file_extension  == 'csv':
    if column_schema:
      ##read with schema
      df = spark.read.schema(column_schema).format("csv").option("header",header_flag).load(full_file_path)
    else:
      ##read with no schema
      df = spark.read.format("csv").option("header",header_flag).load(full_file_path)
  elif file_extension  == 'txt':
    if column_schema:
      ##read with schema
      df = spark.read.schema(column_schema).option("header", header_flag).option("delimiter",file_delimiter).csv(full_file_path)
    else:
      ##read with no schema
      df = spark.read.option("header", header_flag).option("delimiter",file_delimiter).csv(full_file_path)
  elif file_extension == 'json':
    if column_schema:
      ##read with schema
      df = spark.read.schema(column_schema).option("multiline","true").json(full_file_path)
    else:
      ##read with no schema
      df = spark.read.option("multiline","true").json(full_file_path)
  elif file_extension == 'parquet':
    if column_schema:
      ##read with schema
      df = spark.read.schema(column_schema).parquet(full_file_path)
    else:
      ##read with no schema
      df = spark.read.parquet(full_file_path)
  elif file_extension == 'delta':
    spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
    if column_schema:
      ##read with schema
      df = spark.read.schema(column_schema).format("delta").load(full_file_path)
    else:
      ##read with no schema
      df = spark.read.format("delta").load(full_file_path)
  elif file_extension == 'xml':
    mount_path = mount_to_dbfs(kv_scope_name, storage_name, key_secret_name, container_name, folder_path)
    mount_full_path = '/dbfs' + mount_path + '/' + file_name

    df_pd = pd.read_xml(mount_full_path)
    df_pd = df_pd.replace('nan','NaN').fillna('NaN')

    if column_schema:
      ##read with schema
      df = spark.createDataFrame(df_pd, column_schema)
    else:
      ##read with no schema
      df = spark.createDataFrame(df_pd)
  elif file_extension  == 'xls' or file_extension=='xlsx':
    #import glob
    #path = '/dbfs/mnt/mdmf/temp/FS/VM1_FS/Financial/Full/Financial_20220715215952'
    #path2 = '/dbfs/mnt/mdmf/temp/FS/VM1_FS/Financial/Full/Financial_20220715215952/*.csv'
    #for filename in glob.glob(path):
    #    print(filename)
    #print(path)

    !pip install openpyxl
    !pip install xlrd

    mount_path = mount_to_dbfs(kv_scope_name, storage_name, key_secret_name, container_name, folder_path)
    mount_full_path = '/dbfs' + mount_path + '/' + file_name

    #sc = SparkContext.getOrCreate()
    #spark_session = SparkSession(sc)
    df_pd = pd.read_excel(mount_full_path, engine='openpyxl')
    df_pd = df_pd.replace('nan','NaN').fillna('NaN').replace('?','NaN')
    df_pd2 = df_pd.rename(columns=lambda x: x.replace(" ", '_'))
    df_pd2 = df_pd2.rename(columns=lambda x: x.replace(":" , ''))

    if column_schema:
      ##read with schema
      df = spark.createDataFrame(df_pd2, column_schema)
    else:
      ##read with no schema
      df = spark.createDataFrame(df_pd2)
  else:
    raise Exception('The function "read_from_adls" does not support the file extension: {}'.format(file_extension))

  return df

# COMMAND ----------

# DBTITLE 1,Write to ADLS
def write_to_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, storage_name, container_name, file_path, file_extension, df, object_schema=None, write_mode=None, object_name=None):  
  
  service_credential = dbutils.secrets.get(scope=kv_scope_name,key=service_principal_secret_name)

  spark.conf.set(f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net", "OAuth")
  spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net", service_principal_id)
  spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net", service_credential)
  spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

  spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
  
  full_file_path = return_file_path(container_name, storage_name, file_path)
  
  if not write_mode:
      write_mode = "overwrite"
  write_mode = write_mode.lower()
  file_extension = file_extension.lower()
  file_delimiter = "|" #pipe as default
  header_flag = "true" #header as default
  primary_key = ""
  #getting values from object_schema
  if object_schema:
    if ('delimiter' in object_schema):
      file_delimiter = object_schema['delimiter']
    if ('headerFlag' in object_schema):
      if object_schema['headerFlag'].upper() == 'Y' or object_schema['headerFlag'].upper() == 'TRUE':
        header_flag = "true"
      else:
        header_flag = "false"
    if ('primaryKey' in object_schema):
      primary_key = object_schema['primaryKey']
  
  if file_extension == 'parquet': 
    df.write.format("parquet").mode(write_mode).save(full_file_path)
  elif file_extension == 'delta':
    
    #If full_file_path does not exists yet then is assumed to be the first execution therefore write_mode is changed to "overwrite"
    try:
      dbutils.fs.ls(full_file_path)
    except Exception as ex:
      if 'FileNotFoundException' in str(ex):
        print('Delta path does not exist yet, write_method changed to overwrite')
        write_mode = "overwrite"
      else:
        raise Exception('ERROR: {}'.format(ex))
    
    if write_mode == 'merge':
      if not primary_key:
        raise Exception('PrimaryKey is not defined, please add the primary key column list on the ObjectSchema column in FwkObjectMetadata table.')
      
      delta_table = DeltaTable.forPath(spark, full_file_path)
      df.createOrReplaceTempView("df_update")
      
      key_list = []
      for key in primary_key.split(','):
        key_list.append('delta_table.{id} = df_update.{id}'.format(id=key.strip()))
      key_list = ' AND '.join(key_list)
      
      spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true") #Allow schema evolution
      
      delta_table.alias('delta_table') \
       .merge(
          df.alias('df_update'), 
          key_list) \
       .whenMatchedUpdateAll() \
       .whenNotMatchedInsertAll() \
       .execute()
    else:
      df.write.format("delta").option("mergeSchema", "true").mode(write_mode).save(full_file_path) #Allow schema evolution
  elif file_extension  == 'csv':
    df.write.format("csv").option("header", header_flag).mode(write_mode).save(full_file_path)
  elif file_extension  == 'txt':
    df.write.format("csv").option("header", header_flag).options("delimiter",file_delimiter).mode(write_mode).save(full_file_path)
  elif file_extension == 'json':
    df.coalesce(1).write.format('json').save(full_file_path)
  elif file_extension == 'xml':
    df.write.format("com.databricks.spark.xml")\
    .option("rootTag", object_name + 's')\
    .option("rowTag", object_name)\
    .mode(write_mode)\
    .save(full_file_path)
  elif file_extension  == 'xls' or file_extension=='xlsx':
    df.write.format("csv").option("header", header_flag).mode(write_mode).save(full_file_path)
  else:
    raise Exception('The function "write_to_adls" does not support the file extension: {}'.format(file_extension))
