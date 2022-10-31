# Databricks notebook source
# DBTITLE 1,Metadata-Driven Ingestion Framework
# MAGIC %md
# MAGIC #### Data Ingestion: Convert to Parquet
# MAGIC Connect to sink instance and convert the file to parquet. Validate the result and send it to Azure Data Factory.

# COMMAND ----------

# DBTITLE 1,Utilities
# MAGIC %run "/Shared/Metadata Driven Ingestion Framework/Tools/utilities"

# COMMAND ----------

# DBTITLE 1,Widgets
# Obtain the parameters sent by Azure Data Factory
dbutils.widgets.text("SinkGlobalParameters", "", "")
sink_params = dbutils.widgets.get("SinkGlobalParameters") 

dbutils.widgets.text("FwkParameters", "", "")
fwk_params = dbutils.widgets.get("FwkParameters")

dbutils.widgets.text("SinkPath", "", "")
sink_path = str(dbutils.widgets.get("SinkPath")).replace('///','/').replace('//','/')

dbutils.widgets.text("File", "", "")
landing_path_file_name = str(dbutils.widgets.get("File")).replace('///','/').replace('//','/')

# COMMAND ----------

# DBTITLE 1,Variables
fwk_params_dict = json.loads(fwk_params)
sink_params_dict = json.loads(sink_params)

kv_scope_name = sink_params_dict["kv_scope_name"]                                     # Name of the Azure Key Vault-backed scope
kv_workspace_id = sink_params_dict["kv_workspace_id"].strip()                         # Name of the secret for the log analytics workspace id
kv_workspace_pk = sink_params_dict["kv_workspace_pk"].strip()                         # Name of the secret for the log analytics primary key
ing_sink_storage_secret_name = sink_params_dict["ing_sink_storage_secret_name"]
adls_source_secret = fwk_params_dict['SecretName']
ing_sink_container_name = sink_params_dict["ing_sink_container_name"] 
ing_sink_storage_name = sink_params_dict["ing_sink_storage_name"]
ing_sink_container_name = sink_params_dict["ing_sink_container_name"]
file_format = (landing_path_file_name.split('.')[-1]).lower()
file_name = landing_path_file_name.split('/')[-1]


# COMMAND ----------

# DBTITLE 1,Main Function
parquet_status = True
path = "abfss://{}@{}.dfs.core.windows.net/{}".format(ing_sink_container_name, ing_sink_storage_name, landing_path_file_name)
  
try:   
  spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(ing_sink_storage_name),"{}".format(dbutils.secrets.get(scope = "{}".format(kv_scope_name), key= "{}".format(ing_sink_storage_secret_name))))
  print(file_format)
  """Read data Output Files and create delta tables """    
  if file_format  == 'csv':      
    df = spark.read.format("csv").option("header","true").load(path)        
  elif file_format  == 'txt':
    df = spark.read.option("header", "true").option("delimiter","|").csv(path)  #as of spark1.6 you can use  csv to read txt    
  elif file_format == 'json':
    df = spark.read.option("multiline","true").json(path)    
  elif file_format == 'parquet':
    df = spark.read.parquet(path)    
  elif file_format == 'xml':
    #file_name = landing_path_file_name.split('/')[-1]
    print('file name: {}'.format(file_name))
    landing_path_file_name = landing_path_file_name.replace(file_name, '')
    print('landing path file name: {}'.format(landing_path_file_name))
    mnt_path = mount_to_mnt(landing_path_file_name, kv_scope_name, ing_sink_storage_secret_name,ing_sink_storage_name,ing_sink_container_name)
    print('mnt path: {}'.format(mnt_path))
    finalpath=mnt_path + file_name
    print(finalpath)
    df_pd = pd.read_xml(finalpath) 
    df=spark.createDataFrame(df_pd)
    #unmount(mnt_path)
  elif file_format  == 'xls' or file_format=='xlsx':
    !pip install openpyxl
    #file_name = landing_path_file_name.split('/')[-1]
    print('file name: {}'.format(file_name))
    landing_path_file_name = landing_path_file_name.replace(file_name, '')
    print('landing path file name: {}'.format(landing_path_file_name))
    mnt_path = mount_to_mnt(landing_path_file_name, kv_scope_name, ing_sink_storage_secret_name,ing_sink_storage_name,ing_sink_container_name)
    print('mnt path: {}'.format(mnt_path))
    df_pd = pd.read_excel(mnt_path + file_name)
    df= spark.createDataFrame(df_pd.astype(str))
    #unmount(mnt_path)
  
  save_to_adls = "abfss://{}@{}.dfs.core.windows.net/{}".format(ing_sink_container_name, ing_sink_storage_name, sink_path)
  df.write.format("parquet").mode("overwrite").save(save_to_adls)
except Exception as ex:
  print('ERROR: {}'.format(ex))
  raise Exception(f'Error: {ex}')
  msg_error = {'ExecutionStatus': 'Failed','Error Message':'ERROR in convert_tables method','FunctionName':'convert_tables'}
  post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)

# COMMAND ----------


