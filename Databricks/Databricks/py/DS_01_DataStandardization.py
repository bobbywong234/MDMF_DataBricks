# Databricks notebook source
# MAGIC %md
# MAGIC #### Data Standardization
# MAGIC Reads the landing file and applies the Standardization rules, output as parquet format.

# COMMAND ----------

# DBTITLE 1,Utilities
# MAGIC %run "/Shared/MDMF/Tools/utilities"

# COMMAND ----------

# DBTITLE 1,Widgets
# Obtain the parameters sent by Azure Data Factory
#dbutils.widgets.removeAll()

##source params
dbutils.widgets.text("NbRunParameters", "", "")
dbutils.widgets.text("SourceInstanceURL", "", "")
dbutils.widgets.text("SourceContainerName", "", "")
dbutils.widgets.text("SourceFilePath", "", "")
dbutils.widgets.text("SourceFileExtension", "", "")
dbutils.widgets.text("SourceKeySecretName", "", "")
dbutils.widgets.text("ObjectSchema", "", "")
dbutils.widgets.text("ColumnSchema", "", "")
dbutils.widgets.text("InputParameters", "", "")
##output params
dbutils.widgets.text("OutputInstanceURL", "", "")
dbutils.widgets.text("OutputContainerName", "", "")
dbutils.widgets.text("OutputFilePath", "", "")
dbutils.widgets.text("OutputFileExtension", "", "")
dbutils.widgets.text("OutputKeySecretName", "", "")

# COMMAND ----------

# DBTITLE 1,Variables
##source vars
nb_run_parameters = json.loads(dbutils.widgets.get("NbRunParameters"))
source_instance_url = dbutils.widgets.get("SourceInstanceURL") 
source_container_name = dbutils.widgets.get("SourceContainerName") 
source_file_path = dbutils.widgets.get("SourceFilePath") 
source_file_extension = dbutils.widgets.get("SourceFileExtension") 
source_key_secret_name = dbutils.widgets.get("SourceKeySecretName")
source_object_schema = json.loads(dbutils.widgets.get("ObjectSchema"))
source_column_schema = json.loads(dbutils.widgets.get("ColumnSchema"))
input_parameters = json.loads(dbutils.widgets.get("InputParameters"))

##output vars
output_instance_url = dbutils.widgets.get("OutputInstanceURL") 
output_container_name = dbutils.widgets.get("OutputContainerName") 
output_file_path = dbutils.widgets.get("OutputFilePath") 
output_file_extension = dbutils.widgets.get("OutputFileExtension") 
output_key_secret_name = dbutils.widgets.get("OutputKeySecretName")
output_storage_name = return_storage_name(output_instance_url)

##derived vars
kv_scope_name = nb_run_parameters['kvScopeName']
service_principal_id = nb_run_parameters['servicePrincipalId']
service_principal_secret_name = nb_run_parameters['servicePrincipalSecretName']
tenant_id = nb_run_parameters['tenantId']
source_storage_name = return_storage_name(source_instance_url)

# COMMAND ----------

try:
  #read from ingested file (using column schema)
  pyspark_column_schema = format_column_schema(source_column_schema)
  df = read_from_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, source_storage_name, source_container_name, source_file_path, source_file_extension, source_object_schema, pyspark_column_schema)

  #writes into parquet format (this is the standardized fomart for "conformed" container)
  write_to_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, output_storage_name, output_container_name, output_file_path, output_file_extension, df)
except Exception as ex:
  raise Exception('ERROR: {}'.format(ex))

# COMMAND ----------

dbutils.notebook.exit("DS_01_Data_Standardization Notebook succeeded")
