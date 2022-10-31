# Databricks notebook source
# DBTITLE 1,Data Segregation
# This notebook is used to apply the Data Segregation logic to the source file located at "SourceFilePath" and outputs it to the "OutputFilePath"
# a. It parses the JSON parameters to get required details
# b. It mounts the path where the source file is located
# c. It reads the file from the mounting path
# d. it applies the segregation logic i.e. either generic data or sensitive data and outputs the file in the output container

# COMMAND ----------

# DBTITLE 1,Utilities
# MAGIC %run "/Shared/MDMF/Tools/utilities"

# COMMAND ----------

# DBTITLE 1,Widgets
# Obtain the parameters sent by Azure Data Factory

# To remove widgets
#dbutils.widgets.removeAll()

##source params
dbutils.widgets.text("NbRunParameters", "", "")
dbutils.widgets.text("SourceInstanceURL", "", "")
dbutils.widgets.text("SourceContainerName", "", "")
dbutils.widgets.text("SourceFilePath", "", "")
dbutils.widgets.text("SourceFileExtension", "", "")
dbutils.widgets.text("SourceKeySecretName", "", "")
dbutils.widgets.text("ObjectSchema", "", "")

##output params
dbutils.widgets.text("OutputParams", "", "")

# COMMAND ----------

# DBTITLE 1,Variables
##source variables
nb_run_parameters = json.loads(dbutils.widgets.get("NbRunParameters"))
source_instance_url = dbutils.widgets.get("SourceInstanceURL") 
source_container_name = dbutils.widgets.get("SourceContainerName") 
source_file_path = dbutils.widgets.get("SourceFilePath") 
source_file_extension = dbutils.widgets.get("SourceFileExtension") 
source_key_secret_name = dbutils.widgets.get("SourceKeySecretName")
object_schema = json.loads(dbutils.widgets.get("ObjectSchema"))
source_storage_name = return_storage_name(source_instance_url)

##output variables
output_params = json.loads(dbutils.widgets.get("OutputParams"))

#derived variables
kv_scope_name = nb_run_parameters['kvScopeName']
service_principal_id = nb_run_parameters['servicePrincipalId']
service_principal_secret_name = nb_run_parameters['servicePrincipalSecretName']
tenant_id = nb_run_parameters['tenantId']
dataset = output_params['Datasets']

# COMMAND ----------

# DBTITLE 1,Read the file from the source path
try:
  df = read_from_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, source_storage_name, source_container_name, source_file_path, source_file_extension)
  df.count()
  
except Exception as ex:
  #print('ERROR: {}'.format(ex))
  raise Exception('ERROR: {}'.format(ex))

# COMMAND ----------

# DBTITLE 1,Retrieving general/sensitive output fields from Dataset
try:
  for key in dataset.keys():

      column_names = dataset.get(key)["columnNames"]
      write_mode = dataset.get(key)["writeMode"]
      output_container_name = dataset.get(key)["outputContainerName"]
      output_file_path = dataset.get(key)["outputFilePath"]
      output_file_extension = dataset.get(key)["outputFileExtension"]
      output_key_secret_name = dataset.get(key)["outputKeySecretName"]
      output_instance_url = dataset.get(key)["outputInstanceURL"]
      output_storage_name = return_storage_name(output_instance_url)
      df2 = df.select(column_names)
      write_to_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, output_storage_name, output_container_name, output_file_path, output_file_extension, df2, object_schema = object_schema, write_mode = write_mode)

except Exception as ex:
  raise Exception('ERROR: {}'.format(ex))

# COMMAND ----------

# DBTITLE 1,Exiting the Notebook
dbutils.notebook.exit("DSeg_01_DataSegregation Notebook Succeded")
