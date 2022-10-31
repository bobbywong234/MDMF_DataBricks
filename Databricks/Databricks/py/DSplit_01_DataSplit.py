# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Data Split
# MAGIC Reads the landing file and applies the Split rules, output as parquet format.

# COMMAND ----------

# DBTITLE 1,Utilities
# MAGIC %run "/Shared/MDMF/Tools/utilities"

# COMMAND ----------

# DBTITLE 1,Widgets
# Obtain the parameters sent by Azure Data Factory
#dbutils.widgets.removeAll()

# Input Parameters
dbutils.widgets.text("SourceInstanceURL", "", "")
dbutils.widgets.text("SourceContainerName", "", "")
dbutils.widgets.text("SourceFilePath", "", "")
dbutils.widgets.text("SourceFileExtension", "", "")
dbutils.widgets.text("ObjectName", "", "")
dbutils.widgets.text("ObjectSchema", "", "")
dbutils.widgets.text("ColumnSchema", "", "")
dbutils.widgets.text("InputParameters", "", "")
dbutils.widgets.text("SourceKeySecretName", "", "")
dbutils.widgets.text("NbRunParameters", "", "")

# Output Parameters
dbutils.widgets.text("OutputParameters", "", "")

# COMMAND ----------

# DBTITLE 1,Variables
##source variables
nb_run_parameters = json.loads(dbutils.widgets.get("NbRunParameters"))
source_instance_url = dbutils.widgets.get("SourceInstanceURL")
source_container_name = dbutils.widgets.get("SourceContainerName")
source_object_schema = json.loads(dbutils.widgets.get("ObjectSchema"))
source_column_schema = json.loads(dbutils.widgets.get("ColumnSchema"))
source_file_path = dbutils.widgets.get("SourceFilePath")
source_file_extension = dbutils.widgets.get("SourceFileExtension")
object_name = dbutils.widgets.get("ObjectName")
input_parameters = json.loads(dbutils.widgets.get("InputParameters"))
source_key_secret_name = dbutils.widgets.get("SourceKeySecretName")

##output variables
output_parameters = json.loads(dbutils.widgets.get("OutputParameters"))

#derived variables
kv_scope_name = nb_run_parameters['kvScopeName']
service_principal_id = nb_run_parameters['servicePrincipalId']
service_principal_secret_name = nb_run_parameters['servicePrincipalSecretName']
tenant_id = nb_run_parameters['tenantId']
storage_name = return_storage_name(source_instance_url)

# COMMAND ----------

def split_module(df, dataset_type_dict):
  df.createOrReplaceTempView(object_name)
  df = spark.sql(f"""{dataset_type_dict["query"]}""")
  return df

def save_to_sink(df, dataset_type_dict):
  for each_output in dataset_type_dict["outputs"]:
    output_storage_name = return_storage_name(each_output["outputInstanceURL"])
    write_to_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, output_storage_name, each_output["outputContainerName"], each_output["outputFilePath"], each_output["outputFileExtension"], df, object_name = object_name, object_schema = source_object_schema)
  
  return None

# COMMAND ----------

try:
  ##read from ADLS (without schema)
  df = read_from_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, storage_name, source_container_name, source_file_path, source_file_extension, object_schema = source_object_schema)
  
  ##creates dataframes using input parameters and writes into ADLS
  for each_dataset_type in input_parameters:
    print("Reading from Input: ", input_parameters[each_dataset_type])
    df_split = split_module(df, input_parameters[each_dataset_type])
    print("Reading to Output: ", str(output_parameters[each_dataset_type]))
    save_to_sink(df_split, output_parameters[each_dataset_type])

except Exception as ex:
  raise Exception(f"Error: {ex}")

# COMMAND ----------

dbutils.notebook.exit("DSplit_01_DataSplit Notebook succeeded")
