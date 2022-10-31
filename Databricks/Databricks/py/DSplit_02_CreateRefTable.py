# Databricks notebook source
# MAGIC %md
# MAGIC #### Data Split: Create Reference Delta Table
# MAGIC Connects to the delta lake entity on ADLS and creates the delta table.

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
dbutils.widgets.text("DatabaseName", "", "")
dbutils.widgets.text("TableName", "", "")
dbutils.widgets.text("ColumnNames", "", "")
dbutils.widgets.text("WhereClause", "", "")

# COMMAND ----------

# DBTITLE 1,Variables
##source vars
nb_run_parameters = json.loads(dbutils.widgets.get("NbRunParameters"))
source_instance_url = dbutils.widgets.get("SourceInstanceURL") 
source_container_name = dbutils.widgets.get("SourceContainerName") 
source_file_path = dbutils.widgets.get("SourceFilePath") 
source_file_extension = dbutils.widgets.get("SourceFileExtension") 
source_key_secret_name = dbutils.widgets.get("SourceKeySecretName")

##output vars
database_name = dbutils.widgets.get("DatabaseName") 
table_name = dbutils.widgets.get("TableName")
column_names = dbutils.widgets.get("ColumnNames") 
where_clause = dbutils.widgets.get("WhereClause")
sql_query = "select {columns} from {table} where {clause}".format(columns=column_names, table=table_name, clause=where_clause)

##derived vars
kv_scope_name = nb_run_parameters['kvScopeName']
# application-id
service_principal_id = nb_run_parameters['servicePrincipalId']
service_principal_secret_name = nb_run_parameters['servicePrincipalSecretName']
# directory-id
tenant_id = nb_run_parameters['tenantId']
source_storage_name = return_storage_name(source_instance_url)

# COMMAND ----------

# DBTITLE 1,Create Reference Delta Table
##read from ADLS
df = read_from_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, source_storage_name, source_container_name, source_file_path, source_file_extension)
df.createOrReplaceTempView(table_name)

##create database if not exists
create_database_statement = "CREATE DATABASE IF NOT EXISTS {database}".format(database=database_name)
print(create_database_statement)
spark.sql(create_database_statement)

##create or replace delta table
create_table_statement = "CREATE OR REPLACE TABLE {database}.{table} AS {query}".format(database=database_name, table=table_name, query=sql_query)
print(create_table_statement)
spark.sql(create_table_statement)

# COMMAND ----------

dbutils.notebook.exit("DSplit_02_CreateRefTable Notebook succeeded")
