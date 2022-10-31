# Databricks notebook source
# MAGIC %md 
# MAGIC ### Metadata-Driven Ingestion Framework 
# MAGIC #### Data Validation: (Data Validation Name)
# MAGIC (Data Validation description).

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate functions & variables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import libraries

# COMMAND ----------

# Import required libraries


# COMMAND ----------

# MAGIC %md
# MAGIC #### Declaration of functions

# COMMAND ----------

# Function to pass from ADF string params to a Databricks Python dictionary

def adf_to_dict(param_str):
  """ Convert the parameter passed by ADF from string to dictionary """
  dictionary = {}
  param_str = param_str.replace("{", "").replace("}", "").replace("'", "").replace("https:", "").replace("ftp:","").replace("C:","").replace("\\\\","/")
  param_str_split = param_str.split(",")

  for item in param_str_split:
    item_split = item.replace("\"", "").split(":")
    dictionary[item_split[0]] = item_split[1]
  
  return dictionary

# COMMAND ----------

# Function to pass from ADF string param to two Databricks Python dictionaries that include input parameters

def adf_to_dict_dv(param_str):
  ''' Convert the data validation parameter passed by ADF from string to two dictionaries. '''
  dictionary1 = {}
  dictionary2 = {}
  
  param_str = param_str.replace('"{',"").replace('"}', "").replace("{", "").replace("}", "").replace("'", "").replace("https:", "").replace("ftp:","").replace("\"\"", "\"").replace('\\',"")
  inputparameter_split = param_str.split(',"InputParameter":')
  param_str_split = inputparameter_split[0].split(",")
  for item in param_str_split:
    item_split = item.replace("\"", "").split(":")
    dictionary1[item_split[0]] = item_split[1].lstrip()
   
  input_parameter = inputparameter_split[1].replace("],","].").split(".")
  for item in input_parameter:
    item_split = item.split(":")  
    key = item_split[0].replace('"',"").lstrip()
    array = item_split[1].replace("[","").replace("]","").replace('"',"").split(",")
    dictionary2[key] = array
      
  return dictionary1, dictionary2

# COMMAND ----------

# Function to obtain values for Data Validation in Blob Storage Account
# Add the required input paramaters for the validation function inside the dv_blob function parameters like (input_parameter1, input_parameter2)
# blob_mount_name, kv_scope_name, blob_secret_name, sink_container_name, storage_account_name, sink_path, src_object_child parameters are always needed to connect to the sink
def dv_blob(kv_scope_name, blob_secret_name, sink_container_name, storage_account_name, sink_path, file_extension, input_parameter1, input_parameter2):
  """ Mount Blob Storage service using the read_blob function """
  is_connected, sink_file = read_blob(kv_scope_name, blob_secret_name, sink_container_name, storage_account_name, sink_path, file_extension)
  
  if is_connected and sink_file != None:
    print("Successfull connection and sink file loaded") 
    # Call the validation function with the required input parameters like (input_parameter1, input_parameter2) to save the variable/s nedeed for validation from the sink file
    # sink_file parameter is always required
    # Save the value/s received from validation function into a variable/s
    value_to_validate = validation_function(sink_file, input_parameter1, input_paramater2)
    # If two values are expected
   #value_to_validate, value_to_validate2 = validation_function(sink_file, input_parameter1, input_paramater2)
  else:
    print("Unsuccessfull connection, please see the logs")
    
  # Send the variable to validate  
  return value_to_validate
  # If two variables are expected
 #return value_to_validate, value_to_validate2 

# COMMAND ----------

# Function to connect and read from Blob Storage 

def read_blob(kv_scope_name, blob_secret_name, sink_container_name, storage_account_name, sink_path, file_extension):
  """ Connect to Blob Storage and read the file stored """   
  # Authenticate the filesystem initialization  
  file = None
  connection_status = True
  
  # Connect to an Azure Blob storage container
  try:
    spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name), "{}".format(dbutils.secrets.get(scope = "{}".format(kv_scope_name), key = "{}".format(blob_secret_name))))
  except:
    connection_status = False
    print("ERROR")
  
  # Read the file and store it in a variable 
  if connection_status:
    if file_extension.lower() == "txt":
      file_extension = "csv"
    file = spark.read.load("wasbs://{}@{}.blob.core.windows.net/{}".format(sink_container_name, storage_account_name, sink_path), format = file_extension, header = "true")
  
  return connection_status, file

# COMMAND ----------

# Function to return a value/s from sink file to validate
# Add the required input parameters like (input_parameter1, input_paramater2)
# sink_file paramater is always required
def validation_function(sink_file, input_parameter1, input_paramater2):
  """ Brief description about the validation function. example Count the number of input parameter in file """
  # Insert function code to return a value/s from sink file to validate
  
  
  # Send the value/s to validate     
  return value_to_validate 
  # if two values are expected 
 #return value_to_validate, value_to_validate2 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Declaration of variables and execution of functions

# COMMAND ----------

# Obtain the parameters sent by Azure Data Factory

dbutils.widgets.text("DataValidationParameters", "", "")
dv_params = dbutils.widgets.get("DataValidationParameters")

dbutils.widgets.text("SinkGlobalParameters", "", "")
sink_params = dbutils.widgets.get("SinkGlobalParameters")

# COMMAND ----------

# Convert the string sent from ADF to a dictionary using adf_to_dict and adf_to_dict_dv functions

#dv_params_dict, input_parameter_dict = adf_to_dict_dv(dv_params)
#sink_params_dict = adf_to_dict(sink_params)

dv_params_dict = json.loads(dv_params)
sink_params_dict = json.loads(sink_params)

# COMMAND ----------

# Declare general variables

fwklog_id = dv_params_dict["FwkLogId"]                             # ID of the Framework Log
kv_scope_name = sink_params_dict["kv_scope_name"]                  # Name of the Azure Key Vault-backed scope

# Declare sink variables 

storage_account_name = sink_params_dict["ing_sink_storage_name"]       # Name of the Azure Datalake Storage Account 
blob_secret_name = sink_params_dict["ing_sink_storage_secret_name"]    # Name of the secret that stores the Storage's Account Key on AKV
sink_container_name = sink_params_dict["ing_sink_container_name"]      # Name of the container in the Azure Blob Storage Account 
sink_path = dv_params_dict["SinkPath"]                             # Path of the sink file
file_extension = dv_params_dict["FileFormat"]                      # Format of the sink file

#Add any number of required parameters from input parameters 

input_parameter1 = input_parameter_dict["input_parameter1"]        # First input parameter
input_parameter2 = input_parameter_dict["input_parameter2"]        # Second input parameter

#Add other parameters from temporal parameters (Not always required)

src_type = dv_params_dict["SrcType"]                               # Type of source
src_object = dv_params_dict["SrcObjectName"]                       # Name of the Source Object
instance_url = dv_params_dict["InstanceURL"]                       # Instance URL of the source
port = dv_params_dict["Port"]                                      # Port number of the source
username = dv_params_dict["UserName"]                              # Username of the source
secret_name = dv_params_dict["SecretName"]                         # Name of the secret for the source
path_folder = dv_params_dict["PathFolderName"]                     # Path of the folder
ip_address = dv_params_dict["IPAddress"]                           # IP adrress of the source

#Add any other parameters


# COMMAND ----------

# Obtain the count of values that do not meet the regex parameter

variable_to_validate = dv_blob(kv_scope_name, blob_secret_name, sink_container_name, storage_account_name, sink_path, file_extension, input_parameter1, input_parameter2) 
# if two variables are used to validate
#variable_to_validate, variable_to_validate2 = dv_blob(kv_scope_name, blob_secret_name, sink_container_name, storage_account_name, sink_path, file_extension, input_parameter1, input_parameter2) 

# COMMAND ----------

# Build the inputs for the DataValidationLog table

if variable_to_validate:                                      # Add the condition to validate the variable
  validation_status = "Succeeded"
  validation_bool = "True"
  message = "Null"
else:
  validation_status = "Failed"
  validation_bool = "False"
  # Add details for fail message
  message = "Message with information of the failure with the results"

json_output = {"FwkLogId": fwklog_id, "Output": {"Count": variable_to_validate, "Validation": { "Status": validation_bool, "Message": message}}}

# COMMAND ----------

# Pass parameter to ADF

dbutils.notebook.exit(json_output)

# COMMAND ----------

# Declare the functions & variables.

functions = ["adf_to_dict", "my_func"]
variables = ["fwklog_id", "kv_scope_name", "my_var"]

# Iterate over each function to validate its existance in the notebook. Save the functions not found in an array.

missing_functions = []

for function in functions:
  if function not in globals():
    missing_functions.append(function)
    
# Iterate over each variable to validate its existance in the notebook. Save the functions not found in an array.
missing_variables = []

for variable in variables:
  if variable not in globals():
    missing_variables.append(variable)
    
# Notify the user if a function or variable is missing.

# FUNCTIONS
if missing_functions:
  print(f"The function(s) not found in the notebook is/are: {missing_functions}. \n")
else:
  print("All the functions were found.")

# VARIABLES
if missing_variables:
  print(f"The variable(s) not found in the notebook is/are: {missing_variables}.")
else:
  print("All the variables were found.")