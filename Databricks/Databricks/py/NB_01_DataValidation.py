# Databricks notebook source
# MAGIC %run "/Shared/MDMF/Tools/utilities"

# COMMAND ----------

# DBTITLE 1,Dependencies
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("DataValidationParameters", "", "")
Widget_DataValidationParameters = dbutils.widgets.get("DataValidationParameters")

dbutils.widgets.text("SinkGlobalParameters", "", "")
Widget_SinkGlobalParameters = dbutils.widgets.get("SinkGlobalParameters")

# COMMAND ----------

# DBTITLE 1,DataTransformationParameters to Vars
DataValidationParameters = json.loads(Widget_DataValidationParameters)

dvlog_id = DataValidationParameters["DvLogId"]
"""WriteMode"""
write_mode = DataValidationParameters["WriteMode"]

"""InputParameters"""
input_parameters = json.loads(DataValidationParameters["InputParameters"])


"""SourceDataSets"""
import re
DataValidationParameters["SourceDatasets"]=re.sub('[\n\t\r]','',DataValidationParameters["SourceDatasets"])


source_datasets_father = json.loads(DataValidationParameters["SourceDatasets"])

#json.loads take a string as input and returns a dictionary as output.
#json.dumps take a dictionary as input and returns a string as output.
source_datasets_son = json.loads((DataValidationParameters["SourceDatasets"]).replace('\r\n\t\t\t\t\t',''))['sourceDatasets']
dsval=[x for x in source_datasets_son.keys()][0]
dataset_a = json.loads(json.dumps(source_datasets_son[dsval]))
source_module = dataset_a["sourceModule"]
source_path = dataset_a["sourcePath"]
landing_path=dataset_a["landingPath"]
source_file_format = dataset_a["sourceFileFormat"]
columnSchema=json.loads(dataset_a['columnSchema'])
columnschema = [i ['name'] for i in columnSchema] #list of columns only

"""Output"""
output_path = DataValidationParameters["OutputPath"]

"""Errors"""
errors = []
notebook_output = {}

# COMMAND ----------

# DBTITLE 1,SinkGlobalParameters to Vars
SinkGlobalParameters = json.loads(Widget_SinkGlobalParameters)

kv_scope_name = SinkGlobalParameters['kv_scope_name']
kv_workspace_id = SinkGlobalParameters['kv_workspace_id']
kv_workspace_pk = SinkGlobalParameters['kv_workspace_pk']
ing_sink_storage_type = SinkGlobalParameters['ing_sink_storage_type']
dv_schema_container_name = SinkGlobalParameters['dv_schema_container_name']


"""dynamic values"""
storage_account_name = ''
storage_access_key_secret_name = ''
container_name = ''

if 'ingestion' in source_module.lower():
  storage_account_name = SinkGlobalParameters['ing_sink_storage_name']
  storage_access_key_secret_name = SinkGlobalParameters['ing_sink_storage_secret_name']
  container_name = SinkGlobalParameters['ing_sink_container_name']

if 'validation' in source_module.lower():
  storage_account_name = SinkGlobalParameters['dv_sink_storage_name']
  storage_access_key_secret_name = SinkGlobalParameters['dv_sink_storage_secret_name']
  container_name = SinkGlobalParameters['dv_sink_container_name']

adls_source_name = '{}.dfs.core.windows.net/'.format(storage_account_name)


# COMMAND ----------

DataValidationParameters

# COMMAND ----------

class DataValidation: # All the validations available pending great expect py library 
  
  def __init__(self,dataframe=None):
    print('Data Validation MDMF')
    
    if dataframe==None or dataframe.count()<1:

        #self.dataframe = read_pqt(kv_scope_name, storage_access_key_secret_name,
         #              storage_account_name,container_name, source_path)
        self.dataframe=read_source_data(kv_scope_name, storage_account_name,storage_access_key_secret_name,
                       container_name, source_path,source_file_format)
        
    else:
      self.dataframe=dataframe


#VALIDATION FOR ROW COUNT
  def rowcount(self,*args,**kwargs):
    self.function_name = kwargs.get('function_name',"default value")
    self.Dvmethod = kwargs.get('Dvmethod',"default value")
    self.input_dict = kwargs.get('input_dict',"default value")
    try:
      """Read data Output Files and create delta tables """
      spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name),"{}".format(dbutils.secrets.get(scope = "{}".format(kv_scope_name), key= "{}".format(storage_access_key_secret_name))))    
      source_path2=source_path.replace(source_path.split('/')[-1],'')
      df_converted = read_source_data(kv_scope_name, storage_account_name,storage_access_key_secret_name,
                       container_name, source_path,source_file_format)
      #read_pqt(kv_scope_name, storage_access_key_secret_name, storage_account_name, container_name, source_path2,self.colschema)
      row_count_converted = df_converted.count() 

      row_count_landing = 0

      if source_file_format.lower() == 'parquet' or source_file_format.lower() == 'delta':
        df_landing = read_source_data(kv_scope_name, storage_account_name,storage_access_key_secret_name,
                       container_name, source_path,source_file_format) 
        #read_pqt(kv_scope_name, storage_access_key_secret_name, storage_account_name, container_name, landing_path )
        row_count_landing = df_landing.count()
      else:
        extension_file = source_file_format.lower()
        path_landing = "abfss://{}@{}{}".format(container_name, adls_source_name, landing_path)

        """ xml, xls and xlsx need be mounted using /mnt/... """
        if extension_file == 'xml': # XML 
          file_name = landing_path.split('/')[-1]
          
          landing_path2 = landing_path.replace(file_name, '')
          mnt_path = mount_to_mnt(landing_path2, kv_scope_name, storage_access_key_secret_name,storage_account_name,container_name)
          df = pd.read_xml(mnt_path + file_name)        
          row_count_landing = len(df)      
        elif extension_file  == 'xls' or extension_file=='xlsx':
          file_name = landing_path.split('/')[-1]
          landing_path2 = landing_path.replace(file_name, '')
          mnt_path = mount_to_mnt(landing_path2, kv_scope_name, storage_access_key_secret_name,storage_account_name,container_name)
          df=pd.read_excel(mnt_path+file_name) 
          row_count_landing = len(df)

          """ Read landing path directly without utilities: """
        elif extension_file == 'csv':          
          df_landing = spark.read.format("csv").option("header","true").load(path_landing)
          row_count_landing = df_landing.count()          
        elif extension_file == 'parquet':
          df_landing = spark.read.parquet(path_landing)
          row_count_landing = df_landing.count()
        elif extension_file == 'parquet folder':
          df_landing = spark.read.parquet(path_landing + '*.parquet')
          print('parquet folder, path_landing: {}'.format(path_landing))
          row_count_landing = df_landing.count()          
        elif extension_file == 'json':          
          df_landing = spark.read.option("multiline","true").json(path_landing)
          row_count_landing = df_landing.count()
        elif extension_file  == 'txt':
          headerList=spark.read.csv(path_landing).take(1) 
          delims=['|',';',',','\t','\n','\\','/','//']
          delimiter=" "
          for d in delims:
            if headerList[0][0].find(d)!=-1:
                delimiter=d
            else:
              pass                
          df_landing = spark.read.option("header", "true").option("delimiter",delimiter).csv(path_landing)   #as of spark1.6 you can use  csv to read txt
          row_count_landing = df_landing.count()
      """ Validation """
      if row_count_converted == row_count_landing:
        validation_status = "Succeeded"
        message = "RowCout Validation was applied. Source and Converted records match"
      else:
        validation_status = "Failed"
        message = "Source and sink records do not match. Source count: {} , Sink count {}.".format(str(row_count_landing),str(row_count_converted))

      json_output = {"ExecutionStatus": "Successfull","DvLogId": DataValidationParameters['DvLogId'], "Output": {"Count": str(row_count_converted), "Validation": { "Status": validation_status, "Message": message}}}      
    except Exception as ex:    
      raise Exception(f'Error: {ex}')
      msg_error = {'ExecutionStatus': 'Failed','Error Message':'Fail to execute function to validate source type and count records','DvLogId': dvlog_id,'FunctionName': self.function_name ,'DvMethod':self.Dvmethod}
      #post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)
    return (json_output,self.dataframe)
    
  def nullcount(self,*args,**kwargs): #split between valid and invalid data of nulls
    self.function_name = kwargs.get('function_name',"default value")
    self.Dvmethod = kwargs.get('Dvmethod',"default value")
    self.input_dict = kwargs.get('input_dict',"default value")

    def union_dataframes(dfs):
      """Will union two or more dataframes into one single dataframe."""
      return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)
    
    def get_corrupted_data(dataframe, column_names, get_df=True):
      """Will get the Valid and Invalid data just using the index."""
      with_null_values = []
      message_result = []
      total_array = []
      empty = sqlContext.createDataFrame(sc.emptyRDD(), dataframe.schema)
      for item in column_names:
        sin_data = dataframe.filter(col(item).isNull() | isnan(col(item)))
        message_result.append(f"Column '{item}' with '{sin_data.count()}' null values.")
        total_array.append(str(sin_data.count()))

        if sin_data.count() != 0 or sin_data.rdd.isEmpty():
          with_null_values.append(sin_data)

      if get_df:
        unioned_df = union_dataframes(with_null_values)
        unioned_df = unioned_df.dropDuplicates()
        nullCount_result = '\n'.join(message_result)

      else:
        unioned_df = []
        nullCount_result = '\n'.join(message_result)

      result_data = {
                      "unioned_df": unioned_df,
                      "nullCount_result": nullCount_result,
                      "total_array": total_array
                     }

      return result_data


    def condition_output(dataframe, column_names, dv_sink_container_name,
                         sink_path, dv_sink_storage_name, kv_scope_name, kv_workspace_id,
                         kv_workspace_pk, function_name, dv_method, dvlog_id):

      """Will split up a Dataframe into two new dataframes, one for Valid Data and one for Invalid Data, this depends on 'get-df' value if True."""
      try:
        #corrupt_data, nullCount_result, total_array

        result_data = get_corrupted_data(self.dataframe, column_names)

        if result_data['unioned_df'].count() != 0:

          SinkValid = sink_path + '/_NullCount_Valid'
          SinkInvalid = sink_path + '/_NullCount_Invalid'
          
          # joining valid and invalidad data
          dataframes_join = dataframe.union(result_data['unioned_df'])
          DF_join = dataframes_join.toPandas()

          # Invalid data
          DF_No_corrupted = DF_join.drop_duplicates(keep=False)

          # Valid data
          DF_corrupted = result_data['unioned_df'].toPandas()

          print(f"* Corrupted records: {len(DF_corrupted)}")
          print(f"* No corrupted records: {len(DF_No_corrupted)}")

          if len(DF_No_corrupted) > 0:
          # writing valid data
            correctSpk = spark.createDataFrame(DF_No_corrupted)
            self.dataframe=correctSpk #df wil now be input for future validations
            save_to_delta_format(correctSpk,'datavalidation',storage_account_name, output_path+SinkValid, write_mode, input_parameters,DeltaTableName='nullcount') # save to delta

          if len(DF_corrupted) > 0:
          # writing invalid data
            corrupt = DF_No_corrupted.fillna(np.nan)
            corruptSpk = spark.createDataFrame(corrupt)
            save_to_delta_format(corruptSpk,'datavalidation',storage_account_name, output_path+SinkInvalid, write_mode, input_parameters,DeltaTableName='nullcountinvalid')
          print("\n> Currated files saved successfully.")
          
        return result_data['nullCount_result'], result_data['total_array']
      except Exception as ex:
        print(f"*** ERROR in condition_output: {ex}")
        msg_error = {'ExecutionStatus': 'Failed', 'Error Message': 'Fail to execute function split data', 'DvLogId': dvlog_id, 'FunctionName': self.function_name , 'DvMethod': self.Dvmethod}
        #post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)

    try:
      
      message_result, total_array = condition_output(self.dataframe, self.input_dict['columns'], container_name,
                                                   output_path, storage_account_name, kv_scope_name, kv_workspace_id,
                                                   kv_workspace_pk, self.function_name, self.Dvmethod, dvlog_id)
      print(message_result, total_array)
      validation_status = "Succeeded"
      json_output={'ExecutionStatus': 'Successfull', 'DvLogId': dvlog_id, 'Output': {'Count': total_array, 'Validation': { 'Status': validation_status, 'Message': message_result}}}
    except Exception as err:
      print(f"An error has occurred...{err}")
      msg_error = {'ExecutionStatus': 'Failed', 'Error Message': 'Fail to Build the inputs for the DataValidationLog table', 'DvLogId': dvlog_id, 'FunctionName': self.function_name , 'DvMethod': self.Dvmethod}
     # post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)
      json_output = msg_error
    return (json_output,self.dataframe)

      

#### COLUMN LEVEL VALIDATION###
  def columnlevel(self,*args,**kwargs): #split valid and invlid regex
    self.function_name = kwargs.get('function_name',"default value")
    self.Dvmethod = kwargs.get('Dvmethod',"default value")
    self.input_dict = kwargs.get('input_dict',"default value")
    input_dict=self.input_dict['columns'][0]

  
    def get_expressions(input_parameter_dict):
      """Get the regular expresion as well as column's name"""
      try:
        columns = []
        expressions = []
        key_pairs = str(input_parameter_dict).split(',')

        for item in key_pairs:

          key_pair = item.split(':')
          column = key_pair[0].replace("{","").replace('"','').replace("'",'').strip()
          key_value = key_pair[1].replace("}","").replace('"','').replace("'",'').strip()

          columns.append(column)
          expressions.append(key_value)

        return columns, expressions

      except Exception as ex:

        print(f"**** ERROR {ex}")
        #return get_list_regex(input_parameter_dict)

    # Function to count values that do not meet the regex parameter in a specified coulmn from the loaded file
    def count_regex(dataframe, column_names, target_expressions):                                 
      """Count the number of input parameter in file."""

      result = {}
      indexes = []

      try:
        pd_dataframe = dataframe.toPandas()
        for column, reg_exp in zip(column_names, target_expressions):
          print(f"* Column: {column} | Regex: {reg_exp}")

          null_values = []
          no_match = []
          match = []

          for index_b, item in enumerate(pd_dataframe[column]):

            if item is None:
              null_values.append(int(index_b))

              if int(index_b) not in indexes:
                indexes.append(int(index_b))

            #regex function expects string so convert item to string
            elif not re.match(reg_exp, str(item), flags=0):
              no_match.append(int(index_b))

              if int(index_b) not in indexes:
                indexes.append(int(index_b))

            else:
              match.append(int(index_b))

          result[column] = {"null": len(null_values), "match": len(match), "no_match": len(no_match)}

        result['corrupted_indexes'] = indexes
        result['records'] = len(pd_dataframe)


      except Exception as ex:

        print(f"*** ERROR in count_regex: {ex}")
        msg_error = {'ExecutionStatus': 'Failed', 'Error Message': 'Fail to execute function to count values that do not meet the regex parameter in a specified column from the loaded file', 'DvLogId': dvlog_id, 'FunctionName': self.function_name , 'DvMethod': self.Dvmethod}
        #post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)
        result = msg_error
      return (result)


    def split_columnlevel(dataframe, sink_regex_array, sink_container_name, sink_path, adls_storage_account_name):  
      try:
        pd_dataframe = dataframe.toPandas()

        if sink_regex_array['corrupted_indexes']:

          SinkValid = sink_path + '/_ColumnLevel_Valid'
          SinkInvalid = sink_path + '/_ColumnLevel_Invalid'

          corrupted = pd_dataframe.loc[sink_regex_array['corrupted_indexes']]
          no_corrupted = pd_dataframe.drop(sink_regex_array['corrupted_indexes'])
          

          # No corrupted data
          if len(no_corrupted) > 0:
            no_corrupted_Spk = spark.createDataFrame(no_corrupted)
            self.dataframe=no_corrupted_Spk
            save_to_delta_format(no_corrupted_Spk,'datavalidation',storage_account_name, output_path+SinkValid, write_mode, input_parameters,DeltaTableName='columnlevel')

          # Corrupted data
          corrupted = corrupted.fillna('')
          corrupted_Spk = spark.createDataFrame(corrupted)
          save_to_delta_format(corrupted_Spk,'datavalidation',storage_account_name, output_path+SinkInvalid, write_mode, input_parameters,DeltaTableName='columnlevelinvalid')
          

          print("> Currated (Valid & Invalid) files loaded")
          
      except Exception as ex:
        print(f"*** ERROR in condition_output: {ex}")
        msg_error = {'ExecutionStatus': 'Failed', 'Error Message': 'Fail to execute function to split data', 'DvLogId': dvlog_id, 'FunctionName': self.function_name , 'DvMethod': self.Dvmethod}
        #post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)
        return msg_error
    # Build the inputs for the DataValidationLog table
    def Validate_Column_Regex(sink_regex_array):
      """An Output will generate to indicate if the process were well or an error was found."""
      
      try:
        message = ''

        for key, value_e in sink_regex_array.items():

          if isinstance(value_e, dict):
            message += f"The column '{key}' has '{value_e['null']} null', '{value_e['match']} do match', '{value_e['no_match']} no match' values.\n"

        validation_status = "Succeeded"
        output = {'ExecutionStatus': 'Successfull', 'DvLogId': dvlog_id, 'Output': {'Count': sink_regex_array['records'], 'Validation': {'Status': validation_status, 'Message': message}}}

        return output

      except Exception as ex:

        print(f"*** ERROR in Validate_Column_Regex: {ex}")
        msg_error = {'ExecutionStatus': 'Failed', 'Error Message': 'Fail to execute Build the inputs for the DataValidationLog table', 'DvLogId': dvlog_id, 'FunctionName': self.function_name , 'DvMethod': self.Dvmethod}
       # post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)
        return msg_error  
    try:


      column_names, target_expressions = get_expressions(input_dict)  
      sink_regex_array = count_regex(self.dataframe, column_names, target_expressions)
      split_columnlevel(self.dataframe, sink_regex_array, container_name, output_path,storage_account_name)

      json_output = Validate_Column_Regex(sink_regex_array)
    except Exception as ex:
      print(f"*** ERROR: {ex}")
      msg_error = {'ExecutionStatus': 'Failed','Error Message':'Fail at Checking columns with regex','DvLogId': dvlog_id,'FunctionName': self.function_name ,'DvMethod':self.Dvmethod}
     # post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)
      json_output = msg_error
    return (json_output,self.dataframe)


## RECORD LEVEL VALIDATION   
  def recordlevel(self,*args,**kwargs): #split valid and invalid 
    self.function_name = kwargs.get('function_name',"default value")
    self.Dvmethod = kwargs.get('Dvmethod',"default value")
    self.input_dict = kwargs.get('input_dict',"default value")
    target_count=self.input_dict['Count']

  
    def count_columns(kv_scope_name, adls_blob_secret_name, adls_storage_account_name, source_container_name, source_path):
      """Will count the number of columns of a Dataframe."""
      try:
        print("> Reading file")
        size = len(self.dataframe.columns)
        return size
      except Exception as ex:
        print(f"*** ERROR: {ex}")
        
    try:
        sink_column_count = count_columns(kv_scope_name, storage_access_key_secret_name, storage_account_name, container_name, source_path)
        if target_count == sink_column_count:
          validation_status = "Succeeded"
          validation_bool = "True"
          message = "Target Count parameter and sink column count do match."

        else:    
          validation_status = "Failed"
          validation_bool = "False"
          message = "Target Count parameter and sink column count do not match. Target count: {} but were found {}".format(target_count, sink_column_count)
        
        json_output={'ExecutionStatus': 'Successfull', 'DvLogId': dvlog_id, 'Output': {'Count': sink_column_count, 'Validation': {'Status': validation_bool, 'Message': message}}}

       # post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, json_output)
    except Exception as ex:
        print(f"*** ERROR: {ex}")
        msg_error = {'ExecutionStatus': 'Failed', 'Error Message': 'Fail in Record Level', 'DvLogId': dvlog_id, 'FunctionName': self.function_name ,'DvMethod': self.Dvmethod}
      #  post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)
        json_output = msg_error
    return(json_output,self.dataframe)
      

## MIN MAX ROW COUNT VALIDATION
  def minmaxrowcount(self,*args,**kwargs): #no split
    print('MIN MAX ROW COUNT')
    self.function_name = kwargs.get('function_name',"default value")
    self.Dvmethod = kwargs.get('Dvmethod',"default value")
    self.input_dict = kwargs.get('input_dict',"default value")
    
    min_value = self.input_dict['Min Value']
    max_value = self.input_dict['Max Value']
    
    
    def Compare_Count_Files(sink_file_rows_count, min_value, max_value, dvlog_id): #helper function for minmaxrow
      """ Compares the files and validates its row counts are between the given range"""
    # Build the inputs for the DataValidationLog table
    # If Min Value is defined as zero, null, or empty -> Min Value will be taken as zero.
      if min_value <= max_value:
        if isinstance(min_value, str):
          if min_value.lower() == "null" or min_value == "":
            min_value = 0
          else: 
            min_value = int(min_value)

        if isinstance(max_value, str):
          if max_value.lower() == "null" or max_value == "":
            max_value = 0
          else:
            max_value = int(max_value)
        # Validate that the sink record count is inside the thresholds.
        if max_value == 0:
          if sink_file_rows_count >= min_value:
            validation_status = "Succeeded"
            validation_bool = "True"
            message = f"MinMaxRowCount was applied. The values are between {min_value} and {max_value}"
          else:
            validation_status = "Failed"
            validation_bool = "False"
            message = "MinMaxRowCount was applied. The number of records is not higher than {}. The actual number is: {}.".format(str(min_value), str(sink_file_rows_count))
        else:
          if sink_file_rows_count >= min_value and sink_file_rows_count <= max_value:
            validation_status = "Succeeded"
            validation_bool = "True"
            message = "MinMaxRowCount was applied."
          else:
            validation_status = "Failed"
            validation_bool = "False"
            message = "MinMaxRowCount was applied. The number of records is not between {} and {}. The actual number is: {}.".format(str(min_value), str(max_value), str(sink_file_rows_count))
      else:
        validation_status = "Failed"
        validation_bool = "False"
        message = "MinMaxRowCount was applied. The min value is greater than the max value."

      output = {'ExecutionStatus': 'Successfull', 'DvLogId': dvlog_id, 'Output': {'Count': sink_file_rows_count, 'Validation': { 'Status': validation_bool, 'Message': message}}}

      return output
    try:
      df=read_source_data(kv_scope_name, storage_account_name,storage_access_key_secret_name,
                       container_name, source_path,source_file_format)
      json_output = Compare_Count_Files(df.count(), min_value, max_value, DataValidationParameters['DvLogId'])
      #post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, json_output)
    except Exception as err:
      raise Exception(f"{err}")
      msg_error = {'ExecutionStatus': 'Failed','Error Message':'Fail at Comparing rows MinMaxRow counts','DvLogId': dvlog_id,'FunctionName': self.function_name ,'DvMethod':self.Dvmethod}
    #  post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)
      json_output=msg_error
    return (json_output,self.dataframe)

  
#### SCHEMA DRIFT VALIDATION
  def schemadrift(self,*args,**kwargs): #split data between columns present and not present
    self.function_name = kwargs.get('function_name',"default value")
    self.Dvmethod = kwargs.get('Dvmethod',"default value")
    self.input_dict = kwargs.get('input_dict',"default value")
    try:
      allow_evolution_bool = self.input_dict['Allow Evolution']
    except:
      allow_evolution_bool=False
    
    #Comparing schemas
    schema_file_columns = columnschema
    sink_file_columns = self.dataframe.columns
    new_cols=list(set(sink_file_columns).difference(schema_file_columns))
    del_cols=list(set(schema_file_columns).difference(sink_file_columns))
    valid_cols=list(set(sink_file_columns).difference(new_cols+del_cols))
    
    # Build the inputs for the DvLog table
    def Build_Logs(del_cols_list, new_cols_list,fwklog_id,allow_evolution_bool):
      """Builds the logs for the DVLog table."""
      validation_bool = "Succeeded"
      if new_cols_list: 
        count_drifted_values = len(del_cols_list) + len(new_cols_list)
      else:
        count_drifted_values = 0
      if del_cols_list != [] or new_cols_list != []:
        message = "Schema drift identified."
        if del_cols_list != []:
          message += f"Deleted columns: {del_cols_list}.\n"
          if allow_evolution_bool.lower() == "false":
            validation_bool = "Failed"
        if new_cols_list != []:
          message += f"Added columns: {new_cols_list}.\n"
          if allow_evolution_bool.lower() == "false":
            validation_bool = "Failed"
      else:
        message = ""
      output = {'ExecutionStatus': 'Successfull', 'dvLogId': dvlog_id, 'Output': {'Drifted Values Count': str(count_drifted_values), 'Validation': {'Status': validation_bool, 'Message': message}}}
      return output
      
    try:
      #splitting the data
      SinkValid = output_path + '/_Schema_Valid'      
      correctSpk = self.dataframe.select(valid_cols)
      self.dataframe=correctSpk

      
      SinkInvalid = output_path + '/_Schema_Invalid'
      corruptSpk = self.dataframe.select(new_cols+del_cols)
      if not allow_evolution_bool:
        save_to_delta_format(correctSpk,'datavalidation',storage_account_name, output_path+SinkValid, write_mode, input_parameters,DeltaTableName='schema') # save to delta
        if CorruptSpk.count()>0:
          save_to_delta_format(corruptSpk,'datavalidation',storage_account_name, output_path+SinkInvalid, write_mode, input_parameters,DeltaTableName='schemainvalid')

      
      json_output = Build_Logs(del_cols, new_cols, dvlog_id, allow_evolution_bool)
    except Exception as ex:
      print("ERROR: {}".format(ex))
      msg_error = {'ExecutionStatus': 'Failed', 'Error Message': 'Fail at Getting schema and comparing with sink file', 'dvLogId': dvlog_id, 'FunctionName': self.function_name , 'DvMethod': self.Dvmethod}
     # post_data(kv_scope_name, kv_workspace_id, kv_workspace_pk, msg_error)
      json_output = msg_error
    return json_output,self.dataframe
  
  #for complex validations like orphans
  def orphans(self,*args,**kwargs):
    
    sqlQueryValid = kwargs.get('sqlQueryValid',"default value")
    sqlQueryInvalid = kwargs.get('sqlQueryInvalid',"default value")
    #Create temporary views to perform sql query from input parameters
    for n,dataset_x in enumerate(source_datasets_son):
      source_path = source_datasets_son[dataset_x]['sourcePath']
      source_file_format = source_datasets_son[dataset_x]['sourceFileFormat']
      columnschema = set_format_schema(json.loads(source_datasets_son[dataset_x]['columnSchema']))
      storage_access_key_secret_name, storage_account_name, container_name = get_global_values(source_datasets_son[dataset_x]["sourceModule"])


      df = read_source_data(kv_scope_name,storage_account_name, storage_access_key_secret_name,container_name, source_path, source_file_format)
      df.createOrReplaceTempView("dataset{}".format(n+1))
      df.unpersist()
      
    try:
      dfValid = spark.sql(sqlQueryValid)
      save_to_delta_format(dfValid,'datavalidation',storage_account_name, output_path+'/Validated', write_mode, input_parameters,DeltaTableName='orphans')
      self.dataframe=dfValid
      dfInValid = spark.sql(sqlQueryInvalid)
      
      save_to_delta_format(dfInValid,'datavalidation',storage_account_name, output_path+'/Invalid', write_mode, input_parameters,DeltaTableName='oprhansinvalid')
      json_output={'ExecutionStatus': 'Successfull', 'dvLogId': dvlog_id, 'Output': 'Orphans validation performed', 'Validation': {'Status': 'Succeeded', 'Message': f'found {dfValid.count()} records for the Valid query and {dfInValid.count()} for the Invalid '}}
      
    except Exception as ex:
      json_output={'ExecutionStatus': 'Error', 'dvLogId': dvlog_id, 'Output': 'An error occurred in the validation', 'Validation': {'Status': 'Failed', 'Message': ex}}
    for n2,dataset_x in enumerate(source_datasets_son):
      spark.catalog.dropTempView("dataset{}".format(n2+1))
    print('orphans validation finished')
    return json_output, self.dataframe
      

  def validate(self,functions):
    outputfinal=[]

    for function_name in functions:  
      print(f'Performing {function_name} Validation')
      if function_name.lower()=='orphans':
        sqlQueryValid = functions[function_name]['sqlQueryValid']
        sqlQueryInvalid = functions[function_name]['sqlQueryInvalid']
        outputfinal.append(getattr(self, function_name.lower())(input_dict=functions[function_name],function_name=function_name,Dvmethod='Databricks',sqlQueryValid=sqlQueryValid,sqlQueryInvalid=sqlQueryInvalid)[0])
      
      else:
        outputfinal.append(getattr(self, function_name.lower())(input_dict=functions[function_name],function_name=function_name,Dvmethod='Databricks')[0])

        
    #write Final dataframe to delta
    self.dataframe = (getattr(self, function_name.lower())(input_dict=functions[function_name],function_name=function_name,Dvmethod='Databricks')[1])
    finalschema=save_to_delta_format(self.dataframe,'datavalidation',storage_account_name, output_path+'/FinalValidation', write_mode, input_parameters,DeltaTableName=DataValidationParameters['FunctionName'])
    
    return outputfinal,self.dataframe,finalschema
 
    

# COMMAND ----------

notebookoutput=DataValidation().validate(input_parameters['functions'])


# COMMAND ----------

notebookoutput

# COMMAND ----------

# DBTITLE 1,Output
if len(errors) > 0:
  notebookoutput = {'Errors':'. \n'.join(errors)}
  raise Exception(str(errors))
dbutils.notebook.exit(notebookoutput[2])
