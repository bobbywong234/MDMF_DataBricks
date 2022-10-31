# Databricks notebook source
# MAGIC %md
# MAGIC #### Data Standardization: Get Column Schema
# MAGIC Connects to the ingested file and infers the column schema data types that json schema value is stored on FwkObjectMetadata table.

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

##derived vars
kv_scope_name = nb_run_parameters['kvScopeName']
service_principal_id = nb_run_parameters['servicePrincipalId']
service_principal_secret_name = nb_run_parameters['servicePrincipalSecretName']
tenant_id = nb_run_parameters['tenantId']
source_storage_name = return_storage_name(source_instance_url)

# COMMAND ----------

# DBTITLE 1,Get Column Schema Function
def remove_null(df, column2evaluate):
    df = df.filter(f'{column2evaluate} != "NULL"')
    df = df.filter(f'{column2evaluate} != ""')
    df = df.na.drop(subset=[column2evaluate])
    return df

#Class that will evaluate cast for specific column of each datatype 

class DataTypecheck: # it works for pyspark dataframe only

    def __init__(self, dataframe):
        #when class is initialized store dataframe to shorter names
        self.df = dataframe
        self.acceptance = 0.1

    #functions to cast schemas
    def bit(self,column2evaluate): #function to evaluate if values are boolean, 1st function to evaluate
        
        data_type = str(df.schema[column2evaluate].dataType)
        if 'BooleanType' in data_type:
            return 1
        elif 'StringType' in data_type:   
            df2 = self.df.select(column2evaluate)
            df2 = remove_null(df2, column2evaluate)
            df2 = df2.groupBy(column2evaluate).agg(count("*"))
            row_count = df2.select(column2evaluate).count()

            if row_count > 3:
    #             print("Not Bit")
                return 0
            elif row_count < 2:
    #             print("Not Bit")
                return 0
            else:
                flag = False
                column_data_list = df2.select(column2evaluate).rdd.flatMap(lambda x: x).collect()
                for each_data in column_data_list:
                    if each_data in ['1', '0']:
                        flag = True
                    else:
                        return 0

                if flag == True:
                    return 1
        else:
            return 0

    def integer(self,column2evaluate): # use this function to validate for int if and only if bit failed, 2nd funct to evaluate 
        
        data_type = str(df.schema[column2evaluate].dataType)
        if 'IntegerType' in data_type or 'LongType' in data_type:
            return 1
        elif 'StringType' in data_type:   
            self.col=column2evaluate
            #check for null values to avoid counting more nulls
            try:
                df2 = remove_null(self.df, column2evaluate)
                df3 = df2.withColumn(self.col, round(df2[self.col]).cast(IntegerType())) #round up to make sure first they are numbers
                if df3.filter(df3[self.col].isNull()).count()>self.acceptance*(df2.count()): #if more than 70% are nulls then its sure not an integer
                    return 0
                else:
                    return 1
            except:
                return 0
        else:
            return 0

    def decimal(self,column2evaluate): #this funct should only be called if integer returned 1, 3rd function to evaluate
        
        data_type = str(df.schema[column2evaluate].dataType)
        if 'DecimalType' in data_type:
            return 1
        elif 'StringType' in data_type:
            self.col = column2evaluate
            df2 = self.df
            try:
                df2 = remove_null(self.df, column2evaluate)
                df3 = df2.withColumn(self.col, round(df2[self.col]).cast(IntegerType())) #round up to make sure first they are numbers
                if df3.filter(df3[self.col].isNull()).count()>self.acceptance*(df2.count()): #if more than 70% are nulls then its sure not an integer
                    return 0
                else:
                    if (self.df.filter(self.df[self.col].contains('.'))).count()==0:
                        return 0
                    else:
                        return 1
            except:
                return 0 
        else:
            return 0

    def biginteger(self,column2evaluate): #this function should be called if integer is validated and decimal is not 4th funct to evaluate
        data_type = str(df.schema[column2evaluate].dataType)
        if 'IntegerType' in data_type or 'LongType' in data_type:
            if df.where(df[column2evaluate]>2147483647).count()>0: #check if there is a value higher than the largest int it will set it to max int
                return 1
            else:
                return 0
        elif 'StringType' in data_type: 
            self.col=column2evaluate
            df2 = remove_null(self.df, column2evaluate)
            df3 = self.df.withColumn(self.col, round(df2[self.col]).cast(IntegerType())) #round up to make sure first they are numbers
            if df3.where(df3[self.col]>2147483647).count()>0: #check if there is a value higher than the largest int it will set it to max int
                return 1
            else:
                return 0
            
        else:
            return 0
        
    def date(self,column2evaluate): #if integer returned 0, bit must've returned 0 too, so we try date 5th function to evaluate
        data_type = str(df.schema[column2evaluate].dataType)
        if 'DateType' in data_type or 'TimestampType' in data_type:
            return 1
        elif 'StringType' in data_type:   
            df2 = remove_null(self.df, column2evaluate)
            self.col=column2evaluate
            cont=0
            for el in df2.select(self.col).collect(): #iterate over each value of the columns
                try: 
                    dateutil.parser.parse(el[0]) #try to parse it to date
                except:
                    cont+=1 #if you cant parse it to date store the issue 
            if cont>=self.acceptance*df2.select(self.col).count():#if more than 70% of the data cant be converted to datetime this col shouldn't be datetime
                return 0
            else:
                return 1
            
        else:
            return 0

    def datetime(self,column2evaluate): #this func should only be called after validating that date is 1 
        data_type = str(df.schema[column2evaluate].dataType)
        if 'TimestampType' in data_type:
            return 1
        elif 'StringType' in data_type:   
            df2 = remove_null(self.df, column2evaluate)
            self.col=column2evaluate
            contv=0
    #         print(self.df.select(self.col).collect()[0][0])
    #         print((dateutil.parser.parse(self.df.select(self.col).collect()[0][0])))
            for el in df2.select(self.col).collect():
                try:
                    val=(dateutil.parser.parse(el[0])) #we know that data can be parsed since date returned 1

                    try:
                        if val.hour>=0 or val.minute>=0 or (df2.withColumn(self.col, col(self.col).cast('timestamp'))): #if at least 1 value has hour or minute set to more than 1 its datetime
                            contv+=1
                    except:
                        pass
                except:#if it cant be parsed it is an empty string so we just ignore it
                    pass
            if contv>0: #if we registered that at least one element that is datetime has hours then we use datetime not date
                return 1
            else:
                return 0
            
        else:
            return 0

#     def binary(self,column2evaluate):
#         df2 = remove_null(self.df, column2evaluate)
#         self.col=column2evaluate
# #         df2=self.df    
#         val=0
#         try:
#             df3 = df2.withColumn(self.col, df2[self.col].cast(BinaryType())) #perform casting
#             for c in df3.select(self.col).collect():
#                 if c[0]==None:
#                     pass
#                 else:
#                     c[0].decode()
#         except:
#             val+=1
    
#         if val>0:
#             return 1
#         else:
#             return 0

    
    def maxvarchar(self,column2evaluate): #we should use this function only after validating it it is not anything else 
        #this function will be evaluated second to last
        df2 = remove_null(self.df, column2evaluate)
        self.col=column2evaluate
        if df2.where(length(col(self.col))>255).count()>0: #if we have at least one value higher than 255 use var char max
            return 1
        else:
            return 0

    def varchar(self,column2evaluate):
        #this function should be evaluated last
        self.col=column2evaluate
        if self.maxvarchar(self.col)==0:
            return 1

    def remove_null(self, df, column2evaluate):
        df = df.filter(f'{column2evaluate} != "NULL"')
        df = df.filter(f'{column2evaluate} != ""')
        df = df.na.drop(column2evaluate=[column])
        return df
    
    def get_schema(self):

        schema = []
        cols = self.df.columns
        
        for n,c in enumerate(cols, 1): #Main logic to validate schemas for each column
            
            data_type = str(df.schema[c].dataType)
#             print(c + ": " + data_type)
#             print(type(data_type))

                  
            data = {}
            data['id'] = str(n)
            data['name'] = c
            
#             if (remove_null(self.df, c)).count() == 0:
#                 data['type']='Varchar (8000)'
            #check if column is boolean
            if self.bit(c) == 1:
                data['type']='Bit'
            elif self.decimal(c) == 1:
                    data['type'] = 'Decimal(18,4)'
            elif self.integer(c) == 1: #now we know this column is a number so we can check for .
                if self.biginteger(c) == 1: #if it is not a decimal it still may be a big integer
                    data['type']='Bigint'
                else:
                    data['type']='Int'
            elif self.date(c) == 1: #if it is not an integer check it can be parsed to date 
                if self.datetime(c) == 1:
                    data['type'] = 'Datetime'
                else:
                    data['type'] = 'Date'
            else: 
                #it should definitely be a string
                #if self.binary(c) == 1:
                #    data['type']='Binary'          
                if self.maxvarchar(c)==1:
                    data['type']='Varchar(8000)'
                else: #if its not any of those it should be this one
                    data['type']='Varchar(255)'
            
            data['isSensitive'] = 'N'
            
            schema.append(data)

        return schema

# COMMAND ----------

# DBTITLE 1,Infer Column Schema From Ingested File
try:
    #read from ingested file (with out column schema)
    df = read_from_adls(kv_scope_name, service_principal_id, service_principal_secret_name, tenant_id, source_storage_name, source_container_name, source_file_path, source_file_extension, source_object_schema)
    #display(df)
    
    if df.count() > 0:
        #getting columnSchema from the object, limiting to 5000 records
        columnSchema = DataTypecheck(df.limit(5000)).get_schema()
    else:
        columnSchema = source_column_schema
except Exception as ex:
    raise Exception('ERROR: {}'.format(ex))

# COMMAND ----------

# DBTITLE 1,Output
dbutils.notebook.exit(columnSchema)

# COMMAND ----------

#pyspark_column_schema = format_column_schema(columnSchema)
#print(pyspark_column_schema)
#pyspark_column_schema = "CustomerID INTEGER, NameStyle BOOLEAN, Title STRING, FirstName STRING, MiddleName STRING, LastName STRING, Suffix STRING, CompanyName STRING, SalesPerson STRING, EmailAddress STRING, Phone STRING, PasswordHash STRING, PasswordSalt STRING, rowguid STRING, ModifiedDate TIMESTAMP"

#df2 = read_from_adls(kv_scope_name, source_storage_name, source_key_secret_name, source_container_name, source_file_path, source_file_extension, source_object_schema, pyspark_column_schema)
#display(df2)
