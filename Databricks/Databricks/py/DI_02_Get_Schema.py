# Databricks notebook source
# MAGIC %md
# MAGIC ### Metadata-Driven Ingestion Framework 
# MAGIC #### Data Ingestion: Get Schemas
# MAGIC Connect to sink instance and get the schema of the columns of parquet file. Validate the result and send it to Azure Data Factory.

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMPORTANT!
# MAGIC #### Configuration for testing and debug
# MAGIC Change the value of "testing=False" for production environment.
# MAGIC Change the value of debug variables to see or hide prints with information.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Declaration of variables and execution of functions

# COMMAND ----------

#Class that will evaluate cast for specific column of each datatype 

class DTypecheck: # it works for pyspark dataframe only

    def __init__(self, dataframe):
        #when class is initialized store dataframe to shorter names
        import dateutil
        self.df = dataframe
        self.acceptance = 0.7

    @classmethod
    def from_pdf(cls,pdf): #this is if we have a pandas dataframe it should run like DTypecheck.from_pdf(nameofpandasdf).get_schema()
        df=spark.createDataFrame(pdf) 
        return cls(df)
    
    #function to format schema
    @staticmethod
    def set_format_schema(a_jsonschema):
        schema = []
        try:
            for item in a_jsonschema:
                if "varchar" in item['type'].lower():
                    new_schema = f"{item['name']} STRING"
                elif item['type'].lower() == 'int':
                    new_schema = f"{item['name']} INTEGER"
                elif item['type'].lower() == 'bigint':
                    new_schema = f"{item['name']} LONG"
                elif item['type'].lower() == 'decimal(18,4)':
                    new_schema = f"{item['name']} DECIMAL"
                elif item['type'].lower() == 'datetime':
                    new_schema = f"{item['name']} TIMESTAMP"
                elif item['type'].lower() == 'bit':
                    new_schema = f"{item['name']} BOOLEAN"
                else:
                    new_schema = f"{item['name']} {item['type']}"
                schema.append(new_schema)
            schema = ', '.join(schema)

            return schema
        except Exception as exp:
            print(f"Error set_format_schema: {exp}")

    #functions to cast schemas
    def bit(self,column2evaluate): #function to evaluate if values are boolean, 1st function to evaluate

        self.col=column2evaluate
        df2=self.df
        try:
            #if 1s or 0s replace them with none bc it will think that it is a True or False when casting
            df2 = df2.withColumn(self.col, when(df2[self.col] == '1', None).otherwise(df2[self.col]))
            df2 = df2.withColumn(self.col, when(df2[self.col] == '0', None).otherwise(df2[self.col]))

            df3 = df2.withColumn(self.col, df2[self.col].cast(BooleanType())) #perform casting
            if df3.filter(df3[self.col].isNull()).count()>=self.acceptance*(df3.count()) or df2.where(f'{self.col} rlike "[0-9]"').count()>0: #if more than 70% are null values then casting failed and col is not bit
                return 0
            else:
                return 1
        except:
            return 0

    def integer(self,column2evaluate): # use this function to validate for int if and only if bit failed, 2nd funct to evaluate 

        self.col=column2evaluate
        #check for null values to avoid counting more nulls
        df2=self.df
        try:
            df3 = df2.withColumn(self.col, round(df2[self.col]).cast(IntegerType())) #round up to make sure first they are numbers
            if df3.filter(df3[self.col].isNull()).count()>=self.acceptance*(df3.count()): #if more than 70% are nulls then its sure not an integer
                return 0
            else:
                return 1
        except:
            return 0

    def decimal(self,column2evaluate): #this funct should only be called if integer returned 1, 3rd function to evaluate
        self.col = column2evaluate
        df2 = self.df
        try:
            df3 = df2.withColumn(self.col, (df2[self.col]).cast("decimal(18,4)")) #round up to make sure first they are numbers
            if df3.filter(df3[self.col].isNull()).count()>=self.acceptance*(df3.count()): #if more than 70% are nulls then its sure not an integer
                return 0
            else:
                if (self.df.filter(self.df[self.col].contains('.'))).count()==0:
                    return 0
                else:
                    return 1
        except:
            return 0 

    def biginteger(self,column2evaluate): #this function should be called ig integer is validated and decimal is not 4th funct to evaluate

        self.col=column2evaluate
        df3 = self.df.withColumn(self.col, round(self.df[self.col]).cast(IntegerType())) #round up to make sure first they are numbers
        if df3.where(df3[self.col]==2147483647).count()>0: #check if there is a value higher than the largest int it will set it to max int
            return 1
        else:
            return 0

    def date(self,column2evaluate): #if integer returned 0, bit must've returned 0 too, so we try date 5th function to evaluate

        import dateutil
        self.col=column2evaluate
        cont=0
        for el in self.df.select(self.col).collect(): #iterate over each value of the columns
            try: 
                dateutil.parser.parse(el[0]) #try to parse it to date
            except:
                cont+=1 #if you cant parse it to date store the issue 
        if cont>=self.acceptance*self.df.select(self.col).count():#if more than 70% of the data cant be converted to datetime this col shouldn't be datetime
            return 0
        else:
            return 1

    def datetime(self,column2evaluate): #this func should only be called after validating that date is 1 

        import dateutil
        self.col=column2evaluate
        contv=0
        for el in self.df.select(self.col).collect():
            try:
                val=(dateutil.parser.parse(el[0])) #we know that data can be parsed since date returned 1
                try:
                    if val.hour>0 or val.minute>0 or (self.df.withColumn(self.col, col(self.col).cast('timestamp'))): #if at least 1 value has hour or minute set to more than 1 its datetime
                        contv+=1
                except:
                    pass
            except:#if it cant be parsed it is an empty string so we just ignore it
                pass
        if contv>0: #if we registered that at least one element that is datetime has hours then we use datetime not date
            return 1
        else:
            return 0

    def binary(self,column2evaluate):
        self.col=column2evaluate
        df2=self.df    
        val=0
        try:
            df3 = df2.withColumn(self.col, df2[self.col].cast(BinaryType())) #perform casting
            for c in df3.select(self.col).collect():
                if c[0]==None:
                    pass
                else:
                    c[0].decode()
        except:
            val+=1
    
        if val>0:
            return 1
        else:
            return 0

    
    def maxvarchar(self,column2evaluate): #we should use this function only after validating it it is not anything else 
        #this function will be evaluated second to last
        self.col=column2evaluate
        if self.df.where(length(col(self.col)) >255).count()>0: #if we have at least one value higher than 255 use var char max
            return 1
        else:
            return 0

    def varchar(self,column2evaluate):
        #this function should be evaluated last
        self.col=column2evaluate
        if self.maxvarchar(self.col)==0:
            return 1

    def get_schema(self):

        schema = []
        cols = self.df.columns

        for n,c in enumerate(cols, 1): #Main logic to validate schemas for each column

            data = {}
            data['id'] = str(n)
            data['name'] = c
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

            else: #it should definitely be a string
                if self.binary(c) == 1:
                    data['type']='Binary'          
                
                elif self.maxvarchar(c)==1:
                    data['type']='Varchar (8000)'
                
                       
                else: #if its not any of those it should be this one
                    data['type']='Varchar (255)'
                        
            schema.append(data)

        return schema


# COMMAND ----------

#jsonschema
#new_schema=DTypecheck(df).set_format_schema(jsonschema)
