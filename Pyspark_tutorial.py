'''
[Tutorial 1]
Installing the PySpark and basic operations on PySpark

? what is a DataFrame:
A dataframe is a data structure constructed with rows and columns, similar to a database or Excel spreadsheet
'''
!pip install pyspark
# Install java on your system too for pyspark to work

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.buidler.appName('Practice').getOrCreate()
spark     # show the session

# reading a file from spark
df_pyspark = spark.read.csv('filename')
      # or to include headers from the file itself use below;
      # df_pyspark = spark.read.option('header','true').csv('fielname')
df_pyspark.show()
# type(df_pyspark)    # output to be 'pyspark.sql.dataframe.DataFrame
df_pyspark.head()

# information regarding the columns
      # will give information on the column data types
df_pyspark.printSchema()  

# =======================================================================
'''
[Tutorial 2]  - [Part 1]
Covers data preprocessing
use a dataset saved in the same location as the lab
'''
# Step1: start a new spark session with name 'spark'

# Step2: reading the dataset, used a csv file for example
df_pyspark = spark.read.option('header','true').csv('filename',header=True, inferSchem=True)
df_pyspark.show()
df_pyspark.dtypes()
df_pyspark.describe().show()

# checking the schema, gives the information on the columns
df_pyspark.printSchema()
df_pyspark.columns            # getting the column names

# Getting a particular column from the df
df.select('colname')            # output is basic info on the column
df.select('colname').show()
df.select(['colname1','colname2'])      # selecting multiple columns from the dataframe
df['colname'] # will only understand that there is a column with the given name and would not fetch any information from the column

# Adding and Droping columns
'''
These are not inplace operations, these changes need to be assigned to a variable
'''
# 1 Adding
df_new = df_pyspark.withColumn('Col_New',df_pyspark['col_old']+2)

# 2 Dropping
df_new = df_pyspark.drop('col_name').show()

# 3 Renaming a column
df_new = df_pyspark.withColumnRenamed('col_old_name','col_new_name')
df_new.show()

'''
Tutorial Part 3
'''

# this feature below drops the complete where it find the null values are present
df_pyspark.na.drop()

# how = any/all
df.na.drop(how='all')      # the row gets dropped only when all column values are null
df.na.drop(how='any')      # the row gets dropped only when any column values are null

# threshold
df.na.drop(how='any',thresh=2)
# row gets dropped when the row has non null values less than thresh



















