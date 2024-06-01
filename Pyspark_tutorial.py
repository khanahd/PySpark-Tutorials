'''
[Tutorial 1]
Installing the PySpark and basic operations on PySpark
[https://github.com/krishnaik06/Pyspark-With-Python]
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
# if thresh non null values are in a row, it will be kept

# subset
df_pyspark.na.drop(how='any',subset=['Col_name']).show()
# only those rows will be removed where there is null values in the given col_name for subset

# Filling the Missing values [na.fill() & fillna()]
# value = the value to replace with the null values
# subset = the column where the null values are to be replaced, if None then all columns
df_new = df_pyspark.na.fill(value,subset=['col_name'])
df_new = df_pyspark.na.fill(val_replace,subset=['col_name1','col_name2'])

# Imputer function
from pyspark.ml.feature import Inputer
inputer = Imputer(inputCols=['Col1','col2'], # name of columns to apply imputer
                 outputCols=["{}_imputed".format(c) for c in ['col1','col2']]
                 ).setStrategy("mean")
# .setStrategy("median")/.setStrategy("mode")

# fit and transform
imputer.fit(df_pyspark).transform(df_pyspark).show()
'''
The imputer function add new column to the df 
with the null values replaced with the choosen
strategy.
'''

# ############################################
'''
Tutorial Part 4
Filter Operation
& [AND], | [OR], ==
~ [NOT]

1. The dataset used contains the cols as Name, Age, Experience and Salary
'''

# Filter Operations
# 1) FInding salary less than or equal to 20,000
# Approach 1
df_new = df_pyspark.filter('Salary<=20000")
df_new.select('Name','Age').show()

# Approach 2
df_pyspark.filter(df_pyspark['Salary']<=20000).show()

# Multiple Conditions
# df.filter((df['col1']=Cond1) & (df['col2']=Cond2))
df_pyspark.filter((df_pyspark['Salary']<=20000) & (df_pyspark['Age']>=30)).show()
df_pyspark.filter((df_pyspark['Salary']<=20000) | (df_pyspark['Age']>=30)).show()

# Not operator
df_pyspark.filter((~df_pyspark['Salary']<=20000)).show() 

'''
Tutorial Part 5
Group by and Aggregate function
Dataset used contain columns [Name,Department,Salary]
'''                           

# groupby always work with aggregate functions
# df.groupby('ColName').functions()
df_pyspark.groupBy('Name').sum().show()
df_pyspark.groupBy('Name').mean().show()

# agg
df_pyspark.agg({'Salary':'sum'}).show()

