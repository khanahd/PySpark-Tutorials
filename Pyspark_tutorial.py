'''
[Tutorial 1]
Installing the PySpark and basic operations on PySpark
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

df_pyspark = spark.read.option('header','true').csv('filename', inferSchem=True)
df_pyspark.show()

# checking the schema, gives the information on the columns
df_pyspark.printSchema()
