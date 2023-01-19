df = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/csv/mockdata.csv")

display(df)

#CRUCIAL STEP : This library must be imported to use 'lit'
from pyspark.sql.functions import *

#Adding a new Column using withColumn() and ffilling it with default value using 'lit()'

df = df.withColumn("Country",lit("Italy"))

display(df)

#Chaining together multiple withColumn() functions to add multiple new columns in a single statement

df = df.withColumn("Region",lit("Lazio"))\
.withColumn("City",lit("Rome"))

display(df)
