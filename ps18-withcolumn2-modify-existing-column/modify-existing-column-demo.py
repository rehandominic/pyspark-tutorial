df = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/csv/mockdata.csv")

display(df)

#CRUCIAL STEP : This library must be imported to use 'lit'
from pyspark.sql.functions import *

#Modifying Population Column by multiplying it's values by 4

df = df.withColumn("Population",df.Population*4)

display(df)

#Modifying Population Column by setting a new default value using 'lit()'

df = df.withColumn("Population",lit(1000))

# Multiple withcolumn() functions can be chained together to modify several existing columns

display(df)
