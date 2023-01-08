-- load json data into a dataframe : method 1

df1 = spark.read.format("json").load("FilePath (DBFS)")

-- load json data into a dataframe : method 2
  
df2 = spark.read.json(path="FilePath (DBFS)")

-- display dataframes

display(df1)
display(df2)
