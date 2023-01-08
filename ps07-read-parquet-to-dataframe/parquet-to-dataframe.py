-- Read the parquet file into a dataframe (Method 1)

df1=spark.read.format("parquet").load("Filepath(DBFS)")

-- Display the dataframe

display(df1)

-- Count number of rows

print(df1.count())

##########################################################################

-- Read the parquet file into a dataframe (Method 2)

df2=spark.read.parquet("Filepath(DBFS)")

-- Display the dataframe

display(df2)

-- Count number of rows

print(df2.count())

###########################################################################

Read multiple parquet files in a folder into a single dataframe

df3=spark.read.parquet("dbfs:/FileStore/parquet/*.parquet")

display(df3)

print(df3.count())

