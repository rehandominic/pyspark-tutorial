df = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/csv/mockdata.csv")
display(df)
df.printSchema()

# Using drop() function to drop column from a dataframe

df = df.drop("ip_address")

display(df)
df.printSchema()

# Using the drop() function to drop multiple columns from a dataframe

df = df.drop("email","gender")

display(df)
df.printSchema()
