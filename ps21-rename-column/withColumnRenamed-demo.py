df = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/csv/mockdata.csv")
display(df)

# Using the .withColumnRenamed() function

df = df.withColumnRenamed("ip_address","network_address")

display(df)


# Using .withColumnRenamed() to rename multiple Columns

df = df.withColumnRenamed("email","email_id").withColumnRenamed("id","school_id")

display(df)
