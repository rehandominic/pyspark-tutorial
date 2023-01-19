df = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/csv/mockdata.csv")

display(df)

# Deriving a new column as an exact replica of a present column

df2 = df.withColumn("id2",df.id)

display(df2)

# You can also perform python arithmetic operations 

df2 = df.withColumn("id2",df.id * 3)

display(df2)

# Deriving a new column as the product of two existing columns

df3 = df2.withColumn("idproduct", df2.id * df2.id2)

display(df3)

# Deriving a new column as the concatenation of two string columns using 'concat()'

df4 = df3.withColumn("fullname", concat(df3.first_name,df3.last_name))

display(df4)

# Using 'concat_ws()' to create a concatenated column with a separator

df5 = df4.withColumn("fullname_space", concat_ws(" ",df4.first_name,df4.last_name))

display(df5)
