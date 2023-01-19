df = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/csv/mockdata.csv")

display(df)

# View schema of the dataframe using printSchema()

df.printSchema()

#root
# |-- id: string (nullable = true)
# |-- first_name: string (nullable = true)
# |-- last_name: string (nullable = true)
# |-- email: string (nullable = true)
# |-- gender: string (nullable = true)
# |-- ip_address: string (nullable = true)

# Using withColumn to change 'id' column datatype from string to integer

df = df.withColumn("id",df.id.cast("Integer"))

# Verifying change using printSchema()

df.printSchema()

#root
# |-- id: integer (nullable = true)
# |-- first_name: string (nullable = true)
# |-- last_name: string (nullable = true)
# |-- email: string (nullable = true)
# |-- gender: string (nullable = true)
# |-- ip_address: string (nullable = true)
