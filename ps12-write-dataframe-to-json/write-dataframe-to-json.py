# Reading data from a CSV into a dataframe

df = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/csv/mockdata.csv")

# NOTE : Change the filepath to match your requirements

# View the new dataframe

display(df)

# Write the dataframe to json file called "outputjson.json" inside the Output Folder

df.write.json("Output/outputjson.json")

# NOTE : Output will be partitioned because of the way in which Pyspark and Databricks works

# To confirm that output file is accurate, you can write

display(spark.read.json("dbfs:/Output/outputjson.json"))
