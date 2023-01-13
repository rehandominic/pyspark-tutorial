# Reading data from a CSV into a dataframe

df = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/csv/mockdata.csv")

# NOTE : Change the filepath to match your requirements

# View the new dataframe

display(df)

# Write the dataframe to csv file called "output.csv" inside the Output Folder

df.write.csv("Output/writecsv.csv",header=True)
