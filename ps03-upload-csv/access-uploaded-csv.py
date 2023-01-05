# To create a dataframe with the csv file that you had uploaded directly into the databricks file store

df = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/MOCK_DATA.csv")

# To display the dataframe

display(df)
