df2 = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/csv/mockdata.csv")

df2.printSchema()

#root
# |-- id: integer (nullable = true)
# |-- first_name: string (nullable = true)
# |-- last_name: string (nullable = true)
# |-- email: string (nullable = true)
# |-- gender: string (nullable = true)
# |-- ip_address: string (nullable = true)

# Return Dataframe data where Gender is Male

display(df2.where(df.gender == "Male"))

# Return Dataframe data where ID is greater than 20

display(df.where(df.id >20))

# !!NOTE!! Use '&' for 'and' , '|' for 'or' , '~' for 'not' 

# Return Dataframe data where ID is greater than 20 AND Gender is Female

display(df.where((df.id >20) & (df.gender == "Female")))

# Return Dataframe data where ID is greater than 20 OR Gender is Female

display(df.where((df.id >20) | (df.gender == "Female")))

# Return Dataframe data where ID is NOT greater than 20

display(df.where(~(df.id >20)))

# Return Dataframe data where ID is greater than 20 AND Gender is NOT Female

display(df.where((df.id >20) & ~(df.gender == "Female")))
