# Creating a dictionary list on which to create a dataframe

data = [{"Category": 'A', "ID": 1, "Value": 121.44, "Truth": True},
        {"Category": 'B', "ID": 2, "Value": 300.01, "Truth": False},
        {"Category": 'C', "ID": 3, "Value": 10.99, "Truth": None},
        {"Category": 'E', "ID": 4, "Value": 33.87, "Truth": True}
        ]

# Using the createDataFrame() function to convert data (Dictionary list) into a Dataframe

df = spark.createDataFrame(data)

# Confirming that df is in fact a dataframe

type(df)

# Displaying the Dataframe to verify

display(df)

# ============================================================================================

# Method 2 is to create a RDD from a Dictionary List and then Create a Dataframe based on the RDD

data = [{"Category": 'A', "ID": 1, "Value": 121.44, "Truth": True},
        {"Category": 'B', "ID": 2, "Value": 300.01, "Truth": False},
        {"Category": 'C', "ID": 3, "Value": 10.99, "Truth": None},
        {"Category": 'E', "ID": 4, "Value": 33.87, "Truth": True}
        ]

# Create an RDD based on the data

rdd = sc.parallelize(data)

# Verify that type is RDD

type(rdd)

# Convert the RDD to Dataframe using toDF()

df2 = rdd.toDF()

# Displaying the dataframe to verify

display(df2)
