
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
import pandas as pd


#local[3]
spark = SparkSession.builder.appName('CheckPyspark').master("local").getOrCreate()

schema = StructType() \
        .add("Name", StringType(), True)\
        .add("Age", IntegerType(), True)\
        .add("Email", StringType(), True)

# df3 = spark.read.options(delimiter=',') \
#   .csv("/home/arnavdadarya/HelloWorld.csv")
df3 = spark.read.format("csv")\
    .options(header='True',  delimiter=';')\
    .schema(schema)\
    .load("/home/arnavdadarya/HelloWorld.csv")
#Implement these
# df3.select()
# df3.count()
# df3.agg()
df3.printSchema()
df3.write.option("header", True)
#Save
# df3.write.save("")
print(df3.count())
# print(df3.show())
print("Email Grouping: ")
df3.groupby("Email").count().show()
print("Age Grouping")
df3.groupby("Age").count().show()
print("Name Grouping")
df3.groupby("Name").count().show()
print("Users over 50: ")
df3.filter(df3["Age"]>50).show()
print("All Data: ")
print(df3.show())
print(spark)