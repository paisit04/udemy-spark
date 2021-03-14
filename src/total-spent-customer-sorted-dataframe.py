from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([ \
                     StructField("cus_id", IntegerType(), True), \
                     StructField("item_id", IntegerType(), True), \
                     StructField("amount_spent", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///opt/bitnami/spark/datasets/customer-orders.csv")
df.printSchema()

# Select only cus_id and amount_spent
customerDF = df.select("cus_id", "amount_spent")

# Aggregate to find minimum temperature for every station
totalByCustomer = customerDF.groupBy("cus_id").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent"))
totalByCustomerSorted = totalByCustomer.sort("total_spent")

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()
