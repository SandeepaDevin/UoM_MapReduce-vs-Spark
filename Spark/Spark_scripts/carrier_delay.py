import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Spark CSV Query") \
    .getOrCreate()

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(sys.argv[1])  

# Create a temporary view to run SQL queries
df.createOrReplaceTempView("carrier_data")

result = spark.sql("SELECT Year, avg((CarrierDelay /ArrDelay)*100) from carrier_data GROUP BY Year")  
result.show()
result.write.format("csv").mode("overwrite").save(sys.argv[2])
spark.stop()

