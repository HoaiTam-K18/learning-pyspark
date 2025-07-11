from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Extract
df = spark.read.text("data/txtFiles/WordData_ETL.txt")
df.show()

# Transformation
df = df.withColumn("Splited", f.split("value", " "))
df.show()
df = df.withColumn("Words", explode("Splited")).select("Words")
df.show()

dfGroupby = df.groupBy("Words").count()
dfGroupby.show()