from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Stream").getOrCreate()
df = spark.readStream.text("data/StreamingData/")

df.writeStream.outputMode("complete").format("console").start()


