from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Streaming")
sc = SparkContext.getOrCreate(conf=conf)

ssc = StreamingContext(sc, 1)

rdd = ssc.textFileStream("data/StreamingData/")

rdd = rdd.map(lambda x: (x, 1))
rdd = rdd.reduceByKey(lambda x, y: x + y)

rdd.pprint()



ssc.start()
ssc.awaitTerminationOrTimeout(10)
ssc.stop()