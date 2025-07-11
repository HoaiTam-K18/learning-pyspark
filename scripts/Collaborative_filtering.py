from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col,explode
spark = SparkSession.builder.appName("Collaborative filtering").getOrCreate()

moviesDF = spark.read.options(header="True", inferSchema="True").csv("data/csvFiles/movies.csv")
ratingsDF = spark.read.options(header="True", inferSchema="True").csv("data/csvFiles/ratings.csv")

moviesDF.show()

ratingsDF.show()

ratings = ratingsDF.join(moviesDF, 'movieId', 'left')

(train, test) = ratings.randomSplit([0.8,0.2])

ratings.count()

print(train.count())
train.show()

print(test.count())
test.show()

als = ALS(userCol = "userId", itemCol="movieId", ratingCol="rating", nonnegative=True,implicitPrefs=False, coldStartStrategy="drop")

param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 50, 100, 150]) \
            .addGrid(als.regParam, [.01, .05, .1, .15]) \
            .build()

evaluator = RegressionEvaluator(
           metricName="rmse", 
           labelCol="rating", 
           predictionCol="prediction")

cv = CrossValidator(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=5)

model = cv.fit(train)
best_model = model.bestModel
test_predictions = best_model.transform(test)
RMSE = evaluator.evaluate(test_predictions)
print(RMSE)


recommendations = best_model.recommendForAlUsers(5)
df = recommendations
df.show()
df2 = df.withColumn("movieid_rating", explode("recommendations"))
df2.show()
df2.select("userId", col("movieid_rating.movieId"), col("movieid_rating.rating")).show()

