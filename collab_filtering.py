from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession\
    .builder\
    .appName("pdmn-tp-final - collab filtering")\
    .getOrCreate()

# Cluster
# spark.sparkContext.setCheckpointDir(
#     'hdfs://compute1:9000/user/danilo-p/tp_final_collab_filtering_checkpoints')

# Local
spark.sparkContext.setCheckpointDir(
    './output/tp_final_collab_filtering_checkpoints')

# Read data (Cluster)
# profiles_df = spark.read.option("header", True).option("delimiter", "\t").csv(
#     "hdfs://compute1:9000/datasets/last_fm/userid-profile.tsv")
# tracks_df = spark.read.option("delimiter", "\t")\
#     .csv("hdfs://compute1:9000/datasets/last_fm/userid-timestamp-artid-artname-traid-traname.tsv")\
#     .withColumnRenamed("_c0", "user_id")\
#     .withColumnRenamed("_c1", "timestamp")\
#     .withColumnRenamed("_c2", "artist_id")\
#     .withColumnRenamed("_c3", "artist_name")\
#     .withColumnRenamed("_c4", "track_id")\
#     .withColumnRenamed("_c5", "track_name")

# Read data (Local)
profiles_df = spark.read.option("header", True).option(
    "delimiter", "\t").csv("./last_fm/userid-profile.tsv")
tracks_df = spark.read.option("header", True)\
    .csv("./last_fm/userid-timestamp-artid-artname-traid-traname-sample.csv")\
    .withColumnRenamed("_c0", "user_id")\
    .withColumnRenamed("_c1", "timestamp")\
    .withColumnRenamed("_c2", "artist_id")\
    .withColumnRenamed("_c3", "artist_name")\
    .withColumnRenamed("_c4", "track_id")\
    .withColumnRenamed("_c5", "track_name")


user_and_track_plays_df = tracks_df\
    .filter("user_id IS NOT NULL and track_id IS NOT NULL")\
    .select("user_id", "track_id")\
    .groupBy("user_id", "track_id")\
    .count()\
    .withColumnRenamed("count", "plays")

profiles_with_gen_id_df = profiles_df\
    .filter("id IS NOT NULL")\
    .rdd.zipWithIndex().toDF()\
    .select(f.col("_1.*"), f.col("_2").alias('user_gen_id'))

tracks_with_gen_id_df = tracks_df\
    .filter("track_id IS NOT NULL")\
    .select("track_id", "track_name")\
    .dropDuplicates(["track_id"])\
    .rdd.zipWithIndex().toDF()\
    .select(f.col("_1.*"), f.col("_2").alias('track_gen_id'))

user_and_track_plays_with_gen_ids_df = user_and_track_plays_df\
    .join(tracks_with_gen_id_df, user_and_track_plays_df["track_id"] == tracks_with_gen_id_df["track_id"])\
    .join(profiles_with_gen_id_df, user_and_track_plays_df["user_id"] == profiles_with_gen_id_df["id"])\
    .select("user_gen_id", "track_gen_id", "plays")

(training, test) = user_and_track_plays_with_gen_ids_df.randomSplit([0.8, 0.2])

als = ALS(implicitPrefs=True, userCol="user_gen_id",
          itemCol="track_gen_id", ratingCol="plays", coldStartStrategy="drop")

# Cluster
# param_grid = ParamGridBuilder() \
#     .addGrid(als.maxIter, [5, 10, 50]) \
#     .addGrid(als.regParam, [0.01, 0.03, 0.1, 0.3, 1]) \
#     .build()

# Local
param_grid = ParamGridBuilder() \
    .addGrid(als.maxIter, [5]) \
    .addGrid(als.regParam, [0.01]) \
    .build()

crossval = CrossValidator(estimator=als,
                          estimatorParamMaps=param_grid,
                          evaluator=RegressionEvaluator(
                              metricName="rmse", labelCol="plays", predictionCol="prediction"),
                          numFolds=3)

cv_model = crossval.fit(training)

user_recommendations_with_gen_ids_df = cv_model.bestModel.recommendForAllUsers(
    10)

user_recommendations_df = user_recommendations_with_gen_ids_df\
    .select("user_gen_id", f.explode("recommendations").alias("recommendations"))\
    .select("user_gen_id", f.col("recommendations.*"))\
    .withColumnRenamed("user_gen_id", "rec_user_gen_id")\
    .withColumnRenamed("track_gen_id", "rec_track_gen_id")

user_recommendations_with_info_df = user_recommendations_df\
    .join(tracks_with_gen_id_df, user_recommendations_df["rec_track_gen_id"] == tracks_with_gen_id_df["track_gen_id"])\
    .join(profiles_with_gen_id_df, user_recommendations_df["rec_user_gen_id"] == profiles_with_gen_id_df["user_gen_id"])\
    .select("id", "user_gen_id", "track_id", "track_gen_id", "track_name", "rating")

# Write data (Cluster)
# user_recommendations_with_info_df.write.csv(
#     'hdfs://compute1:9000/user/danilo-p/tp_final_collab_filtering_user_recommendations_with_info_df.csv')

# Write data (Local)
user_recommendations_with_info_df.write.csv(
    './output/local_tp_final_collab_filtering_user_recommendations_with_info_df.csv')
