from operator import add
import math
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession\
    .builder\
    .appName("pdmn-tp-final - 5")\
    .getOrCreate()


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

user_play_counts_df = tracks_df.filter("user_id IS NOT NULL")\
    .groupBy("user_id")\
    .count()

plays_by_country_df = user_play_counts_df\
    .join(profiles_df, user_play_counts_df["user_id"] == profiles_df["id"])\
    .select("country", "count")

plays_by_country_rdd = plays_by_country_df.rdd\
    .reduceByKey(add)\
    .map(lambda a: '"{}", "{}"'.format(a[0], a[1]))

# Write data (Cluster)
# plays_by_country_rdd.saveAsTextFile(
#     'hdfs://compute1:9000/user/danilo-p/tp_final_5_plays_by_country_rdd.csv')

# Write data (Local)
plays_by_country_rdd.saveAsTextFile(
    './output/local_tp_final_5_plays_by_country_rdd.csv')
