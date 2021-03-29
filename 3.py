from operator import add
import math
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession\
    .builder\
    .appName("pdmn-tp-final - 2")\
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

# ----

top_100_artists_df = tracks_df.filter("artist_id IS NOT NULL")\
  .select("artist_id")\
  .groupBy("artist_id")\
  .count()\
  .sort('count', ascending=False)\
  .limit(100)

artists_id_name = tracks_df\
  .filter("artist_id IS NOT NULL")\
  .select("artist_id", "artist_name")\
  .dropDuplicates(["artist_id"])

top_100_artists_with_name_df = top_100_artists_df\
  .join(artists_id_name, top_100_artists_df["artist_id"] == artists_id_name["artist_id"])\
  .drop("artist_id")\
  .sort('count', ascending=False)

# Write data (Cluster)
# top_100_artists_with_name_df.write.csv(
#     'hdfs://compute1:9000/user/danilo-p/tp_final_3_top_100_artists_with_name_df.csv')

# Write data (Local)
top_100_artists_with_name_df.write.csv(
    './output/local_tp_final_3_top_100_artists_with_name_df.csv')
