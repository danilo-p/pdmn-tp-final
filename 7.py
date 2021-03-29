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

from pyspark.sql import functions as f
from pyspark.sql import Window

dates_df = tracks_df.filter("timestamp IS NOT NULL and track_id IS NOT NULL")\
  .withColumn('date', f.substring('timestamp', 0, 10))\
  .select("date", "track_id")\
  .sort('date', ascending=False)

tracks_by_week = dates_df.withColumn("date", f.from_utc_timestamp(f.col("date"), "GMT"))\
    .groupBy(f.window("date", '1 week')) \
    .agg(f.collect_list('track_id'))\
    .withColumnRenamed('collect_list(track_id)', 'tracks')

tracks_by_week_counts = tracks_by_week\
  .select("window", f.explode("tracks").alias("track"))\
  .groupBy("window", "track")\
  .count()

max_tracks_by_week = tracks_by_week_counts\
  .withColumn(
    'max',
    f.max('count').over(Window.partitionBy('window'))
  )\
  .where(f.col('count') == f.col('max'))\
  .drop('max')\
  .sort('window', ascending=True)

tracks_id_name = tracks_df\
  .filter("track_id IS NOT NULL")\
  .select("track_id", "track_name", "artist_name")\
  .dropDuplicates(["track_id"])

max_tracks_by_week_with_name = max_tracks_by_week\
  .join(tracks_id_name, max_tracks_by_week["track"] == tracks_id_name["track_id"])\
  .drop("track_id")\
  .sort('window', ascending=True)

max_tracks_by_week_with_name_pandas = max_tracks_by_week_with_name.toPandas()

def find_hits(df):
    it = df.iteritems()
    i, count = next(it) # Get first
    for j, next_count in it:
        if 1.5 * count <= next_count:
            yield j
        count = next_count

hits = max_tracks_by_week_with_name_pandas.loc[
  list(
    find_hits(max_tracks_by_week_with_name_pandas['count'])
  )
]

hits.to_csv('./tp_final_7_hits.csv', encoding='utf-8', index=False)
