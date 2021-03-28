from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession\
    .builder\
    .appName("pdmn-tp-final")\
    .getOrCreate()


profiles_df = spark.read.option("header", True).option("delimiter", "\t").csv(
    "hdfs://compute1:9000/datasets/last_fm/userid-profile.tsv")
tracks_df = spark.read.option("delimiter", "\t").csv(
    "hdfs://compute1:9000/datasets/last_fm/userid-timestamp-artid-artname-traid-traname.tsv")

print("********************** profiles_df schema *********************")
print(profiles_df.printSchema())
profiles_df.limit(10).write.csv(
    'hdfs://compute1:9000/user/danilo-p/tp_final_explore_profiles_df.csv', header=True)

print("********************** tracks_df schema *********************")
print(tracks_df.printSchema())
tracks_df.limit(10).write.csv(
    'hdfs://compute1:9000/user/danilo-p/tp_final_explore_tracks_df.csv', header=True)
