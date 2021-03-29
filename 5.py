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

# Read data (Local)
profiles_df = spark.read.option("header", True).option(
    "delimiter", "\t").csv("./last_fm/userid-profile.tsv")

user_play_counts_df = profiles_df.groupBy("country").count()

user_play_counts_rdd = user_play_counts_df.rdd\
    .map(lambda a: '"{}", "{}"'.format(a[0], a[1]))

# Write data (Cluster)
# user_play_counts_rdd.saveAsTextFile(
#     'hdfs://compute1:9000/user/danilo-p/tp_final_5_user_play_counts_rdd.csv')

# Write data (Local)
user_play_counts_rdd.saveAsTextFile(
    './output/local_tp_final_5_user_play_counts_rdd.csv')
