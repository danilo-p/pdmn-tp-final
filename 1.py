from operator import add
import math
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession\
    .builder\
    .appName("pdmn-tp-final - 1")\
    .getOrCreate()


# Read data (Cluster)
# profiles_df = spark.read.option("header", True).option("delimiter", "\t").csv(
#     "hdfs://compute1:9000/datasets/last_fm/userid-profile.tsv")
# tracks_df = spark.read.option("delimiter", "\t").csv(
#     "hdfs://compute1:9000/datasets/last_fm/userid-timestamp-artid-artname-traid-traname.tsv")

# Read data (Local)
profiles_df = spark.read.option("header", True).option(
    "delimiter", "\t").csv("./last_fm/userid-profile.tsv")
tracks_df = spark.read.option("header", True).csv(
    "./last_fm/userid-timestamp-artid-artname-traid-traname-sample.csv")

profiles_age_to_count = profiles_df.groupBy("age")\
    .count()\
    .sort('age')


error_label = "Error"
none_label = "Not set"
intervals = [
    (-math.inf, 20, "0 - 20"),
    (21, 30, "21 - 30"),
    (31, 40, "31 - 40"),
    (41, 50, "41 - 50"),
    (51, 65, "51 - 65"),
    (65, math.inf, "65+")
]


def produce_histogram_keys(row):
    if (row["age"] == None):
        return [(none_label, row["count"])]

    age = int(row["age"])

    label = None
    for min_age, max_age, interval_label in intervals:
        if min_age <= age and age <= max_age:
            label = interval_label
            break

    if label == None:
        print("Failed to find interval for: ", row)
        label = error_label

    return [(label, row["count"])]


histogram_rdd = profiles_age_to_count.rdd\
    .flatMap(produce_histogram_keys)\
    .reduceByKey(add)\
    .sortByKey()\
    .map(lambda a: "{},{}".format(a[0], a[1]))

# Write data (Cluster)
# histogram_rdd.saveAsTextFile(
#     "hdfs://compute1:9000/user/danilo-p/tp_final_1_histogram.csv")

# Write data (Local)
histogram_rdd.saveAsTextFile("./output/local_tp_final_1_histogram.csv")
