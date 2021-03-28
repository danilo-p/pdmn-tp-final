from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("pdmn-tp-final")\
    .getOrCreate()

tracks_df = spark.read.option("delimiter", "\t").csv(
    "hdfs://compute1:9000/datasets/last_fm/userid-timestamp-artid-artname-traid-traname.tsv")

tracks_df.createOrReplaceTempView("tracks")

spark.sql("SELECT * FROM tracks TABLESAMPLE (20 PERCENT)").write.csv(
    'hdfs://compute1:9000/user/danilo-p/tp_final_tracks_sample.csv', header=True)
