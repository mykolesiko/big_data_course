import sys
import pyspark
from pyspark.sql.functions import *
import org.apache.spark.sql.cassandra
from pyspark.sql import SparkSession

sc = pyspark.SparkContext("local", "task_movies")
spark = SparkSession.builder.appName("task_movies").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", 3)

DATA_FILE = "/data/movielens/movies.csv"
keyspace_arg = sys.argv[1]


data = spark.read.csv(path=DATA_FILE, header=True)
data_processed = data.filter(col("genres") != "(no genres listed)") \
    .withColumn("title", trim(data.title)) \
    .withColumn("movieid", col("movieId").cast("int")) \
    .withColumn("year", regexp_extract("title", r".*\((\d+)\)", 1)) \
    .withColumn("year", col("year").cast("int")) \
    .withColumn("genres", split("genres", r"\|")) \
    .dropna()

data_processed.write.format("org.apache.spark.sql.cassandra") \
    .options(table="movies", keyspace = keyspace_arg) \
    .save(mode ="append")
