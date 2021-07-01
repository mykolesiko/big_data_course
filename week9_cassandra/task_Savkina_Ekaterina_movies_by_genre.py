import sys
from pyspark.sql.functions import *
import org.apache.spark.sql.cassandra
import pyspark
from pyspark.sql import SparkSession

DATA_FILE = "/data/movielens/movies.csv"
TABLE_NAME = "movies_by_genre"

sc = pyspark.SparkContext("local", "task_movies_by_genre")
spark = SparkSession.builder.appName("task_movies_by_genre").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", 3)
keyspace_arg = sys.argv[1]
data = spark.read.csv(path=DATA_FILE, header=True)

data_processed = data.filter(col("genres") != "(no genres listed)") \
.withColumn("title", trim(data.title)) \
.withColumn("movieid", col("movieId").cast("int")) \
.withColumn("year", regexp_extract('title', r".+\((\d+)\)", 1)) \
.withColumn("year", col("year").cast("int")) \
.withColumn("genres", split('genres', r"\|")) \
.dropna()

data_processed_1 = data_processed.select(explode(data_processed.genres).alias("genre"), data_processed.year, data_processed.title, data_processed.movieid)
data_processed_1.write.format("org.apache.spark.sql.cassandra") \
.options(table=TABLE_NAME, keyspace = keyspace_arg) \
.save(mode ="append")
