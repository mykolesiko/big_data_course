import sys
import pyspark
from pyspark.sql.functions import *
import org.apache.spark.sql.cassandra
from pyspark.sql import SparkSession

DATA_FILE = "/data/movielens/movies.csv"
RATING_FILE = "/data/movielens/ratings.csv"
TABLE_NAME = "movies_by_genre_rating"

sc = pyspark.SparkContext("local", "task_movies_by_genre_rating")
spark = SparkSession.builder.appName("task_movies_by_genre_rating").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", 3)
 
keyspace_arg = sys.argv[1]

data = spark.read.csv(path=DATA_FILE, header=True)
rating = spark.read.csv(path=RATING_FILE, header=True)
rating_avg = rating.groupBy("movieId")\
             .agg(avg("rating"))\
             .withColumnRenamed("avg(rating)", "rating")

data_processed = data.filter(col("genres") != "(no genres listed)") \
    .withColumn("title", trim(data.title)) \
    .withColumn("movieid", col("movieId").cast("int")) \
    .withColumn("year", regexp_extract('title', r".*\((\d+)\)", 1)) \
    .withColumn("year", col("year").cast("int")) \
    .withColumn("genres",  split('genres', r"\|")) \
    .dropna()

movies_with_rating = data_processed.join(rating_avg, ["movieid"])
result = movies_with_rating.select(explode(movies_with_rating.genres).alias("genre"),
                                      movies_with_rating.year,
                                      movies_with_rating.rating,
                                      movies_with_rating.title,
                                      movies_with_rating.movieid) 


result.write.format("org.apache.spark.sql.cassandra") \
    .options(table=TABLE_NAME, keyspace = keyspace_arg) \
    .save(mode ="append")
