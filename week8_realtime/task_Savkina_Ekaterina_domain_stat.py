"""
    domain
"""
import argparse 
import pyspark
import  pyspark.sql.functions as funcs
from pyspark.sql import SparkSession


sc = pyspark.SparkContext("local", "task_Savkina_Ekaterina")

spark = spark = SparkSession.builder.appName("task_Savkina_Ekaterina").getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", 3)


parser = argparse.ArgumentParser()
parser.add_argument("--kafka-brokers", required=True)
parser.add_argument("--topic-name", required=True)
parser.add_argument("--starting-offsets", default="latest")
group = parser.add_mutually_exclusive_group()
group.add_argument("--processing-time", default="0 seconds")
group.add_argument("--once", action="store_true")
args = parser.parse_args()
if args.once:
    args.processing_time = None
else:
    args.once = None

#print(args.kafka_brokers)

input_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", args.kafka_brokers)
    .option("subscribe",args.topic_name)
    .option("numPartitions", 3)
    .option("startingOffsets", args.starting_offsets)
    .load()
)

result = input_df.selectExpr("cast(value as string)")
value_col = pyspark.sql.functions.split(result["value"], "\t")
result = result.withColumn("ts", value_col.getItem(0))
result = result.withColumn("uid", value_col.getItem(1))
result = result.withColumn("url", value_col.getItem(2))
result = result.withColumn("title", value_col.getItem(3))
result = result.withColumn("ua", value_col.getItem(4))
result = result.drop("value")
result = result.selectExpr("*", "parse_url(url, 'HOST') as domain")


views = funcs.count("*").alias("view")
uids = funcs.approx_count_distinct("uid").alias("unique")

agg = result.groupBy("domain").agg(views, uids).orderBy(funcs.desc("view")).limit(10)

query = (
    agg.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(once=args.once, processingTime=args.processing_time)
    .start()
)

query.awaitTermination()
#query.stop()
#sc.stop()
