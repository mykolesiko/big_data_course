#!/usr/bin/python
from pyspark import SparkContext
from pyspark.sql import SparkSession
from  pyspark.sql.functions import col

sc = SparkContext("local", "task_Savkina_Ekaterina_sssp")
spark = spark = SparkSession \
    .builder \
    .appName("task_Savkina_Ekaterina_sssp") \
    .getOrCreate()
PATH = "/data/twitter/twitter.txt"
NODE_FIRST = "34"
NODE_LAST = "12"

def test(df, val: str)->int:
    return df.select(col("end")).where(col("end") == val).count()

def iteration(begin, end):
    return begin.alias("from")\
    .join(end.alias("to"), col("from.end") == col("to.begin"))\
    .select(col("to.begin"), col("to.end"))

df = spark.read.csv(PATH, sep="\t").toDF("begin", "end")
data = [{"begin":NODE_FIRST, "end":NODE_FIRST}]
rdd = sc.parallelize(data)
start = spark.read.json(rdd)
next1 = start
len1 = 0
while test(next1, NODE_LAST) == 0:
    len1 = len1 + 1
    next1 = iteration(next1, df)
print(len1)
