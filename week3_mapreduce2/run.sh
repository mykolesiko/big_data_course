set -x
HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
hdfs dfs -rm -r -skipTrash $2
hdfs dfs -rm -r -skipTrash $2_tmp
(yarn jar $HADOOP_STREAMING_JAR -D stream.num.map.output.key.fields=2 \
	-D stream.num.reduce.output.key.fields=2 \
	-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
	-D mapreduce.partition.keycomparator.options="-k1,1nr -k2,2" \
	-input $1 \
	-output $2_tmp \
	-file mapper.py \
	-file reducer.py \
	-file combiner.py \
	-mapper "python mapper.py" \
	-combiner "python combiner.py" \
	-reducer "python reducer.py" \
	-numReduceTasks 3 &&
yarn jar $HADOOP_STREAMING_JAR -D stream.num.map.output.key.fields=2 \
	-D stream.num.reduce.output.key.fields=2 \
	-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
	-D mapreduce.partition.keycomparator.options="-k1,1n -k2,2nr" \
	-input $2_tmp \
	-output $2 \
	-file mapper1.py \
	-file reducer1.py \
	-mapper "python mapper1.py" \
	-reducer "python reducer1.py" \
	-numReduceTasks 1) 
hdfs dfs -rm -r -skipTrash $2_tmp
#hdfs dfs -cat $2/part-00000 | head -n 20 >> hw3_mr_advanced_output.out
hdfs dfs -cat $2/part-00000   | head -n 20 

