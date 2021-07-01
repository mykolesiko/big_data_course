HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar

hdfs dfs -rm -r hw2_mr_data_ids

set -x
yarn jar $HADOOP_STREAMING_JAR -input  $1 -output $2 -file mapper.py -file reducer.py -mapper "python mapper.py" -reducer "python reducer.py" \
	-numReduceTasks 3

hdfs dfs -cat $2/part-00000   | head -n 50 >> hw2_mr_data_ids.out
hdfs dfs -cat $2/part-00000   | head -n 50 


