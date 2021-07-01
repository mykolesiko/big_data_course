ADD JAR /usr/local/hive/lib/hive-serde.jar;

DROP TABLE IF EXISTS users; 
CREATE EXTERNAL TABLE   users (
			ip STRING,
			browser STRING,
			sex STRING,
			age INT)
	COMMENT 'User data'
	ROW FORMAT
	serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
	with serdeproperties (
		"input.regex" = "^(\\S*)\\t(.\\S*)\\t(.\\S*)\\t(.\\S*)"
	)
	STORED AS TEXTFILE
	LOCATION '/data/user_logs/user_data_M';

DROP TABLE IF EXISTS ip_regions; 

CREATE EXTERNAL TABLE   ip_regions (
			ip STRING,
			region STRING)
	COMMENT 'ip regions'
	ROW FORMAT
	serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
	with serdeproperties (
		"input.regex" = "^(\\S*)\\t(.*)"
	)
	STORED AS TEXTFILE
	LOCATION '/data/user_logs/ip_data_M';


DROP TABLE IF EXISTS logs_raw; 

CREATE EXTERNAL TABLE logs_raw (
		ip string,
		`date` string,
		request string,
		page_size int,
		http_status int,
		user_agent string)
	COMMENT 'Logs table'
	ROW FORMAT
	serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
	with serdeproperties (
		"input.regex" = "^(\\S*)\\t\\t\\t(\\d{8})\\S*\\t(.*)\\t(.*)\\t(.*)\\t(.\\S*)\\s.*"
	)
	STORED AS TEXTFILE
	LOCATION '/data/user_logs/user_logs_M';



set hive.exec.max.dynamic.partitions.pernode=116;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS logs;
CREATE EXTERNAL TABLE logs 
	(ip STRING,
	request STRING,
	page_size INT,
	http_status INT,
	user_agent STRING)
	partitioned by (`date` STRING) 
	STORED AS textfile;   


INSERT OVERWRITE TABLE logs PARTITION  ( `date`) 
                SELECT ip, request, page_size, http_status, user_agent , `date`
                FROM logs_raw;

