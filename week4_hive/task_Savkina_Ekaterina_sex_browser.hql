set hive.auto.convert.join=false;
set mapreduce.job.reduces=3;

select  users.browser, sum(if(users.sex == 'male', 1, 0)) as male, sum(if(users.sex == 'female', 1, 0)) as female
 	from users join logs on (logs.ip = users.ip)
 	group by users.browser;
