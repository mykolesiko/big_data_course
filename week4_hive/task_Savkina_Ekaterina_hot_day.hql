select `date`, count(1) as trafic from logs group by `date` order by trafic desc LIMIT 10;
