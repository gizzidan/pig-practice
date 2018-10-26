batters = LOAD 'hdfs:/user/maria_dev/pigtest/batting/Batting.csv' using PigStorage(',');
the_80s = FILTER batters BY $1<1990 AND $1>1979;
run_data = FOREACH the_80s GENERATE $0 AS id, $1 AS year, $8 + $9 + $10 AS ebh;
grouped_by_id = GROUP run_data BY id;
most_hits = FOREACH grouped_by_id GENERATE group, SUM(run_data.ebh) AS total_ebh;
sorted = ORDER most_hits by total_ebh DESC;
top = LIMIT sorted 1;
final = FOREACH top GENERATE $0 AS id;
DUMP final;


