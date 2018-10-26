batters = LOAD 'hdfs:/user/maria_dev/pigtest/batting/Batting.csv' using PigStorage(',');
realbatters = FILTER batters BY $1>0 AND $7>0;
run_data = FOREACH realbatters GENERATE $0 AS id, $7 AS hits;
grouped_by_id = GROUP run_data BY id;
most_hits = FOREACH grouped_by_id GENERATE group, SUM(run_data.hits) AS total_hits;
info = LOAD 'hdfs:/user/maria_dev/pigtest/master/Master.csv' using PigStorage(',');
master_data = FOREACH info GENERATE $0 AS id, $2 AS month, $7 AS death, $18 as hand;
filtered = FILTER master_data BY month == 10 AND death == 2011 AND hand == 'R';
final_join = JOIN most_hits BY $0, filtered BY id;
sorted = ORDER final_join BY total_hits DESC;
top = LIMIT sorted 1;
final = FOREACH top GENERATE $0 AS id;
DUMP final;








