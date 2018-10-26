batters = LOAD 'hdfs:/user/maria_dev/pigtest/batting/Batting.csv' using PigStorage(',');
realbatters = FILTER batters BY $1>0;
run_data = FOREACH realbatters GENERATE $0 AS id, $1 AS year, $9 AS triples;
batters_2005 = FILTER run_data BY year==2005;
sorted = ORDER batters_2005 BY triples DESC;
top = LIMIT sorted 1;
final = FOREACH top GENERATE $0 AS id;
DUMP final;

