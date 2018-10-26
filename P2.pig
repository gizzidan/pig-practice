batters = LOAD 'hdfs:/user/maria_dev/pigtest/batting/Batting.csv' using PigStorage(',');
realbatters = FILTER batters BY $1>0;
run_data = FOREACH realbatters GENERATE $0 AS id, $1 AS year, $2 AS team;
player_year = GROUP run_data BY (id,year);
most_teams = FOREACH player_year GENERATE FLATTEN(group), COUNT(run_data.team) AS most;
sorted = ORDER most_teams by most DESC;
top = LIMIT sorted 1;
final = FOREACH top GENERATE $0 AS id;
DUMP final;


