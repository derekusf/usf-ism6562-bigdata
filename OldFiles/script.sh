docker exec -it Cloudera /bin/bash
cd /home/cloudera/DEV01
hdfs dfs -mkdir /user/cloudera/assignment
hdfs dfs -copyFromLocal originalDataset.csv /user/cloudera/assignment/
hive
create schema assignment;
show databases;
use assignment;
create table match (scorecard String, team1 String, team2 String, winner String, margin String, ground String, matchdate String, matchyear String) row format delimited fields terminated by ',' ;
alter table match set tblproperties("skip.header.line.count"="1");
load data inpath '/user/cloudera/assignment/originalDataset.csv' overwrite into table match;

select count(1) from match;
select distinct team from (select team1 as team from match a union all select team2 as team from match b) c;
select count(distinct team) from (select team1 as team from match a union all select team2 as team from match b) c;

create table matchanalysis (matchid String, winner String, matchyear int, decade String);
insert into matchanalysis 
select scorecard,winner, cast(substr(matchyear,2,4) as int),
case
when cast(substr(matchyear,2,4) as int) between 1971 and 1980 then "71-80"
when cast(substr(matchyear,2,4) as int) between 1981 and 1990 then "81-90"
when cast(substr(matchyear,2,4) as int) between 1991 and 2000 then "91-2000"
when cast(substr(matchyear,2,4) as int) between 2001 and 2010 then "2001-2010"
when cast(substr(matchyear,2,4) as int) between 2011 and 2018 then "2011-2018"
else "2018 onward"
end as decade
from match;

with agg as (select winner, count(1) as winmatch from matchanalysis group by winner)
select winner, winmatch from agg a
join (select max(winmatch) as maxwin from agg) b
on a.winmatch = b.maxwin;

with agg as (select winner, decade, count(1) as winmatch from matchanalysis group by winner, decade)
select a.decade, a.winner, a.winmatch from agg a
join (select decade, max(winmatch) as maxwin from agg group by decade) b
on a.winmatch = b.maxwin and a.decade = b.decade;

