#!/bin/bash
hdfs dfs -rm -r /user/cloudera/poll_analysis/output

/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-files mapper_poll_analysis.py,reducer_poll_analysis.py \
-input /user/cloudera/poll_analysis/input \
-output /user/cloudera/poll_analysis/output \
-mapper mapper_poll_analysis.py \
-reducer reducer_poll_analysis.py 

