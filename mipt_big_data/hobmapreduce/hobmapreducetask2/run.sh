#!/usr/bin/env bash

OUT_DIR="minecraft_result"
NUM_REDUCERS=8

hadoop fs -rm -r -skipTrash ${OUT_DIR}.tmp > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="minecraft_value" \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input  /data/minecraft-server-logs \
    -output ${OUT_DIR}.tmp > /dev/null

hadoop fs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D stream.num.map.output.key.fields=3 \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options='-k 1,1 -k 3nr' \
    -mapper cat \
    -reducer cat \
    -input ${OUT_DIR}.tmp \
    -output ${OUT_DIR} > /dev/null



# Checking result
hdfs dfs -cat ${OUT_DIR}/part-00000 | head -10
