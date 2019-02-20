#!/usr/bin/env bash
source /home/wirelessdev/.bash_profile;

# Created by abecedarian on 2019/2/20
# Hadoop-streaming 实现查找hdfs上包含指定字符串的数据 demo

input=$1
output=$2
search_str=$3

sudo -uwirelessdev hadoop jar /home/q/hadoop/hadoop-2.2.0/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
-D stream.non.zero.exit.is.failure=false \
-D mapred.job.name="hdfs-contain_abecedarian" \
-D mapred.job.queue.name=wirelessdev \
-D mapred.job.priority=VERY_HIGH \
-D mapreduce.map.memory.mb=8192 \
-D stream.memory.limit=8192 \
-D mapred.child.map.java.opts=-Xmx2048m \
-D mapreduce.map.java.opts=-Xmx2048m \
-D mapreduce.reduce.memory.mb=8192 \
-D mapred.child.reduce.java.opts=-Xmx2048m \
-D mapreduce.reduce.java.opts=-Xmx2048m \
-D mapred.compress.map.output=true \
-D mapred.map.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
-D mapred.output.compress=true \
-D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
-D stream.map.output.field.seperator="\t" \
-D mapreduce.job.reduces=1 \
-input ${input} \
-output ${output} \
-mapper  "egrep '${search_str}'" \
-reducer "cat"