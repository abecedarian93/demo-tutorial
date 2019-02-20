#!/usr/bin/env bash
source /home/wirelessdev/.bash_profile;

basedir=$(cd "$(dirname "$0")"; pwd)
basedir=$(cd "$(dirname "$basedir")"; pwd)
export TRANSDATA_HOME=$(cd "$(dirname "$basedir")"; pwd)

# Created by abecedarian on 2019/2/20
# Hadoop-streaming 实现对hdfs数据按指定数量随机抽取

inputPath=$1
outputPath=$2
reduNum=$3
filterNumPerMap=$4

sudo -uwirelessdev hadoop jar /home/q/hadoop/hadoop-2.2.0/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
-D mapred.job.name="hdfs-random_abecedarian" \
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
-D mapreduce.job.reduces=$reduNum \
-input $inputPath \
-output $outputPath \
-mapper  "cat" \
-reducer  "python hdfs-random-mapper.py ${filterNumPerMap}" \
-file $TRANSDATA_HOME/src/shell/hadoop-streaming/hdfs-random-mapper.py

