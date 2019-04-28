#!/usr/bin/env bash

${SPARK_HOME}/bin/spark-submit \
    --class com.abecedarian.demo.spark.HelloWorldOnCluster \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 1g \
    --executor-memory 2g \
    --num-executors 2 \
    --executor-cores 2 \
    --queue dev \
    http://litianle.com/jars/demo-tutorial-1.0.0-jar-with-dependencies.jar